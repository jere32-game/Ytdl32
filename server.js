'use strict';
/**
 * Servidor de videos temporales de YouTube
 *
 *  GET /api/create?url=<url de YouTube>  -> empieza (o reutiliza) la preparación del video
 *  GET /api/status/<id>                  -> estado; cuando está listo trae la URL directa
 *  GET /api/health                       -> diagnóstico (versión de Node, yt-dlp, último error)
 *  GET /v/<id>                           -> el video en sí (inline + Range, NO fuerza descarga)
 *  GET /watch/<id>                       -> mini reproductor sin botón de descarga
 *
 * Cada video se borra solo 5 minutos después de quedar listo.
 */

const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

// ───────────── Configuración (se puede cambiar con variables de entorno) ─────────────
const PORT = process.env.SERVER_PORT || process.env.PORT || 3000;
const PUBLIC_URL = (process.env.PUBLIC_URL || '').replace(/\/+$/, ''); // ej: https://mi-app.wispbyte.app
const TTL_MS = 5 * 60 * 1000; // vida del video: 5 minutos
const REUSE_MIN_MS = 60 * 1000; // si piden el mismo video, se reutiliza solo si le queda > 1 min
const MAX_DURATION_S = Number(process.env.MAX_DURATION_S || 1800); // 30 min
const MAX_HEIGHT = Number(process.env.MAX_HEIGHT || 720);
const MAX_FILESIZE = process.env.MAX_FILESIZE || '300M';
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT || 1); // descargas a la vez
const MAX_QUEUE = Number(process.env.MAX_QUEUE || 10);
const RATE_PER_HOUR = Number(process.env.RATE_PER_HOUR || 15); // solicitudes por IP por hora
const NODE_MAJOR = Number(process.versions.node.split('.')[0]); // yt-dlp necesita Node >= 20 para resolver los retos de YouTube

const VIDEO_DIR = path.join(__dirname, 'videos');
const BIN_DIR = path.join(__dirname, 'bin');
const META_FILE = path.join(__dirname, 'meta.json');
const COOKIES_FILE = path.join(__dirname, 'cookies.txt'); // opcional (ver notas)
const YTDLP = path.join(BIN_DIR, process.platform === 'win32' ? 'yt-dlp.exe' : 'yt-dlp');

// ffmpeg es opcional: sin él se usan formatos "todo en uno" (normalmente 360p).
// Con `npm i ffmpeg-static` (o FFMPEG_PATH) se puede llegar hasta MAX_HEIGHT.
let FFMPEG = process.env.FFMPEG_PATH || null;
if (!FFMPEG) {
  try { FFMPEG = require('ffmpeg-static'); } catch (e) { /* no instalado */ }
}

// ───────────── Estado ─────────────
const videos = new Map(); // token -> { token, ytId, status, title, duration, file, mime, createdAt, expiresAt, error }
const byYtId = new Map(); // id de YouTube -> token vigente
const hits = new Map(); // ip -> [timestamps]
const queue = [];
let running = 0;
let lastError = null; // último error crudo de yt-dlp (se ve en /api/health)

const fileOf = (v) => path.join(VIDEO_DIR, v.file);

function alive(token) {
  const v = videos.get(token);
  return v && v.status === 'ready' && Date.now() < v.expiresAt ? v : null;
}

// ───────────── Persistencia (para sobrevivir a reinicios) ─────────────
function saveMeta() {
  try {
    const ready = [...videos.values()].filter((v) => v.status === 'ready');
    fs.writeFileSync(META_FILE, JSON.stringify(ready));
  } catch (e) { /* no pasa nada */ }
}

function loadMeta() {
  fs.mkdirSync(VIDEO_DIR, { recursive: true });
  let list = [];
  try { list = JSON.parse(fs.readFileSync(META_FILE, 'utf8')); } catch (e) { /* primera vez */ }
  const keep = new Set();
  for (const v of list) {
    if (v.expiresAt > Date.now() && v.file && fs.existsSync(fileOf(v))) {
      videos.set(v.token, v);
      byYtId.set(v.ytId, v.token);
      keep.add(v.file);
    }
  }
  // Borra cualquier archivo huérfano o a medio descargar
  for (const f of fs.readdirSync(VIDEO_DIR)) {
    if (!keep.has(f)) fs.rmSync(path.join(VIDEO_DIR, f), { force: true, recursive: true });
  }
  saveMeta();
}

function deleteFiles(token) {
  for (const f of fs.readdirSync(VIDEO_DIR)) {
    if (f.startsWith(token + '.')) fs.rm(path.join(VIDEO_DIR, f), { force: true }, () => {});
  }
}

function removeVideo(v) {
  videos.delete(v.token);
  if (byYtId.get(v.ytId) === v.token) byYtId.delete(v.ytId);
  deleteFiles(v.token);
}

// ───────────── Limpieza automática (cada minuto) ─────────────
function sweep() {
  const now = Date.now();
  let changed = false;
  for (const v of [...videos.values()]) {
    const expired = v.status === 'ready' && now > v.expiresAt;
    const oldError = v.status === 'error' && now - v.createdAt > 10 * 60 * 1000;
    if (expired || oldError) { removeVideo(v); changed = true; }
  }
  for (const [ip, arr] of hits) {
    const recent = arr.filter((t) => now - t < 3600_000);
    if (recent.length) hits.set(ip, recent); else hits.delete(ip);
  }
  if (changed) saveMeta();
}

// ───────────── Utilidades ─────────────
function extractVideoId(input) {
  const text = String(input || '').trim();
  if (/^[\w-]{11}$/.test(text)) return text;
  let u;
  try { u = new URL(text); } catch (e) { return null; }
  if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
  const host = u.hostname.replace(/^(www|m|music)\./, '');
  let id = null;
  if (host === 'youtu.be') {
    id = u.pathname.slice(1).split('/')[0];
  } else if (host === 'youtube.com' || host === 'youtube-nocookie.com') {
    if (u.pathname === '/watch') id = u.searchParams.get('v');
    else {
      const m = u.pathname.match(/^\/(shorts|embed|live|v)\/([^/?]+)/);
      if (m) id = m[2];
    }
  }
  return id && /^[\w-]{11}$/.test(id) ? id : null;
}

function rateLimited(ip) {
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter((t) => now - t < 3600_000);
  if (arr.length >= RATE_PER_HOUR) { hits.set(ip, arr); return true; }
  arr.push(now);
  hits.set(ip, arr);
  return false;
}

function cleanError(raw) {
  return String(raw || '').replace(/^ERROR:\s*(\[[^\]]*\]\s*)?([\w-]{11}:\s*)?/, '').trim().slice(0, 300);
}

function friendlyError(raw) {
  const msg = String(raw || '');
  if (/confirm you.?re not a bot|sign in to confirm/i.test(msg)) {
    return 'YouTube bloqueó al servidor (anti-bot). Prueba con otro video o configura cookies.txt en el server';
  }
  // Va ANTES que el de "no disponible": dice "is not available" pero NO significa que el video no exista
  if (/requested format is not available|only images are available/i.test(msg)) {
    return 'yt-dlp no pudo sacar un formato descargable (mira /api/health: versión de Node y de yt-dlp)';
  }
  if (/private video|this video is private|video unavailable|this video is (not|no longer) available|has been removed|content isn.t available/i.test(msg)) {
    return 'El video es privado o no está disponible';
  }
  return cleanError(msg) || 'Error desconocido';
}

// ───────────── yt-dlp ─────────────
async function ensureYtDlp() {
  if (fs.existsSync(YTDLP)) return;
  const asset = process.platform === 'win32' ? 'yt-dlp.exe'
    : process.platform === 'darwin' ? 'yt-dlp_macos'
    : process.arch === 'arm64' ? 'yt-dlp_linux_aarch64' : 'yt-dlp_linux';
  console.log('Descargando yt-dlp…');
  const res = await fetch(`https://github.com/yt-dlp/yt-dlp/releases/latest/download/${asset}`);
  if (!res.ok) throw new Error(`No se pudo bajar yt-dlp (HTTP ${res.status})`);
  fs.mkdirSync(BIN_DIR, { recursive: true });
  const tmp = YTDLP + '.tmp';
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmp));
  fs.chmodSync(tmp, 0o755);
  fs.renameSync(tmp, YTDLP);
  console.log('yt-dlp listo');
}

let ytdlpReady = null;
const getYtDlp = () => (ytdlpReady ||= ensureYtDlp().catch((e) => { ytdlpReady = null; throw e; }));

function runYtDlp(args, timeoutMs) {
  return new Promise((resolve, reject) => {
    const p = spawn(YTDLP, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    const timer = setTimeout(() => p.kill('SIGKILL'), timeoutMs);
    p.stdout.on('data', (d) => { out += d; });
    p.stderr.on('data', (d) => { err = (err + d).slice(-4000); });
    p.on('error', (e) => { clearTimeout(timer); reject(e); });
    p.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) return resolve(out);
      if (code === null) return reject(new Error('Tiempo de descarga agotado'));
      const lines = err.trim().split('\n');
      reject(new Error(lines.filter((l) => l.includes('ERROR')).pop() || lines.pop() || `yt-dlp terminó con código ${code}`));
    });
  });
}

function baseArgs() {
  // Desde yt-dlp 2025.11.12 hace falta un runtime de JS para YouTube: usamos el mismo Node del server.
  const a = ['--no-playlist', '--no-warnings', '--js-runtimes', `node:${process.execPath}`];
  if (fs.existsSync(COOKIES_FILE)) a.push('--cookies', COOKIES_FILE);
  if (process.env.YTDLP_EXTRA) a.push(...process.env.YTDLP_EXTRA.split(/\s+/).filter(Boolean)); // ajustes sin tocar el código
  return a;
}

// ───────────── Cola de trabajos ─────────────
async function processJob(v) {
  v.status = 'downloading';
  const url = `https://www.youtube.com/watch?v=${v.ytId}`;
  try {
    await getYtDlp();

    // 1) Datos del video (sin descargar) para validar duración
    const info = await runYtDlp([...baseArgs(), '--ignore-no-formats-error', '--print', '%(duration)s\t%(is_live)s\t%(title)s', url], 90_000);
    const [dur, live, ...rest] = info.trim().split('\n').pop().split('\t');
    v.title = rest.join('\t');
    v.duration = Number(dur);
    if (live === 'True') throw new Error('No se admiten transmisiones en vivo');
    if (!Number.isFinite(v.duration)) throw new Error('No se pudo leer la duración del video');
    if (v.duration > MAX_DURATION_S) throw new Error(`El video dura más de ${Math.round(MAX_DURATION_S / 60)} minutos`);

    // 2) Descarga. Forzamos formato MP4/M4A nativos para evitar que FFmpeg recodifique el video
    //    y consuma CPU/GPU. También limitamos a 1 hilo por seguridad.
    const args = [
      ...baseArgs(), '-q', '--no-progress', '--no-part',
      '--max-filesize', MAX_FILESIZE,
      '-f', `bestvideo[ext=mp4][height<=${MAX_HEIGHT}]+bestaudio[ext=m4a]/best[ext=mp4][height<=${MAX_HEIGHT}]/best`,
      '--merge-output-format', 'mp4',
      '--postprocessor-args', 'ffmpeg:-threads 1',
      '-o', path.join(VIDEO_DIR, `${v.token}.%(ext)s`),
    ];
    if (FFMPEG) args.push('--ffmpeg-location', FFMPEG);
    await runYtDlp([...args, url], 10 * 60_000);

    const file = ['mp4', 'webm'].map((e) => `${v.token}.${e}`).find((f) => fs.existsSync(path.join(VIDEO_DIR, f)));
    if (!file) throw new Error(`No se pudo descargar (¿pesa más de ${MAX_FILESIZE}?)`);

    v.file = file;
    v.mime = file.endsWith('.webm') ? 'video/webm' : 'video/mp4';
    v.expiresAt = Date.now() + TTL_MS; // los 5 minutos empiezan cuando queda listo
    v.status = 'ready';
    saveMeta();
  } catch (e) {
    console.error(`[${v.ytId}]`, e.message);
    v.status = 'error';
    v.error = friendlyError(e.message);
    v.detail = cleanError(e.message);
    lastError = { at: new Date().toISOString(), id: v.ytId, message: v.detail };
    if (byYtId.get(v.ytId) === v.token) byYtId.delete(v.ytId);
    deleteFiles(v.token);
  }
}

function enqueue(v) { queue.push(v); pump(); }
function pump() {
  while (running < MAX_CONCURRENT && queue.length) {
    const v = queue.shift();
    running++;
    processJob(v).finally(() => { running--; pump(); });
  }
}

// ───────────── Servidor HTTP ─────────────
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);

app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*'); // la extensión de TurboWarp necesita CORS
  next();
});
app.use('/api', (req, res, next) => { res.set('Cache-Control', 'no-store'); next(); });

const baseUrl = (req) => PUBLIC_URL || `${req.protocol}://${req.get('host')}`;

function publicInfo(v, req) {
  const o = { id: v.token, status: v.status, title: v.title || null, duration: v.duration || null };
  if (v.status === 'ready') {
    o.url = `${baseUrl(req)}/v/${v.token}`;
    o.watch = `${baseUrl(req)}/watch/${v.token}`;
    o.expiresAt = v.expiresAt;
    o.secondsLeft = Math.max(0, Math.round((v.expiresAt - Date.now()) / 1000));
  }
  if (v.status === 'error') { o.error = v.error; o.detail = v.detail || null; }
  return o;
}

app.get('/', (req, res) => res.type('text').send('OK'));

let versionCache = { at: 0, value: null };

// Diagnóstico: ábrelo en el navegador -> https://tu-server/api/health
app.get('/api/health', async (req, res) => {
  if (Date.now() - versionCache.at > 10 * 60_000) {
    try {
      await getYtDlp();
      versionCache = { at: Date.now(), value: (await runYtDlp(['--version'], 30_000)).trim() };
    } catch (e) {
      versionCache = { at: Date.now(), value: 'error: ' + cleanError(e.message) };
    }
  }
  res.json({
    node: process.version,
    nodeOk: NODE_MAJOR >= 20,
    ytdlp: versionCache.value,
    ffmpeg: !!FFMPEG,
    cookies: fs.existsSync(COOKIES_FILE),
    running,
    queued: queue.length,
    videos: videos.size,
    lastError,
  });
});

app.get('/api/create', (req, res) => {
  const ytId = extractVideoId(req.query.url);
  if (!ytId) return res.status(400).json({ error: 'URL de YouTube no válida' });
  if (rateLimited(req.ip)) return res.status(429).json({ error: 'Demasiadas solicitudes, intenta más tarde' });

  const existing = videos.get(byYtId.get(ytId));
  if (existing && (existing.status !== 'ready' || existing.expiresAt - Date.now() > REUSE_MIN_MS)) {
    return res.json(publicInfo(existing, req));
  }
  if (queue.length >= MAX_QUEUE) return res.status(503).json({ error: 'Servidor ocupado, intenta en unos minutos' });

  const v = {
    token: crypto.randomBytes(16).toString('base64url'), // enlace único e imposible de adivinar
    ytId,
    status: 'queued',
    createdAt: Date.now(),
  };
  videos.set(v.token, v);
  byYtId.set(ytId, v.token);
  enqueue(v);
  res.status(202).json(publicInfo(v, req));
});

app.get('/api/status/:id', (req, res) => {
  const v = videos.get(req.params.id);
  if (!v) return res.status(404).json({ error: 'No existe o ya expiró' });
  res.json(publicInfo(v, req));
});

// El video directo: se muestra en el navegador (inline), nunca se fuerza la descarga.
app.get('/v/:token', (req, res) => {
  const token = String(req.params.token).replace(/\.[A-Za-z0-9]+$/, '');
  const v = alive(token);
  if (!v) return res.status(404).type('text').send('Video no encontrado o expirado');
  res.set({
    'Content-Type': v.mime,
    'Content-Disposition': 'inline',
    'Cache-Control': 'private, no-store',
    'X-Content-Type-Options': 'nosniff',
    'X-Robots-Tag': 'noindex',
    'Cross-Origin-Resource-Policy': 'cross-origin',
  });
  res.sendFile(fileOf(v), { acceptRanges: true, cacheControl: false, lastModified: false, etag: false }, (err) => {
    if (err && !res.headersSent) res.status(404).end();
  });
});

// Reproductor mínimo, sin botón de descarga
app.get('/watch/:token', (req, res) => {
  const v = alive(req.params.token);
  if (!v) return res.status(404).type('text').send('Video no encontrado o expirado');
  res.set({ 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' });
  res.type('html').send(
    '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Video</title><style>html,body{margin:0;height:100%;background:#000}video{width:100%;height:100%}</style>' +
    `<video src="/v/${v.token}" controls autoplay playsinline controlsList="nodownload noplaybackrate" ` +
    'disablePictureInPicture oncontextmenu="return false"></video>'
  );
});

// ───────────── Arranque ─────────────
loadMeta();
setInterval(sweep, 60_000);
// yt-dlp se rompe seguido por cambios de YouTube: se auto-actualiza cada 24 h
setInterval(() => { getYtDlp().then(() => runYtDlp(['-U'], 120_000)).catch(() => {}); }, 24 * 3600_000);

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor listo en el puerto ${PORT} (Node ${process.version})`);
  if (NODE_MAJOR < 20) console.warn('⚠️  yt-dlp necesita Node 20 o superior para YouTube. Cambia la versión de Node en el panel.');
  getYtDlp().catch((e) => console.error(e.message));
});
