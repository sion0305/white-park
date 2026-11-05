'use strict';
/*
  White Park realtime server
  - Serves the game page (public/index.html) over HTTP.
  - Relays where everyone is (presence) over WebSocket at /ws, 10 times a second, per channel.
  - Keeps the shared snow objects (snowballs, snowmen, snow angels) for each channel.
  - Answers clock pings so every browser runs lifts and queues on the same time.
  Channels: up to ROOM_CAP people each. New visitors join the fullest channel that still has room,
  so people actually meet; ?ch=7 on the page address joins channel 7 (to meet friends).
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const ROOM_CAP = Number(process.env.ROOM_CAP) || 40;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const PUBLIC = path.join(__dirname, 'public');
const TICK_MS = 100;
const LIFE = { balls: 60 * 60e3, snowmen: 60 * 60e3, angels: 5 * 60e3 };
const MAX_DOCS = { balls: 50, snowmen: 50, angels: 80 };
const MAX_PRESENCE_BYTES = 2048, MAX_DOC_BYTES = 1024;
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.webmanifest': 'application/manifest+json', '.txt': 'text/plain; charset=utf-8'
};

/* ---------- static files ---------- */
const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://local'); } catch { res.writeHead(400); return res.end(); }
  if (url.pathname === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  let p;
  try { p = decodeURIComponent(url.pathname); } catch { res.writeHead(400); return res.end(); }
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end(); }
  const type = TYPES[path.extname(file).toLowerCase()];
  if (!type) { res.writeHead(404); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('not found'); }
    res.writeHead(200, {
      'content-type': type,
      'cache-control': type.startsWith('text/html') ? 'no-cache' : 'public, max-age=86400',
      'x-content-type-options': 'nosniff'
    });
    res.end(buf);
  });
});

/* ---------- channels ---------- */
const channels = new Map();
function newChannel(id) {
  const ch = { id, members: new Map(), dirty: new Set(), removed: new Set(), world: { balls: new Map(), snowmen: new Map(), angels: new Map() }, emptySince: 0 };
  channels.set(id, ch);
  return ch;
}
function pickChannel(want) {
  if (want && /^[0-9]{1,4}$/.test(want)) {
    const ch = channels.get(want) || newChannel(want);
    if (ch.members.size < ROOM_CAP) return ch;
  }
  // the fullest channel that still has room, so visitors run into each other
  let best = null;
  for (const ch of channels.values()) if (ch.members.size < ROOM_CAP && (!best || ch.members.size > best.members.size)) best = ch;
  if (best) return best;
  let n = 1; while (channels.has(String(n))) n++;
  return newChannel(String(n));
}
const sendJson = (ws, msg) => { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); };
function broadcast(ch, msg, exceptId) {
  const text = JSON.stringify(msg);
  for (const [id, m] of ch.members) if (id !== exceptId && m.ws.readyState === 1 && m.ws.bufferedAmount < 512 * 1024) m.ws.send(text);
}
const isPlainObject = v => v !== null && typeof v === 'object' && !Array.isArray(v);

/* ---------- websocket ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8 * 1024 });
wss.on('connection', (ws, req) => {
  if (ORIGINS.length && !ORIGINS.includes(req.headers.origin)) { ws.close(1008, 'origin not allowed'); return; }
  let want = null;
  try { want = new URL(req.url, 'http://local').searchParams.get('ch'); } catch {}
  const ch = pickChannel(want);
  const id = crypto.randomBytes(6).toString('hex');
  const m = { ws, presence: null, tokens: 60, last: Date.now(), alive: true };
  ch.members.set(id, m); ch.emptySince = 0;

  sendJson(ws, {
    t: 'welcome', you: id, ch: ch.id, st: Date.now(),
    peers: [...ch.members].filter(([, v]) => v.presence).map(([k, v]) => [k, v.presence]),
    world: Object.fromEntries(Object.entries(ch.world).map(([c, mp]) => [c, [...mp]]))
  });

  ws.on('pong', () => { m.alive = true; });
  ws.on('message', raw => {
    // simple rate limit: about 30 messages a second, bursts up to 60
    const now = Date.now();
    m.tokens = Math.min(60, m.tokens + (now - m.last) * 0.03); m.last = now;
    if (m.tokens < 1) return;
    m.tokens -= 1;
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (!isPlainObject(msg)) return;

    if (msg.t === 'p') {
      if (!isPlainObject(msg.d) || JSON.stringify(msg.d).length > MAX_PRESENCE_BYTES) return;
      m.presence = msg.d; ch.dirty.add(id);
    } else if (msg.t === 'ping') {
      sendJson(ws, { t: 'pong', ct: Number(msg.ct) || 0, st: Date.now() });
    } else if (msg.t === 'set') {
      const col = ch.world[msg.c];
      if (!col || typeof msg.id !== 'string' || !/^[A-Za-z0-9_-]{1,40}$/.test(msg.id) || !isPlainObject(msg.d)) return;
      if (JSON.stringify(msg.d).length > MAX_DOC_BYTES) return;
      const d = { ...msg.d };
      if (!Number.isFinite(d.ts) || Math.abs(d.ts - now) > 60e3) d.ts = now;
      col.set(msg.id, d);
      while (col.size > MAX_DOCS[msg.c]) {
        let oldest = null; for (const [k, v] of col) if (!oldest || v.ts < oldest[1].ts) oldest = [k, v];
        col.delete(oldest[0]); broadcast(ch, { t: 'wdel', c: msg.c, id: oldest[0] });
      }
      broadcast(ch, { t: 'wset', c: msg.c, id: msg.id, d }, id);
    } else if (msg.t === 'del') {
      const col = ch.world[msg.c];
      if (col && typeof msg.id === 'string' && col.delete(msg.id)) broadcast(ch, { t: 'wdel', c: msg.c, id: msg.id }, id);
    }
  });
  ws.on('close', () => {
    ch.members.delete(id); ch.dirty.delete(id); ch.removed.add(id);
    if (!ch.members.size) ch.emptySince = Date.now();
  });
  ws.on('error', () => {});
});

/* ---------- presence relay: changes only, 10 times a second ---------- */
setInterval(() => {
  for (const ch of channels.values()) {
    if (!ch.dirty.size && !ch.removed.size) continue;
    const up = [...ch.dirty].filter(k => ch.members.has(k)).map(k => [k, ch.members.get(k).presence]);
    const msg = { t: 'pd', up, rm: [...ch.removed] };
    ch.dirty.clear(); ch.removed.clear();
    broadcast(ch, msg);
  }
}, TICK_MS);

/* ---------- housekeeping: expire snow objects, drop idle channels, cut dead connections ---------- */
setInterval(() => {
  const now = Date.now();
  for (const [cid, ch] of channels) {
    for (const [c, col] of Object.entries(ch.world)) for (const [k, v] of col) if (now - v.ts > LIFE[c]) { col.delete(k); broadcast(ch, { t: 'wdel', c, id: k }); }
    if (!ch.members.size && ch.emptySince && now - ch.emptySince > 10 * 60e3) channels.delete(cid);
  }
}, 10e3);
setInterval(() => {
  for (const ch of channels.values()) for (const [, m] of ch.members) {
    if (!m.alive) { m.ws.terminate(); continue; }
    m.alive = false; try { m.ws.ping(); } catch {}
  }
}, 20e3);
setInterval(() => {
  let people = 0; for (const ch of channels.values()) people += ch.members.size;
  console.log(`[white-park] channels=${channels.size} people=${people}`);
}, 60e3);

server.listen(PORT, () => console.log(`[white-park] http://localhost:${PORT}  (websocket: /ws, ${ROOM_CAP} per channel)`));
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { wss.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
