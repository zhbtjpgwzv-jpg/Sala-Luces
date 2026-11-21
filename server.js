const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// Opcional: si no se define, la página usa la dirección desde la que se abre.
const PUBLIC_URL = (process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const HEX = /^#[0-9a-fA-F]{6}$/;

const server = http.createServer((req, res) => {
  const p = new URL(req.url, 'http://x').pathname;
  if (p === '/config') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ publicUrl: PUBLIC_URL }));
  }
  if (p === '/health') return res.end('ok');
  // Una sola página: el modo (controlador/invitado) se decide con ?view=guest
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
});

const rooms = new Map(); // código -> { state, clients:Set, timer }
const blank = () => ({ mode: 'off', color: '#000000', effect: null, period: 1400, beat: false, auto: false, message: null, v: 0 });

function getRoom(code) {
  let r = rooms.get(code);
  if (!r) rooms.set(code, (r = { state: blank(), clients: new Set(), timer: null }));
  clearTimeout(r.timer);
  return r;
}
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
function bcast(r, o) { const s = JSON.stringify(o); for (const c of r.clients) if (c.readyState === 1) c.send(s); }
function counts(r) {
  let guests = 0, controllers = 0;
  for (const c of r.clients) c.role === 'guest' ? guests++ : controllers++;
  return { t: 'count', guests, controllers };
}

function sanitize(p) {
  const o = {};
  if (['color', 'effect', 'off'].includes(p.mode)) o.mode = p.mode;
  if (HEX.test(p.color)) o.color = p.color;
  if ('effect' in p) o.effect = ['strobe', 'rainbow', 'pulse', 'siren'].includes(p.effect) ? p.effect : null;
  if ('period' in p) o.period = Math.min(6000, Math.max(300, +p.period || 1400));
  if ('beat' in p) o.beat = !!p.beat;
  if ('auto' in p) o.auto = !!p.auto;
  if ('message' in p) {
    const m = p.message;
    o.message = m && typeof m.text === 'string' && m.text.trim()
      ? { text: m.text.trim().slice(0, 60), bg: HEX.test(m.bg) ? m.bg : null, fg: HEX.test(m.fg) ? m.fg : '#ffffff' }
      : null;
  }
  return o;
}

function leave(ws) {
  const code = ws.room; if (!code) return;
  ws.room = null;
  const r = rooms.get(code); if (!r) return;
  r.clients.delete(ws);
  bcast(r, counts(r));
  if (!r.clients.size) r.timer = setTimeout(() => rooms.delete(code), 3600e3); // la sala conserva su estado 1 h
}

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => (ws.alive = true));
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'ping') return send(ws, { t: 'pong', c: m.c, s: Date.now() });
    if (m.t === 'join') {
      leave(ws);
      const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 10);
      if (!code) return;
      ws.room = code;
      ws.role = m.role === 'controller' ? 'controller' : 'guest';
      const r = getRoom(code);
      r.clients.add(ws);
      send(ws, { t: 'state', state: r.state }); // estado actual al entrar
      bcast(r, counts(r));
      return;
    }
    if (m.t === 'set' && ws.room && ws.role === 'controller') {
      const r = rooms.get(ws.room); if (!r) return;
      r.state = { ...r.state, ...sanitize(m.patch || {}), v: r.state.v + 1 };
      bcast(r, { t: 'state', state: r.state });
    }
  });
  ws.on('close', () => leave(ws));
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 30000);

server.listen(PORT, '0.0.0.0', () => console.log('Luces sincronizadas en el puerto ' + PORT));
