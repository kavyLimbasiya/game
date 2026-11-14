#!/usr/bin/env node
// Ludo multiplayer server. No dependencies.
// Usage:  node server.js        then open  http://localhost:8080
// Put this file in the same folder as Ludo_3D.html.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const GAME_FILE = fs.existsSync(path.join(__dirname, 'index.html'))
  ? path.join(__dirname, 'index.html')
  : path.join(__dirname, 'Ludo_3D.html');
const WS_PATH = '/ws';
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const COUNTDOWN_SECONDS = 3;

let nextId = 1;
const clients = new Map();   // id -> client
const rooms = new Map();     // lowercase server name -> room

// ---------- HTTP: serve the game ----------
const httpServer = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(GAME_FILE, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        return res.end('Game file was not found next to server.js');
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
  }
});

// ---------- WebSocket (RFC 6455, minimal) ----------
httpServer.on('upgrade', (req, sock, head) => {
  const key = req.headers['sec-websocket-key'];
  if (req.url !== WS_PATH || !key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\n' +
             'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
             'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);

  const c = { id: nextId++, sock, name: '', room: null, ready: false, buf: head && head.length ? head : Buffer.alloc(0) };
  clients.set(c.id, c);
  let gone = false;
  const drop = () => {
    if (gone) return;
    gone = true;
    clients.delete(c.id);
    removeFromRoom(c);
  };
  sock.on('data', d => { c.buf = Buffer.concat([c.buf, d]); parseFrames(c); });
  sock.on('close', drop);
  sock.on('error', drop);
  send(c, { t: 'hello', id: c.id });
  parseFrames(c);
});

function parseFrames(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (b.length < 4) return;
      len = b.readUInt16BE(2); off = 4;
    } else if (len === 127) {
      if (b.length < 10) return;
      len = Number(b.readBigUInt64BE(2)); off = 10;
    }
    let maskKey = null;
    if (masked) {
      if (b.length < off + 4) return;
      maskKey = b.subarray(off, off + 4); off += 4;
    }
    if (b.length < off + len) return;
    const payload = Buffer.from(b.subarray(off, off + len));
    c.buf = b.subarray(off + len);
    if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i % 4];

    if (opcode === 0x8) { c.sock.end(); return; }            // close
    if (opcode === 0x9) { sendRaw(c, 0xA, payload); continue; } // ping -> pong
    if (opcode === 0x1) {                                      // text
      let msg;
      try { msg = JSON.parse(payload.toString('utf8')); } catch (e) { continue; }
      try { handle(c, msg); } catch (e) { console.error('handler error', e); }
    }
  }
}

function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) {
    head = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([head, payload]);
}
function sendRaw(c, opcode, payload) { if (c.sock.writable) c.sock.write(frame(opcode, payload)); }
function send(c, o) { sendRaw(c, 0x1, Buffer.from(JSON.stringify(o), 'utf8')); }
function err(c, msg) { send(c, { t: 'error', msg }); }

// ---------- helpers ----------
const cleanName = s => String(s == null ? '' : s).replace(/[\u0000-\u001f]/g, '').trim().slice(0, 24);
const cleanPlayer = s => cleanName(s).slice(0, 16);
const clampInt = (v, lo, hi, dflt) => { const n = Math.trunc(Number(v)); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt; };
const makeCode = () => { let s = ''; for (let i = 0; i < 6; i++) s += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]; return s; };
const roomKey = name => name.toLowerCase();

function roomView(r) {
  return {
    name: r.name,
    isPublic: r.isPublic,
    code: r.isPublic ? '' : r.code,
    hostId: r.host,
    seats: r.seats,
    pieces: r.pieces,
    mode: r.mode,
    state: r.state,
    cdN: r.cdN,
    players: r.members.map(m => ({ id: m.id, name: m.name, ready: m.ready })),
  };
}
function broadcastRoom(r) {
  const view = roomView(r);
  r.members.forEach(m => send(m, { t: 'room', you: m.id, room: view }));
}
function listPublic() {
  return [...rooms.values()].filter(r => r.isPublic).map(r => ({
    name: r.name, players: r.members.length, seats: r.seats, state: r.state,
  }));
}
function broadcastList() {
  const list = listPublic();
  for (const c of clients.values()) if (!c.room) send(c, { t: 'list', rooms: list });
}

// Central state update: decides countdown / cancel, then notifies everyone.
function update(r) {
  if (r.state !== 'playing') {
    const allReady = r.members.length >= 2 && r.members.every(m => m.ready);
    if (!allReady) {
      if (r.cdTimer) { clearInterval(r.cdTimer); r.cdTimer = null; }
      r.state = 'lobby';
    } else if (r.state === 'lobby') {
      r.state = 'countdown';
      r.cdN = COUNTDOWN_SECONDS;
      r.cdTimer = setInterval(() => tick(r), 1000);
    }
  }
  broadcastRoom(r);
  broadcastList();
}

function tick(r) {
  r.cdN--;
  if (r.cdN > 0) { broadcastRoom(r); return; }
  clearInterval(r.cdTimer); r.cdTimer = null;
  const allReady = r.members.length >= 2 && r.members.every(m => m.ready);
  if (!allReady) { r.state = 'lobby'; broadcastRoom(r); broadcastList(); return; }
  r.state = 'playing';
  const ms = r.members;
  const ids = ms.map(m => m.id), names = ms.map(m => m.name);
  ms.forEach(m => send(m, { t: 'start', ids, names, pieces: r.pieces, mode: r.mode }));
  broadcastRoom(r);
  broadcastList();
}

function addMember(r, c, playerName) {
  c.name = cleanPlayer(playerName) || ('Player ' + c.id);
  c.ready = false;
  c.room = r;
  r.members.push(c);
  update(r);
}

function createRoom(c, m) {
  if (c.room) return err(c, 'Leave your current server first');
  const name = cleanName(m.name);
  if (!name) return err(c, 'Enter a server name');
  const key = roomKey(name);
  if (rooms.has(key)) return err(c, 'A server with that name already exists');
  const r = {
    name, isPublic: !!m.public, code: m.public ? '' : makeCode(), host: c.id,
    seats: clampInt(m.seats, 2, 10, 4), pieces: clampInt(m.pieces, 1, 4, 4),
    mode: m.mode === 'team' ? 'team' : 'solo',
    state: 'lobby', members: [], cdN: 0, cdTimer: null,
  };
  rooms.set(key, r);
  addMember(r, c, m.playerName);
}

function joinRoom(c, m) {
  if (c.room) return err(c, 'Leave your current server first');
  const r = rooms.get(roomKey(cleanName(m.name)));
  if (!r) return err(c, 'No server with that name');
  if (!r.isPublic && String(m.code || '').trim().toUpperCase() !== r.code) return err(c, 'Wrong code for this private server');
  if (r.state === 'playing') return err(c, 'A game is already running on this server');
  if (r.members.length >= r.seats) return err(c, 'This server is full');
  addMember(r, c, m.playerName);
}

function endGame(r) {
  r.state = 'lobby';
  r.members.forEach(m => { m.ready = false; });
  update(r);
}

function removeFromRoom(c) {
  const r = c.room;
  if (!r) return;
  c.room = null;
  c.ready = false;
  r.members = r.members.filter(m => m !== c);
  if (r.cdTimer) { clearInterval(r.cdTimer); r.cdTimer = null; }
  if (!r.members.length) { rooms.delete(roomKey(r.name)); broadcastList(); return; }
  if (r.host === c.id) r.host = r.members[0].id;
  const wasPlaying = r.state === 'playing';
  r.state = 'lobby';
  r.members.forEach(m => { m.ready = false; });
  if (wasPlaying) {
    r.members.forEach(m => send(m, { t: 'aborted', msg: (c.name || 'A player') + ' left, so the game ended' }));
  }
  broadcastRoom(r);
  broadcastList();
}

function handle(c, m) {
  switch (m.t) {
    case 'list':
      send(c, { t: 'list', rooms: listPublic() });
      break;
    case 'create':
      createRoom(c, m);
      break;
    case 'join':
      joinRoom(c, m);
      break;
    case 'ready': {
      const r = c.room;
      if (!r || r.state === 'playing') break;
      c.ready = !!m.ready;
      update(r);
      break;
    }
    case 'leave':
      removeFromRoom(c);
      broadcastList();
      break;
    case 'roll': {
      // Dice are rolled here, so no player can pick their own numbers.
      const r = c.room;
      if (r && r.state === 'playing') {
        const v = 1 + crypto.randomInt(6);
        r.members.forEach(x => send(x, { t: 'roll', v, by: c.id }));
      }
      break;
    }
    case 'move': {
      // The server only relays the chosen piece; each browser checks it against the rules.
      const r = c.room;
      if (r && r.state === 'playing') {
        const tok = clampInt(m.tok, 0, 9999, -1);
        if (tok >= 0) r.members.forEach(x => send(x, { t: 'move', tok, by: c.id }));
      }
      break;
    }
    case 'finished': {
      const r = c.room;
      if (r && r.state === 'playing') endGame(r);
      break;
    }
  }
}

httpServer.listen(PORT, () => {
  console.log('Ludo server running.');
  console.log('  Open in browser:  http://localhost:' + PORT);
});
