// 小游戏大全 · 联机中转服务（CloudBase 云托管 / Node.js 原生 http + WebSocket）
// 职责：房间管理 + 双人实时对战消息转发 + 在线状态心跳
// 零依赖（用原生 http + 手写 WebSocket 握手帧），无 npm install 需要
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 80;
const rooms = new Map();          // code -> room
const clients = new Map();        // ws -> {roomId, seat, openId, nick}
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function newCode() {
  let c;
  do { c = Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join(''); }
  while (rooms.has(c));
  return c;
}

function prune() {
  const now = Date.now();
  for (const [code, r] of rooms) {
    for (const seat of ['A', 'B']) {
      const p = r.players[seat];
      if (p && now - p.lastSeen > 20000) { r.players[seat] = null; r.seats &= (seat === 'A' ? ~1 : ~2); }
    }
    if (!r.players.A && !r.players.B && now - r.created > 60000) rooms.delete(code);
  }
}
setInterval(prune, 5000);

function broadcast(room, msg, exceptWs) {
  const data = JSON.stringify(msg);
  for (const [ws, c] of clients) {
    if (c.roomId !== room.code || ws === exceptWs) continue;
    try { wsSend(ws, data); } catch (e) { clients.delete(ws); }
  }
}

function roomState(r) {
  return {
    t: 'state', code: r.code, game: r.game,
    players: {
      A: r.players.A ? { nick: r.players.A.nick, openId: r.players.A.openId } : null,
      B: r.players.B ? { nick: r.players.B.nick, openId: r.players.B.openId } : null
    },
    started: !!r.started, turn: r.turn, data: r.data || null
  };
}

/* ---------- 极简 WebSocket（服务端） ---------- */
function wsAccept(key) {
  return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}
function wsSend(ws, str) {
  const payload = Buffer.from(str);
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.from([0x81, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  ws.write(Buffer.concat([head, payload]));
}
function wsFrame(buf) { // 解析一个客户端帧（假定单帧）
  if (buf.length < 2) return null;
  const op = buf[0] & 0x0f, masked = buf[1] & 0x80, len = buf[1] & 0x7f;
  let off = 2, mask = null, plen = len;
  if (len === 126) { plen = buf.readUInt16BE(2); off = 4; }
  else if (len === 127) { plen = Number(buf.readBigUInt64BE(2)); off = 10; }
  if (masked) { mask = buf.slice(off, off + 4); off += 4; }
  let payload = buf.slice(off, off + plen);
  if (mask) { const out = Buffer.alloc(plen); for (let i = 0; i < plen; i++) out[i] = payload[i] ^ mask[i & 3]; payload = out; }
  return { op, payload };
}

const server = http.createServer((req, res) => {
  // HTTP 健康检查
  if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"ok":true,"rooms":' + rooms.size + '}'); return; }
  res.writeHead(404); res.end();
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');
  socket.setNoDelay(true); // 关键：关 Nagle，压低延迟
  let buf = Buffer.alloc(0);
  socket.on('data', d => { buf = Buffer.concat([buf, d]); 
    const f = wsFrame(buf);
    if (!f) return;
    buf = buf.slice(buf.length - 0); // 每次处理完整帧（游戏消息都很小，简化处理）
    try {
      if (f.op === 0x8) { clients.delete(socket); socket.destroy(); return; } // close
      if (f.op === 0x9) { socket.write(Buffer.from([0x8A, 0x00])); return; }  // ping->pong
      const m = JSON.parse(f.payload.toString());
      handle(socket, m);
    } catch (e) { }
  });
  socket.on('error', () => clients.delete(socket));
  socket.on('close', () => {
    const c = clients.get(socket);
    if (c) { clients.delete(socket); const r = rooms.get(c.roomId); if (r) broadcast(r, { t: 'leave', seat: c.seat }); }
  });
});

function handle(ws, m) {
  let r;
  switch (m.t) {
    case 'create': { // {t, game, openId, nick}
      const code = newCode();
      r = { code, game: m.game || 'unknown', players: { A: null, B: null }, seats: 0, started: false, turn: 'A', data: null, created: Date.now() };
      r.players.A = { openId: m.openId, nick: m.nick || '玩家A', lastSeen: Date.now() };
      r.seats = 1; rooms.set(code, r);
      clients.set(ws, { roomId: code, seat: 'A', openId: m.openId });
      wsSend(ws, JSON.stringify({ t: 'created', code, seat: 'A' }));
      break;
    }
    case 'join': { // {t, code, openId, nick}
      r = rooms.get((m.code || '').toUpperCase());
      if (!r) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOT_FOUND', msg: '房间号不存在' })); return; }
      const seat = !r.players.A ? 'A' : (!r.players.B ? 'B' : null);
      if (!seat) { wsSend(ws, JSON.stringify({ t: 'err', code: 'FULL', msg: '房间已满员' })); return; }
      r.players[seat] = { openId: m.openId, nick: m.nick || '玩家B', lastSeen: Date.now() };
      r.seats |= (seat === 'A' ? 1 : 2);
      r.started = true;
      clients.set(ws, { roomId: r.code, seat, openId: m.openId });
      wsSend(ws, JSON.stringify({ t: 'joined', code: r.code, seat }));
      broadcast(r, roomState(r));
      break;
    }
    case 'rejoin': { // 掉线重连：{t, code, openId}
      r = rooms.get((m.code || '').toUpperCase());
      if (!r) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOT_FOUND', msg: '房间已解散' })); return; }
      const seat = (r.players.A && r.players.A.openId === m.openId) ? 'A' : (r.players.B && r.players.B.openId === m.openId) ? 'B' : null;
      if (!seat) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOT_MEMBER', msg: '不在该房间' })); return; }
      r.players[seat].lastSeen = Date.now();
      clients.set(ws, { roomId: r.code, seat, openId: m.openId });
      wsSend(ws, JSON.stringify(Object.assign(roomState(r), { t: 'rejoined', seat })));
      break;
    }
    case 'heartbeat': {
      const c = clients.get(ws);
      if (c) { r = rooms.get(c.roomId); if (r && r.players[c.seat]) r.players[c.seat].lastSeen = Date.now(); }
      break;
    }
    case 'move': { // 对局数据转发（含 tick 状态），直接广播给对手
      const c = clients.get(ws);
      if (!c) return;
      r = rooms.get(c.roomId);
      if (!r) return;
      r.data = m.data || null; r.turn = m.turn || r.turn;
      broadcast(r, { t: 'move', seat: c.seat, data: r.data, turn: r.turn }, ws);
      break;
    }
    case 'end': { // 一方结算，通知对手
      const c = clients.get(ws);
      if (!c) return;
      r = rooms.get(c.roomId);
      if (r) broadcast(r, { t: 'end', seat: c.seat, result: m.result || null }, ws);
      break;
    }
  }
}

server.listen(PORT, () => console.log('relay on ' + PORT));
