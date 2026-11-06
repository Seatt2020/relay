// 小游戏大全 · 联机中转服务 v2（CloudBase 云托管 / Node 原生 http + 手写 WebSocket）
// 房间系统：8 位房间码 / 2-4 人 / 公开-私密 / 观战 / 房主设置 / 踢人 / 断线托管 / 房间列表
// 对局同步：房主权威（host-authoritative）——房主计算完整状态，服务器广播快照，客人只渲染
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 80;
const rooms = new Map();      // code -> room
const clients = new Map();    // socket -> {code, role:'member'|'spec', seat, openId}
const pres = new Map();       // openId -> {nick, ws, lastSeen, friends:Set}
const freqs = new Map();      // toOpenId -> Map<fromOpenId,{nick,code,ts}>
const FCODES = new Map();     // 双向：openId->码, 码->openId
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 去掉 I O 0 1，32 个字符
const CODE_LEN = 8;

/* ---------------- 工具 ---------------- */
function newCode() {
  let c;
  do { c = ''; for (let i = 0; i < CODE_LEN; i++) c += ALPHA[Math.floor(Math.random() * ALPHA.length)]; }
  while (rooms.has(c));
  return c;
}
function norm(s) { return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }
function fcode(openId) {
  let h = 0; const s = String(openId);
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  let c = ''; for (let i = 0; i < 6; i++) { c += ALPHA[h % 32]; h = (Math.floor(h / 32) + ((h << 7) >>> 0) % 7 + 1) >>> 0; }
  return c;
}
function regFC(openId) {
  let c = FCODES.get(openId);
  if (!c) { c = fcode(openId); while (FCODES.has(c) && FCODES.get(c) !== openId) { c = fcode(c + Math.random()); } FCODES.set(openId, c); FCODES.set(c, openId); }
  return c;
}
function presOf(openId) { let p = pres.get(openId); if (!p) { p = { nick: '', ws: null, lastSeen: 0, friends: new Set() }; pres.set(openId, p); } return p; }
function online(openId) { const p = pres.get(openId); return !!(p && p.ws); }
function pushTo(openId, msg) { const p = pres.get(openId); if (p && p.ws) { try { wsSend(p.ws, JSON.stringify(msg)); } catch (e) { p.ws = null; } } }
function friendView(openId) {
  const p = presOf(openId), out = [];
  for (const fid of p.friends) { const fp = pres.get(fid); out.push({ openId: fid, nick: (fp && fp.nick) || '', fcode: regFC(fid), online: online(fid) }); }
  return out;
}

/* ---------------- WebSocket 底层 ---------------- */
function wsAccept(key) { return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64'); }
function wsSend(ws, str) {
  const payload = Buffer.from(str), len = payload.length;
  let head;
  if (len < 126) { head = Buffer.from([0x81, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  ws.write(Buffer.concat([head, payload]));
}
function wsFrames(buf) { // 返回 [{op,payload}] 与剩余 buf
  const out = []; let rest = buf;
  while (rest.length >= 2) {
    const op = rest[0] & 0x0f, masked = rest[1] & 0x80, len = rest[1] & 0x7f;
    let off = 2, plen = len;
    if (len === 126) { if (rest.length < 4) break; plen = rest.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (rest.length < 10) break; plen = Number(rest.readBigUInt64BE(2)); off = 10; }
    let mask = null;
    if (masked) { if (rest.length < off + 4) break; mask = rest.slice(off, off + 4); off += 4; }
    if (rest.length < off + plen) break;
    let payload = rest.slice(off, off + plen);
    if (mask) { const b = Buffer.alloc(plen); for (let i = 0; i < plen; i++) b[i] = payload[i] ^ mask[i & 3]; payload = b; }
    out.push({ op, payload });
    rest = rest.slice(off + plen);
  }
  return { frames: out, rest };
}

/* ---------------- 房间 ---------------- */
function mkRoom(host, m) {
  return {
    code: newCode(),
    game: m.game || 'doudaxiao',
    gname: m.gname || m.game || '',
    hostId: host.openId,
    maxPlayers: Math.min(4, Math.max(2, parseInt(m.maxPlayers, 10) || 2)),
    isPublic: m.isPublic !== false,
    started: false,
    turn: 0, seq: 0, state: null,
    members: [],   // {openId,nick,seat,ready,ws,lastSeen,bot}
    specs: [],     // {openId,nick,ws,lastSeen}
    created: Date.now(),
    lastAct: Date.now()
  };
}
function rmOf(ws) { return rooms.get((clients.get(ws) || {}).code); }
function memberOf(r, openId) { return r.members.find(x => x.openId === openId) || null; }
function seatOf(r, openId) { const m = memberOf(r, openId); return m ? m.seat : -1; }
function roomView(r) {
  const driver = r.members.filter(x => x.ws && !x.bot).map(x => x.seat).sort((a, b) => a - b)[0];
  const hostM = memberOf(r, r.hostId);
  return {
    t: 'room', code: r.code, game: r.game, gname: r.gname,
    hostId: r.hostId, hostNick: hostM ? hostM.nick : '',
    maxPlayers: r.maxPlayers, isPublic: r.isPublic,
    started: r.started, turn: r.turn, seq: r.seq,
    members: r.members.map(x => ({ openId: x.openId, nick: x.nick, seat: x.seat, ready: !!x.ready, online: !!x.ws, bot: !!x.bot, host: x.openId === r.hostId })),
    specs: r.specs.map(x => ({ openId: x.openId, nick: x.nick, online: !!x.ws })),
    driver: typeof driver === 'number' ? driver : -1
  };
}
function pushRoom(r, exceptWs) {
  const view = JSON.stringify(Object.assign(roomView(r), { t: 'room' }));
  for (const m of r.members) if (m.ws && m.ws !== exceptWs) { try { wsSend(m.ws, view); } catch (e) { m.ws = null; } }
  for (const s of r.specs) if (s.ws && s.ws !== exceptWs) { try { wsSend(s.ws, view); } catch (e) { s.ws = null; } }
}
function pushAll(r, msg, exceptWs) {
  const data = JSON.stringify(msg);
  for (const m of r.members) if (m.ws && m.ws !== exceptWs) { try { wsSend(m.ws, data); } catch (e) { m.ws = null; } }
  for (const s of r.specs) if (s.ws && s.ws !== exceptWs) { try { wsSend(s.ws, data); } catch (e) { s.ws = null; } }
}
function listRooms(gameFilter) {
  const out = [];
  for (const r of rooms.values()) {
    if (!r.isPublic) continue;
    if (gameFilter && r.game !== gameFilter) continue;
    if (r.members.filter(m => m.ws).length === 0) continue;
    out.push({
      code: r.code, game: r.game, gname: r.gname,
      hostNick: (memberOf(r, r.hostId) || {}).nick || '',
      count: r.members.length, max: r.maxPlayers,
      specs: r.specs.length, started: r.started, created: r.created
    });
  }
  out.sort((a, b) => b.created - a.created);
  return out.slice(0, 80);
}
function dropFromRoom(r, openId) {
  const i = r.members.findIndex(x => x.openId === openId);
  if (i >= 0) {
    r.members.splice(i, 1);
    r.members.forEach((m, k) => { m.seat = k; });
  }
  const j = r.specs.findIndex(x => x.openId === openId);
  if (j >= 0) r.specs.splice(j, 1);
  if (r.hostId === openId) {
    const nx = r.members.filter(m => m.ws)[0] || r.members[0];
    if (nx) r.hostId = nx.openId;
  }
  if (!r.members.length) { rooms.delete(r.code); return true; }
  if (r.members.length < 2) { r.started = false; }
  return false;
}

/* 心跳 / 托管 / 清洁 */
setInterval(() => {
  const now = Date.now();
  for (const [code, r] of rooms) {
    let changed = false;
    for (const m of r.members) {
      const dead = now - m.lastSeen > 15000;
      if (dead && m.ws) { m.ws = null; changed = true; }
      if (r.started && dead && !m.bot) { m.bot = true; changed = true; }
      if (!dead && m.bot && m.ws) { m.bot = false; changed = true; }
    }
    for (const s of r.specs) if (s.ws && now - s.lastSeen > 15000) s.ws = null;
    if (changed) pushRoom(r);
    if (now - r.lastAct > 3600000) rooms.delete(code);
  }
}, 5000);

/* ---------------- HTTP ---------------- */
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, players: clients.size }));
    return;
  }
  if (u === '/rooms') {
    const q = new URL(req.url, 'http://x').searchParams.get('game') || '';
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ rooms: listRooms(q) }));
    return;
  }
  res.writeHead(404); res.end();
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');
  socket.setNoDelay(true);
  let buf = Buffer.alloc(0);
  socket.on('data', d => {
    buf = Buffer.concat([buf, d]);
    const { frames, rest } = wsFrames(buf);
    buf = rest;
    for (const f of frames) {
      try {
        if (f.op === 0x8) { onClose(socket); try { socket.destroy(); } catch (e) { } return; }
        if (f.op === 0x9) { try { socket.write(Buffer.from([0x8A, 0x00])); } catch (e) { } continue; }
        if (f.op !== 0x1) continue;
        handle(socket, JSON.parse(f.payload.toString()));
      } catch (e) { }
    }
  });
  socket.on('error', () => onClose(socket));
  socket.on('close', () => onClose(socket));
});

function onClose(ws) {
  const c = clients.get(ws);
  if (!c) return;
  clients.delete(ws);
  const r = rooms.get(c.code);
  if (r) {
    const m = memberOf(r, c.openId);
    if (m && m.ws === ws) { m.ws = null; m.bot = r.started; }
    const s = r.specs.find(x => x.openId === c.openId);
    if (s && s.ws === ws) r.specs = r.specs.filter(x => x !== s);
    r.lastAct = Date.now();
    pushRoom(r);
    // 房主掉线→移交
    if (r.hostId === c.openId) {
      const nx = r.members.filter(x => x.ws)[0];
      if (nx) { r.hostId = nx.openId; pushRoom(r); }
    }
  }
  const p = pres.get(c.openId);
  if (p && p.ws === ws) p.ws = null;
}

/* ---------------- 消息处理 ---------------- */
function handle(ws, m) {
  const now = Date.now();
  const oid = String(m.openId || '');
  switch (m.t) {

    /* ---- 身份 / 好友（沿用 v33.3） ---- */
    case 'hello': {
      if (!oid) return;
      const p = presOf(oid); p.nick = m.nick || p.nick || '玩家'; p.ws = ws; p.lastSeen = now;
      const reqs = []; const fm = freqs.get(oid);
      if (fm) for (const [from, info] of fm) reqs.push({ from, nick: info.nick, fcode: regFC(from), code: info.code || '' });
      wsSend(ws, JSON.stringify({ t: 'hello', fcode: regFC(oid), reqs }));
      break;
    }
    case 'friends': {
      if (!oid) return;
      wsSend(ws, JSON.stringify({ t: 'friends', list: friendView(oid) }));
      break;
    }
    case 'fadd': {
      const target = FCODES.get(norm(m.fcode)), from = String(m.from || '');
      if (!target || target === from) { wsSend(ws, JSON.stringify({ t: 'ferr', msg: '好友码不存在' })); return; }
      const tp = presOf(target), fp = presOf(from);
      fp.nick = m.nick || fp.nick; fp.lastSeen = now;
      if (tp.friends.has(from)) { wsSend(ws, JSON.stringify({ t: 'ferr', msg: '对方已是你的好友' })); return; }
      if (!freqs.has(target)) freqs.set(target, new Map());
      const info = { nick: m.nick || fp.nick || '玩家', code: m.code || '', ts: now };
      freqs.get(target).set(from, info);
      pushTo(target, { t: 'freq', from, nick: info.nick, fcode: regFC(from), code: info.code });
      wsSend(ws, JSON.stringify({ t: 'fok', msg: '请求已发送' }));
      break;
    }
    case 'facc': {
      const from = String(m.from || '');
      const fm = freqs.get(oid); if (fm && fm.has(from)) { fm.delete(from); if (!fm.size) freqs.delete(oid); }
      presOf(oid).friends.add(from); presOf(from).friends.add(oid);
      pushTo(from, { t: 'facc', by: oid, nick: presOf(oid).nick });
      wsSend(ws, JSON.stringify({ t: 'facc', by: from, nick: presOf(from).nick, list: friendView(oid) }));
      break;
    }
    case 'fdel': {
      const from = String(m.from || '');
      presOf(oid).friends.delete(from); presOf(from).friends.delete(oid);
      pushTo(from, { t: 'fdel', by: oid });
      wsSend(ws, JSON.stringify({ t: 'fdel', by: from, list: friendView(oid) }));
      break;
    }
    case 'finv': {
      const to = String(m.to || '');
      if (!online(to)) { wsSend(ws, JSON.stringify({ t: 'ferr', msg: '对方不在线' })); return; }
      pushTo(to, { t: 'finv', from: oid, nick: m.nick || '好友', gid: m.gid || '', code: m.code || '', gname: m.gname || '' });
      wsSend(ws, JSON.stringify({ t: 'fok', msg: '邀请已发出' }));
      break;
    }

    /* ---- 房间列表 ---- */
    case 'rooms': {
      wsSend(ws, JSON.stringify({ t: 'rooms', list: listRooms(m.game || '') }));
      break;
    }

    /* ---- 建房 ---- */
    case 'create': {
      if (!oid) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOLOGIN', msg: '请先登录' })); return; }
      const p = presOf(oid); p.nick = m.nick || p.nick || '玩家'; p.ws = ws; p.lastSeen = now;
      const r = mkRoom({ openId: oid }, m);
      r.members.push({ openId: oid, nick: p.nick, seat: 0, ready: true, ws, lastSeen: now, bot: false });
      rooms.set(r.code, r);
      clients.set(ws, { code: r.code, role: 'member', seat: 0, openId: oid });
      wsSend(ws, JSON.stringify({ t: 'created', code: r.code, seat: 0 }));
      pushRoom(r);
      break;
    }

    /* ---- 加入（人满自动转观战） ---- */
    case 'join': {
      if (!oid) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOLOGIN', msg: '请先登录' })); return; }
      const r = rooms.get(norm(m.code));
      if (!r) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOT_FOUND', msg: '房间不存在' })); return; }
      const ex = memberOf(r, oid);
      if (ex) { // 已在房里（含重连）
        ex.ws = ws; ex.lastSeen = now; ex.bot = false;
        clients.set(ws, { code: r.code, role: 'member', seat: ex.seat, openId: oid });
        wsSend(ws, JSON.stringify({ t: 'joined', code: r.code, seat: ex.seat, role: 'member' }));
        pushRoom(r); break;
      }
      if (r.members.length < r.maxPlayers && !r.started) {
        const p = presOf(oid); p.nick = m.nick || p.nick || '玩家'; p.ws = ws; p.lastSeen = now;
        const seat = r.members.length;
        r.members.push({ openId: oid, nick: p.nick, seat, ready: false, ws, lastSeen: now, bot: false });
        clients.set(ws, { code: r.code, role: 'member', seat, openId: oid });
        wsSend(ws, JSON.stringify({ t: 'joined', code: r.code, seat, role: 'member' }));
      } else {
        const p = presOf(oid); p.nick = m.nick || p.nick || '玩家'; p.ws = ws; p.lastSeen = now;
        if (!r.specs.some(x => x.openId === oid)) r.specs.push({ openId: oid, nick: p.nick, ws, lastSeen: now });
        clients.set(ws, { code: r.code, role: 'spec', seat: -1, openId: oid });
        wsSend(ws, JSON.stringify({ t: 'joined', code: r.code, seat: -1, role: 'spec' }));
      }
      r.lastAct = now;
      pushRoom(r);
      break;
    }

    case 'rejoin': {
      const r = rooms.get(norm(m.code));
      if (!r) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NOT_FOUND', msg: '房间已关闭' })); return; }
      const mm = memberOf(r, oid);
      const sp = r.specs.find(x => x.openId === oid);
      if (mm) {
        mm.ws = ws; mm.lastSeen = now; mm.bot = false;
        clients.set(ws, { code: r.code, role: 'member', seat: mm.seat, openId: oid });
        wsSend(ws, JSON.stringify({ t: 'rejoined', code: r.code, seat: mm.seat, state: r.state, turn: r.turn, seq: r.seq }));
      } else if (sp) {
        sp.ws = ws; sp.lastSeen = now;
        clients.set(ws, { code: r.code, role: 'spec', seat: -1, openId: oid });
        wsSend(ws, JSON.stringify({ t: 'rejoined', code: r.code, seat: -1, state: r.state, turn: r.turn, seq: r.seq }));
      } else {
        wsSend(ws, JSON.stringify({ t: 'err', code: 'GONE', msg: '你已不在房间中' })); return;
      }
      const p = presOf(oid); p.ws = ws; p.lastSeen = now;
      r.lastAct = now; pushRoom(r);
      break;
    }

    case 'leave': {
      const c = clients.get(ws); if (!c) return;
      const r = rooms.get(c.code); clients.delete(ws);
      if (r) { const empty = dropFromRoom(r, c.openId); if (!empty) pushRoom(r); }
      wsSend(ws, JSON.stringify({ t: 'left' }));
      break;
    }

    /* ---- 房主：改设置 / 踢人 / 开局 ---- */
    case 'setcfg': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r || r.hostId !== oid) return;
      if (m.maxPlayers) {
        const mp = Math.min(4, Math.max(2, parseInt(m.maxPlayers, 10) || 2));
        r.maxPlayers = mp;
        while (r.members.length > mp) { const d = r.members.pop(); dropFromRoom(r, d.openId); }
      }
      if (typeof m.isPublic === 'boolean') r.isPublic = m.isPublic;
      if (m.gname) r.gname = String(m.gname).slice(0, 20);
      r.lastAct = now; pushRoom(r);
      break;
    }
    case 'kick': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r || r.hostId !== oid) return;
      const tgt = String(m.target || '');
      if (tgt === r.hostId) return;
      const tm = memberOf(r, tgt) || r.specs.find(x => x.openId === tgt);
      if (tm && tm.ws) { try { wsSend(tm.ws, JSON.stringify({ t: 'kicked', msg: '你已被房主移出房间' })); } catch (e) { } clients.delete(tm.ws); }
      const empty = dropFromRoom(r, tgt);
      if (!empty) { r.lastAct = now; pushRoom(r); }
      break;
    }
    case 'start': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r || r.hostId !== oid) return;
      if (r.members.length < 2) { wsSend(ws, JSON.stringify({ t: 'err', code: 'NEED2', msg: '至少需要 2 人才能开局' })); return; }
      r.started = true; r.state = m.state || null; r.turn = 0; r.seq++;
      r.members.forEach(x => { x.bot = false; });
      r.lastAct = now;
      pushAll(r, { t: 'started', seq: r.seq, state: r.state, turn: 0 });
      pushRoom(r);
      break;
    }
    case 'ready': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r) return;
      const mm = memberOf(r, oid); if (!mm) return;
      mm.ready = !!m.ready; mm.lastSeen = now;
      pushRoom(r);
      break;
    }

    /* ---- 对局同步：房主权威 ---- */
    case 'sync': { // 房主推送完整状态 {t:'sync', state, turn, seq}
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r || !r.started) return;
      if (r.hostId !== oid) return; // 只有房主能推
      r.state = m.state === undefined ? r.state : m.state;
      if (typeof m.turn === 'number') r.turn = m.turn;
      r.seq++;
      r.lastAct = now;
      pushAll(r, { t: 'sync', state: r.state, turn: r.turn, seq: r.seq }, ws);
      break;
    }
    case 'over': { // 结算 {t:'over', result:{winner:seat, scores:[..]}}
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r) return;
      pushAll(r, { t: 'over', seat: seatOf(r, oid), result: m.result || null }, ws);
      r.started = false; r.state = null; r.seq++;
      r.members.forEach(x => { x.ready = false; x.bot = false; });
      r.lastAct = now;
      pushRoom(r);
      break;
    }

    /* ---- 客人力请求：转发给房主 ---- */
    case 'req': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r || !r.started) return;
      const hm = memberOf(r, r.hostId);
      if (hm && hm.ws && hm.ws !== ws) {
        try { wsSend(hm.ws, JSON.stringify({ t: 'req', seat: seatOf(r, oid), from: oid, a: m.a, n: m.n })); } catch (e) { }
      }
      break;
    }

    /* ---- 房间内快捷消息 ---- */
    case 'chat': {
      const c = clients.get(ws); const r = c && rooms.get(c.code);
      if (!r) return;
      const p = pres.get(oid);
      pushAll(r, { t: 'chat', from: oid, nick: (p && p.nick) || '', seat: seatOf(r, oid), text: String(m.text || '').slice(0, 40) });
      break;
    }

    /* ---- 心跳 ---- */
    case 'heartbeat': {
      const p = pres.get(oid); if (p) { p.lastSeen = now; p.ws = ws; }
      const c = clients.get(ws); if (!c) return;
      const r = rooms.get(c.code); if (!r) return;
      const mm = memberOf(r, oid); if (mm) { mm.lastSeen = now; mm.ws = ws; if (mm.bot) { mm.bot = false; pushRoom(r); } }
      const sp = r.specs.find(x => x.openId === oid); if (sp) { sp.lastSeen = now; sp.ws = ws; }
      break;
    }
  }
}

server.listen(PORT, () => console.log('relay v2 on ' + PORT));
