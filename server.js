// Sync Start — synchronized countdown rooms for co-op games.
// Rooms live in memory and are saved to DATA_FILE on shutdown, so a restart
// (e.g. a deploy) keeps them: clients reconnect with their tokens and carry on.
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const MAX_ROOMS = 500;
const MAX_PLAYERS = 100;
const GRACE_MS = Number(process.env.GRACE_MS) || 30_000; // keep a disconnected player this long
const ROOM_IDLE_MS = 6 * 3600_000;
const COUNTDOWNS = [3, 5, 10, 15];
const DATA_FILE = process.env.DATA_FILE || ""; // where rooms are saved between restarts
const PING_EVERY_MS = 5_000;

const INDEX = fs.readFileSync(path.join(__dirname, "public", "index.html"));
const ADMIN_HTML = fs.readFileSync(path.join(__dirname, "public", "admin.html"));
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || ""; // admin panel is off when empty
const STARTED = Date.now();
const ICONS = {
  "/icon.png": fs.readFileSync(path.join(__dirname, "public", "icon.png")),
  "/favicon-32.png": fs.readFileSync(path.join(__dirname, "public", "favicon-32.png")),
};
ICONS["/favicon.ico"] = ICONS["/favicon-32.png"];

// ---------- admin ----------
let adminFails = [];
function adminAuthorized(req) {
  if (!ADMIN_PASSWORD) return false;
  adminFails = adminFails.filter((t) => Date.now() - t < 10 * 60_000);
  if (adminFails.length >= 10) return false; // too many wrong passwords: locked for a while
  const m = /^Basic (.+)$/.exec(req.headers.authorization || "");
  const pass = m ? Buffer.from(m[1], "base64").toString().split(":").slice(1).join(":") : "";
  const a = crypto.createHash("sha256").update(pass).digest();
  const b = crypto.createHash("sha256").update(ADMIN_PASSWORD).digest();
  const ok = crypto.timingSafeEqual(a, b);
  if (!ok && m) adminFails.push(Date.now());
  return ok;
}

function adminSnapshot() {
  const list = [...rooms.values()].map((r) => {
    const players = [...r.players.values()];
    const host = r.players.get(r.host);
    return {
      code: r.code,
      createdAt: r.createdAt,
      touched: r.touched,
      host: host ? { name: host.name, online: !!host.ws } : null,
      total: players.length,
      online: players.filter((p) => p.ws).length,
      ready: players.filter((p) => p.ready).length,
      banned: r.banned.size,
      countdown: r.countdown,
      seconds: r.seconds,
      players: players.map((p) => ({ name: p.name, ready: p.ready, online: !!p.ws, host: p.id === r.host, ping: p.ws ? p.ws.rtt ?? null : null })),
    };
  });
  list.sort((a, b) => b.touched - a.touched);
  return {
    now: Date.now(),
    startedAt: STARTED,
    connections: wss.clients.size,
    memoryMB: Math.round(process.memoryUsage().rss / 1048576),
    rooms: list,
  };
}

function handleAdmin(req, res) {
  if (!ADMIN_PASSWORD) {
    res.writeHead(404);
    return res.end("not found");
  }
  if (!adminAuthorized(req)) {
    res.writeHead(401, { "WWW-Authenticate": 'Basic realm="admin", charset="UTF-8"', "Content-Type": "text/plain; charset=utf-8" });
    return res.end("Нужен пароль");
  }
  const sec = { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer" };
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/admin/api/rooms" && req.method === "GET") {
    res.writeHead(200, { ...sec, "Content-Type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify(adminSnapshot()));
  }
  if (url.pathname === "/admin/api/close" && req.method === "POST") {
    const room = rooms.get(String(url.searchParams.get("code") || "").toUpperCase());
    if (room) closeRoom(room);
    res.writeHead(room ? 200 : 404, { ...sec, "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: !!room }));
  }
  res.writeHead(200, { ...sec, "Content-Type": "text/html; charset=utf-8" });
  res.end(ADMIN_HTML);
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") return res.end("ok");
  if (req.url === "/admin" || req.url.startsWith("/admin/") || req.url.startsWith("/admin?")) return handleAdmin(req, res);
  const icon = ICONS[req.url.split("?")[0]];
  if (icon) {
    res.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "public, max-age=86400" });
    return res.end(icon);
  }
  res.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
  });
  res.end(INDEX);
});

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });
const rooms = new Map(); // code -> room

const now = () => Date.now();
const rid = (n) => crypto.randomBytes(n).toString("hex");

function newCode() {
  const A = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  for (;;) {
    let c = "";
    for (let i = 0; i < 5; i++) c += A[crypto.randomInt(A.length)];
    if (!rooms.has(c)) return c;
  }
}

function cleanName(s) {
  s = String(s ?? "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, 24);
  return s; // empty means "no name" — callers reject it
}

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function publicState(room) {
  return {
    code: room.code,
    host: room.host,
    seconds: room.seconds,
    countdown: room.countdown, // {startAt, by, forced} | null
    players: [...room.players.values()].map((p) => ({
      id: p.id,
      name: p.name,
      ready: p.ready,
      online: !!p.ws,
      ping: p.ws ? p.ws.rtt ?? null : null,
    })),
  };
}

// Changes within BATCH_MS are merged into one update, so 100 people pressing
// "ready" at once cost one broadcast instead of a hundred.
const BATCH_MS = 50;
function broadcast(room) {
  room.touched = now();
  if (room.bcastTimer) return;
  room.bcastTimer = setTimeout(() => {
    room.bcastTimer = null;
    if (!rooms.has(room.code)) return;
    const st = JSON.stringify({ t: "state", room: publicState(room) });
    for (const p of room.players.values()) if (p.ws && p.ws.readyState === 1) p.ws.send(st);
  }, BATCH_MS);
}

// The creator is the host for the whole life of the room; the role never moves.
function removePlayer(room, id) {
  room.players.delete(id);
  if (room.players.size === 0) {
    rooms.delete(room.code);
    return;
  }
  broadcast(room);
}

function closeRoom(room) {
  clearTimeout(room.cdTimer);
  for (const p of room.players.values()) {
    clearTimeout(p.dropTimer);
    send(p.ws, { t: "closed" });
    if (p.ws) p.ws.ctx = null;
  }
  rooms.delete(room.code);
}

function joinRoom(ws, room, name, token) {
  if (room.banned.has(token)) return send(ws, { t: "error", msg: "Хост забанил тебя в этой комнате" });
  let p = [...room.players.values()].find((x) => x.token === token);
  if (p) {
    if (p.ws && p.ws !== ws) {
      send(p.ws, { t: "replaced" });
      p.ws.close();
    }
    clearTimeout(p.dropTimer);
    p.ws = ws;
    p.name = name;
  } else {
    if (room.players.size >= MAX_PLAYERS) return send(ws, { t: "error", msg: "Комната заполнена" });
    p = { id: rid(4), token, name, ready: false, ws, dropTimer: null };
    room.players.set(p.id, p);
    if (!room.host) room.host = p.id;
  }
  ws.ctx = { room, player: p };
  send(ws, { t: "joined", code: room.code, you: p.id });
  broadcast(room);
}

function finishCountdownLater(room) {
  clearTimeout(room.cdTimer);
  const ms = room.countdown.startAt - now() + 5000;
  room.cdTimer = setTimeout(() => {
    room.countdown = null;
    for (const p of room.players.values()) p.ready = false;
    broadcast(room);
  }, Math.max(ms, 0));
}

wss.on("connection", (ws, req) => {
  ws.ctx = null;
  ws.isAlive = true;
  ws.rtt = null;
  ws.missed = 0;
  ws.on("pong", () => {
    ws.missed = 0;
    if (!ws.pingSentAt) return;
    const rtt = Date.now() - ws.pingSentAt;
    ws.pingSentAt = 0;
    const old = ws.rtt;
    ws.rtt = old == null ? rtt : Math.round(old * 0.6 + rtt * 0.4); // smoothed
    // re-broadcast only when the shown value changes noticeably
    if (ws.ctx && (old == null || Math.abs(ws.rtt - old) >= Math.max(10, old * 0.25))) broadcast(ws.ctx.room);
  });

  // simple flood protection
  let bucket = 20;
  const refill = setInterval(() => (bucket = Math.min(20, bucket + 10)), 1000);

  ws.on("message", (raw) => {
    if (--bucket < 0) return;
    let m;
    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }
    if (!m || typeof m.t !== "string") return;

    if (m.t === "ping") return send(ws, { t: "pong", c: m.c, s: now() });

    const token = typeof m.token === "string" && /^[a-f0-9]{32}$/.test(m.token) ? m.token : null;

    const NO_NAME = { t: "error", msg: "Введи ник", field: "nick" };

    if (m.t === "create") {
      if (!token) return;
      if (!cleanName(m.name)) return send(ws, NO_NAME);
      if (rooms.size >= MAX_ROOMS) return send(ws, { t: "error", msg: "Сервер переполнен, попробуй позже" });
      const room = {
        code: newCode(),
        host: null,
        seconds: 10,
        countdown: null,
        cdTimer: null,
        players: new Map(),
        banned: new Set(),
        touched: now(),
        createdAt: now(),
      };
      rooms.set(room.code, room);
      return joinRoom(ws, room, cleanName(m.name), token);
    }

    if (m.t === "join") {
      if (!token) return;
      const room = rooms.get(String(m.code || "").toUpperCase().trim());
      if (!room) return send(ws, { t: "error", msg: "Комната не найдена" });
      if (!cleanName(m.name)) return send(ws, NO_NAME);
      return joinRoom(ws, room, cleanName(m.name), token);
    }

    const ctx = ws.ctx;
    if (!ctx || !rooms.has(ctx.room.code)) return;
    const { room, player } = ctx;
    const isHost = room.host === player.id;

    switch (m.t) {
      case "ready":
        if (room.countdown) return;
        player.ready = !!m.v;
        return broadcast(room);

      case "rename":
        if (!cleanName(m.name)) return;
        player.name = cleanName(m.name);
        return broadcast(room);

      case "seconds":
        if (!isHost || room.countdown || !COUNTDOWNS.includes(m.v)) return;
        room.seconds = m.v;
        return broadcast(room);

      case "start": {
        if (!isHost || room.countdown) return;
        const allReady = [...room.players.values()].every((p) => p.ready);
        if (!allReady && !m.force) return send(ws, { t: "error", msg: "Не все готовы" });
        room.countdown = { startAt: now() + room.seconds * 1000 + 300, by: player.name, forced: !allReady };
        finishCountdownLater(room);
        return broadcast(room);
      }

      case "cancel":
        if (!isHost || !room.countdown) return;
        clearTimeout(room.cdTimer);
        room.countdown = null;
        return broadcast(room);

      case "kick": {
        if (!isHost || m.id === player.id) return;
        const target = room.players.get(m.id);
        if (!target) return;
        if (m.ban) room.banned.add(target.token);
        clearTimeout(target.dropTimer);
        send(target.ws, { t: "kicked", ban: !!m.ban });
        if (target.ws) {
          target.ws.ctx = null;
          target.ws.close();
        }
        return removePlayer(room, target.id);
      }

      case "leave":
        ws.ctx = null;
        clearTimeout(player.dropTimer);
        if (isHost) return closeRoom(room);
        return removePlayer(room, player.id);
    }
  });

  ws.on("close", () => {
    clearInterval(refill);
    const ctx = ws.ctx;
    if (!ctx) return;
    const { room, player } = ctx;
    if (player.ws !== ws) return;
    player.ws = null;
    broadcast(room);
    // the host stays in the room (offline) until they come back; others drop after the grace period
    if (room.host === player.id) return;
    player.dropTimer = setTimeout(() => {
      if (!player.ws && room.players.get(player.id) === player) removePlayer(room, player.id);
    }, GRACE_MS);
  });
});

// ping every client (measures latency); drop connections silent for 3 pings
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.pingSentAt && ++ws.missed >= 3) {
      ws.terminate();
      continue;
    }
    if (!ws.pingSentAt) ws.pingSentAt = Date.now();
    ws.ping();
  }
}, PING_EVERY_MS);

// idle room cleanup
setInterval(() => {
  for (const room of rooms.values()) {
    if (now() - room.touched > ROOM_IDLE_MS) rooms.delete(room.code);
  }
}, 60_000);

// ---------- save / restore rooms across restarts ----------
function saveRooms() {
  if (!DATA_FILE) return;
  const data = {
    savedAt: now(),
    rooms: [...rooms.values()].map((r) => ({
      code: r.code,
      host: r.host,
      seconds: r.seconds,
      countdown: r.countdown,
      createdAt: r.createdAt,
      touched: r.touched,
      banned: [...r.banned],
      players: [...r.players.values()].map((p) => ({ id: p.id, token: p.token, name: p.name, ready: p.ready })),
    })),
  };
  const tmp = DATA_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(data));
  fs.renameSync(tmp, DATA_FILE);
  return data.rooms.length;
}

function restoreRooms() {
  if (!DATA_FILE || !fs.existsSync(DATA_FILE)) return 0;
  let data;
  try {
    data = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch (e) {
    console.error("cannot read saved rooms:", e.message);
    return 0;
  }
  for (const r of data.rooms || []) {
    if (now() - (r.touched || 0) > ROOM_IDLE_MS) continue;
    const room = {
      code: r.code,
      host: r.host,
      seconds: COUNTDOWNS.includes(r.seconds) ? r.seconds : 10,
      countdown: r.countdown && r.countdown.startAt > now() ? r.countdown : null,
      cdTimer: null,
      players: new Map(),
      banned: new Set(r.banned || []),
      touched: r.touched || now(),
      createdAt: r.createdAt || now(),
    };
    for (const p of r.players || []) {
      const player = { id: p.id, token: p.token, name: p.name, ready: !!p.ready, ws: null, dropTimer: null };
      room.players.set(player.id, player);
      // everyone starts offline; regular players get the usual grace period to reconnect
      if (player.id !== room.host) {
        player.dropTimer = setTimeout(() => {
          if (!player.ws && room.players.get(player.id) === player) removePlayer(room, player.id);
        }, GRACE_MS);
      }
    }
    if (!room.players.size) continue;
    rooms.set(room.code, room);
    if (room.countdown) finishCountdownLater(room);
  }
  fs.renameSync(DATA_FILE, DATA_FILE + ".restored");
  return rooms.size;
}

function shutdown(sig) {
  try {
    const n = saveRooms();
    if (n != null) console.log(`${sig}: saved ${n} room(s)`);
  } catch (e) {
    console.error("save failed:", e.message);
  }
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

console.log(`restored ${restoreRooms()} room(s)`);

server.listen(PORT, () => console.log(`Sync Start listening on :${PORT}`));
