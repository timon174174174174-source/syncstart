// Sync Start — synchronized countdown rooms for co-op games.
// Rooms live in memory; they disappear when the server restarts.
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const MAX_ROOMS = 500;
const MAX_PLAYERS = 16;
const GRACE_MS = Number(process.env.GRACE_MS) || 30_000; // keep a disconnected player this long
const ROOM_IDLE_MS = 6 * 3600_000;
const COUNTDOWNS = [3, 5, 10, 15];

const INDEX = fs.readFileSync(path.join(__dirname, "public", "index.html"));
const ICONS = {
  "/icon.png": fs.readFileSync(path.join(__dirname, "public", "icon.png")),
  "/favicon-32.png": fs.readFileSync(path.join(__dirname, "public", "favicon-32.png")),
};
ICONS["/favicon.ico"] = ICONS["/favicon-32.png"];

const server = http.createServer((req, res) => {
  if (req.url === "/health") return res.end("ok");
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
  return s || "Игрок";
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
    })),
  };
}

function broadcast(room) {
  room.touched = now();
  const st = { t: "state", room: publicState(room) };
  for (const p of room.players.values()) send(p.ws, st);
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
  ws.on("pong", () => (ws.isAlive = true));

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

    if (m.t === "create") {
      if (!token) return;
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
      };
      rooms.set(room.code, room);
      return joinRoom(ws, room, cleanName(m.name), token);
    }

    if (m.t === "join") {
      if (!token) return;
      const room = rooms.get(String(m.code || "").toUpperCase().trim());
      if (!room) return send(ws, { t: "error", msg: "Комната не найдена" });
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

// heartbeat + idle room cleanup
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
  for (const room of rooms.values()) {
    if (now() - room.touched > ROOM_IDLE_MS) rooms.delete(room.code);
  }
}, 15_000);

server.listen(PORT, () => console.log(`Sync Start listening on :${PORT}`));
