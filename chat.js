"use strict";
// Stream chat aggregator: watches a YouTube channel and a Twitch channel, and while either is live
// reads its chat and fans the messages out to browsers over Server-Sent Events (/api/chat).
// No API keys: YouTube via its public web endpoints, Twitch via anonymous IRC.
const WebSocket = require("ws");

const YT_CHANNEL = process.env.YT_CHANNEL ?? "UCFyXybb8JwZlBsbl9faj_aQ"; // empty = off
const TWITCH_CHANNEL = (process.env.TWITCH_CHANNEL ?? "muzerere").toLowerCase().replace(/[^a-z0-9_]/g, ""); // empty = off
const CHECK_MS = 60_000;        // live check while someone has the site open
const IDLE_CHECK_MS = 600_000;  // and while nobody does
const BACKLOG = 60;             // messages a newly opened page gets
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36";
const YT_HEADERS = { "User-Agent": UA, "Accept-Language": "en", Cookie: "SOCS=CAI" }; // SOCS skips the EU consent page
const TWITCH_GQL_CLIENT = "kimne78kx3ncx6brgo4mv6wki5h1ko"; // Twitch's public web client id

const state = { youtube: null, twitch: false }; // youtube: videoId | null, twitch: bool
const clients = new Set(); // SSE responses
const backlog = [];
let lastAsked = 0;
let lastCheck = 0;

const log = (...a) => console.log("[chat]", ...a);

function publicState() {
  return { youtube: state.youtube, twitch: state.twitch ? TWITCH_CHANNEL : null };
}

function emit(event, data) {
  const chunk = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) res.write(chunk);
}

function push(msg) {
  backlog.push(msg);
  if (backlog.length > BACKLOG) backlog.shift();
  pending.push(msg);
  if (!flushTimer) flushTimer = setTimeout(flush, 250); // batch bursts into one write per client
}
let pending = [];
let flushTimer = null;
function flush() {
  flushTimer = null;
  if (pending.length) emit("msgs", pending);
  pending = [];
}

function setLive(key, value) {
  if (state[key] === value) return;
  state[key] = value;
  log(key, value ? `live (${value === true ? TWITCH_CHANNEL : value})` : "offline");
  emit("live", publicState());
  if (key === "youtube") value ? ytStart(value) : ytStop();
  if (key === "twitch") value ? twStart() : twStop();
}

// ---------------- live checks ----------------
async function checkYoutube() {
  const r = await fetch(`https://www.youtube.com/channel/${encodeURIComponent(YT_CHANNEL)}/live`, {
    headers: YT_HEADERS, signal: AbortSignal.timeout(15_000),
  });
  const html = await r.text();
  const m = html.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/watch\?v=([\w-]{11})"/);
  setLive("youtube", m && /"isLive":true/.test(html) ? m[1] : null);
}

async function checkTwitch() {
  const r = await fetch("https://gql.twitch.tv/gql", {
    method: "POST",
    headers: { "Client-Id": TWITCH_GQL_CLIENT, "Content-Type": "application/json", "User-Agent": UA },
    body: JSON.stringify({ query: `query{user(login:${JSON.stringify(TWITCH_CHANNEL)}){stream{id}}}` }),
    signal: AbortSignal.timeout(15_000),
  });
  const j = await r.json();
  if (!j || !j.data) throw new Error("unexpected twitch answer");
  setLive("twitch", !!(j.data.user && j.data.user.stream));
}

let checking = false;
async function checkAll() {
  if (checking) return;
  checking = true;
  lastCheck = Date.now();
  const jobs = [];
  if (YT_CHANNEL) jobs.push(checkYoutube().catch((e) => log("youtube check failed:", e.message)));
  if (TWITCH_CHANNEL) jobs.push(checkTwitch().catch((e) => log("twitch check failed:", e.message)));
  await Promise.all(jobs);
  checking = false;
}

// ---------------- YouTube chat ----------------
let yt = null; // { videoId, key, version, continuation, timer, seen }

function ytRuns(runs) {
  const parts = [];
  for (const r of runs || []) {
    if (r.text != null) parts.push({ t: r.text });
    else if (r.emoji) {
      const e = r.emoji;
      const thumbs = (e.image && e.image.thumbnails) || [];
      const url = thumbs.length ? thumbs[thumbs.length - 1].url : "";
      // standard emoji have a unicode id; custom channel emoji are images
      if (!e.isCustomEmoji && e.emojiId && !/^UC/.test(e.emojiId)) parts.push({ t: e.emojiId });
      else if (/^https:\/\/[\w.-]+\.(ggpht|googleusercontent|ytimg)\.com\//.test(url))
        parts.push({ img: url, alt: (e.shortcuts && e.shortcuts[0]) || "" });
      else parts.push({ t: (e.shortcuts && e.shortcuts[0]) || "" });
    }
  }
  return parts;
}

function ytActions(actions) {
  for (const a of actions || []) {
    const item = a.addChatItemAction && a.addChatItemAction.item;
    if (!item) continue;
    const r = item.liveChatTextMessageRenderer || item.liveChatPaidMessageRenderer;
    if (!r || yt.seen.has(r.id)) continue;
    yt.seen.add(r.id);
    if (yt.seen.size > 2000) yt.seen = new Set([...yt.seen].slice(-500));
    const msg = {
      id: "y" + r.id, src: "yt",
      name: (r.authorName && r.authorName.simpleText) || "?",
      parts: ytRuns(r.message && r.message.runs),
    };
    if (item.liveChatPaidMessageRenderer && r.purchaseAmountText) msg.paid = r.purchaseAmountText.simpleText;
    push(msg);
  }
}

async function ytStart(videoId) {
  ytStop();
  const me = (yt = { videoId, seen: new Set(), timer: null, fails: 0 });
  try {
    const r = await fetch(`https://www.youtube.com/live_chat?v=${videoId}`, { headers: YT_HEADERS, signal: AbortSignal.timeout(15_000) });
    const html = await r.text();
    if (yt !== me) return;
    me.key = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [])[1];
    me.version = (html.match(/"INNERTUBE_CLIENT_VERSION":"([^"]+)"/) || [])[1];
    me.continuation = (html.match(/"continuation":"([^"]+)"/) || [])[1];
    if (!me.key || !me.version || !me.continuation) throw new Error("chat page format changed");
    const init = html.match(/(?:window\["ytInitialData"\]|var ytInitialData)\s*=\s*(\{.+?\});\s*<\/script>/s);
    if (init) {
      try {
        const d = JSON.parse(init[1]);
        const lc = d.contents && d.contents.liveChatRenderer;
        ytActions(lc && lc.actions);
      } catch {}
    }
    log("youtube chat connected");
    ytPoll(me, 1000);
  } catch (e) {
    log("youtube chat failed:", e.message);
    if (yt === me) me.timer = setTimeout(() => yt === me && ytStart(videoId), 30_000);
  }
}

function ytPoll(me, delay) {
  me.timer = setTimeout(async () => {
    if (yt !== me) return;
    try {
      const r = await fetch(`https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?key=${me.key}&prettyPrint=false`, {
        method: "POST",
        headers: { ...YT_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ context: { client: { clientName: "WEB", clientVersion: me.version, hl: "en" } }, continuation: me.continuation }),
        signal: AbortSignal.timeout(15_000),
      });
      const j = await r.json();
      if (yt !== me) return;
      const c = j.continuationContents && j.continuationContents.liveChatContinuation;
      if (!c) throw new Error("no continuation (stream over?)");
      ytActions(c.actions);
      const next = (c.continuations || [])[0] || {};
      const cd = next.invalidationContinuationData || next.timedContinuationData || next.reloadContinuationData;
      if (!cd || !cd.continuation) throw new Error("chat ended");
      me.continuation = cd.continuation;
      me.fails = 0;
      ytPoll(me, Math.min(Math.max(cd.timeoutMs || 2000, 1000), 8000));
    } catch (e) {
      if (yt !== me) return;
      if (++me.fails >= 5) { log("youtube chat lost:", e.message); return ytStart(me.videoId); }
      ytPoll(me, 5000);
    }
  }, delay);
}

function ytStop() {
  if (yt) clearTimeout(yt.timer);
  yt = null;
}

// ---------------- Twitch chat (anonymous IRC over WebSocket) ----------------
let tw = null;

function twEmotes(text, tag) {
  // tag: "25:0-4,12-16/1902:6-10"
  if (!tag) return [{ t: text }];
  const chars = Array.from(text); // twitch positions count code points
  const spans = [];
  for (const group of tag.split("/")) {
    const [id, ranges] = group.split(":");
    if (!/^[\w-]+$/.test(id) || !ranges) continue;
    for (const rg of ranges.split(",")) {
      const [a, b] = rg.split("-").map(Number);
      if (Number.isInteger(a) && Number.isInteger(b) && b >= a && b < chars.length) spans.push([a, b, id]);
    }
  }
  spans.sort((x, y) => x[0] - y[0]);
  const parts = [];
  let pos = 0;
  for (const [a, b, id] of spans) {
    if (a < pos) continue;
    if (a > pos) parts.push({ t: chars.slice(pos, a).join("") });
    parts.push({ img: `https://static-cdn.jtvnw.net/emoticons/v2/${id}/default/dark/1.0`, alt: chars.slice(a, b + 1).join("") });
    pos = b + 1;
  }
  if (pos < chars.length) parts.push({ t: chars.slice(pos).join("") });
  return parts;
}

function twParse(line) {
  // @tags :nick!nick@nick.tmi.twitch.tv PRIVMSG #chan :text
  let tags = {};
  if (line[0] === "@") {
    const sp = line.indexOf(" ");
    for (const kv of line.slice(1, sp).split(";")) {
      const i = kv.indexOf("=");
      tags[kv.slice(0, i)] = kv.slice(i + 1);
    }
    line = line.slice(sp + 1);
  }
  const m = line.match(/^:(\w+)!\S+ PRIVMSG #\w+ :(.*)$/);
  if (!m) return null;
  let text = m[2];
  const action = /^\x01ACTION (.*)\x01$/.exec(text);
  if (action) text = action[1];
  const color = /^#[0-9a-fA-F]{6}$/.test(tags.color || "") ? tags.color : null;
  return {
    id: "t" + (tags.id || Math.random().toString(36).slice(2)), src: "tw",
    name: (tags["display-name"] || m[1]).replace(/\\s/g, " "),
    color, parts: twEmotes(text, tags.emotes),
  };
}

function twStart() {
  twStop();
  const ws = new WebSocket("wss://irc-ws.chat.twitch.tv:443");
  const me = (tw = { ws, retry: null });
  ws.on("open", () => {
    ws.send("CAP REQ :twitch.tv/tags");
    ws.send("PASS SCHMOOPIIE");
    ws.send("NICK justinfan" + (10000 + Math.floor(Math.random() * 80000)));
    ws.send("JOIN #" + TWITCH_CHANNEL);
    log("twitch chat connected");
  });
  ws.on("message", (buf) => {
    for (const line of buf.toString().split("\r\n")) {
      if (!line) continue;
      if (line.startsWith("PING")) { ws.send(line.replace("PING", "PONG")); continue; }
      const msg = twParse(line);
      if (msg) push(msg);
    }
  });
  ws.on("error", () => {});
  ws.on("close", () => {
    if (tw !== me) return;
    log("twitch chat disconnected, retrying");
    me.retry = setTimeout(() => tw === me && twStart(), 10_000);
  });
}

function twStop() {
  if (!tw) return;
  const old = tw;
  tw = null;
  clearTimeout(old.retry);
  try { old.ws.close(); } catch {}
}

// ---------------- HTTP ----------------
function handle(req, res) {
  lastAsked = Date.now();
  if (Date.now() - lastCheck > CHECK_MS) checkAll(); // first visitor after a quiet spell gets a fresh check
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",
    Connection: "keep-alive",
  });
  res.write("retry: 5000\n\n");
  res.write(`event: live\ndata: ${JSON.stringify(publicState())}\n\n`);
  if (backlog.length) res.write(`event: msgs\ndata: ${JSON.stringify(backlog)}\n\n`);
  clients.add(res);
  const ka = setInterval(() => res.write(": ka\n\n"), 25_000);
  req.on("close", () => { clients.delete(res); clearInterval(ka); });
}

function start() {
  if (!YT_CHANNEL && !TWITCH_CHANNEL) return;
  checkAll();
  setInterval(() => {
    lastAsked = clients.size ? Date.now() : lastAsked;
    const busy = Date.now() - lastAsked < 5 * 60_000;
    if (busy || Date.now() - lastCheck >= IDLE_CHECK_MS) checkAll();
  }, CHECK_MS).unref();
}

module.exports = { start, handle, publicState, clientCount: () => clients.size };
