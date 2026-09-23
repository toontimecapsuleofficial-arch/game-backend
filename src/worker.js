// Memory Matrix — Cloudflare Worker + Durable Object
// Automatic matchmaking + realtime multiplayer.
// Match IDs are internal protocol identifiers only; there is no room-code UI.

const MAX_PLAYERS_PER_MATCH = 2;
const TOTAL_ROUNDS = 3;
const RECONNECT_GRACE = 15_000;
const ROUND_TIME = 15_000;
const NEXT_ROUND_DELAY = 5_000;
const FINAL_RESULT_DELAY = 1_500;

const DEFAULT_NICK = "Player";
const DEFAULT_CC = "XX";
const NICK_MAX_LEN = 16;
const CC_REGEX = /^[A-Za-z]{2}$/;
const MP_AVATARS = [
  "brain", "cube", "robot", "fox", "penguin", "bolt", "owl",
  "cat", "dragon", "astro", "ninja"
];

function sanitizeNick(raw) {
  if (typeof raw !== "string") return DEFAULT_NICK;
  let value = raw.replace(/[^\p{L}\p{N}\s_\-.]/gu, "").trim();
  if (!value) return DEFAULT_NICK;
  return value.slice(0, NICK_MAX_LEN);
}

function sanitizeCC(raw) {
  if (typeof raw !== "string") return DEFAULT_CC;
  const value = raw.trim().toUpperCase();
  return CC_REGEX.test(value) ? value : DEFAULT_CC;
}

function validMessage(msg) {
  return msg && typeof msg === "object" && !Array.isArray(msg) &&
    typeof msg.t === "string" && msg.t.length <= 32;
}

function validRoundNumber(value) {
  return Number.isInteger(Number(value)) && Number(value) >= 1 && Number(value) <= TOTAL_ROUNDS;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      const stub = env.MATCHMAKER.get(env.MATCHMAKER.idFromName("global"));
      const response = await stub.fetch(new Request("https://internal/status"));
      return new Response(await response.text(), {
        status: response.status,
        headers: { "content-type": "application/json", "access-control-allow-origin": "*" }
      });
    }
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("WebSocket upgrade required", { status: 426 });
      }
      const stub = env.MATCHMAKER.get(env.MATCHMAKER.idFromName("global"));
      return stub.fetch(request);
    }
    return new Response("Memory Matrix Multiplayer Backend", {
      status: 200,
      headers: { "content-type": "text/plain" }
    });
  }
};

export class MatchMakerDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.queue = [];
    this.players = new Map();
    this.matches = new Map();
    this.loaded = false;
  }

  async load() {
    if (this.loaded) return;
    this.loaded = true;
    const data = await this.state.storage.get("state");
    if (!data) return;
    this.queue = Array.isArray(data.queue) ? data.queue : [];
    this.matches = new Map(Array.isArray(data.matches) ? data.matches : []);
    for (const match of this.matches.values()) {
      if (!(match.disconnected instanceof Map)) match.disconnected = new Map();
      // Backward-compatible defaults for matches persisted by older versions.
      match.nicks ||= [DEFAULT_NICK, DEFAULT_NICK];
      match.ccs ||= [DEFAULT_CC, DEFAULT_CC];
      match.avatars ||= ["brain", "brain"];
      match.round ||= 0;
      match.nextRoundAt ??= null;
    }
  }

  async save() {
    await this.state.storage.put("state", {
      queue: this.queue,
      matches: [...this.matches.entries()]
    });
  }

  async fetch(request) {
    await this.load();
    const url = new URL(request.url);
    if (url.pathname === "/status") {
      return new Response(JSON.stringify({
        ok: true, queue: this.queue.length, rooms: this.matches.size, players: this.players.size
      }), { headers: { "content-type": "application/json" } });
    }
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket endpoint", { status: 200 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const socket = pair[1];
    socket.accept();
    const player = {
      id: crypto.randomUUID(), ws: socket, avatar: "brain", nick: DEFAULT_NICK,
      cc: DEFAULT_CC, matchId: null, seat: null, connected: true, joinedAt: Date.now()
    };
    this.players.set(player.id, player);

    socket.addEventListener("message", async event => {
      try {
        const msg = typeof event.data === "string" ? JSON.parse(event.data) : event.data;
        if (!validMessage(msg)) throw new Error("invalid message");
        await this.handleMessage(player, msg);
      } catch {
        this.send(player, { t: "error", code: "BAD_MESSAGE", message: "Invalid message" });
      }
    });
    const disconnect = async () => {
      if (!player.connected) return;
      player.connected = false;
      await this.handleDisconnect(player);
    };
    socket.addEventListener("close", disconnect);
    socket.addEventListener("error", disconnect);
    this.send(player, { t: "ready", pid: player.id });
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleMessage(player, msg) {
    switch (msg.t) {
      case "queue": return this.joinQueue(player, msg);
      case "leave_queue": return this.leaveQueue(player);
      case "ping":
        this.send(player, { t: "pong", now: Date.now(), echo: msg.now ?? null });
        return;
      case "tap": return this.handleTap(player, msg);
      case "round_done": return this.handleRoundDone(player, msg);
      case "resume": return this.handleResume(player, msg);
      case "leave": return this.leaveMatch(player);
      default: return;
    }
  }

  async joinQueue(player, msg) {
    if (this.queue.includes(player.id)) return;
    if (player.matchId) {
      this.send(player, { t: "error", code: "ALREADY_IN_MATCH" });
      return;
    }
    player.avatar = MP_AVATARS.includes(msg.av) ? msg.av : "brain";
    player.nick = sanitizeNick(msg.nick);
    player.cc = sanitizeCC(msg.cc);
    player.joinedAt = Date.now();
    this.queue.push(player.id);
    this.send(player, { t: "queued", pos: this.queue.length });
    await this.tryMatch();
    await this.save();
  }

  async leaveQueue(player) {
    this.queue = this.queue.filter(id => id !== player.id);
    this.send(player, { t: "queue_left" });
    await this.save();
  }

  async tryMatch() {
    this.queue = this.queue.filter(id => {
      const p = this.players.get(id);
      return p && p.connected && !p.matchId;
    });
    while (this.queue.length >= MAX_PLAYERS_PER_MATCH) {
      const a = this.players.get(this.queue.shift());
      const b = this.players.get(this.queue.shift());
      if (!a || !b || !a.connected || !b.connected) continue;
      await this.createMatch(a, b);
    }
  }

  profile(player) {
    return { nick: player.nick, cc: player.cc, av: player.avatar };
  }

  async createMatch(a, b) {
    const matchId = crypto.randomUUID();
    const match = {
      id: matchId, seed: randomSeed(), players: [a.id, b.id],
      avatars: [a.avatar, b.avatar], nicks: [a.nick, b.nick], ccs: [a.cc, b.cc],
      scores: [0, 0], rounds: [], round: 0, state: "matched", createdAt: Date.now(),
      roundState: null, nextRoundAt: null, disconnected: new Map()
    };
    this.matches.set(matchId, match);
    a.matchId = b.matchId = matchId;
    a.seat = 0; b.seat = 1;
    this.send(a, { t: "matched", m: matchId, you: 0, opp: this.profile(b), rounds: TOTAL_ROUNDS, now: Date.now() });
    this.send(b, { t: "matched", m: matchId, you: 1, opp: this.profile(a), rounds: TOTAL_ROUNDS, now: Date.now() });
    setTimeout(() => {
      const m = this.matches.get(matchId);
      if (!m || m.state !== "matched") return;
      m.state = "countdown";
      this.broadcast(m, { t: "count", n: 3 });
      setTimeout(() => this.startRound(m), 3000);
    }, 200);
    await this.save();
  }

  async startRound(match) {
    if (!match || match.round >= TOTAL_ROUNDS || !["countdown", "next_round"].includes(match.state)) return;
    const round = match.round + 1;
    if (round > TOTAL_ROUNDS) return;
    const startedAt = Date.now();
    const deadline = startedAt + ROUND_TIME;
    const spec = createRoundSpec(round);
    const seed = mixSeed(match.seed, round);
    match.round = round;
    match.state = "round";
    match.nextRoundAt = null;
    match.roundState = {
      round, spec, seed, startedAt, deadline,
      roundStartAt: startedAt, roundEndAt: deadline,
      taps: [new Set(), new Set()], hits: [0, 0], misses: [0, 0], done: [false, false]
    };
    this.broadcast(match, {
      t: "round", r: round, spec, seed, reveal: spec.reveal, recall: spec.recall,
      deadline: ROUND_TIME, startedAt, endsAt: deadline, roundStartAt: startedAt,
      roundEndAt: deadline, now: Date.now()
    });
    setTimeout(() => this.finishRound(match.id), ROUND_TIME + 100);
    await this.save();
  }

  async handleTap(player, msg) {
    const match = player.matchId && this.matches.get(player.matchId);
    const rs = match?.roundState;
    if (!match || !rs || match.state !== "round" || !Number.isInteger(player.seat)) return;
    if (!validRoundNumber(msg.r) || Number(msg.r) !== rs.round || Date.now() > rs.roundEndAt) {
      this.send(player, { t: "fix", r: rs.round, i: msg.i });
      return;
    }
    const index = Number(msg.i);
    const size = rs.spec.size;
    if (!Number.isInteger(index) || index < 0 || index >= size * size) {
      this.send(player, { t: "fix", r: rs.round, i: index });
      return;
    }
    const seat = player.seat;
    if (rs.done[seat] || rs.taps[seat].has(index)) return;
    rs.taps[seat].add(index);
    const hit = deriveBoard(rs.spec, rs.seed).required.has(index);
    if (hit) rs.hits[seat]++; else rs.misses[seat]++;
    this.broadcastExcept(match, player.id, { t: "opp", r: rs.round, i: index, k: hit ? "ok" : "miss", s: rs.hits[seat] + rs.misses[seat] });
    this.send(player, { t: "fix", r: rs.round, i: index, k: hit ? "ok" : "miss" });
    if (rs.hits[seat] >= deriveBoard(rs.spec, rs.seed).required.size) {
      rs.done[seat] = true;
      await this.finishRound(match.id);
    }
  }

  async handleRoundDone(player, msg) {
    const match = player.matchId && this.matches.get(player.matchId);
    const rs = match?.roundState;
    if (!match || !rs || match.state !== "round" || !Number.isInteger(player.seat)) return;
    if (!validRoundNumber(msg.r) || Number(msg.r) !== rs.round || Date.now() > rs.roundEndAt || rs.done[player.seat]) return;
    if (rs.hits[player.seat] < deriveBoard(rs.spec, rs.seed).required.size) return;
    rs.done[player.seat] = true;
    await this.finishRound(match.id);
  }

  async finishRound(matchId) {
    const match = this.matches.get(matchId);
    const rs = match?.roundState;
    if (!match || !rs || match.state !== "round") return;
    match.state = "round_end";
    const score = [calculateScore(rs.hits[0], rs.misses[0]), calculateScore(rs.hits[1], rs.misses[1])];
    match.scores[0] += score[0];
    match.scores[1] += score[1];
    match.rounds.push({ r: rs.round, score, hits: [...rs.hits], misses: [...rs.misses] });
    const reason = seat => rs.done[seat] ? "completed" : (rs.hits[seat] === 0 && rs.misses[seat] === 0 ? "no_attempt" : "timeout");
    const winner = score[0] === score[1] ? null : (score[0] > score[1] ? 0 : 1);
    for (let seat = 0; seat < 2; seat++) {
      const p = this.players.get(match.players[seat]);
      if (!p) continue;
      this.send(p, { t: "round_end", p: {
        r: rs.round, tot: [...match.scores], you: { score: score[seat], reason: reason(seat) },
        opp: { score: score[1 - seat], reason: reason(1 - seat) },
        win: winner === null ? "draw" : winner === seat ? "you" : "opp", final: rs.round === TOTAL_ROUNDS, now: Date.now()
      }});
    }
    if (rs.round === TOTAL_ROUNDS) {
      match.state = "final";
      await this.save();
      setTimeout(() => this.finishMatch(match.id), FINAL_RESULT_DELAY);
      return;
    }
    match.state = "next_round";
    const nextRoundAt = Date.now() + NEXT_ROUND_DELAY;
    match.nextRoundAt = nextRoundAt;
    this.broadcast(match, {
      t: "next_round", r: rs.round, next: rs.round + 1, rounds: TOTAL_ROUNDS,
      score: [...score], tot: [...match.scores],
      win: [winner === null ? "draw" : winner === 0 ? "you" : "opp", winner === null ? "draw" : winner === 1 ? "you" : "opp"],
      nextRoundAt, now: Date.now(), players: match.players.map((id, i) => ({
        id, nick: match.nicks[i], cc: match.ccs[i], av: match.avatars[i], connected: !!this.players.get(id)?.connected,
        score: score[i], total: match.scores[i]
      }))
    });
    setTimeout(() => {
      const current = this.matches.get(match.id);
      if (current && current.state === "next_round" && current.nextRoundAt === nextRoundAt) this.startRound(current);
    }, NEXT_ROUND_DELAY);
    await this.save();
  }

  async finishMatch(matchId) {
    const match = this.matches.get(matchId);
    if (!match || match.state === "ended") return;
    match.state = "ended";
    const winner = match.scores[0] === match.scores[1] ? null : (match.scores[0] > match.scores[1] ? 0 : 1);
    for (let seat = 0; seat < 2; seat++) {
      const p = this.players.get(match.players[seat]);
      if (p) this.send(p, { t: "match_end", p: { win: winner === null ? "draw" : winner === seat ? "you" : "opp", tot: [...match.scores], rounds: match.rounds, reason: "completed", now: Date.now() } });
    }
    await this.save();
    setTimeout(async () => {
      const m = this.matches.get(matchId);
      if (!m) return;
      for (const id of m.players) {
        const p = this.players.get(id);
        if (p && p.matchId === matchId) { p.matchId = null; p.seat = null; }
      }
      this.matches.delete(matchId);
      await this.save();
    }, 30_000);
  }

  async handleDisconnect(player) {
    this.queue = this.queue.filter(id => id !== player.id);
    if (!player.matchId) {
      this.players.delete(player.id);
      await this.save();
      return;
    }
    const match = this.matches.get(player.matchId);
    if (!match) { this.players.delete(player.id); await this.save(); return; }
    const seat = player.seat;
    match.disconnected.set(seat, Date.now());
    const opponent = this.players.get(match.players[1 - seat]);
    if (opponent) this.send(opponent, { t: "opp_left" });
    await this.save();
    setTimeout(async () => {
      const m = this.matches.get(match.id);
      if (!m || !m.disconnected.has(seat)) return;
      const current = this.players.get(player.id);
      if (current?.connected) return;
      const other = this.players.get(m.players[1 - seat]);
      if (other) {
        this.send(other, { t: "opp_left", final: true });
        this.send(other, { t: "match_end", p: { win: "you", tot: [...m.scores], rounds: m.rounds, reason: "opponent_left" } });
      }
      m.state = "ended";
      this.matches.delete(m.id);
      this.players.delete(player.id);
      await this.save();
    }, RECONNECT_GRACE);
  }

  async handleResume(player, msg) {
    const matchId = typeof msg.m === "string" ? msg.m : "";
    const pid = typeof msg.pid === "string" ? msg.pid : "";
    const match = this.matches.get(matchId);
    const oldPlayer = this.players.get(pid);
    if (!match || !oldPlayer || !match.players.includes(pid)) {
      this.send(player, { t: "resume_fail" });
      return;
    }
    const seat = match.players.indexOf(pid);
    oldPlayer.ws = player.ws;
    oldPlayer.connected = true;
    oldPlayer.matchId = match.id;
    oldPlayer.seat = seat;
    this.players.delete(player.id);
    match.disconnected.delete(seat);
    const rs = match.roundState;
    this.send(oldPlayer, { t: "resume_ok", m: match.id, you: seat,
      opp: { nick: match.nicks[1 - seat], cc: match.ccs[1 - seat], av: match.avatars[1 - seat] },
      rounds: TOTAL_ROUNDS, tot: [...match.scores], r: match.round, st: match.state,
      nextRoundAt: match.state === "next_round" ? match.nextRoundAt : null, now: Date.now(),
      rs: rs ? { r: rs.round, spec: rs.spec, seed: rs.seed, reveal: rs.spec.reveal, recall: rs.spec.recall,
        startedAt: rs.startedAt, endsAt: rs.deadline, roundStartAt: rs.roundStartAt, roundEndAt: rs.roundEndAt,
        yourTaps: [...rs.taps[seat]], yourHits: rs.hits[seat], yourMisses: rs.misses[seat], yourDone: rs.done[seat] } : null
    });
    const opponent = this.players.get(match.players[1 - seat]);
    if (opponent) this.send(opponent, { t: "opp_back" });
    await this.save();
  }

  async leaveMatch(player) {
    const match = player.matchId && this.matches.get(player.matchId);
    if (!match) return;
    const other = this.players.get(match.players[1 - player.seat]);
    if (other) this.send(other, { t: "match_end", p: { win: "you", tot: [...match.scores], rounds: match.rounds, reason: "opponent_left" } });
    match.state = "ended";
    this.matches.delete(match.id);
    player.matchId = null;
    player.seat = null;
    await this.save();
  }

  send(player, data) {
    if (!player?.ws || !player.connected) return;
    try { player.ws.send(JSON.stringify(data)); } catch { /* socket is closing */ }
  }

  broadcast(match, data) {
    for (const id of match.players) this.send(this.players.get(id), data);
  }

  broadcastExcept(match, exceptId, data) {
    for (const id of match.players) if (id !== exceptId) this.send(this.players.get(id), data);
  }
}

function randomSeed() {
  return Math.floor(Math.random() * 0x7fffffff);
}

function mixSeed(a, b) {
  let x = (a ^ (b * 0x45d9f3b)) >>> 0;
  x = Math.imul(x ^ (x >>> 16), 0x45d9f3b) >>> 0;
  return (x ^ (x >>> 16)) >>> 0;
}

function rng(seed) {
  let x = seed >>> 0;
  return () => {
    x = Math.imul(x ^ (x >>> 16), 0x45d9f3b) >>> 0;
    x ^= x >>> 16;
    return (x >>> 0) / 4294967296;
  };
}

function createRoundSpec(round) {
  const size = Math.min(3 + round, 7);
  return { size, reveal: Math.max(900, 1800 - round * 120), recall: Math.max(2500, 6000 - round * 400), count: Math.min(2 + round, Math.floor(size * size * 0.45)), round };
}

function deriveBoard(spec, seed) {
  const indexes = Array.from({ length: spec.size * spec.size }, (_, i) => i);
  const random = rng(seed);
  for (let i = indexes.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [indexes[i], indexes[j]] = [indexes[j], indexes[i]];
  }
  return { required: new Set(indexes.slice(0, spec.count)) };
}

function calculateScore(hits, misses) {
  return Math.max(0, hits * 100 - misses * 25);
}
