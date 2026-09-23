// Memory Matrix — Cloudflare Worker + Durable Object
// Automatic matchmaking + realtime multiplayer
// No room IDs / invite codes required.

const MAX_PLAYERS_PER_MATCH = 2;

// ── EXACTLY 3 ROUNDS ─────────────────────────────────────────────
// startRound() refuses to advance match.round beyond this value.
// After the 3rd round's results, the match goes straight to the final
// result — there is no code path that can start a 4th round.
const TOTAL_ROUNDS = 3;

const QUEUE_TIMEOUT = 60_000;
const RECONNECT_GRACE = 15_000;

// Timing (snappier, but with more play time per round).
const ROUND_TIME = 20_000;              // more time to play each round
const NEXT_ROUND_DELAY = 3_000;         // 3→2→1 between-round transition
const FINAL_RESULT_DELAY = 800;         // gap after round 3 before match_end
const MATCHMAKING_COUNTDOWN = 1_500;    // "waiting for rival" → round 1
const POST_MATCH_REQUEUE_DELAY = 1_000; // Play Again availability
const MATCH_RETENTION_AFTER_END = 30_000;

// Live scoreboard sync: every second during an active round the
// server pushes a `sync` snapshot with both players' full state.
const LIVE_SYNC_INTERVAL_MS = 1_000;

// Hearts: purely informational. hearts = MAX_HEARTS - misses (floor 0).
const MAX_HEARTS = 3;

// --- Profile validation defaults ---
const DEFAULT_NICK = "Player";
const DEFAULT_CC = "XX";
const NICK_MAX_LEN = 16;
const CC_REGEX = /^[A-Za-z]{2}$/;

const MAX_MESSAGE_BYTES = 4_096;

function sanitizeNick(raw) {
  if (typeof raw !== "string") return DEFAULT_NICK;
  let cleaned = raw.replace(/[^\p{L}\p{N}\s_\-.]/gu, "").trim();
  if (!cleaned) return DEFAULT_NICK;
  if (cleaned.length > NICK_MAX_LEN) cleaned = cleaned.slice(0, NICK_MAX_LEN);
  return cleaned;
}

function sanitizeCC(raw) {
  if (typeof raw !== "string") return DEFAULT_CC;
  const trimmed = raw.trim().toUpperCase();
  return CC_REGEX.test(trimmed) ? trimmed : DEFAULT_CC;
}

function computeHearts(misses) {
  return Math.max(0, MAX_HEARTS - (misses | 0));
}

// Compact per-seat scoreboard row used in matched / round / sync /
// next_round / round_end / resume_ok. Same shape everywhere so the
// client can render the SAME scoreboard regardless of which message
// refreshed it.
function scoreboardRow(match, seat, opts) {
  opts = opts || {};
  const rs = match.roundState;
  const hearts = typeof opts.hearts === "number"
    ? opts.hearts
    : (rs ? computeHearts(rs.misses[seat]) : MAX_HEARTS);
  const roundScore = typeof opts.score === "number"
    ? opts.score
    : (rs ? calculateScore(rs.hits[seat], rs.misses[seat], rs.spec) : 0);
  const baseTot = match.scores[seat];
  const dispTot = typeof opts.tot === "number"
    ? opts.tot
    : baseTot + roundScore;

  const row = {
    seat,
    nick: match.nicks[seat],
    cc: match.ccs[seat],
    av: match.avatars[seat],
    hearts,
    score: roundScore,
    tot: dispTot
  };

  if (rs) {
    row.hits = rs.hits[seat];
    row.misses = rs.misses[seat];
    row.done = rs.done[seat];
  }

  return row;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      const id = env.MATCHMAKER.idFromName("global");
      const stub = env.MATCHMAKER.get(id);
      const res = await stub.fetch(new Request("https://internal/status"));
      return new Response(await res.text(), {
        status: res.status,
        headers: {
          "content-type": "application/json",
          "access-control-allow-origin": "*"
        }
      });
    }

    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket upgrade required", { status: 426 });
      }
      const id = env.MATCHMAKER.idFromName("global");
      const stub = env.MATCHMAKER.get(id);
      return stub.fetch(request);
    }

    return new Response("Memory Matrix Multiplayer Backend", {
      status: 200,
      headers: { "content-type": "text/plain" }
    });
  }
};


/* =========================================================
   MATCHMAKER DURABLE OBJECT
   ========================================================= */

export class MatchMakerDO {

  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.queue = [];
    this.players = new Map();
    this.matches = new Map();

    // Per-match live-sync interval handles.
    this.syncTimers = new Map();

    this.loaded = false;
  }


  async load() {
    if (this.loaded) return;
    this.loaded = true;

    const data = await this.state.storage.get("state");
    if (!data) return;

    this.queue = data.queue || [];

    const restored = new Map();

    for (const entry of (data.matches || [])) {
      if (!Array.isArray(entry) || entry.length !== 2) continue;
      const [id, m] = entry;
      if (!m) continue;

      if (!(m.disconnected instanceof Map)) {
        const src = m.disconnected;
        const pairs = src && typeof src === "object" ? Object.entries(src) : [];
        m.disconnected = new Map(pairs);
      }

      // Matches that were mid-round when the DO was evicted cannot
      // resume (their timers are gone). Force them to "ended" so
      // they never leak into fresh matchmaking.
      if (m.state === "round" || m.state === "countdown" || m.state === "next_round") {
        m.state = "ended";
      }

      restored.set(id, m);
    }

    this.matches = restored;
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
      return new Response(
        JSON.stringify({
          ok: true,
          queue: this.queue.length,
          rooms: this.matches.size,
          players: this.players.size
        }),
        { headers: { "content-type": "application/json" } }
      );
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket endpoint", { status: 200 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();

    const playerId = crypto.randomUUID();

    const player = {
      id: playerId,
      ws: server,
      avatar: "brain",
      nick: DEFAULT_NICK,
      cc: DEFAULT_CC,
      matchId: null,
      seat: null,
      connected: true,
      joinedAt: Date.now()
    };

    this.players.set(playerId, player);

    server.addEventListener("message", async event => {
      try {
        const raw = typeof event.data === "string" ? event.data : "";

        if (typeof event.data === "string" && raw.length > MAX_MESSAGE_BYTES) {
          this.send(player, {
            t: "error",
            code: "TOO_LARGE",
            message: "Message too large"
          });
          return;
        }

        const msg = typeof event.data === "string"
          ? JSON.parse(event.data)
          : event.data;

        await this.handleMessage(player, msg);
      } catch (err) {
        this.send(player, {
          t: "error",
          code: "BAD_MESSAGE",
          message: "Invalid message"
        });
      }
    });

    server.addEventListener("close", async () => {
      player.connected = false;
      await this.handleDisconnect(player);
    });

    server.addEventListener("error", async () => {
      player.connected = false;
      await this.handleDisconnect(player);
    });

    this.send(player, { t: "ready", pid: playerId });

    return new Response(null, { status: 101, webSocket: client });
  }


  /* =======================================================
     MESSAGE ROUTER
     ======================================================= */

  async handleMessage(player, msg) {
    if (!msg || typeof msg.t !== "string") return;

    switch (msg.t) {
      case "queue":       await this.joinQueue(player, msg); break;
      case "leave_queue": await this.leaveQueue(player); break;
      case "tap":         await this.handleTap(player, msg); break;
      case "round_done":  await this.handleRoundDone(player, msg); break;
      case "resume":      await this.handleResume(player, msg); break;
      case "leave":       await this.leaveMatch(player); break;

      case "ping":
        this.send(player, {
          t: "pong",
          now: Date.now(),
          echo: typeof msg.now !== "undefined" ? msg.now : null
        });
        break;
    }
  }


  /* =======================================================
     MATCHMAKING
     ======================================================= */

  async joinQueue(player, msg) {

    if (this.queue.includes(player.id)) return;

    if (player.matchId) {
      const stale = this.matches.get(player.matchId);
      if (!stale || stale.state === "ended") {
        player.matchId = null;
        player.seat = null;
      } else {
        this.send(player, { t: "error", code: "ALREADY_IN_MATCH" });
        return;
      }
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
    const i = this.queue.indexOf(player.id);
    if (i !== -1) this.queue.splice(i, 1);
    this.send(player, { t: "queue_left" });
    await this.save();
  }


  async tryMatch() {
    this.queue = this.queue.filter(id => {
      const p = this.players.get(id);
      return p && p.connected && !p.matchId;
    });

    while (this.queue.length >= MAX_PLAYERS_PER_MATCH) {
      const aId = this.queue.shift();
      const bId = this.queue.shift();

      const a = this.players.get(aId);
      const b = this.players.get(bId);

      if (!a || !b || !a.connected || !b.connected) continue;

      await this.createMatch(a, b);
    }
  }


  /* =======================================================
     CREATE MATCH
     ======================================================= */

  async createMatch(a, b) {

    // Internal room / match id. Surfaced to clients as `m` + `roomId`
    // so the frontend can silently bind to a CrazyGames SDK room.
    // Never rendered as a room-code UI.
    const matchId = crypto.randomUUID();
    const seed = randomSeed();

    const match = {
      id: matchId,
      seed,
      players: [a.id, b.id],
      avatars: [a.avatar, b.avatar],
      nicks: [a.nick, b.nick],
      ccs: [a.cc, b.cc],
      scores: [0, 0],
      rounds: [],
      round: 0,
      state: "matched",
      createdAt: Date.now(),
      roundState: null,
      nextRoundAt: null,
      // Authoritative timestamp the very first round will begin at.
      firstRoundAt: null,
      disconnected: new Map()
    };

    this.matches.set(matchId, match);

    a.matchId = matchId;
    b.matchId = matchId;
    a.seat = 0;
    b.seat = 1;

    // Identical scoreboard payload delivered to both players.
    const startBoard = [
      scoreboardRow(match, 0, { hearts: MAX_HEARTS, score: 0, tot: 0 }),
      scoreboardRow(match, 1, { hearts: MAX_HEARTS, score: 0, tot: 0 })
    ];

    this.send(a, {
      t: "matched",
      m: matchId,
      roomId: matchId,
      you: 0,
      opp: { nick: b.nick, cc: b.cc, av: b.avatar },
      players: startBoard,
      hearts: MAX_HEARTS,
      rounds: TOTAL_ROUNDS,
      now: Date.now()
    });

    this.send(b, {
      t: "matched",
      m: matchId,
      roomId: matchId,
      you: 1,
      opp: { nick: a.nick, cc: a.cc, av: a.avatar },
      players: startBoard,
      hearts: MAX_HEARTS,
      rounds: TOTAL_ROUNDS,
      now: Date.now()
    });

    // Very short matchmaking countdown, then round 1 begins.
    // We capture the EXACT future timestamp round 1 will start at so
    // both clients can schedule their countdown from the same instant.
    const firstRoundAt = Date.now() + 120 + MATCHMAKING_COUNTDOWN;
    match.firstRoundAt = firstRoundAt;

    setTimeout(() => {
      const m = this.matches.get(matchId);
      if (!m || m.state !== "matched") return;

      m.state = "countdown";

      this.broadcast(m, {
        t: "count",
        n: 3,
        ms: MATCHMAKING_COUNTDOWN,
        // Server-authoritative start time of round 1.
        roundStartAt: firstRoundAt,
        players: startBoard,
        rounds: TOTAL_ROUNDS,
        now: Date.now()
      });

      setTimeout(() => this.startRound(m, firstRoundAt), MATCHMAKING_COUNTDOWN);
    }, 120);

    await this.save();
  }


  /* =======================================================
     ROUND CREATION
     ======================================================= */

  /**
   * Starts a round. Hard-caps at TOTAL_ROUNDS (=3).
   *
   * `providedStartAt` — when supplied, this is used as the round's
   * authoritative startedAt. Callers pass the SAME timestamp they
   * previously broadcast (via `next_round` / `count`) so both players
   * receive an identical start reference and their local clocks can
   * align to it exactly.
   */
  async startRound(match, providedStartAt) {

    if (!match) return;

    // ── EXACTLY 3 ROUNDS ──
    // Refuse to advance past TOTAL_ROUNDS. Even if a stray timer
    // somehow fired twice, this guard keeps the match at 3 rounds.
    if (match.round >= TOTAL_ROUNDS) return;

    if (match.state !== "countdown" && match.state !== "next_round") return;

    match.round++;
    match.state = "round";
    match.nextRoundAt = null;

    const spec = createRoundSpec(match.round);
    const roundSeed = mixSeed(match.seed, match.round);

    const startedAt = typeof providedStartAt === "number"
      ? providedStartAt
      : Date.now();

    match.roundState = {
      round: match.round,
      spec,
      seed: roundSeed,
      startedAt,
      deadline: startedAt + ROUND_TIME,
      taps: [new Set(), new Set()],
      hits: [0, 0],
      misses: [0, 0],
      done: [false, false]
    };

    // Both players see score reset to 0 for this round, hearts back
    // to MAX, and previous cumulative totals preserved.
    const roundBoard = [
      scoreboardRow(match, 0, { hearts: MAX_HEARTS, score: 0 }),
      scoreboardRow(match, 1, { hearts: MAX_HEARTS, score: 0 })
    ];

    // Single broadcast → both clients receive the identical payload
    // at the same server tick, guaranteeing synced round start.
    this.broadcast(match, {
      t: "round",
      r: match.round,
      rounds: TOTAL_ROUNDS,
      spec,
      seed: roundSeed,
      reveal: spec.reveal,
      recall: spec.recall,
      deadline: ROUND_TIME,
      startedAt,
      endsAt: match.roundState.deadline,
      hearts: MAX_HEARTS,
      players: roundBoard,
      now: Date.now()
    });

    // Begin live scoreboard sync for this round.
    this.startLiveSync(match);

    // Server-owned round timer.
    setTimeout(() => this.finishRound(match.id), ROUND_TIME + 100);
  }


  /* =======================================================
     LIVE SCOREBOARD SYNC
     ======================================================= */

  startLiveSync(match) {

    this.stopLiveSync(match.id);

    const id = match.id;

    const tick = () => {

      const m = this.matches.get(id);
      if (!m || m.state !== "round") { this.stopLiveSync(id); return; }

      const rs = m.roundState;
      if (!rs) { this.stopLiveSync(id); return; }

      // Full per-seat snapshot — identical to both players.
      const board = [
        scoreboardRow(m, 0),
        scoreboardRow(m, 1)
      ];

      this.broadcast(m, {
        t: "sync",
        r: rs.round,
        rounds: TOTAL_ROUNDS,
        startedAt: rs.startedAt,
        endsAt: rs.deadline,
        players: board,
        now: Date.now()
      });
    };

    const handle = setInterval(tick, LIVE_SYNC_INTERVAL_MS);
    this.syncTimers.set(id, handle);

    // Immediate first snapshot so nobody waits a full interval.
    tick();
  }


  stopLiveSync(matchId) {
    const handle = this.syncTimers.get(matchId);
    if (handle !== undefined) {
      clearInterval(handle);
      this.syncTimers.delete(matchId);
    }
  }


  /* =======================================================
     TAP VALIDATION
     ======================================================= */

  async handleTap(player, msg) {

    if (!player.matchId) return;

    const match = this.matches.get(player.matchId);
    if (!match) return;

    const rs = match.roundState;

    if (!rs || match.state !== "round" || typeof player.seat !== "number") return;

    if (Date.now() > rs.deadline) {
      this.send(player, { t: "fix", r: rs.round, i: msg.i, k: "late" });
      return;
    }

    if (Number(msg.r) !== rs.round) return;

    const index = Number(msg.i);
    const size = rs.spec.size;

    if (!Number.isInteger(index) || index < 0 || index >= size * size) {
      this.send(player, { t: "fix", r: rs.round, i: index, k: "bad" });
      return;
    }

    const seat = player.seat;

    if (rs.done[seat]) return;
    if (rs.taps[seat].has(index)) return;

    rs.taps[seat].add(index);

    const board = deriveBoard(rs.spec, rs.seed);
    const isHit = board.required.has(index);

    if (isHit) rs.hits[seat]++; else rs.misses[seat]++;

    const liveScore = calculateScore(rs.hits[seat], rs.misses[seat], rs.spec);
    const liveHearts = computeHearts(rs.misses[seat]);
    const tapTotal = rs.hits[seat] + rs.misses[seat];
    // Display total = previous-round cumulative + this round's
    // current live score. Both players see the SAME value.
    const dispTot = match.scores[seat] + liveScore;

    // Relay opponent event with the tapper's live scoreboard state.
    this.broadcastExcept(match, player.id, {
      t: "opp",
      r: rs.round,
      i: index,
      k: isHit ? "ok" : "miss",
      s: tapTotal,
      h: rs.hits[seat],
      mi: rs.misses[seat],
      hp: liveHearts,
      sc: liveScore,
      tot: dispTot,
      seat,
      endsAt: rs.deadline,
      now: Date.now()
    });

    // Confirm to the tapper with the SAME enriched payload.
    this.send(player, {
      t: "fix",
      r: rs.round,
      i: index,
      k: isHit ? "ok" : "miss",
      h: rs.hits[seat],
      mi: rs.misses[seat],
      hp: liveHearts,
      sc: liveScore,
      tot: dispTot,
      endsAt: rs.deadline,
      now: Date.now()
    });

    if (rs.hits[seat] >= board.required.size) {
      rs.done[seat] = true;
      await this.finishRound(match.id);
    }
  }


  /* =======================================================
     ROUND DONE
     ======================================================= */

  async handleRoundDone(player, msg) {

    if (!player.matchId) return;

    const match = this.matches.get(player.matchId);
    if (!match) return;

    const rs = match.roundState;

    if (!rs || match.state !== "round" || typeof player.seat !== "number") return;
    if (Number(msg.r) !== rs.round) return;
    if (Date.now() > rs.deadline) return;
    if (rs.done[player.seat]) return;

    const board = deriveBoard(rs.spec, rs.seed);
    if (rs.hits[player.seat] < board.required.size) return;

    rs.done[player.seat] = true;
    await this.finishRound(match.id);
  }


  /* =======================================================
     FINISH ROUND
     ======================================================= */

  async finishRound(matchId) {

    const match = this.matches.get(matchId);
    if (!match) return;

    const rs = match.roundState;
    if (!rs || match.state !== "round") return;

    match.state = "round_end";
    this.stopLiveSync(matchId);

    const scoreA = calculateScore(rs.hits[0], rs.misses[0], rs.spec);
    const scoreB = calculateScore(rs.hits[1], rs.misses[1], rs.spec);

    match.scores[0] += scoreA;
    match.scores[1] += scoreB;

    match.rounds.push({
      r: rs.round,
      score: [scoreA, scoreB],
      hits: [rs.hits[0], rs.hits[1]],
      misses: [rs.misses[0], rs.misses[1]],
      hearts: [
        computeHearts(rs.misses[0]),
        computeHearts(rs.misses[1])
      ]
    });

    const roundReason = seat => {
      if (rs.done[seat]) return "completed";
      if (rs.hits[seat] === 0 && rs.misses[seat] === 0) return "no_attempt";
      return "timeout";
    };

    const reasonA = roundReason(0);
    const reasonB = roundReason(1);

    const totNow = [match.scores[0], match.scores[1]];
    const isFinalRound = match.round >= TOTAL_ROUNDS;

    let roundWinnerSeat = null;
    if (scoreA > scoreB) roundWinnerSeat = 0;
    else if (scoreB > scoreA) roundWinnerSeat = 1;

    const roundWinnerFor = seat => {
      if (roundWinnerSeat === null) return "draw";
      return roundWinnerSeat === seat ? "you" : "opp";
    };

    // Per-seat round-end payload — includes both players' names,
    // flags, avatars, hearts, this-round score, and cumulative total.
    for (let seat = 0; seat < 2; seat++) {

      const p = this.players.get(match.players[seat]);
      if (!p) continue;

      const oppSeat = seat === 0 ? 1 : 0;

      const youScore = seat === 0 ? scoreA : scoreB;
      const oppScore = seat === 0 ? scoreB : scoreA;
      const youReason = seat === 0 ? reasonA : reasonB;
      const oppReason = seat === 0 ? reasonB : reasonA;
      const youHearts = computeHearts(rs.misses[seat]);
      const oppHearts = computeHearts(rs.misses[oppSeat]);

      this.send(p, {
        t: "round_end",
        p: {
          r: rs.round,
          rounds: TOTAL_ROUNDS,
          tot: totNow,
          // Full scoreboard both seats (identical to both players).
          players: [
            scoreboardRow(match, 0, { hearts: computeHearts(rs.misses[0]), score: scoreA, tot: totNow[0] }),
            scoreboardRow(match, 1, { hearts: computeHearts(rs.misses[1]), score: scoreB, tot: totNow[1] })
          ],
          you: {
            score: youScore,
            reason: youReason,
            nick: match.nicks[seat],
            cc: match.ccs[seat],
            av: match.avatars[seat],
            hearts: youHearts,
            hits: rs.hits[seat],
            misses: rs.misses[seat],
            tot: totNow[seat]
          },
          opp: {
            score: oppScore,
            reason: oppReason,
            nick: match.nicks[oppSeat],
            cc: match.ccs[oppSeat],
            av: match.avatars[oppSeat],
            hearts: oppHearts,
            hits: rs.hits[oppSeat],
            misses: rs.misses[oppSeat],
            tot: totNow[oppSeat]
          },
          win: roundWinnerFor(seat),
          final: isFinalRound,
          now: Date.now()
        }
      });
    }

    if (isFinalRound) {

      // Round 3 is done → straight to the final result. There is
      // deliberately no `next_round` transition here, so round 4
      // can never be reached.
      match.state = "final";

      setTimeout(() => this.finishMatch(match.id), FINAL_RESULT_DELAY);

    } else {

      // Rounds 1 and 2 → short 3→2→1 transition, then the next round.
      match.state = "next_round";

      const nextRound = match.round + 1;
      const nextRoundAt = Date.now() + NEXT_ROUND_DELAY;
      match.nextRoundAt = nextRoundAt;

      // Both clients receive the SAME `nextRoundAt`, so their
      // countdowns tick in perfect sync.
      this.broadcast(match, {
        t: "next_round",
        r: rs.round,
        next: nextRound,
        rounds: TOTAL_ROUNDS,
        tot: totNow,
        score: [scoreA, scoreB],
        win: [roundWinnerFor(0), roundWinnerFor(1)],
        players: [
          scoreboardRow(match, 0, { hearts: computeHearts(rs.misses[0]), score: scoreA, tot: totNow[0] }),
          scoreboardRow(match, 1, { hearts: computeHearts(rs.misses[1]), score: scoreB, tot: totNow[1] })
        ],
        // Authoritative start time of round N+1.
        nextRoundAt,
        now: Date.now()
      });

      setTimeout(() => {
        const m = this.matches.get(match.id);
        if (!m || m.state !== "next_round" || m.nextRoundAt !== nextRoundAt) return;
        // Pass the SAME timestamp the clients saw so both sides
        // agree on the round's startedAt.
        this.startRound(m, nextRoundAt);
      }, NEXT_ROUND_DELAY);
    }

    await this.save();
  }


  /* =======================================================
     MATCH END
     ======================================================= */

  async finishMatch(matchId) {

    const match = this.matches.get(matchId);
    if (!match) return;

    match.state = "ended";
    this.stopLiveSync(matchId);

    const a = match.scores[0];
    const b = match.scores[1];

    // Global winner from seat-0's perspective ("you" == seat 0 won).
    let globalWinner = "draw";
    if (a > b) globalWinner = "you";
    if (b > a) globalWinner = "opp";

    const playersInfo = [
      { seat: 0, nick: match.nicks[0], cc: match.ccs[0], av: match.avatars[0], tot: a },
      { seat: 1, nick: match.nicks[1], cc: match.ccs[1], av: match.avatars[1], tot: b }
    ];

    for (let seat = 0; seat < 2; seat++) {

      const p = this.players.get(match.players[seat]);
      if (!p) continue;

      // Per-player win/lose. The winning player always reads "you"
      // and the losing player always reads "opp" — nobody sees the
      // opponent's outcome.
      let win = globalWinner;
      if (globalWinner !== "draw") {
        win = globalWinner === (seat === 0 ? "you" : "opp") ? "you" : "opp";
      }

      this.send(p, {
        t: "match_end",
        p: {
          win,
          tot: [a, b],
          rounds: match.rounds,
          players: playersInfo,
          reason: "completed",
          now: Date.now()
        }
      });
    }

    await this.save();

    // Free players quickly so "Play Again" works instantly.
    setTimeout(() => {
      for (const pid of match.players) {
        const p = this.players.get(pid);
        if (p && p.matchId === matchId) {
          p.matchId = null;
          p.seat = null;
        }
      }
      this.save();
    }, POST_MATCH_REQUEUE_DELAY);

    // Then purge the match so stale state can't affect matchmaking.
    setTimeout(() => {
      const m = this.matches.get(matchId);
      if (!m) return;
      this.matches.delete(matchId);
      this.stopLiveSync(matchId);
      this.save();
    }, MATCH_RETENTION_AFTER_END);
  }


  /* =======================================================
     RECONNECT / RESUME
     ======================================================= */

  async handleDisconnect(player) {

    if (!player.matchId) {
      this.queue = this.queue.filter(id => id !== player.id);
      this.players.delete(player.id);
      await this.save();
      return;
    }

    const match = this.matches.get(player.matchId);

    if (!match) {
      this.players.delete(player.id);
      return;
    }

    if (match.state === "ended") {
      this.players.delete(player.id);
      await this.save();
      return;
    }

    match.disconnected.set(player.seat, Date.now());

    const opponent = this.players.get(
      match.players[player.seat === 0 ? 1 : 0]
    );

    if (opponent) {
      this.send(opponent, { t: "opp_left" });
    }

    setTimeout(async () => {

      const m = this.matches.get(match.id);
      if (!m) return;

      const lostAt = m.disconnected.get(player.seat);
      if (!lostAt) return;

      const current = this.players.get(player.id);
      if (current && current.connected) return;

      const otherSeat = player.seat === 0 ? 1 : 0;
      const other = this.players.get(m.players[otherSeat]);

      if (other) {
        this.send(other, { t: "opp_left", final: true });
        this.send(other, {
          t: "match_end",
          p: {
            win: "you",
            tot: m.scores,
            rounds: m.rounds,
            players: [
              { seat: 0, nick: m.nicks[0], cc: m.ccs[0], av: m.avatars[0], tot: m.scores[0] },
              { seat: 1, nick: m.nicks[1], cc: m.ccs[1], av: m.avatars[1], tot: m.scores[1] }
            ],
            reason: "opponent_left",
            now: Date.now()
          }
        });
      }

      m.state = "ended";

      for (const pid of m.players) {
        const pl = this.players.get(pid);
        if (pl && pl.matchId === m.id) {
          pl.matchId = null;
          pl.seat = null;
        }
      }

      this.stopLiveSync(m.id);
      this.matches.delete(m.id);
      this.players.delete(player.id);

      await this.save();

    }, RECONNECT_GRACE);

    await this.save();
  }


  async handleResume(player, msg) {

    const matchId = String(msg.m || "");
    const pid = String(msg.pid || "");

    const match = this.matches.get(matchId);

    if (!match || match.state === "ended") {
      this.send(player, { t: "resume_fail" });
      return;
    }

    const oldPlayer = this.players.get(pid);

    if (!oldPlayer || !match.players.includes(pid)) {
      this.send(player, { t: "resume_fail" });
      return;
    }

    if (oldPlayer.matchId && oldPlayer.matchId !== match.id && oldPlayer.connected) {
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

    // Full current scoreboard, identical shape to every other message.
    const board = [
      scoreboardRow(match, 0),
      scoreboardRow(match, 1)
    ];

    this.send(oldPlayer, {
      t: "resume_ok",
      m: match.id,
      roomId: match.id,
      you: seat,
      opp: {
        nick: match.nicks[seat === 0 ? 1 : 0],
        cc: match.ccs[seat === 0 ? 1 : 0],
        av: match.avatars[seat === 0 ? 1 : 0]
      },
      players: board,
      rounds: TOTAL_ROUNDS,
      tot: match.scores,
      r: match.round,
      st: match.state,
      nextRoundAt: match.state === "next_round" ? match.nextRoundAt : null,
      now: Date.now(),
      rs: rs ? {
        r: rs.round,
        spec: rs.spec,
        seed: rs.seed,
        reveal: rs.spec.reveal,
        recall: rs.spec.recall,
        startedAt: rs.startedAt,
        endsAt: rs.deadline,
        yourTaps: [...rs.taps[seat]],
        yourHits: rs.hits[seat],
        yourMisses: rs.misses[seat],
        yourHearts: computeHearts(rs.misses[seat]),
        yourScore: calculateScore(rs.hits[seat], rs.misses[seat], rs.spec),
        yourDone: rs.done[seat]
      } : null
    });

    const opponent = this.players.get(
      match.players[seat === 0 ? 1 : 0]
    );

    if (opponent) {
      this.send(opponent, { t: "opp_back" });
    }

    // Re-arm live sync if a round is active.
    if (match.state === "round") {
      this.startLiveSync(match);
    }

    await this.save();
  }


  /* =======================================================
     LEAVE MATCH
     ======================================================= */

  async leaveMatch(player) {

    if (!player.matchId) return;

    const match = this.matches.get(player.matchId);
    if (!match) return;

    const otherSeat = player.seat === 0 ? 1 : 0;
    const other = this.players.get(match.players[otherSeat]);

    if (other) {
      this.send(other, {
        t: "match_end",
        p: {
          win: "you",
          tot: match.scores,
          rounds: match.rounds,
          players: [
            { seat: 0, nick: match.nicks[0], cc: match.ccs[0], av: match.avatars[0], tot: match.scores[0] },
            { seat: 1, nick: match.nicks[1], cc: match.ccs[1], av: match.avatars[1], tot: match.scores[1] }
          ],
          reason: "opponent_left",
          now: Date.now()
        }
      });
    }

    match.state = "ended";
    this.stopLiveSync(match.id);
    this.matches.delete(match.id);

    player.matchId = null;
    player.seat = null;

    await this.save();
  }


  /* =======================================================
     SOCKET HELPERS
     ======================================================= */

  send(player, data) {
    if (!player || !player.ws || !player.connected) return;
    try { player.ws.send(JSON.stringify(data)); } catch (e) {}
  }

  broadcast(match, data) {
    for (const pid of match.players) {
      const p = this.players.get(pid);
      if (p) this.send(p, data);
    }
  }

  broadcastExcept(match, exceptId, data) {
    for (const pid of match.players) {
      if (pid === exceptId) continue;
      const p = this.players.get(pid);
      if (p) this.send(p, data);
    }
  }
}


/* =========================================================
   GAME / BOARD HELPERS
   ========================================================= */

const MP_AVATARS = [
  "brain", "cube", "robot", "fox", "penguin",
  "bolt", "owl", "cat", "dragon", "astro", "ninja"
];

function randomSeed() {
  return Math.floor(Math.random() * 0x7fffffff);
}

function mixSeed(a, b) {
  let x = (a ^ (b * 0x45d9f3b)) >>> 0;
  x = Math.imul(x ^ (x >>> 16), 0x45d9f3b) >>> 0;
  x ^= x >>> 16;
  return x >>> 0;
}

function rng(seed) {
  let x = seed >>> 0;
  return function() {
    x = Math.imul(x ^ (x >>> 16), 0x45d9f3b) >>> 0;
    x ^= x >>> 16;
    return (x >>> 0) / 4294967296;
  };
}

function createRoundSpec(round) {
  const size = Math.min(3 + round, 7);
  return {
    size,
    reveal: Math.max(900, 1800 - round * 120),
    recall: Math.max(2500, 6000 - round * 400),
    count: Math.min(2 + round, Math.floor(size * size * 0.45)),
    round
  };
}

function deriveBoard(spec, seed) {
  const total = spec.size * spec.size;
  const random = rng(seed);

  const indexes = Array.from({ length: total }, (_, i) => i);

  for (let i = total - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [indexes[i], indexes[j]] = [indexes[j], indexes[i]];
  }

  const required = new Set(indexes.slice(0, spec.count));
  return { required };
}

function calculateScore(hits, misses, spec) {
  const base = hits * 100;
  const missPenalty = misses * 25;
  return Math.max(0, base - missPenalty);
}
