// Memory Matrix — Cloudflare Worker + Durable Object
// Automatic matchmaking + realtime multiplayer
// No room IDs / invite codes required.

const MAX_PLAYERS_PER_MATCH = 2;
const TOTAL_ROUNDS = 3; // server-authoritative: exactly 3 rounds per match, never 2 or 4
const QUEUE_TIMEOUT = 60_000;
const RECONNECT_GRACE = 15_000;

// Timing tuning (reduced waiting / snappier sync).
const ROUND_TIME = 20_000;              // was 15_000 — more time to play each round
const NEXT_ROUND_DELAY = 3_000;         // was 5_000 — faster 3→2→1 transition between rounds
const FINAL_RESULT_DELAY = 800;         // was 1_500 — snappier jump into the final screen
const MATCHMAKING_COUNTDOWN = 1_500;    // was hardcoded 3_000 — faster "waiting for rival" → round 1
const POST_MATCH_REQUEUE_DELAY = 1_000; // was 3_000 — instant "Play again" availability
const MATCH_RETENTION_AFTER_END = 30_000;

// Live score / status sync. Every LIVE_SYNC_INTERVAL_MS during an active
// round, the server broadcasts a compact `sync` snapshot so both clients
// stay locked on the opponent's hearts, taps, hits, and current score
// even if an individual tap message is lost in transit.
const LIVE_SYNC_INTERVAL_MS = 1_000;

// Hearts / lives. Purely informational state that the frontend MAY render.
// Server is the single source of truth: hearts = MAX_HEARTS - misses (floor 0).
// Never used to force-end a round — that decision stays with the client.
const MAX_HEARTS = 5;

// --- Profile validation defaults ---
const DEFAULT_NICK = "Player";
const DEFAULT_CC = "XX";
const NICK_MAX_LEN = 16;
const CC_REGEX = /^[A-Za-z]{2}$/;

// Maximum accepted raw WebSocket frame size (bytes, approximate).
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      const id = env.MATCHMAKER.idFromName("global");
      const stub = env.MATCHMAKER.get(id);

      const res = await stub.fetch(
        new Request("https://internal/status")
      );

      return new Response(await res.text(), {
        status: res.status,
        headers: {
          "content-type": "application/json",
          "access-control-allow-origin": "*"
        }
      });
    }

    // WebSocket endpoint
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket upgrade required", {
          status: 426
        });
      }

      const id = env.MATCHMAKER.idFromName("global");
      const stub = env.MATCHMAKER.get(id);

      return stub.fetch(request);
    }

    return new Response(
      "Memory Matrix Multiplayer Backend",
      {
        status: 200,
        headers: {
          "content-type": "text/plain"
        }
      }
    );
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

    // Per-match interval handle for the live sync loop. Keeps a
    // single timer per active round instead of spawning one per
    // tap, keeping the DO cheap.
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
        const pairs =
          src && typeof src === "object"
            ? Object.entries(src)
            : [];
        m.disconnected = new Map(pairs);
      }

      // Any match that was mid-round when the DO was evicted is
      // stale — it can't resume without its timers. Mark it ended
      // so new queue activity never attaches to it.
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
        {
          headers: {
            "content-type": "application/json"
          }
        }
      );
    }


    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket endpoint", {
        status: 200
      });
    }


    const pair = new WebSocketPair();

    const client = pair[0];
    const server = pair[1];

    server.accept();

    const playerId =
      crypto.randomUUID();

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


    server.addEventListener(
      "message",
      async event => {

        try {

          const raw =
            typeof event.data === "string"
              ? event.data
              : "";

          if (
            typeof event.data === "string" &&
            raw.length > MAX_MESSAGE_BYTES
          ) {
            this.send(
              player,
              {
                t: "error",
                code: "TOO_LARGE",
                message: "Message too large"
              }
            );
            return;
          }

          const msg =
            typeof event.data === "string"
              ? JSON.parse(event.data)
              : event.data;

          await this.handleMessage(
            player,
            msg
          );

        } catch (err) {

          this.send(
            player,
            {
              t: "error",
              code: "BAD_MESSAGE",
              message: "Invalid message"
            }
          );

        }

      }
    );


    server.addEventListener(
      "close",
      async () => {

        player.connected = false;

        await this.handleDisconnect(
          player
        );

      }
    );


    server.addEventListener(
      "error",
      async () => {

        player.connected = false;

        await this.handleDisconnect(
          player
        );

      }
    );


    this.send(
      player,
      {
        t: "ready",
        pid: playerId
      }
    );


    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }


  /* =======================================================
     MESSAGE ROUTER
     ======================================================= */

  async handleMessage(player, msg) {

    if (!msg || typeof msg.t !== "string")
      return;


    switch (msg.t) {

      case "queue":
        await this.joinQueue(
          player,
          msg
        );
        break;


      case "leave_queue":
        await this.leaveQueue(player);
        break;


      case "ping":
        this.send(
          player,
          {
            t: "pong",
            now: Date.now(),
            echo:
              typeof msg.now !==
              "undefined"
                ? msg.now
                : null
          }
        );
        break;


      case "tap":
        await this.handleTap(
          player,
          msg
        );
        break;


      case "round_done":
        await this.handleRoundDone(
          player,
          msg
        );
        break;


      case "resume":
        await this.handleResume(
          player,
          msg
        );
        break;


      case "leave":
        await this.leaveMatch(
          player
        );
        break;

    }
  }


  /* =======================================================
     MATCHMAKING
     ======================================================= */

  async joinQueue(player, msg) {

    if (
      this.queue.includes(player.id)
    ) {
      return;
    }


    if (player.matchId) {

      // If the player is stuck on an already-ended match id,
      // silently release them so they can queue again immediately.
      const stale =
        this.matches.get(player.matchId);

      if (!stale || stale.state === "ended") {
        player.matchId = null;
        player.seat = null;
      } else {

        this.send(
          player,
          {
            t: "error",
            code: "ALREADY_IN_MATCH"
          }
        );

        return;
      }
    }


    player.avatar =
      MP_AVATARS.includes(msg.av)
        ? msg.av
        : "brain";


    player.nick =
      sanitizeNick(msg.nick);

    player.cc =
      sanitizeCC(msg.cc);


    player.joinedAt =
      Date.now();


    this.queue.push(
      player.id
    );


    this.send(
      player,
      {
        t: "queued",
        pos: this.queue.length
      }
    );


    await this.tryMatch();

    await this.save();
  }


  async leaveQueue(player) {

    const i =
      this.queue.indexOf(
        player.id
      );

    if (i !== -1) {

      this.queue.splice(
        i,
        1
      );

    }


    this.send(
      player,
      {
        t: "queue_left"
      }
    );


    await this.save();
  }


  async tryMatch() {

    this.queue =
      this.queue.filter(
        id => {

          const p =
            this.players.get(id);

          return (
            p &&
            p.connected &&
            !p.matchId
          );

        }
      );


    while (
      this.queue.length >=
      MAX_PLAYERS_PER_MATCH
    ) {

      const aId =
        this.queue.shift();

      const bId =
        this.queue.shift();


      const a =
        this.players.get(aId);

      const b =
        this.players.get(bId);


      if (
        !a ||
        !b ||
        !a.connected ||
        !b.connected
      ) {
        continue;
      }


      await this.createMatch(
        a,
        b
      );
    }
  }


  /* =======================================================
     CREATE MATCH
     ======================================================= */

  async createMatch(a, b) {

    // Internal room / match id. Same value is surfaced to the client
    // as `m` (legacy) and `roomId` (alias for CrazyGames SDK sync).
    // Never rendered as a room-code UI.
    const matchId =
      crypto.randomUUID();


    const seed =
      randomSeed();


    const match = {

      id: matchId,

      seed,

      players: [
        a.id,
        b.id
      ],

      avatars: [
        a.avatar,
        b.avatar
      ],

      nicks: [
        a.nick,
        b.nick
      ],

      ccs: [
        a.cc,
        b.cc
      ],

      scores: [
        0,
        0
      ],

      rounds: [],

      round: 0,

      state: "matched",

      createdAt: Date.now(),

      roundState: null,

      nextRoundAt: null,

      disconnected: new Map()
    };


    this.matches.set(
      matchId,
      match
    );


    a.matchId = matchId;
    b.matchId = matchId;

    a.seat = 0;
    b.seat = 1;


    // "matched" payload includes the internal room id in `m` and
    // `roomId`. Both players may reuse the SAME `m`/`roomId` to
    // silently synchronize via CrazyGames SDK — never shown as UI.
    this.send(
      a,
      {
        t: "matched",

        m: matchId,
        roomId: matchId,

        you: 0,

        opp: {
          nick: b.nick,
          cc: b.cc,
          av: b.avatar
        },

        // Both seats' identity + initial totals, so the moment the
        // matched screen mounts both clients show the same
        // 0 / 0 scoreboard with the same names and flags.
        players: [
          { seat: 0, nick: a.nick, cc: a.cc, av: a.avatar, tot: 0, hearts: MAX_HEARTS, score: 0 },
          { seat: 1, nick: b.nick, cc: b.cc, av: b.avatar, tot: 0, hearts: MAX_HEARTS, score: 0 }
        ],

        hearts: MAX_HEARTS,

        rounds: TOTAL_ROUNDS,

        now: Date.now()
      }
    );


    this.send(
      b,
      {
        t: "matched",

        m: matchId,
        roomId: matchId,

        you: 1,

        opp: {
          nick: a.nick,
          cc: a.cc,
          av: a.avatar
        },

        players: [
          { seat: 0, nick: a.nick, cc: a.cc, av: a.avatar, tot: 0, hearts: MAX_HEARTS, score: 0 },
          { seat: 1, nick: b.nick, cc: b.cc, av: b.avatar, tot: 0, hearts: MAX_HEARTS, score: 0 }
        ],

        hearts: MAX_HEARTS,

        rounds: TOTAL_ROUNDS,

        now: Date.now()
      }
    );


    // Short matchmaking countdown, then round 1 begins.
    setTimeout(
      () => {

        const m =
          this.matches.get(matchId);

        if (
          !m ||
          m.state !== "matched"
        )
          return;


        m.state =
          "countdown";


        this.broadcast(
          m,
          {
            t: "count",
            n: 3,
            ms: MATCHMAKING_COUNTDOWN,
            now: Date.now()
          }
        );


        setTimeout(
          () => this.startRound(m),
          MATCHMAKING_COUNTDOWN
        );

      },
      120
    );


    await this.save();
  }


  /* =======================================================
     ROUND CREATION
     ======================================================= */

  async startRound(match) {

    if (
      !match ||
      match.round >= TOTAL_ROUNDS ||
      (match.state !== "countdown" &&
        match.state !== "next_round")
    ) {
      return;
    }


    match.round++;

    match.state =
      "round";

    match.nextRoundAt =
      null;


    const spec =
      createRoundSpec(
        match.round
      );


    const roundSeed =
      mixSeed(
        match.seed,
        match.round
      );


    const startedAt = Date.now();

    match.roundState = {

      round:
        match.round,

      spec,

      seed:
        roundSeed,

      startedAt,

      deadline:
        startedAt + ROUND_TIME,

      taps: [
        new Set(),
        new Set()
      ],

      hits: [
        0,
        0
      ],

      misses: [
        0,
        0
      ],

      done: [
        false,
        false
      ]
    };


    // Starting snapshot of the round — both seats receive the same
    // payload with hearts and starting scores.
    const startingPlayers = [
      {
        seat: 0,
        nick: match.nicks[0],
        cc: match.ccs[0],
        av: match.avatars[0],
        hearts: MAX_HEARTS,
        score: 0,
        tot: match.scores[0]
      },
      {
        seat: 1,
        nick: match.nicks[1],
        cc: match.ccs[1],
        av: match.avatars[1],
        hearts: MAX_HEARTS,
        score: 0,
        tot: match.scores[1]
      }
    ];

    this.broadcast(
      match,
      {
        t: "round",

        r: match.round,

        spec,

        seed: roundSeed,

        reveal: spec.reveal,

        recall: spec.recall,

        deadline:
          ROUND_TIME,

        startedAt:
          match.roundState.startedAt,

        endsAt:
          match.roundState.deadline,

        hearts: MAX_HEARTS,

        players: startingPlayers,

        tot: [ match.scores[0], match.scores[1] ],

        rounds: TOTAL_ROUNDS,

        now:
          Date.now()
      }
    );


    // Kick off the live-sync loop (hearts + score + clock).
    this.startLiveSync(match);


    // Server-owned round timer
    setTimeout(
      () => {

        this.finishRound(
          match.id
        );

      },
      ROUND_TIME + 100
    );
  }


  /* =======================================================
     LIVE SYNC LOOP
     ======================================================= */

  startLiveSync(match) {

    this.stopLiveSync(match.id);

    const id = match.id;

    const tick = () => {

      const m = this.matches.get(id);

      if (!m || m.state !== "round") {
        this.stopLiveSync(id);
        return;
      }

      const rs = m.roundState;
      if (!rs) {
        this.stopLiveSync(id);
        return;
      }

      const scoreA = calculateScore(rs.hits[0], rs.misses[0], rs.spec);
      const scoreB = calculateScore(rs.hits[1], rs.misses[1], rs.spec);

      const snap = {
        t: "sync",
        r: rs.round,
        endsAt: rs.deadline,
        now: Date.now(),
        tot: [ m.scores[0], m.scores[1] ],
        players: [
          {
            seat: 0,
            hearts: computeHearts(rs.misses[0]),
            score: scoreA,
            hits: rs.hits[0],
            misses: rs.misses[0],
            done: rs.done[0]
          },
          {
            seat: 1,
            hearts: computeHearts(rs.misses[1]),
            score: scoreB,
            hits: rs.hits[1],
            misses: rs.misses[1],
            done: rs.done[1]
          }
        ]
      };

      this.broadcast(m, snap);
    };

    const handle = setInterval(tick, LIVE_SYNC_INTERVAL_MS);

    this.syncTimers.set(id, handle);

    // First snapshot fires immediately so a player who just joined
    // doesn't wait a whole interval.
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

    if (!player.matchId)
      return;


    const match =
      this.matches.get(
        player.matchId
      );


    if (!match)
      return;


    const rs =
      match.roundState;


    if (
      !rs ||
      match.state !== "round" ||
      typeof player.seat !== "number"
    )
      return;


    if (
      Date.now() >
      rs.deadline
    ) {

      this.send(
        player,
        {
          t: "fix",
          r: rs.round,
          i: msg.i,
          k: "late"
        }
      );

      return;
    }


    if (
      Number(msg.r) !== rs.round
    )
      return;


    const index =
      Number(msg.i);


    const size =
      rs.spec.size;


    if (
      !Number.isInteger(index) ||
      index < 0 ||
      index >= size * size
    ) {

      this.send(
        player,
        {
          t: "fix",
          r: rs.round,
          i: index,
          k: "bad"
        }
      );

      return;
    }


    const seat =
      player.seat;


    if (rs.done[seat]) {
      return;
    }


    if (rs.taps[seat].has(index)) {
      return;
    }


    rs.taps[seat].add(index);


    const board =
      deriveBoard(rs.spec, rs.seed);


    const isHit =
      board.required.has(index);


    if (isHit) {
      rs.hits[seat]++;
    } else {
      rs.misses[seat]++;
    }


    // Live per-seat snapshot AFTER this tap.
    const liveScore = calculateScore(
      rs.hits[seat],
      rs.misses[seat],
      rs.spec
    );

    const liveHearts = computeHearts(rs.misses[seat]);

    const tapTotal =
      rs.hits[seat] + rs.misses[seat];


    // Relay opponent event — includes hearts, hits, misses,
    // current round score, and the running total so the opponent's
    // HUD updates the instant anything changes.
    this.broadcastExcept(
      match,
      player.id,
      {
        t: "opp",
        r: rs.round,
        i: index,
        k: isHit ? "ok" : "miss",
        s: tapTotal,          // legacy: total taps
        h: rs.hits[seat],     // hits this round
        mi: rs.misses[seat],  // misses this round
        hp: liveHearts,       // hearts remaining (0..MAX_HEARTS)
        sc: liveScore,        // current round score
        tot: match.scores[seat], // cumulative total
        endsAt: rs.deadline,
        now: Date.now()
      }
    );


    // Confirm to the player with the same enriched payload.
    this.send(
      player,
      {
        t: "fix",
        r: rs.round,
        i: index,
        k: isHit ? "ok" : "miss",
        h: rs.hits[seat],
        mi: rs.misses[seat],
        hp: liveHearts,
        sc: liveScore,
        tot: match.scores[seat],
        endsAt: rs.deadline,
        now: Date.now()
      }
    );


    // Round auto-finish when all required tiles found.
    if (
      rs.hits[seat] >=
      board.required.size
    ) {

      rs.done[seat] =
        true;

      await this.finishRound(
        match.id
      );
    }
  }


  /* =======================================================
     ROUND DONE
     ======================================================= */

  async handleRoundDone(
    player,
    msg
  ) {

    if (!player.matchId)
      return;


    const match =
      this.matches.get(
        player.matchId
      );


    if (!match)
      return;


    const rs =
      match.roundState;


    if (
      !rs ||
      match.state !== "round" ||
      typeof player.seat !== "number"
    )
      return;


    if (
      Number(msg.r) !== rs.round
    )
      return;


    if (
      Date.now() >
      rs.deadline
    )
      return;


    if (
      rs.done[player.seat]
    )
      return;


    const board =
      deriveBoard(
        rs.spec,
        rs.seed
      );


    if (
      rs.hits[player.seat] <
      board.required.size
    ) {

      return;
    }


    rs.done[player.seat] =
      true;


    await this.finishRound(
      match.id
    );
  }


  /* =======================================================
     FINISH ROUND
     ======================================================= */

  async finishRound(matchId) {

    const match =
      this.matches.get(
        matchId
      );


    if (!match)
      return;


    const rs =
      match.roundState;


    if (
      !rs ||
      match.state !== "round"
    )
      return;


    match.state =
      "round_end";


    this.stopLiveSync(matchId);


    const scoreA =
      calculateScore(
        rs.hits[0],
        rs.misses[0],
        rs.spec
      );


    const scoreB =
      calculateScore(
        rs.hits[1],
        rs.misses[1],
        rs.spec
      );


    match.scores[0] +=
      scoreA;


    match.scores[1] +=
      scoreB;


    match.rounds.push({

      r: rs.round,

      score: [
        scoreA,
        scoreB
      ],

      hits: [
        rs.hits[0],
        rs.hits[1]
      ],

      misses: [
        rs.misses[0],
        rs.misses[1]
      ],

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

    const totNow = [
      match.scores[0],
      match.scores[1]
    ];

    const isFinalRound =
      match.round >= TOTAL_ROUNDS;

    let roundWinnerSeat = null;
    if (scoreA > scoreB) roundWinnerSeat = 0;
    else if (scoreB > scoreA) roundWinnerSeat = 1;

    const roundWinnerFor = seat => {
      if (roundWinnerSeat === null) return "draw";
      return roundWinnerSeat === seat ? "you" : "opp";
    };


    // round_end payload — includes everything needed for the
    // round-results screen: per-seat score, hearts, name, country,
    // avatar, reason and the running totals.
    for (let seat = 0; seat < 2; seat++) {

      const p =
        this.players.get(
          match.players[seat]
        );

      if (!p) continue;

      const oppSeat = seat === 0 ? 1 : 0;

      const youScore = seat === 0 ? scoreA : scoreB;
      const oppScore = seat === 0 ? scoreB : scoreA;
      const youReason = seat === 0 ? reasonA : reasonB;
      const oppReason = seat === 0 ? reasonB : reasonA;
      const youHearts = computeHearts(rs.misses[seat]);
      const oppHearts = computeHearts(rs.misses[oppSeat]);

      this.send(
        p,
        {
          t: "round_end",

          p: {
            r: rs.round,

            tot: totNow,

            you: {
              score: youScore,
              reason: youReason,
              nick: match.nicks[seat],
              cc: match.ccs[seat],
              av: match.avatars[seat],
              hearts: youHearts,
              hits: rs.hits[seat],
              misses: rs.misses[seat]
            },

            opp: {
              score: oppScore,
              reason: oppReason,
              nick: match.nicks[oppSeat],
              cc: match.ccs[oppSeat],
              av: match.avatars[oppSeat],
              hearts: oppHearts,
              hits: rs.hits[oppSeat],
              misses: rs.misses[oppSeat]
            },

            win:
              roundWinnerFor(seat),

            final:
              isFinalRound,

            now: Date.now()
          }
        }
      );
    }


    if (isFinalRound) {

      // ROUND_3_RESULT -> FINAL_RESULT -> FINISHED.
      match.state =
        "final";

      setTimeout(
        () =>
          this.finishMatch(match.id),
        FINAL_RESULT_DELAY
      );

    } else {

      // ROUND_N_RESULT -> NEXT_ROUND -> ROUND_N+1.
      match.state =
        "next_round";

      const nextRound =
        match.round + 1;

      const nextRoundAt =
        Date.now() + NEXT_ROUND_DELAY;

      match.nextRoundAt =
        nextRoundAt;

      this.broadcast(
        match,
        {
          t: "next_round",

          r: rs.round,

          next: nextRound,

          rounds: TOTAL_ROUNDS,

          tot: totNow,

          score: [
            scoreA,
            scoreB
          ],

          win: [
            roundWinnerFor(0),
            roundWinnerFor(1)
          ],

          players: [
            {
              seat: 0,
              nick: match.nicks[0],
              cc: match.ccs[0],
              av: match.avatars[0],
              score: scoreA,
              tot: totNow[0],
              hearts: computeHearts(rs.misses[0])
            },
            {
              seat: 1,
              nick: match.nicks[1],
              cc: match.ccs[1],
              av: match.avatars[1],
              score: scoreB,
              tot: totNow[1],
              hearts: computeHearts(rs.misses[1])
            }
          ],

          nextRoundAt,

          now: Date.now()
        }
      );

      setTimeout(
        () => {

          const m =
            this.matches.get(
              match.id
            );

          if (
            !m ||
            m.state !== "next_round" ||
            m.nextRoundAt !== nextRoundAt
          )
            return;

          this.startRound(m);

        },
        NEXT_ROUND_DELAY
      );
    }


    await this.save();
  }


  /* =======================================================
     MATCH END
     ======================================================= */

  async finishMatch(matchId) {

    const match =
      this.matches.get(
        matchId
      );


    if (!match)
      return;


    match.state =
      "ended";

    this.stopLiveSync(matchId);


    const a =
      match.scores[0];


    const b =
      match.scores[1];


    // Global winner from seat-0's perspective ("you" == seat 0
    // won, "opp" == seat 1 won, "draw" == tie). Reversed per seat
    // below so each player sees THEIR own outcome.
    let globalWinner =
      "draw";

    if (a > b) globalWinner = "you";
    if (b > a) globalWinner = "opp";


    const playersInfo = [
      {
        seat: 0,
        nick: match.nicks[0],
        cc: match.ccs[0],
        av: match.avatars[0],
        tot: a
      },
      {
        seat: 1,
        nick: match.nicks[1],
        cc: match.ccs[1],
        av: match.avatars[1],
        tot: b
      }
    ];


    for (
      let seat = 0;
      seat < 2;
      seat++
    ) {

      const p =
        this.players.get(
          match.players[seat]
        );


      if (!p)
        continue;


      // Per-player win/lose. seat 0 mirrors globalWinner
      // directly; seat 1 is inverted, so the winning player
      // always reads "you" and the losing player always reads
      // "opp" (both with the correct match outcome).
      let win = globalWinner;

      if (globalWinner !== "draw") {

        win =
          globalWinner ===
          (seat === 0 ? "you" : "opp")
            ? "you"
            : "opp";
      }


      this.send(
        p,
        {
          t: "match_end",

          p: {
            win,

            tot: [
              a,
              b
            ],

            rounds:
              match.rounds,

            players: playersInfo,

            reason:
              "completed",

            now: Date.now()
          }
        }
      );
    }


    await this.save();


    // Immediately release players so a "Play again" flow works.
    setTimeout(
      () => {

        for (
          const pid of match.players
        ) {

          const p =
            this.players.get(pid);

          if (
            p &&
            p.matchId === matchId
          ) {

            p.matchId =
              null;

            p.seat =
              null;
          }
        }

        this.save();

      },
      POST_MATCH_REQUEUE_DELAY
    );


    // Keep match briefly then purge so stale state can't affect
    // any future matchmaking.
    setTimeout(
      () => {

        const m =
          this.matches.get(
            matchId
          );

        if (!m) return;

        this.matches.delete(
          matchId
        );

        this.stopLiveSync(matchId);

        this.save();

      },
      MATCH_RETENTION_AFTER_END
    );
  }


  /* =======================================================
     RECONNECT / RESUME
     ======================================================= */

  async handleDisconnect(player) {

    if (!player.matchId) {

      this.queue =
        this.queue.filter(
          id =>
            id !== player.id
        );

      this.players.delete(
        player.id
      );

      await this.save();

      return;
    }


    const match =
      this.matches.get(
        player.matchId
      );


    if (!match) {

      this.players.delete(
        player.id
      );

      return;
    }


    if (match.state === "ended") {

      this.players.delete(
        player.id
      );

      await this.save();

      return;
    }


    match.disconnected.set(
      player.seat,
      Date.now()
    );


    const opponent =
      this.players.get(
        match.players[
          player.seat === 0
            ? 1
            : 0
        ]
      );


    if (opponent) {

      this.send(
        opponent,
        {
          t: "opp_left"
        }
      );
    }


    setTimeout(
      async () => {

        const m =
          this.matches.get(
            match.id
          );

        if (!m) return;


        const lostAt =
          m.disconnected.get(
            player.seat
          );


        if (!lostAt) return;


        const current =
          this.players.get(
            player.id
          );


        if (
          current &&
          current.connected
        ) {
          return;
        }


        const otherSeat =
          player.seat === 0
            ? 1
            : 0;


        const other =
          this.players.get(
            m.players[
              otherSeat
            ]
          );


        if (other) {

          this.send(
            other,
            {
              t: "opp_left",
              final: true
            }
          );


          this.send(
            other,
            {
              t: "match_end",

              p: {
                win: "you",

                tot: m.scores,

                rounds: m.rounds,

                players: [
                  {
                    seat: 0,
                    nick: m.nicks[0],
                    cc: m.ccs[0],
                    av: m.avatars[0],
                    tot: m.scores[0]
                  },
                  {
                    seat: 1,
                    nick: m.nicks[1],
                    cc: m.ccs[1],
                    av: m.avatars[1],
                    tot: m.scores[1]
                  }
                ],

                reason:
                  "opponent_left",

                now: Date.now()
              }
            }
          );
        }


        m.state =
          "ended";


        for (const pid of m.players) {
          const pl = this.players.get(pid);
          if (pl && pl.matchId === m.id) {
            pl.matchId = null;
            pl.seat = null;
          }
        }


        this.stopLiveSync(m.id);

        this.matches.delete(
          m.id
        );

        this.players.delete(
          player.id
        );

        await this.save();

      },
      RECONNECT_GRACE
    );


    await this.save();
  }


  async handleResume(
    player,
    msg
  ) {

    const matchId =
      String(msg.m || "");


    const pid =
      String(msg.pid || "");


    const match =
      this.matches.get(
        matchId
      );


    if (!match) {

      this.send(
        player,
        {
          t: "resume_fail"
        }
      );

      return;
    }


    if (match.state === "ended") {

      this.send(
        player,
        {
          t: "resume_fail"
        }
      );

      return;
    }


    const oldPlayer =
      this.players.get(pid);


    if (
      !oldPlayer ||
      !match.players.includes(pid)
    ) {

      this.send(
        player,
        {
          t: "resume_fail"
        }
      );

      return;
    }


    if (
      oldPlayer.matchId &&
      oldPlayer.matchId !== match.id &&
      oldPlayer.connected
    ) {

      this.send(
        player,
        {
          t: "resume_fail"
        }
      );

      return;
    }


    const seat =
      match.players.indexOf(
        pid
      );


    oldPlayer.ws =
      player.ws;

    oldPlayer.connected =
      true;

    oldPlayer.matchId =
      match.id;

    oldPlayer.seat =
      seat;


    this.players.delete(
      player.id
    );


    match.disconnected.delete(
      seat
    );


    const rs = match.roundState;

    // Per-seat snapshot so the reconnected player's HUD reflects
    // live hearts / score / taps immediately.
    const playersLive = [
      {
        seat: 0,
        nick: match.nicks[0],
        cc: match.ccs[0],
        av: match.avatars[0],
        tot: match.scores[0],
        hearts: rs ? computeHearts(rs.misses[0]) : MAX_HEARTS,
        score: rs ? calculateScore(rs.hits[0], rs.misses[0], rs.spec) : 0,
        hits: rs ? rs.hits[0] : 0,
        misses: rs ? rs.misses[0] : 0,
        done: rs ? rs.done[0] : false
      },
      {
        seat: 1,
        nick: match.nicks[1],
        cc: match.ccs[1],
        av: match.avatars[1],
        tot: match.scores[1],
        hearts: rs ? computeHearts(rs.misses[1]) : MAX_HEARTS,
        score: rs ? calculateScore(rs.hits[1], rs.misses[1], rs.spec) : 0,
        hits: rs ? rs.hits[1] : 0,
        misses: rs ? rs.misses[1] : 0,
        done: rs ? rs.done[1] : false
      }
    ];


    this.send(
      oldPlayer,
      {
        t: "resume_ok",

        m: match.id,
        roomId: match.id,

        you: seat,

        opp: {
          nick:
            match.nicks[
              seat === 0 ? 1 : 0
            ],
          cc:
            match.ccs[
              seat === 0 ? 1 : 0
            ],
          av:
            match.avatars[
              seat === 0 ? 1 : 0
            ]
        },

        players: playersLive,

        rounds:
          TOTAL_ROUNDS,

        tot:
          match.scores,

        r:
          match.round,

        st:
          match.state,

        nextRoundAt:
          match.state === "next_round"
            ? match.nextRoundAt
            : null,

        now:
          Date.now(),

        rs:
          rs
            ? {
                r: rs.round,
                spec: rs.spec,
                seed: rs.seed,
                reveal: rs.spec.reveal,
                recall: rs.spec.recall,
                startedAt: rs.startedAt,
                endsAt: rs.deadline,
                yourTaps: [ ...rs.taps[seat] ],
                yourHits: rs.hits[seat],
                yourMisses: rs.misses[seat],
                yourHearts: computeHearts(rs.misses[seat]),
                yourScore: calculateScore(rs.hits[seat], rs.misses[seat], rs.spec),
                yourDone: rs.done[seat]
              }
            : null
      }
    );


    const opponent =
      this.players.get(
        match.players[
          seat === 0
            ? 1
            : 0
        ]
      );


    if (opponent) {

      this.send(
        opponent,
        {
          t: "opp_back"
        }
      );
    }


    // Re-arm the live-sync loop so the resumed client keeps seeing
    // the opponent's hearts / score.
    if (match.state === "round") {
      this.startLiveSync(match);
    }


    await this.save();
  }


  /* =======================================================
     LEAVE MATCH
     ======================================================= */

  async leaveMatch(player) {

    if (!player.matchId)
      return;


    const match =
      this.matches.get(
        player.matchId
      );


    if (!match)
      return;


    const otherSeat =
      player.seat === 0
        ? 1
        : 0;


    const other =
      this.players.get(
        match.players[
          otherSeat
        ]
      );


    if (other) {

      this.send(
        other,
        {
          t: "match_end",

          p: {
            win: "you",

            tot:
              match.scores,

            rounds:
              match.rounds,

            players: [
              {
                seat: 0,
                nick: match.nicks[0],
                cc: match.ccs[0],
                av: match.avatars[0],
                tot: match.scores[0]
              },
              {
                seat: 1,
                nick: match.nicks[1],
                cc: match.ccs[1],
                av: match.avatars[1],
                tot: match.scores[1]
              }
            ],

            reason:
              "opponent_left",

            now: Date.now()
          }
        }
      );

    }


    match.state =
      "ended";

    this.stopLiveSync(match.id);


    this.matches.delete(
      match.id
    );


    player.matchId =
      null;


    player.seat =
      null;


    await this.save();
  }


  /* =======================================================
     SOCKET HELPERS
     ======================================================= */

  send(player, data) {

    if (
      !player ||
      !player.ws ||
      !player.connected
    )
      return;


    try {

      player.ws.send(
        JSON.stringify(data)
      );

    } catch (e) {}
  }


  broadcast(match, data) {

    for (
      const pid of match.players
    ) {

      const p =
        this.players.get(pid);

      if (p) {

        this.send(
          p,
          data
        );

      }
    }
  }


  broadcastExcept(
    match,
    exceptId,
    data
  ) {

    for (
      const pid of match.players
    ) {

      if (pid === exceptId)
        continue;


      const p =
        this.players.get(pid);

      if (p) {

        this.send(
          p,
          data
        );

      }
    }
  }
}


/* =========================================================
   GAME / BOARD HELPERS
   ========================================================= */

const MP_AVATARS = [
  "brain",
  "cube",
  "robot",
  "fox",
  "penguin",
  "bolt",
  "owl",
  "cat",
  "dragon",
  "astro",
  "ninja"
];


function randomSeed() {

  return Math.floor(
    Math.random() *
    0x7fffffff
  );
}


function mixSeed(a, b) {

  let x =
    (a ^ (
      b * 0x45d9f3b
    )) >>> 0;


  x =
    Math.imul(
      x ^ (x >>> 16),
      0x45d9f3b
    ) >>> 0;


  x ^=
    x >>> 16;


  return x >>> 0;
}


function rng(seed) {

  let x =
    seed >>> 0;


  return function() {

    x =
      Math.imul(
        x ^ (x >>> 16),
        0x45d9f3b
      ) >>> 0;

    x ^=
      x >>> 16;

    return (
      x >>> 0
    ) / 4294967296;
  };
}


function createRoundSpec(round) {

  const size =
    Math.min(
      3 + round,
      7
    );


  return {

    size,

    reveal:
      Math.max(
        900,
        1800 -
        round * 120
      ),

    recall:
      Math.max(
        2500,
        6000 -
        round * 400
      ),

    count:
      Math.min(
        2 + round,
        Math.floor(
          size * size * 0.45
        )
      ),

    round
  };
}


function deriveBoard(
  spec,
  seed
) {

  const total =
    spec.size *
    spec.size;


  const random =
    rng(seed);


  const indexes =
    Array.from(
      {
        length: total
      },
      (_, i) => i
    );


  // Fisher-Yates shuffle
  for (
    let i = total - 1;
    i > 0;
    i--
  ) {

    const j =
      Math.floor(
        random() *
        (i + 1)
      );


    [
      indexes[i],
      indexes[j]
    ] =
    [
      indexes[j],
      indexes[i]
    ];
  }


  const required =
    new Set(
      indexes.slice(
        0,
        spec.count
      )
    );


  return {
    required
  };
}


function calculateScore(
  hits,
  misses,
  spec
) {

  const base =
    hits * 100;


  const missPenalty =
    misses * 25;


  return Math.max(
    0,
    base -
    missPenalty
  );
}