// Memory Matrix — Cloudflare Worker + Durable Object
// Automatic matchmaking + realtime multiplayer
// No room IDs / invite codes required.

const MAX_PLAYERS_PER_MATCH = 2;
const TOTAL_ROUNDS = 5;
const QUEUE_TIMEOUT = 60_000;
const RECONNECT_GRACE = 15_000;
const ROUND_TIME = 15_000;

// --- Profile validation defaults (added) ---
const DEFAULT_NICK = "Player";
const DEFAULT_CC = "XX";
const NICK_MAX_LEN = 16;
const CC_REGEX = /^[A-Za-z]{2}$/;

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

    this.loaded = false;
  }


  async load() {
    if (this.loaded) return;
    this.loaded = true;

    const data = await this.state.storage.get("state");

    if (!data) return;

    this.queue = data.queue || [];
    this.matches = new Map(data.matches || []);
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
            // Echo the client's send time (if provided) so the
            // client can compute round-trip time and a clock
            // offset: offset = now - (echoedAt + rtt/2).
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

    // Prevent duplicate queueing
    if (
      this.queue.includes(player.id)
    ) {
      return;
    }


    if (player.matchId) {

      this.send(
        player,
        {
          t: "error",
          code: "ALREADY_IN_MATCH"
        }
      );

      return;
    }


    player.avatar =
      MP_AVATARS.includes(msg.av)
        ? msg.av
        : "brain";


    // Safely receive nick + cc from "queue" message (added)
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

    // Remove disconnected players
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

      // Stored opponent profile fields (added)
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


    this.send(
      a,
      {
        t: "matched",

        m: matchId,

        you: 0,

        opp: {
          nick: b.nick,
          cc: b.cc,
          av: b.avatar
        },

        rounds: TOTAL_ROUNDS,

        now: Date.now()
      }
    );


    this.send(
      b,
      {
        t: "matched",

        m: matchId,

        you: 1,

        opp: {
          nick: a.nick,
          cc: a.cc,
          av: a.avatar
        },

        rounds: TOTAL_ROUNDS,

        now: Date.now()
      }
    );


    // Countdown
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
            n: 3
          }
        );


        setTimeout(
          () => this.startRound(m),
          3000
        );

      },
      200
    );


    await this.save();
  }


  /* =======================================================
     ROUND CREATION
     ======================================================= */

  async startRound(match) {

    if (
      !match ||
      match.round >= TOTAL_ROUNDS
    ) {
      return;
    }


    match.round++;

    match.state =
      "round";


    const spec =
      createRoundSpec(
        match.round
      );


    const roundSeed =
      mixSeed(
        match.seed,
        match.round
      );


    match.roundState = {

      round:
        match.round,

      spec,

      seed:
        roundSeed,

      startedAt:
        Date.now(),

      deadline:
        Date.now() + ROUND_TIME,

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


    this.broadcast(
      match,
      {
        t: "round",

        r: match.round,

        spec,

        seed: roundSeed,

        reveal: spec.reveal,

        recall: spec.recall,

        // Legacy duration field (kept for compatibility).
        deadline:
          ROUND_TIME,

        // Server-authoritative absolute timestamps (ms, epoch).
        // Both players resolve remaining time from the SAME
        // startedAt/endsAt pair instead of starting an independent
        // local timer on message-receipt, which is what caused
        // players to see different remaining time under latency.
        startedAt:
          match.roundState.startedAt,

        endsAt:
          match.roundState.deadline,

        // Server clock at send time, so the client can measure its
        // own clock offset (offset = now - Date.now() on receipt)
        // and apply that offset when computing remaining time.
        now:
          Date.now()
      }
    );


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
      match.state !== "round"
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
          i: msg.i
        }
      );

      return;
    }


    if (
      msg.r !== rs.round
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
          i: index
        }
      );

      return;
    }


    const seat =
      player.seat;


    // Ignore duplicate taps
    if (
      rs.taps[seat].has(index)
    ) {
      return;
    }


    rs.taps[seat].add(
      index
    );


    const board =
      deriveBoard(
        rs.spec,
        rs.seed
      );


    const isHit =
      board.required.has(index);


    if (isHit) {

      rs.hits[seat]++;

    } else {

      rs.misses[seat]++;

    }


    // Relay opponent event
    this.broadcastExcept(
      match,
      player.id,
      {
        t: "opp",

        r: rs.round,

        i: index,

        k:
          isHit
            ? "ok"
            : "miss",

        s:
          rs.hits[seat] +
          rs.misses[seat]
      }
    );


    // Inform player of corrected result
    this.send(
      player,
      {
        t: "fix",

        r: rs.round,

        i: index,

        k:
          isHit
            ? "ok"
            : "miss"
      }
    );


    // Finish automatically when all required tiles found
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


    if (!rs)
      return;


    if (
      msg.r !== rs.round
    )
      return;


    const board =
      deriveBoard(
        rs.spec,
        rs.seed
      );


    // Only accept "done" if player actually
    // found every required tile.
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
      ]

    });


    // Per-seat round reason (added).
    // Assumption: "completed" = found all required tiles,
    // "no_attempt" = zero taps registered, otherwise "timeout".
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

    // round_end payload wrapped in "p" for mpRoundEnd(m) compatibility.
    // Each player receives their own "you"/"opp" perspective.
    for (let seat = 0; seat < 2; seat++) {

      const p =
        this.players.get(
          match.players[seat]
        );

      if (!p) continue;

      const youScore = seat === 0 ? scoreA : scoreB;
      const oppScore = seat === 0 ? scoreB : scoreA;
      const youReason = seat === 0 ? reasonA : reasonB;
      const oppReason = seat === 0 ? reasonB : reasonA;

      this.send(
        p,
        {
          t: "round_end",

          p: {
            r: rs.round,

            tot: totNow,

            you: {
              score: youScore,
              reason: youReason
            },

            opp: {
              score: oppScore,
              reason: oppReason
            },

            now: Date.now()
          }
        }
      );
    }


    if (
      match.round >= TOTAL_ROUNDS
    ) {

      setTimeout(
        () =>
          this.finishMatch(match.id),
        1200
      );

    } else {

      setTimeout(
        () => {

          const m =
            this.matches.get(
              match.id
            );

          if (!m) return;

          this.startRound(m);

        },
        1200
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


    const a =
      match.scores[0];


    const b =
      match.scores[1];


    let winner =
      "draw";


    if (a > b)
      winner = "you";

    if (b > a)
      winner = "opp";


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


      let win =
        winner;


      if (winner !== "draw") {

        win =
          winner ===
          (seat === 0
            ? "you"
            : "opp")
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

            reason:
              "completed",

            now: Date.now()
          }
        }
      );
    }


    await this.save();


    // Keep match briefly for reconnect/result delivery
    setTimeout(
      () => {

        const m =
          this.matches.get(
            matchId
          );

        if (!m) return;


        for (
          const pid of m.players
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


        this.matches.delete(
          matchId
        );


        this.save();

      },
      30_000
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


        // Player permanently left
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

                reason:
                  "opponent_left"
              }
            }
          );
        }


        m.state =
          "ended";


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


    // Remove temporary socket identity
    this.players.delete(
      player.id
    );


    match.disconnected.delete(
      seat
    );


    this.send(
      oldPlayer,
      {
        t: "resume_ok",

        m: match.id,

        you: seat,

        opp: {
          nick:
            match.nicks
              ? match.nicks[
                  seat === 0
                    ? 1
                    : 0
                ]
              : DEFAULT_NICK,

          cc:
            match.ccs
              ? match.ccs[
                  seat === 0
                    ? 1
                    : 0
                ]
              : DEFAULT_CC,

          av:
            match.avatars[
              seat === 0
                ? 1
                : 0
            ]
        },

        rounds:
          TOTAL_ROUNDS,

        tot:
          match.scores,

        r:
          match.round,

        st:
          match.state,

        // Server clock at send time (clock-offset calibration).
        now:
          Date.now(),

        // Full in-progress round state, so a reconnecting client
        // can rebuild the exact board/timer instead of starting a
        // fresh (and now desynced) round. Null when no round is
        // currently active (e.g. between rounds or match ended).
        rs:
          match.roundState
            ? {
                r:
                  match.roundState.round,

                spec:
                  match.roundState.spec,

                seed:
                  match.roundState.seed,

                reveal:
                  match.roundState.spec.reveal,

                recall:
                  match.roundState.spec.recall,

                startedAt:
                  match.roundState.startedAt,

                endsAt:
                  match.roundState.deadline,

                // This player's own progress so far, so taps
                // already made are not re-requested or duplicated.
                yourTaps: [
                  ...match.roundState.taps[
                    seat
                  ]
                ],

                yourHits:
                  match.roundState.hits[
                    seat
                  ],

                yourMisses:
                  match.roundState.misses[
                    seat
                  ],

                yourDone:
                  match.roundState.done[
                    seat
                  ]
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

            reason:
              "opponent_left"
          }
        }
      );

    }


    match.state =
      "ended";


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
