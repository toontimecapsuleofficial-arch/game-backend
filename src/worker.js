// Memory Matrix — Cloudflare Worker + Durable Object
// Automatic matchmaking + realtime multiplayer
// No room IDs / invite codes required.

const MAX_PLAYERS_PER_MATCH = 2;
const TOTAL_ROUNDS = 5;            /* exactly 3 rounds: ROUND_1 → ROUND_2 → ROUND_3 → FINAL_RESULT → FINISHED */
const QUEUE_TIMEOUT = 60_000;
const RECONNECT_GRACE = 15_000;
const ROUND_TIME = 15_000;
const TRANSITION_MS = 1_500;       /* the 5-second result transition.
                                      It starts ONLY AFTER ROUND_RESULT (round_end) has been
                                      computed, persisted and sent — it never delays scoring.
                                      nextRoundAt = finishTime + TRANSITION_MS (server clock). */

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

    /* rehydrate the Set/Map structures that were serialized on save() */
    for (const m of this.matches.values()) reviveMatch(m);
  }


  async save() {
    /* JSON-safe snapshot: roundState.taps are Sets and
       match.disconnected is a Map — a naive JSON round-trip would
       silently turn them into plain objects and break resume data.
       Serialize them explicitly so the stored state is reliable. */
    await this.state.storage.put("state", {
      queue: this.queue,
      matches: [...this.matches.entries()].map(([id, m]) => [id, serializeMatch(m)])
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

        /* Heartbeats arrive continuously from both phones — a cheap
           moment to heal a lost transition timer, so a match can
           never stall between rounds even with zero gameplay input. */
        if (player.matchId) {

          await this.syncAdvance(
            this.matches.get(
              player.matchId
            )
          );
        }
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


      case "profile":
        await this.handleProfile(
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

      /* authoritative round-result snapshots (per seat), used for
         instant resume + replay delivery after reconnects */
      lastEnd: null,

      /* authoritative final-result snapshots (per seat) */
      final: null,

      /* server-clock timestamp at which the next round starts
         (ROUND_RESULT + TRANSITION_MS). 0 when no transition pending */
      nextRoundAt: 0,

      /* pre-generated spec/seed/board for the upcoming round so
         ROUND_START needs no expensive calculation after the transition */
      pending: null,

      /* tagged timers (never fire into the wrong round) */
      deathTimer: null,
      nextTimer: null,

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

        pid: a.id,

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

        pid: b.id,

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


    /* A round may only start from the pre-round countdown or from a
       completed previous round's transition. Any stale/duplicate
       trigger (an old timer firing after the match moved on) is
       ignored — it can never corrupt an active round. */
    if (
      match.state !==
        "countdown" &&
      match.state !==
        "round_end"
    ) {
      return;
    }


    match.state =
      "round";

    match.nextRoundAt = 0;

    if (match.nextTimer) {
      clearTimeout(match.nextTimer);
      match.nextTimer = null;
    }


    /* Use the spec/seed/board PRE-GENERATED during the previous
       5-second transition when available → ROUND_START needs no
       expensive calculation and both clients stay perfectly aligned.
       Otherwise generate now (first round, or a safety fallback). */
    let prep = null;

    if (
      match.pending &&
      match.pending.round === match.round + 1
    ) {
      prep = match.pending;
      match.pending = null;
    } else {
      prep = buildRoundData(
        match,
        match.round + 1
      );
    }


    match.round =
      prep.round;


    const spec =
      prep.spec;


    const roundSeed =
      prep.seed;


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
      ],

      /* why each seat was marked finished:
         "clear" (all tiles found) | "dead" (out of hearts) */
      doneReason: [
        null,
        null
      ],

      /* authoritative board for THIS round — identical to what both
         clients build from (spec, seed). Memoized here so every tap is
         validated against the same Set; required.size === spec tiles. */
      board:
        prep.board,

      need:
        prep.board.required.size,

      /* idempotency guard — a round is finalized exactly once */
      finalized:
        false

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


    /* Server-owned round timer — the SECURITY FALLBACK that ends the
       round when one/both players genuinely never finish. Tagged with
       this round's roundState object so a stale timer left over from a
       previous round can NEVER finalize a newer round early. */
    const rs =
      match.roundState;

    if (match.deathTimer)
      clearTimeout(match.deathTimer);

    match.deathTimer =
      setTimeout(
        () => {

          if (
            match.roundState !==
            rs
          )
            return;

          if (
            match.state !==
            "round"
          )
            return;

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


    /* If the round-advance timer was somehow lost during the
       transition, this activity heals the match first. */
    await this.syncAdvance(match);


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


    // This player already finished — their round is locked in.
    if (
      rs.done[seat]
    ) {
      return;
    }


    // Ignore duplicate taps
    if (
      rs.taps[seat].has(index)
    ) {
      return;
    }


    rs.taps[seat].add(
      index
    );


    // Validate against the round's memoized authoritative board —
    // the exact same Set the clients derived from (spec, seed).
    const isHit =
      rs.board.required.has(index);


    if (isHit) {

      rs.hits[seat]++;

    } else {

      rs.misses[seat]++;

    }


    // Relay opponent event (with live progress for the versus HUD)
    this.broadcastExcept(
      match,
      player.id,
      {
        t: "opp",

        r: rs.round,

        i: index,

        found:
          rs.hits[seat],

        req:
          rs.need,

        lives: Math.max(
          0,
          3 - rs.misses[seat]
        ),

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


    // The player has found every required tile → they are FINISHED.
    // Mark + finalize their actions immediately; never end the round
    // early for the rival — the round ends when BOTH players are done
    // (or at the server deadline, whichever comes first).
    if (
      !rs.done[seat] &&
      rs.hits[seat] >=
        rs.need
    ) {

      rs.done[seat] =
        true;

      rs.doneReason[seat] =
        "clear";

      await this.onPlayerDone(
        match,
        seat
      );
    }
  }


  /* =====================================================
     PLAYER FINISHED — finalize that player's actions now.
     If the rival is not finished yet we wait ONLY for the
     rival (never for an artificial timer). The instant both
     are finished the round is scored and ROUND_RESULT is sent.
     ===================================================== */

  async onPlayerDone(match, seat) {

    const rs =
      match.roundState;


    if (!rs) return;


    if (
      rs.done[0] &&
      rs.done[1]
    ) {

      // BOTH finished → scores NOW, no fixed delay.
      await this.finishRound(
        match.id
      );

      return;
    }


    // Only this seat finished so far → acknowledge so its client can
    // legitimately show "WAITING FOR RIVAL" until round_end arrives.
    const p =
      this.players.get(
        match.players[seat]
      );


    if (p) {

      this.send(
        p,
        {
          t: "waiting",

          r: rs.round
        }
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


    /* Heal a lost transition timer before evaluating this action. */
    await this.syncAdvance(match);


    const rs =
      match.roundState;


    if (!rs)
      return;


    if (
      match.state !== "round"
    )
      return;


    // Stale / wrong-round completion → drop silently.
    if (
      msg.r !== rs.round
    )
      return;


    // Duplicate completion → already finalized for this player.
    if (
      rs.done[player.seat]
    )
      return;


    /* The client truthfully reports it can no longer act this round:
       "clear" (it tapped every required tile — the tap path usually
       marks this first) or "dead" (it ran out of hearts). We trust the
       ACTION, never a client score: this player's authoritative round
       score is recomputed purely from server-validated taps.
       Accepting "dead" here is what removes the old 5–8 s stall —
       previously these players were never marked done, so the round
       always ran out the full server timer. */
    rs.done[player.seat] =
      true;

    rs.doneReason[player.seat] =
      msg.reason === "dead"
        ? "dead"
        : "clear";


    await this.onPlayerDone(
      match,
      player.seat
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


    /* Idempotency: wrong state, missing round or an already-finalized
       round (duplicate finish, both-done race, stale death timer) all
       no-op here. The round is scored EXACTLY ONCE. */
    if (
      !rs ||
      rs.finalized ||
      match.state !== "round"
    )
      return;


    rs.finalized = true;

    match.state =
      "round_end";


    if (match.deathTimer) {
      clearTimeout(match.deathTimer);
      match.deathTimer = null;
    }


    /* ---- AUTHORITATIVE SCORING — happens IMMEDIATELY, before any
       transition timer exists. Both round scores are computed from
       server-validated taps only, totals updated in the same tick. ---- */
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


    // Per-seat round reason — vocabulary matches the client's card:
    // "clear" | "dead" (player finished), "timeout" (acted but ran
    // out of time), "no_attempt" (never tapped).
    const roundReason = seat => {
      if (rs.done[seat]) return rs.doneReason[seat] || "clear";
      return "timeout";
    };

    const reasonA = roundReason(0);
    const reasonB = roundReason(1);


    // Round winner / tie — seat-based, identical on both phones.
    const roundWin =
      scoreA === scoreB
        ? "tie"
        : scoreA > scoreB
          ? "a"
          : "b";


    const isFinal =
      match.round >= TOTAL_ROUNDS;


    /* ---- nextRoundAt: SERVER TIME. The 5-second transition starts
       NOW, after scoring, and the next round (or FINAL_RESULT) fires
       exactly at nextRoundAt. Clients render the transition purely as
       a visual countdown off this authoritative timestamp.
       `now` is sampled once so payload.now and nextRoundAt are
       perfectly consistent for client clock-offset math. ---- */
    const now =
      Date.now();

    const nextRoundAt =
      now + TRANSITION_MS;

    match.nextRoundAt =
      nextRoundAt;


    /* ---- PRE-GENERATE the next round's spec/seed/board while the
       5-second transition runs → ROUND_START at nextRoundAt requires
       no expensive calculation and no storage work. ---- */
    if (!isFinal) {

      match.pending =
        buildRoundData(
          match,
          match.round + 1
        );

    }


    const totNow = [
      match.scores[0],
      match.scores[1]
    ];


    /* ---- ROUND_RESULT (round_end) — sent IMMEDIATELY to both
       players, before the transition. Carries every authoritative
       field both clients need to update instantly + resume/replay. ---- */
    const payloads = [];

    for (let seat = 0; seat < 2; seat++) {

      const youScore = seat === 0 ? scoreA : scoreB;
      const oppScore = seat === 0 ? scoreB : scoreA;
      const youReason = seat === 0 ? reasonA : reasonB;
      const oppReason = seat === 0 ? reasonB : reasonA;

      payloads[seat] = {

        r: rs.round,

        // Both updated TOTAL scores (seat order: [A, B])
        tot: totNow,

        // This phone's round score + why
        you: {
          score: youScore,
          reason: youReason
        },

        // Rival round score + why
        opp: {
          score: oppScore,
          reason: oppReason
        },

        // Both round scores (seat order) + round winner/tie
        scores: [
          scoreA,
          scoreB
        ],

        win: roundWin,

        // Identities on both sides (display-only, server-sanitized)
        youSeat: seat,
        nicks: match.nicks,
        ccs: match.ccs,
        avs: match.avatars,

        // What's next: the next round number, 0 when the match
        // moves to FINAL_RESULT at nextRoundAt
        next: isFinal
          ? 0
          : rs.round + 1,

        maxR: TOTAL_ROUNDS,

        // Server-clock transition target + server clock at send.
        nextRoundAt,

        st: "round_end",

        now
      };
    }


    // Snapshots for instant resume/replay delivery.
    match.lastEnd = payloads;


    for (let seat = 0; seat < 2; seat++) {

      const p =
        this.players.get(
          match.players[seat]
        );

      if (!p) continue;

      this.send(
        p,
        {
          t: "round_end",
          p: payloads[seat]
        }
      );
    }


    /* ---- Persist match/round/scores IMMEDIATELY after the result —
       never inside or after the transition. ---- */
    await this.save();


    /* ---- The ONLY use of the 5 seconds: visual transition pacing.
       At nextRoundAt either ROUND_START (pre-generated) fires, or —
       after round 3 — the authoritative FINAL_RESULT. Tagged + state
       checked so it can never leak into a wrong round. ---- */
    if (match.nextTimer)
      clearTimeout(match.nextTimer);

    match.nextTimer =
      setTimeout(
        () => {

          match.nextTimer = null;

          const m =
            this.matches.get(
              matchId
            );

          if (!m) return;

          if (
            m.state !==
            "round_end"
          )
            return;

          if (isFinal) {

            this.finishMatch(
              m.id
            );

          } else {

            this.startRound(m);

          }

        },
        TRANSITION_MS
      );
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
      winner = "a";          // seat-based, identical on both phones

    if (b > a)
      winner = "b";


    const now =
      Date.now();


    /* Build the authoritative FINAL_RESULT once per seat, then store
       it — reconnects during/after the result get the same payload
       replayed instead of a client-side guess. */
    const payloads = [];

    for (
      let seat = 0;
      seat < 2;
      seat++
    ) {

      const win =
        winner === "draw"
          ? "tie"
          : winner ===
              (seat === 0
                ? "a"
                : "b")
            ? "you"
            : "opp";


      payloads[seat] = {

        win,

        tot: [
          a,
          b
        ],

        rounds:
          match.rounds,

        nicks:
          match.nicks,

        ccs:
          match.ccs,

        reason:
          "completed",

        st: "ended",

        now
      };
    }


    match.final =
      payloads;


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


      this.send(
        p,
        {
          t: "match_end",

          p: payloads[seat]
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


  /* =====================================================
     TRANSITION SELF-HEAL — the round-advance timer is the
     ONLY thing that moves round_end → next round / final
     result. If that in-memory timer was ever lost (e.g. the
     Durable Object was restarted mid-transition), any player
     activity arriving after nextRoundAt advances the match
     EXACTLY ONCE. Never closes sockets, never resets player
     state — it only advances the round.
     ===================================================== */

  async syncAdvance(match) {

    if (!match)
      return;

    if (
      match.state !== "round_end"
    )
      return;

    if (
      !match.nextRoundAt ||
      Date.now() <
        match.nextRoundAt
    )
      return;


    if (match.nextTimer) {
      clearTimeout(match.nextTimer);
      match.nextTimer = null;
    }


    if (
      match.round >= TOTAL_ROUNDS
    ) {

      await this.finishMatch(
        match.id
      );

    } else {

      await this.startRound(
        match
      );
    }
  }


  /* =======================================================
     RECONNECT / RESUME
     ======================================================= */

  async handleDisconnect(player) {

    /* After a resume/identity swap, the socket's close handler still
       references the (already deleted) temporary identity the socket
       was opened with. Resolve the REAL player who owns this socket so
       a genuine disconnect is always attributed to the right match
       seat — otherwise a resumed player could vanish silently without
       ever triggering the reconnect-grace flow. */
    if (!player.matchId) {

      for (const p of this.players.values()) {

        if (
          p.ws === player.ws &&
          p.matchId
        ) {

          player = p;

          /* the flag was flipped on the temporary identity — make
             sure the REAL player is also marked disconnected, so the
             reconnect-grace timer evaluates the true state */
          p.connected = false;

          break;
        }
      }
    }


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
          t: "opp_left",

          grace:
            Math.round(
              RECONNECT_GRACE /
                1000
            )
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


    let match =
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


    /* If the resume lands after nextRoundAt but the advance timer was
       lost, advance first so the snapshot below is LIVE state, not a
       stale transition. */
    await this.syncAdvance(match);


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


    /* SAME-SOCKET RESUME GUARD — when the player re-syncs over the
       LIVE socket they already own (our round-transition recovery
       path), oldPlayer IS player. Never swap the ws and NEVER delete
       the identity from this.players: doing so silently cuts the
       player out of every future broadcast (ROUND_START etc.) and is
       exactly what made a healthy phone miss Round 2. For a genuinely
       NEW socket, adopt the old identity onto the new socket and drop
       the temporary identity — one identity per player, always. */
    const selfResume =
      oldPlayer === player;

    oldPlayer.ws =
      player.ws;

    oldPlayer.connected =
      true;

    oldPlayer.matchId =
      match.id;

    oldPlayer.seat =
      seat;


    if (!selfResume) {

      // Remove the temporary socket identity (new connection
      // replacing a dropped one).
      this.players.delete(
        player.id
      );

    }


    match.disconnected.delete(
      seat
    );


    /* Opportunistic display-metadata refresh (client sends nick/cc on
       resume). Server-sanitized; display-only. */
    if (typeof msg.nick !== "undefined") {

      oldPlayer.nick =
        sanitizeNick(msg.nick);

      oldPlayer.cc =
        sanitizeCC(msg.cc);

      if (match.nicks) {

        match.nicks[seat] =
          oldPlayer.nick;

        match.ccs[seat] =
          oldPlayer.cc;
      }
    }


    // ---- Build the authoritative round snapshot for resume ----
    // Uses the field names the client's resume path consumes, and is
    // derived purely from server-validated data (taps/hits/misses).
    const rs =
      match.roundState;

    let roundSnap =
      null;

    if (
      rs &&
      match.state === "round"
    ) {

      const found = [];
      const decoy = [];

      for (const idx of rs.taps[seat]) {

        if (rs.board.required.has(idx)) {

          found.push(idx);

        } else {

          decoy.push(idx);

        }
      }


      roundSnap = {

        r:
          rs.round,

        spec:
          rs.spec,

        seed:
          rs.seed,

        reveal:
          rs.spec.reveal,

        recall:
          rs.spec.recall,

        startedAt:
          rs.startedAt,

        endsAt:
          rs.deadline,

        deadlineLeft:
          Math.max(
            0,
            rs.deadline -
              Date.now()
          ),

        // This player's own progress so far, so taps already made
        // are not re-requested or duplicated.
        found,

        decoy,

        lives: Math.max(
          1,
          3 -
            rs.misses[seat]
        ),

        score:
          calculateScore(
            rs.hits[seat],
            rs.misses[seat],
            rs.spec
          ),

        yourTaps: [
          ...rs.taps[seat]
        ],

        yourHits:
          rs.hits[seat],

        yourMisses:
          rs.misses[seat],

        yourDone:
          rs.done[seat]
      };
    }


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

        // Latest authoritative ROUND_RESULT snapshot for reconnects
        // landing inside the 5-second transition (null otherwise) —
        // identical to what live clients received.
        end:
          match.state ===
            "round_end" &&
          match.lastEnd
            ? match.lastEnd[seat]
            : null,

        // Server-clock transition target so the resumed phone joins
        // the SAME countdown, not a locally invented one.
        nextRoundAt:
          match.nextRoundAt ||
          0,

        // Server clock at send time (clock-offset calibration).
        now:
          Date.now(),

        // Full in-progress round state (see above). Null when no
        // round is currently active (transition / countdown / ended).
        rs:
          roundSnap
      }
    );


    /* If the match already ended, replay the authoritative
       FINAL_RESULT immediately (the live delivery may have been
       missed entirely). */
    if (
      match.state === "ended" &&
      match.final
    ) {

      this.send(
        oldPlayer,
        {
          t: "match_end",
          p: match.final[seat]
        }
      );
    }


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
     PROFILE REFRESH — display-only metadata relay.
     A player may update nickname/country while queued or even
     mid-match; the rival's HUD/panels refresh from this.
     Never touches seeds, validation or scoring.
     ======================================================= */

  async handleProfile(player, msg) {

    player.nick =
      sanitizeNick(msg.nick);

    player.cc =
      sanitizeCC(msg.cc);


    if (!player.matchId) {

      await this.save();

      return;
    }


    const match =
      this.matches.get(
        player.matchId
      );


    if (!match) return;


    const seat =
      player.seat;


    if (
      match.nicks &&
      seat != null
    ) {

      match.nicks[seat] =
        player.nick;

      match.ccs[seat] =
        player.cc;

    }


    const other =
      this.players.get(
        match.players[
          seat === 0
            ? 1
            : 0
        ]
      );


    if (other) {

      this.send(
        other,
        {
          t: "opp_profile",

          nick: player.nick,

          cc: player.cc,

          av: player.avatar
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


/* =========================================================
   CLIENT-PARITY BOARD GENERATION
   ---------------------------------------------------------
   The versus board is authoritative data. Both clients build
   it from (spec, seed) using the game's OWN pattern code
   (mulberry32 + genPattern, identical draw order), and this
   worker re-derives the exact same Set to validate every tap.
   The previous implementation used a different PRNG and a
   different selection algorithm here, so the server's
   "required" tiles almost never matched the phones: hits were
   scored as misses, rounds never auto-completed and results
   waited for the full 15 s server timer. Keep these two
   functions byte-for-byte in sync with buildRound/genPattern
   in game.js (versus specs: waves=1, no decoys, no order,
   no shuffle — deterministic per (size, count, seed)).
   ========================================================= */

function mulberry32(a) {

  return function() {

    a |= 0;

    a =
      (a +
        0x6d2b79f5) |
      0;

    let t = Math.imul(
      a ^ (a >>> 15),
      1 | a
    );

    t =
      (t +
        Math.imul(
          t ^ (t >>> 7),
          61 | t
        )) ^
      t;

    return (
      (t ^ (t >>> 14)) >>>
      0
    ) / 4294967296;
  };
}


function clampNum(v, a, b) {

  return v < a
    ? a
    : v > b
      ? b
      : v;
}


/* Mirror of genPattern() in game.js with recency scoring
   disabled (client passes spec.noRecent for versus, so both
   sides skip the RECENT history identically → the board is a
   pure function of (size, count, seed) and resume can rebuild
   it any number of times without drifting). */
function genPattern(size, count, rngFn) {

  const cells =
    size * size;

  const pool = [];

  for (let i2 = 0; i2 < cells; i2++)
    pool.push(i2);

  count = clampNum(
    count,
    1,
    pool.length - 1
  );

  const tries = 24;

  let best = null;

  let bestScore =
    -1e9;

  for (let t = 0; t < tries; t++) {

    const arr =
      pool.slice();

    for (
      let i = arr.length - 1;
      i > 0;
      i--
    ) {

      const j =
        (rngFn() * (i + 1)) | 0;

      const tmp = arr[i];

      arr[i] = arr[j];

      arr[j] = tmp;
    }

    const cand = arr
      .slice(0, count)
      .sort((a, b) => a - b);

    let score = 0;

    const rows = {};
    const cols = {};
    const quad = [0, 0, 0, 0];

    for (
      let k = 0;
      k < cand.length;
      k++
    ) {

      const x = cand[k] % size;
      const y = (cand[k] / size) | 0;

      rows[y] = 1;
      cols[x] = 1;

      quad[
        (y < size / 2 ? 0 : 1) *
          2 +
          (x < size / 2 ? 0 : 1)
      ]++;
    }

    score +=
      (Object.keys(rows).length +
        Object.keys(cols).length) *
      3;

    let used = 0;

    for (let k = 0; k < 4; k++)
      if (quad[k] > 0) used++;

    score += used * 8;

    const mx = Math.max.apply(
      null,
      quad
    );

    const mn = Math.min.apply(
      null,
      quad
    );

    score -= (mx - mn) * 4;

    let maxRow = 0;
    let maxCol = 0;

    for (const k in rows) {

      let rr = 0;

      for (
        let mm = 0;
        mm < cand.length;
        mm++
      )
        if (
          ((cand[mm] / size) | 0) ===
          parseInt(k, 10)
        )
          rr++;

      if (rr > maxRow) maxRow = rr;
    }

    for (const k in cols) {

      let cc = 0;

      for (
        let mm = 0;
        mm < cand.length;
        mm++
      )
        if (
          cand[mm] % size ===
          parseInt(k, 10)
        )
          cc++;

      if (cc > maxCol) maxCol = cc;
    }

    score -=
      (maxRow + maxCol) *
      (count >= 4 ? 5 : 1);

    if (count >= 4) {

      let symH = true;
      let symV = true;
      let symR = true;

      for (
        let k = 0;
        k < cand.length;
        k++
      ) {

        const x = cand[k] % size;
        const y = (cand[k] / size) | 0;

        if (
          cand.indexOf(
            y * size + (size - 1 - x)
          ) < 0
        )
          symH = false;

        if (
          cand.indexOf(
            (size - 1 - y) * size + x
          ) < 0
        )
          symV = false;

        if (
          cand.indexOf(
            (size - 1 - y) * size +
              (size - 1 - x)
          ) < 0
        )
          symR = false;
      }

      if (symH) score -= 14;
      if (symV) score -= 14;
      if (symR) score -= 14;
    }

    if (count >= 3) {

      let diag = true;

      for (
        let k = 0;
        k < cand.length;
        k++
      ) {

        if (
          cand[k] % size !==
          ((cand[k] / size) | 0)
        ) {

          diag = false;

          break;
        }
      }

      if (diag) score -= 20;
    }

    if (score > bestScore) {

      bestScore = score;

      best = cand;
    }
  }

  return best;
}


/* Mirror of buildRound() in game.js restricted to the versus
   spec shape (single wave, no decoys) → the authoritative
   required-tile Set for one round. */
function deriveMpBoard(spec, seed) {

  const rngFn =
    mulberry32(seed >>> 0);

  const size = spec.size;

  const cells = size * size;

  const tilesCount =
    spec.tiles != null
      ? spec.tiles
      : spec.count != null
        ? spec.count
        : 3;

  const total = clampNum(
    tilesCount,
    2,
    cells - 2
  );

  const n = clampNum(
    Math.max(2, total),
    2,
    cells - 1
  );

  const tiles =
    genPattern(size, n, rngFn);

  return {
    required:
      new Set(tiles)
  };
}


/* Build everything one round needs — spec, seed and the
   authoritative board — in one cheap, pure step. Called when
   a round starts and, one round early, during the previous
   5-second transition (pre-generation). */
function buildRoundData(match, round) {

  const spec =
    createRoundSpec(round);

  const seed = mixSeed(
    match.seed,
    round
  );

  const board =
    deriveMpBoard(spec, seed);

  return {
    round,
    spec,
    seed,
    board
  };
}


/* ---- JSON-safe match snapshot helpers (Set/Map aware) ---- */

function serializeMatch(m) {

  const out = Object.assign(
    {},
    m
  );

  out.disconnected = [
    ...m.disconnected.entries()
  ];

  if (m.roundState) {

    out.roundState =
      Object.assign(
        {},
        m.roundState,
        {
          taps:
            m.roundState.taps.map(
              s => [...s]
            ),
          board: {
            required: [
              ...m.roundState.board
                .required
            ]
          }
        }
      );
  }

  if (m.pending) {

    out.pending =
      Object.assign(
        {},
        m.pending,
        {
          board: {
            required: [
              ...m.pending.board
                .required
            ]
          }
        }
      );
  }

  out.deathTimer = null;
  out.nextTimer = null;

  return out;
}


function reviveMatch(m) {

  m.disconnected =
    new Map(m.disconnected || []);

  if (m.roundState) {

    m.roundState.taps =
      (m.roundState.taps || []).map(
        a => new Set(a || [])
      );

    if (
      m.roundState.board &&
      Array.isArray(
        m.roundState.board.required
      )
    ) {

      m.roundState.board = {
        required: new Set(
          m.roundState.board
            .required
        )
      };
    }
  }

  if (
    m.pending &&
    m.pending.board &&
    Array.isArray(
      m.pending.board.required
    )
  ) {

    m.pending.board = {
      required: new Set(
        m.pending.board.required
      )
    };
  }

  m.deathTimer = null;
  m.nextTimer = null;

  m.lastEnd = m.lastEnd || null;
  m.final = m.final || null;
  m.nextRoundAt =
    m.nextRoundAt || 0;
  m.pending =
    m.pending || null;
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
