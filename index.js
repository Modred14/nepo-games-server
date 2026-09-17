// ROUTE: index.js
const { createServer } = require("http");
const { Server } = require("socket.io");
const https = require("https");
const jwt = require("jsonwebtoken");
require("dotenv").config();

// NEW: background reconciliation loop (see reconcileStaleWithdrawals below
// and its setInterval call near the bottom of this file). Was flagged as
// missing in the withdrawal audit — rows left 'pending' past when a
// webhook should have arrived, or 'unknown' after an ambiguous network
// failure (see the /transfer-status handler above, audit item D.1), had
// no automatic follow-up before this. This server is the natural home
// for it: it's already an always-on process (not a Netlify serverless
// function that can't run a background loop) and already holds
// FLW_SECRET_KEY from the whitelisted IP needed to query Flutterwave.
//
// Requires `pg` (added to package.json) and DATABASE_URL to be set in
// this server's environment — same connection string used by
// nepo-games-main's src/lib/db.js. If DATABASE_URL isn't set, the loop
// logs a warning once and simply doesn't run, rather than crashing the
// socket server (which has nothing to do with the database otherwise).
const { Pool } = require("pg");

const dbPool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 3,
    })
  : null;

// FIX (critical, socket auth + real-time delivery bug): the "join"/"leave"
// handlers used to accept a bare conversationId and do
// `socket.join(\`room:${conversationId}\`)`. The client (see
// nepo-games-main's Conversation.jsx / server.js) was updated to send a
// signed token instead — `socket.emit("join", { room, token })` — so that
// a client can only join a room it's actually a DB participant of.
// Because this server was never updated to match, `conversationId` above
// became the whole `{ room, token }` object, so sockets were joining a
// room literally named "room:[object Object]" instead of e.g. "room:260"
// or "user:80". That's why /emit kept returning 200 (the HTTP call to
// this server succeeded) but no connected client ever received the
// broadcast — nobody was actually in the room being emitted to.
function verifyJoinToken(token) {
  const secret = process.env.SOCKET_SECRET;
  if (!secret || !token) return null;
  try {
    return jwt.verify(token, secret);
  } catch {
    return null;
  }
}

process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});

// How old a 'pending' withdrawal must be before we bother asking
// Flutterwave about it — gives the transfer.completed webhook a fair
// chance to arrive normally first, instead of hammering Flutterwave's
// API for every withdrawal on every sweep.
const RECONCILE_PENDING_AFTER_MS = Number(
  process.env.RECONCILE_PENDING_AFTER_MS || 10 * 60 * 1000, // 10 minutes
);

// How old a row must be before a "Flutterwave has no record of this
// reference" result is trusted enough to mark it 'failed' — protects
// against a transient/eventually-consistent lookup gap right after the
// transfer was created.
const RECONCILE_MARK_FAILED_AFTER_MS = Number(
  process.env.RECONCILE_MARK_FAILED_AFTER_MS || 30 * 60 * 1000, // 30 minutes
);

async function checkTransferOnFlutterwave(reference) {
  const flwRes = await fetch(
    `https://api.flutterwave.com/v3/transfers?reference=${encodeURIComponent(reference)}`,
    { headers: { Authorization: `Bearer ${process.env.FLW_SECRET_KEY}` } },
  );
  const flwData = await flwRes.json();
  const record = Array.isArray(flwData?.data) ? flwData.data[0] : flwData?.data;
  return { ok: flwRes.ok, found: Boolean(record), record };
}

// NEW: the actual reconciliation sweep. Picks up:
//   - 'unknown' rows (ambiguous outcome from a prior /transfer or webhook
//     call, see audit item D.1) — checked on every sweep, since these are
//     rare and already flagged as needing attention
//   - 'pending' rows older than RECONCILE_PENDING_AFTER_MS — the backstop
//     for a missed/delayed transfer.completed webhook
// and resolves each against Flutterwave's own transfer records, the same
// way /transfer-status and the webhook handler do.
async function reconcileStaleWithdrawals() {
  if (!dbPool) return;

  if (!process.env.FLW_SECRET_KEY) {
    console.error("[reconcile] ERROR: FLW_SECRET_KEY is not set, skipping sweep");
    return;
  }

  let rows;
  try {
    const result = await dbPool.query(
      `
      SELECT id, reference, status, created_at
      FROM users_transactions
      WHERE type = 'debit' AND description = 'Withdrawal'
        AND (
          status = 'unknown'
          OR (status = 'pending' AND created_at < NOW() - ($1 || ' milliseconds')::interval)
        )
      ORDER BY created_at ASC
      LIMIT 50
      `,
      [RECONCILE_PENDING_AFTER_MS],
    );
    rows = result.rows;
  } catch (err) {
    console.error("[reconcile] DB query failed:", err.message);
    return;
  }

  if (rows.length === 0) {
    console.log("[reconcile] No stale pending/unknown withdrawals to check.");
    return;
  }

  console.log(`[reconcile] Checking ${rows.length} withdrawal(s) against Flutterwave...`);

  for (const row of rows) {
    try {
      const { ok, found, record } = await checkTransferOnFlutterwave(row.reference);
      const ageMs = Date.now() - new Date(row.created_at).getTime();
      const flwStatus = found ? String(record?.status || "").toUpperCase() : null;

      let newStatus = null;

      if (found && flwStatus === "SUCCESSFUL") {
        newStatus = "success";
      } else if (found && flwStatus === "FAILED") {
        newStatus = "failed";
      } else if (!found && ok && ageMs > RECONCILE_MARK_FAILED_AFTER_MS) {
        // Confirmed no record on Flutterwave's side, and old enough that
        // this isn't just eventual-consistency lag — safe to call it failed.
        newStatus = "failed";
      } else if (!found || !ok) {
        // Either genuinely not found yet (too young to be sure) or we
        // couldn't get a clean answer from Flutterwave this sweep — leave
        // it as 'unknown' so it's still held against the user's balance
        // and gets picked up again next sweep.
        newStatus = "unknown";
      }
      // else: found and still NEW/PENDING on Flutterwave's side — leave
      // as-is, wait for the webhook or a later sweep.

      if (newStatus && newStatus !== row.status) {
        // WITHDRAWAL FEE: update by `reference`, not `id` — the
        // 'Withdrawal fee' credit row (user_id=1, inserted by
        // withdraw/route.js in nepo-games-main) shares this withdrawal's
        // reference specifically so one UPDATE keeps both rows in sync.
        await dbPool.query(`UPDATE users_transactions SET status = $1 WHERE reference = $2`, [
          newStatus,
          row.reference,
        ]);
        console.log(
          `[reconcile] #${row.id} (${row.reference}): ${row.status} → ${newStatus}` +
            (flwStatus ? ` (Flutterwave: ${flwStatus})` : " (not found on Flutterwave)"),
        );
      } else {
        console.log(
          `[reconcile] #${row.id} (${row.reference}): no change (${row.status})`,
        );
      }
    } catch (err) {
      console.error(`[reconcile] #${row.id} (${row.reference}) check failed:`, err.message);
      // Leave it as-is; will be retried next sweep.
    }
  }
}

const httpServer = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    res.end("OK");
    return;
  }

  if (req.url === "/emit" && req.method === "POST") {
    console.log("[/emit] Incoming request from:", req.socket.remoteAddress);

    const secret = req.headers["x-secret"];
    if (secret !== process.env.SOCKET_SECRET) {
      console.error("[/emit] Forbidden — secret mismatch. Received:", secret);
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }

    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      try {
        const { room, event, data } = JSON.parse(body);
        console.log("[/emit] Emitting to room:", room, "event:", event);
        const roomSockets = io.sockets.adapter.rooms.get(room);
        console.log(
          "[/emit] Sockets in room:",
          roomSockets ? roomSockets.size : 0,
        );
        io.to(room).emit(event, data);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true }));
      } catch (err) {
        console.error("[/emit] Parse error:", err);
        res.writeHead(400);
        res.end("Bad request");
      }
    });
    return;
  }

  // FIX (audit item D.1/D.2): reconciliation endpoint. When the
  // Next.js app can't tell whether a /transfer call actually reached
  // Flutterwave (network error/timeout between here and there, or
  // between Next.js and this server), or when the transfer.completed
  // webhook needs to double-check a payload before trusting it, it
  // calls this instead of guessing. GET /v3/transfers is one of the
  // endpoints covered by Flutterwave's mandatory IP whitelist (same
  // bucket as /v3/transfers POST), so — same as the transfer itself —
  // this has to be made from this server's whitelisted IP, not from
  // Netlify.
  if (req.url.startsWith("/transfer-status") && req.method === "GET") {
    console.log(
      "[/transfer-status] Incoming request from:",
      req.socket.remoteAddress,
    );

    const secret = req.headers["x-transfer-secret"];
    if (secret !== process.env.TRANSFER_SECRET) {
      console.error(
        "[/transfer-status] Forbidden — secret mismatch. Received:",
        secret,
      );
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }

    const reqUrl = new URL(req.url, "http://internal");
    const reference = reqUrl.searchParams.get("reference");
    const id = reqUrl.searchParams.get("id");

    if (!reference && !id) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "reference or id is required" }));
      return;
    }

    if (!process.env.FLW_SECRET_KEY) {
      console.error("[/transfer-status] ERROR: FLW_SECRET_KEY is not set!");
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Server misconfigured" }));
      return;
    }

    // The outer request handler isn't async (see createServer below), so
    // wrap the await calls in an IIFE — same pattern already used for
    // the /transfer and /emit handlers via req.on("end", async () => {}).
    (async () => {
      try {
        const flwUrl = id
          ? `https://api.flutterwave.com/v3/transfers/${encodeURIComponent(id)}`
          : `https://api.flutterwave.com/v3/transfers?reference=${encodeURIComponent(reference)}`;

        const flwRes = await fetch(flwUrl, {
          headers: { Authorization: `Bearer ${process.env.FLW_SECRET_KEY}` },
        });

        const flwData = await flwRes.json();
        console.log(
          "[/transfer-status] Flutterwave response:",
          flwRes.status,
          flwData.status,
        );

        res.writeHead(flwRes.ok ? 200 : 400, {
          "Content-Type": "application/json",
        });
        res.end(JSON.stringify(flwData));
      } catch (err) {
        console.error("[/transfer-status] Flutterwave call failed:", err.message);
        // Genuinely can't reach Flutterwave right now — this is still an
        // ambiguous outcome, not a "not found". Say so explicitly so the
        // caller doesn't mistake this for "transfer doesn't exist".
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Status check failed", ambiguous: true }));
      }
    })();
    return;
  }

  if (req.url === "/transfer" && req.method === "POST") {
    console.log(
      "[/transfer] Incoming request from:",
      req.socket.remoteAddress,
    );

    const secret = req.headers["x-transfer-secret"];
    if (secret !== process.env.TRANSFER_SECRET) {
      console.error(
        "[/transfer] Forbidden — secret mismatch. Received:",
        secret,
      );
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }

    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", async () => {
      let payload;
      try {
        payload = JSON.parse(body);
      } catch (err) {
        console.error("[/transfer] Parse error:", err);
        res.writeHead(400);
        res.end("Bad request");
        return;
      }

      const { account_bank, account_number, amount, currency, narration, reference } =
        payload || {};

      if (!account_bank || !account_number || !amount || !reference) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Missing required transfer fields" }));
        return;
      }

      if (!process.env.FLW_SECRET_KEY) {
        console.error("[/transfer] ERROR: FLW_SECRET_KEY is not set!");
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Server misconfigured" }));
        return;
      }

      try {
        const flwRes = await fetch("https://api.flutterwave.com/v3/transfers", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${process.env.FLW_SECRET_KEY}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            account_bank,
            account_number,
            amount,
            currency: currency || "NGN",
            narration: narration || "Wallet withdrawal",
            reference,
          }),
        });

        const flwData = await flwRes.json();
        console.log(
          "[/transfer] Flutterwave response:",
          flwRes.status,
          flwData.status,
        );

        res.writeHead(flwRes.ok ? 200 : 400, {
          "Content-Type": "application/json",
        });
        res.end(JSON.stringify(flwData));
      } catch (err) {
        // FIX (audit item D.1): a thrown error here means the fetch to
        // Flutterwave itself failed/timed out — it does NOT mean
        // Flutterwave never received or processed the transfer. Mark
        // this explicitly `ambiguous: true` so the caller (Next.js
        // withdraw route) knows it must reconcile via
        // /transfer-status instead of assuming the transfer failed
        // and freeing up the user's balance.
        console.error("[/transfer] Flutterwave call failed:", err.message);
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({ error: "Transfer request failed", ambiguous: true }),
        );
      }
    });
    return;
  }

  res.writeHead(404);
  res.end();
});

httpServer.on("error", (err) => {
  console.error("HTTP server error:", err);
});

const io = new Server(httpServer, {
  cors: {
    origin: ["https://nepogames.netlify.app", "http://localhost:3000"],
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("error", (err) => {
    console.error(`Socket error on ${socket.id}:`, err);
  });

  socket.on("join", (payload) => {
    if (!payload || typeof payload !== "object") {
      console.warn(`Socket ${socket.id} sent invalid join payload`);
      return;
    }
    const { room, token } = payload;
    if (!room || !token) return;

    const decoded = verifyJoinToken(token);
    if (!decoded || decoded.room !== room) {
      console.warn(
        `Socket ${socket.id} join rejected — invalid/mismatched token for room:`,
        room,
      );
      return;
    }

    socket.join(room);
    console.log(`Socket ${socket.id} joined ${room}`);
  });

  socket.on("leave", (payload) => {
    const room = typeof payload === "object" ? payload?.room : payload;
    if (!room) return;
    socket.leave(room);
    console.log(`Socket ${socket.id} left ${room}`);
  });

  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

const port = process.env.PORT || 4000;

if (process.env.NODE_ENV === "production") {
  setInterval(
    () => {
      const url =
        process.env.RENDER_URL || "https://your-render-url.onrender.com";
      https
        .get(`${url}/health`, (res) => {
          console.log("Keep-alive ping:", res.statusCode);
        })
        .on("error", (err) => {
          console.error("Keep-alive failed:", err.message);
        });
    },
    10 * 60 * 1000,
  );
}

// NEW: run the withdrawal reconciliation sweep periodically. Interval is
// configurable via RECONCILE_INTERVAL_MS; defaults to every 5 minutes.
// No-ops internally (see reconcileStaleWithdrawals) if DATABASE_URL isn't
// configured, so this is safe to leave in even before that's set up.
if (dbPool) {
  const reconcileIntervalMs = Number(
    process.env.RECONCILE_INTERVAL_MS || 5 * 60 * 1000,
  );
  console.log(
    `[reconcile] Withdrawal reconciliation sweep enabled, every ${reconcileIntervalMs / 1000}s`,
  );
  setInterval(() => {
    reconcileStaleWithdrawals().catch((err) =>
      console.error("[reconcile] Sweep crashed:", err),
    );
  }, reconcileIntervalMs);
  // Also run one sweep shortly after startup rather than waiting a full
  // interval, so a restart doesn't leave stale rows sitting for longer
  // than necessary.
  setTimeout(() => {
    reconcileStaleWithdrawals().catch((err) =>
      console.error("[reconcile] Startup sweep crashed:", err),
    );
  }, 30 * 1000);
} else {
  console.warn(
    "[reconcile] DATABASE_URL is not set — withdrawal reconciliation sweep is DISABLED. " +
      "Stale 'pending'/'unknown' withdrawals will only be resolved by webhooks or manual admin recheck.",
  );
}

httpServer.listen(port, () => {
  console.log(`Socket.io server running on port ${port}`);
});