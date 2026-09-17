// ROUTE: index.js
const { createServer } = require("http");
const { Server } = require("socket.io");
const https = require("https");
const jwt = require("jsonwebtoken");
require("dotenv").config();

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

httpServer.listen(port, () => {
  console.log(`Socket.io server running on port ${port}`);
});