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