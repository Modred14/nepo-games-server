const { createServer } = require("http");
const { Server } = require("socket.io");
const https = require("https");
require("dotenv").config();

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

  socket.on("join", (conversationId) => {
    socket.join(`room:${conversationId}`);
    console.log(`Socket ${socket.id} joined room:${conversationId}`);
  });

  socket.on("leave", (conversationId) => {
    socket.leave(`room:${conversationId}`);
    console.log(`Socket ${socket.id} left room:${conversationId}`);
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
