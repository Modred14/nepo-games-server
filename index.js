const { createServer } = require("http");
const { Server } = require("socket.io");

const httpServer = createServer((req, res) => {
  // Health check endpoint — Render needs this to confirm server is alive
  if (req.url === "/health") {
    res.writeHead(200);
    res.end("OK");
    return;
  }
  if (req.url === "/emit" && req.method === "POST") {
    // Verify secret
    const secret = req.headers["x-secret"];
    if (secret !== process.env.SOCKET_SECRET) {
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
        io.to(room).emit(event, data);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true }));
      } catch (err) {
        res.writeHead(400);
        res.end("Bad request");
      }
    });
    return;
  }

  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, {
  cors: {
    origin: [
      "https://nepogames.netlify.app", // your Netlify URL
      "http://localhost:3000", // local dev
    ],
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("join", (conversationId) => {
    socket.join(`room:${conversationId}`);
    console.log(`Socket ${socket.id} joined room:${conversationId}`);
  });

  socket.on("leave", (conversationId) => {
    socket.leave(`room:${conversationId}`);
  });

  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

const port = process.env.PORT || 4000;
// Add this before the httpServer.listen call:
if (process.env.NODE_ENV === "production") {
  setInterval(
    () => {
      fetch(`https://your-render-url.onrender.com/health`)
        .then(() => console.log("Keep-alive ping"))
        .catch(() => {});
    },
    10 * 60 * 1000,
  );
}
httpServer.listen(port, () => {
  console.log(`Socket.io server running on port ${port}`);
});
