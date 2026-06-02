const { createServer } = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const httpServer = createServer((req, res) => {
  // Health check endpoint — Render needs this to confirm server is alive
  if (req.url === "/health") {
    res.writeHead(200);
    res.end("OK");
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, {
  cors: {
    origin: [
      "https://your-nepo-games.netlify.app", // your Netlify URL
      "http://localhost:3000",                // local dev
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
httpServer.listen(port, () => {
  console.log(`Socket.io server running on port ${port}`);
});