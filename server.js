// Knockout Arena — WebSocket relay server
// Generic room relay: players join a room by code, messages relay to roommates.
// First player in a room is the host; if the host leaves, the next player is promoted.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_PER_ROOM = 4;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Knockout Arena relay is running\n');
});

const wss = new WebSocketServer({ server });

// rooms: Map<roomCode, Map<clientId, socket>>
const rooms = new Map();
let nextId = 1;

function roomOf(ws) {
  return rooms.get(ws._room);
}
function sendTo(sock, obj) {
  if (sock && sock.readyState === 1) sock.send(JSON.stringify(obj));
}
function broadcast(room, exceptId, obj) {
  if (!room) return;
  for (const [id, sock] of room) {
    if (id !== exceptId) sendTo(sock, obj);
  }
}

wss.on('connection', (ws) => {
  ws._id = nextId++;
  ws._room = null;
  ws._name = 'PLAYER';
  ws._host = false;

  ws.on('message', (buf) => {
    let m;
    try { m = JSON.parse(buf.toString()); } catch (_) { return; }

    if (m.type === 'join') {
      const code = String(m.room || 'LOBBY').slice(0, 24).toUpperCase();
      ws._name = String(m.name || 'PLAYER').slice(0, 16);
      let room = rooms.get(code);
      if (!room) { room = new Map(); rooms.set(code, room); }
      if (room.size >= MAX_PER_ROOM) {
        sendTo(ws, { type: 'room-full' });
        return;
      }
      ws._room = code;
      ws._host = room.size === 0;
      const peers = [...room.entries()].map(([id, s]) => ({ id, name: s._name }));
      room.set(ws._id, ws);
      sendTo(ws, { type: 'joined', id: ws._id, host: ws._host, peers });
      broadcast(room, ws._id, { type: 'peer-join', id: ws._id, name: ws._name });
      return;
    }

    // everything else relays to roommates
    const room = roomOf(ws);
    if (!room) return;
    broadcast(room, ws._id, { type: 'data', from: ws._id, data: m });
  });

  ws.on('close', () => {
    const room = roomOf(ws);
    if (!room) return;
    room.delete(ws._id);
    broadcast(room, ws._id, { type: 'peer-leave', id: ws._id });
    if (ws._host && room.size > 0) {
      // promote the longest-waiting remaining player
      const [nid, nsock] = room.entries().next().value;
      nsock._host = true;
      sendTo(nsock, { type: 'promoted' });
    }
    if (room.size === 0) rooms.delete(ws._room);
  });

  ws.on('error', () => {});
});

// keep connections alive through Render's proxy
setInterval(() => {
  for (const room of rooms.values())
    for (const sock of room.values())
      if (sock.readyState === 1) sock.ping();
}, 30000);

server.listen(PORT, () => console.log('relay listening on ' + PORT));
