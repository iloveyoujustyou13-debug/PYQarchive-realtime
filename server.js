// ============================================================
// PYQArchive Real-Time Messaging Server
// Deploy on Render.com
// ============================================================

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        service: 'PYQArchive Realtime',
        rooms: rooms.size,
        clients: wss ? wss.clients.size : 0,
        uptime: process.uptime()
    });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({
    server,
    // Allow large messages (replies with quoted text)
    maxPayload: 16 * 1024
});

// ============================================================
// STATE
// ============================================================
// rooms: Map<roomId, Set<ws>>
const rooms = new Map();

// ============================================================
// CONNECTION HANDLER
// ============================================================
wss.on('connection', (ws, req) => {
    let url;
    try {
        url = new URL(req.url, 'http://localhost');
    } catch (e) {
        ws.close(4000, 'Bad URL');
        return;
    }

    const roomId = url.searchParams.get('room');
    const userId = url.searchParams.get('uid');
    const userName = url.searchParams.get('name') || 'User';
    const isGuest = url.searchParams.get('guest') === '1';

    if (!roomId || !userId) {
        ws.close(4000, 'Missing room or uid');
        return;
    }

    // Attach metadata to socket
    ws.roomId = roomId;
    ws.userId = userId;
    ws.userName = userName;
    ws.isGuest = isGuest;
    ws.isAlive = true;

    // Register
    if (!rooms.has(roomId)) rooms.set(roomId, new Set());
    rooms.get(roomId).add(ws);

    console.log(`[JOIN] ${userName} (${userId.slice(0,6)}) → room ${roomId} | total: ${rooms.get(roomId).size}`);

    // Send current member list to the newcomer
    sendMemberList(roomId);

    // Notify others about new member
    broadcast(roomId, {
        type: 'join',
        uid: userId,
        name: userName,
        isGuest: isGuest
    }, ws);

    // ============================================================
    // MESSAGE HANDLER
    // ============================================================
    ws.on('message', (data) => {
        let msg;
        try {
            msg = JSON.parse(data);
        } catch (e) {
            return;
        }

        // Only handle 'message' type from clients
        if (msg.type !== 'message') return;

        const text = typeof msg.text === 'string' ? msg.text.slice(0, 1000) : '';
        if (!text.trim()) return;

        // Build payload — server is just a relay
        const payload = {
            type: 'message',
            id: msg.id || ('wsm_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8)),
            roomId: roomId,
            userId: userId,
            userName: userName,
            isGuest: isGuest,
            text: text,
            replyTo: msg.replyTo || null,
            replyToName: msg.replyToName || null,
            replyToText: msg.replyToText || null,
            mentions: Array.isArray(msg.mentions) ? msg.mentions.slice(0, 20) : [],
            likedBy: [],
            createdAt: Date.now()
        };

        // Relay to everyone in the room (including sender — sender dedupes by id)
        broadcast(roomId, payload);

        // Send ACK to sender with final id
        try {
            ws.send(JSON.stringify({ type: 'ack', id: payload.id, createdAt: payload.createdAt }));
        } catch (e) {}
    });

    // ============================================================
    // DISCONNECT
    // ============================================================
    ws.on('close', () => {
        const set = rooms.get(roomId);
        if (set) {
            set.delete(ws);
            console.log(`[LEAVE] ${userName} left room ${roomId} | remaining: ${set.size}`);
            if (set.size === 0) {
                rooms.delete(roomId);
                console.log(`[ROOM] ${roomId} is now empty and removed`);
            } else {
                sendMemberList(roomId);
            }
        }
    });

    ws.on('error', (err) => {
        console.warn('[WS ERROR]', err.message);
    });

    ws.on('pong', () => { ws.isAlive = true; });
});

// ============================================================
// HELPERS
// ============================================================
function broadcast(roomId, payload, except) {
    const set = rooms.get(roomId);
    if (!set) return;
    const data = JSON.stringify(payload);
    set.forEach(client => {
        if (client !== except && client.readyState === WebSocket.OPEN) {
            try { client.send(data); } catch (e) {}
        }
    });
}

function sendMemberList(roomId) {
    const set = rooms.get(roomId);
    if (!set) return;
    const members = Array.from(set).map(c => ({
        uid: c.userId,
        name: c.userName,
        isGuest: c.isGuest
    }));
    const payload = JSON.stringify({ type: 'members', members });
    set.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(payload); } catch (e) {}
        }
    });
}

// ============================================================
// HEARTBEAT — kill dead connections
// ============================================================
setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        try { ws.ping(); } catch (e) {}
    });
}, 30000);

// ============================================================
// START
// ============================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ PYQArchive Realtime Server running on port ${PORT}`);
});
