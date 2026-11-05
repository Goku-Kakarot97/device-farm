# farm-stream

Stream the screens of Android devices sitting in a private device farm to browsers on the
other side of a NAT/VPN, through a relay server that the farm **dials out** to.

One small Java 21 jar, three modes:

| Mode | Where it runs | What it does |
| --- | --- | --- |
| `server` | Public host / Kubernetes | Relay: accepts browser connections and forwards them through a WebSocket tunnel to an agent |
| `agent` | Machine with the devices plugged in | Device server (ws-scrcpy compatible API over HTTPS/WSS) **and** tunnel agent that connects out to the relay |
| `wspipe` | Developer laptop | Turns a `/t/...` relay route into stdin/stdout, e.g. for `ssh -o ProxyCommand` |

```
[Browser] ──wss──> [farm-stream server @ k8s] <──wss (dial-out)── [farm-stream agent @ farm host]
                                                                     │  same JVM:
                                                                     ├─ device server https://0.0.0.0:11000
                                                                     └─ tunnel agent (opens TCP/TLS to allowed hosts on demand)
                                                                     │
                                                          adb server :5037 ──> scrcpy-server 1.19-ws6 on device (tcp:8886)
```

Built on [Hydrangea](https://github.com/Goku-Kakarot97/hydrangea) (`hydrangea-http`, `hydrangea-postgres`,
`hydrangea-log`), virtual threads, no Spring. The fat jar is about 1 MB.

## Features

- **Reverse tunnel.** The farm never needs an inbound port. The agent keeps one WebSocket to the relay; every browser
  connection becomes a multiplexed stream (`[type:1][streamId:4][payload]`).
- **ws-scrcpy compatible device server.** Device list, scrcpy video stream proxy, APK install, keep-awake, control
  message recording/replay, all over WebSocket/HTTP JSON.
- **Pure ADB protocol client.** Talks to the adb *server* (tcp 5037) directly: `host:track-devices`, shell, sync push,
  forward, connect/tcpip/usb. No `adb` binary is spawned.
- **Streaming without buffering.** `101` and `2xx` responses are piped byte-by-byte through the tunnel with
  backpressure in both directions, so a slow browser pauses the device instead of filling memory.
- **Optional PostgreSQL persistence** of device status (`aegis_shield.devices`). If the database is unreachable or not
  configured the server keeps running in memory.
- TLS with your own PEM cert/key (PKCS#8 or PKCS#1), or plain HTTP behind an ingress.

## Build

Requires JDK 21 and Maven.

```bash
mvn package              # runs unit tests, produces target/farm-stream.jar
mvn package -DskipTests
```

The farm host only needs a JRE 21 if you copy the jar over.

## Run

### Relay server

```bash
PORT=18000 TUNNEL_TOKEN=<shared-secret> java -jar farm-stream.jar server
```

`docker/Dockerfile` is a multi-stage build (maven → temurin 21 JRE). `docker/k8s.example.yaml` is a sample
Deployment + NodePort Service. Keep `replicas: 1`: tunnels are held in memory.

### Agent (farm host)

```bash
adb start-server
DB_PASSWORD=<pg-password> java -jar farm-stream.jar agent \
  --RELAY_URL=wss://relay.example.com/tunnel --AGENT_ID=farm1 --ALLOW=10.0.0.0/24
```

Successful start logs:

```
... DeviceServer - device server listening on https://0.0.0.0:11000/
... TunnelAgent - tunnel established (allow: 10.0.0.0/24,127.0.0.1/32, default port 11000)
```

Set `RELAY_INSECURE=true` if the relay uses a self-signed certificate.

### wspipe (ssh through the relay)

```bash
ssh -o ProxyCommand='java -jar farm-stream.jar wspipe --insecure wss://relay.example.com/t/farm1/%h:%p' user@10.0.0.5
```

## TLS

`ssl/` is git-ignored. Generate a self-signed pair once per host (the Docker image does this at build time):

```bash
mkdir -p ssl && openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj /CN=farm-stream \
  -keyout ssl/server.key -out ssl/server.crt
```

Or point `CERT_PATH` / `KEY_PATH` at a real certificate, or run with `NO_TLS=true` behind an ingress.

## Configuration

Every option is an environment variable or a `--KEY=value` flag (flags win).

| Option | Mode | Default | Meaning |
| --- | --- | --- | --- |
| `PORT` | server | `18000` | Relay listen port |
| `TUNNEL_TOKEN` | server + agent | _(empty)_ | Shared secret the agent must present |
| `OPEN_TIMEOUT_MS` | server | `10000` | How long the relay waits for the agent to open a target connection |
| `RELAY_URL` | agent | — | `wss://<relay>/tunnel`; empty = run the device server only |
| `AGENT_ID` | agent | hostname | Name used in `/d/<agent-id>/...` routes |
| `ALLOW` | agent | `127.0.0.1/32` | CIDRs the agent may dial; `*` disables the check |
| `DEFAULT_PORT` | agent | `11000` | Port used when a route has no `:port` |
| `RELAY_INSECURE` | agent, wspipe | `false` | `true` = skip relay certificate verification |
| `NO_TUNNEL` / `NO_DEVICE_SERVER` | agent | `false` | Run only one half |
| `LISTEN_PORT` | agent | `11000` | Device server port |
| `CERT_PATH` + `KEY_PATH` | both | `ssl/server.crt\|key` or `../ssl/` | PEM certificate and key (see [TLS](#tls)) |
| `NO_TLS` | both | `false` | Plain HTTP (terminate TLS at the ingress) |
| `ADB_HOST` / `ADB_PORT` | agent | `127.0.0.1` / `5037` | adb server |
| `ADB_AUTOSTART` | agent | `true` | When the local adb server is down (connection refused on `ADB_PORT`) run `adb start-server` and reconnect at once |
| `ADB_BIN` | agent | _(lookup)_ | adb binary for `ADB_AUTOSTART`; default: PATH, then `$ANDROID_HOME`/`$ANDROID_SDK_ROOT`/platform-tools, then `~/Library/Android/sdk/platform-tools` (macOS) or `~/Android/Sdk/platform-tools` (Linux) |
| `DB_HOST` `DB_PORT` `DB_NAME` `DB_USER` | agent | `127.0.0.1` / `5432` / `fs_core` / `fs_core` | PostgreSQL for `aegis_shield.devices` |
| `DB_PASSWORD` | agent | **none** | Required to use the database. If unset, an error is logged and the agent runs in memory |
| `DB_DISABLED` | agent | `false` | `true` = never touch the database |
| `LOG_LEVEL` | both | `INFO` | `DEBUG` adds Hydrangea and stream metadata logs |

Never commit `DB_PASSWORD`. Put it in the environment of the process manager (systemd unit, a git-ignored `.env`
file sourced by your start script, a Kubernetes Secret, ...).

## Relay routes

- `/d/<agent>/<ip[:port]>/<path>` — agent dials the target with **TLS**
- `/p/<agent>/<ip[:port]>/<path>` — plain TCP
- `/dk/`, `/pk/` — keep-path variants (upstream receives the full original URL)
- `/t/<agent>/<ip:port>` — raw TCP over WebSocket (ssh, adb), use `wspipe` on the client side
- `GET /healthz`, `GET /agents`

For HTTP the relay rewrites the request head (`Host`, `Connection: close`) and replays it on a tunnel stream.
`101` (WebSocket upgrade) and `2xx` responses are piped as-is. Other responses are buffered, de-chunked and returned
normally. When the tunnel WebSocket is not writable the relay stops reading from the client/target socket.

## Device server API

WebSocket on `/?action=...` (any origin, messages up to 50 MB):

| action | Description |
| --- | --- |
| `devices-list` | Pushes a JSON array of devices (`device, ipv4, uuid, connect_type, status_recodd, device_status, label, sdk_version, used_by, wifi_ssid`); send any message to refresh |
| `proxy-adb&udid=&remote=tcp:8886[&path=][&user=][&record=][&replay=]` | scrcpy stream: `adb forward` then bidirectional WebSocket proxy to scrcpy-server; marks the device IN_USE/ACTIVE for `user`. Also accepts the path form `/proxy-adb/<udid>/<remote>/<path>` |
| `proxy-ws&ws=<url>[&session=]` | Proxy to an arbitrary WebSocket URL |
| `record-status` | Snapshot of record/run/pause/stop state |
| `goog-device-list` | ws-scrcpy device tracker protocol (`devicelist`/`device`; accepts `kill_server`, `start_server`, `restart_server`, `update_interfaces`) |

HTTP JSON (CORS `*`):
`POST /api/goog/device/{pid,config,restart,send-binary,install-apk,install-apk-binary,install-uploaded}`,
`POST /api/recordings/{start,stop,run,pause,resume,update-name,delete}`, `GET /api/recordings`,
`POST /api/devices/connect`, `POST /api/device/keep-awake`, `POST /api/device/stop-using`,
`GET /api/sync`, `POST /api/sync/{set,clear}`. See `device/ApiRoutes.java` for request/response bodies.

When a device appears, the tracker creates an `AndroidDevice`: reads `getprop` and network interfaces, pushes the
bundled `scrcpy-server.jar` (1.19-ws6) to `/data/local/tmp/`, starts it with `app_process` under `nohup` and waits
for `/data/local/tmp/ws_scrcpy.pid`. Video settings are the 35-byte big-endian ws-scrcpy format.

`recordings/` and `uploads/` are created in the working directory of the process.

## Layout

```
src/main/java/com/mservice/farmstream/
  Main.java, AgentMain.java              entry points
  relay/   Frame, Tunnel, ProxyRoute, RelayServer, ProxyStream, TunnelAgent, WsPipe
  adb/     AdbClient                     ADB server protocol (blocking, virtual threads)
  device/  DeviceServer, ApiRoutes, DeviceRegistry, AndroidDevice, ScrcpyServer, StreamProxy,
           DeviceListService, DeviceStatusService, DeviceRepository (hydrangea-postgres),
           RecordingStore, RecordingStatusService, SyncService, KeepAwakeService, VideoSettings, ...
  util/    Config (env + --flag), Json, Pem (PEM → SSLContext)
src/main/resources/scrcpy-server.jar     runs on the device (Apache-2.0, see scrcpy-server.LICENSE)
```

## Local smoke test

```bash
mvn package
java -jar target/farm-stream.jar server --PORT=18001 --NO_TLS=true &
java -jar target/farm-stream.jar agent --NO_DEVICE_SERVER=true --RELAY_URL=ws://127.0.0.1:18001/tunnel --AGENT_ID=test --ALLOW='*' &
curl http://127.0.0.1:18001/agents
curl http://127.0.0.1:18001/p/test/127.0.0.1:<port of any local http server>/
adb start-server && java -jar target/farm-stream.jar agent --NO_TUNNEL=true --LISTEN_PORT=11001 --DB_DISABLED=true &
curl -k https://127.0.0.1:11001/api/sync
```

## License

`scrcpy-server.jar` is from [ws-scrcpy](https://github.com/NetrisTV/ws-scrcpy) / [scrcpy](https://github.com/Genymobile/scrcpy),
Apache License 2.0. Add a license for the rest of this repository before publishing.
