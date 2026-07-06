# xterm.rs — showcase

A single-page site that demonstrates [xterm.rs] by embedding **real, live terminals**
served by the Rust backend. Three sandboxed back-ends run an interactive shell,
`htop`, and `vim`, so visitors can prove to themselves that it's a real PTY — not a
scripted transcript.

Built in-house at **[fenrir.pro](https://fenrir.pro)**. It powers the labs on
**[hack.courses](https://hack.courses)**. Questions or deployment help:
**https://fenrir.pro/contact**.

## Layout

```
xterm-rs-showcase/
├── docker-compose.yml      web page + 3 terminal back-ends
├── web/
│   └── index.html          the single-page showcase (nginx serves this)
└── server/
    ├── Dockerfile          builds the xterm.rs binary, ships a lean runtime
    ├── Cargo.toml
    ├── src/main.rs         the xterm.rs server
    └── assets/welcome.txt  file the vim demo opens
```

## Run it

```bash
cd xterm-rs-showcase
docker compose up --build
```

Then open **http://localhost:8080**.

| Service      | URL                     | Runs            |
|--------------|-------------------------|-----------------|
| `web`        | http://localhost:8080   | the showcase    |
| `term-shell` | http://localhost:8081   | `bash -i`       |
| `term-htop`  | http://localhost:8082   | `htop`          |
| `term-vim`   | http://localhost:8083   | `vim` on a file |

The page embeds the back-ends by port, building each URL from the address you
loaded it on — so it works on `localhost` or on a server's IP/hostname without
edits. (Each terminal page is served at `/term`; its own script rewrites that to
`/ws` for the WebSocket.)

Stop with `Ctrl-C`, or `docker compose down`.

## How the page talks to the back-ends

Each `xterm.rs` instance serves the terminal HTML on any path and the WebSocket on
`/ws`. The embedded iframes load `http://<host>:<port>/term`; the served page
connects its WebSocket to the same origin. No reverse-proxy gymnastics, because the
backend's WS route is a fixed `/ws`.

To change ports or add a back-end, edit `PORTS` in `web/index.html` and the matching
service in `docker-compose.yml`.

## Security — read before deploying

These terminals hand a **real shell** to anyone who can reach the port. The compose
file is tuned for a **local demo / trusted lab**, not the open internet:

- runs as a non-root `demo` user,
- `cap_drop: ALL` and `no-new-privileges`,
- `pids_limit` and `mem_limit` per container,
- ephemeral containers (`init: true` reaps the per-session shells).

Before exposing it publicly, also:

1. **Put authentication in front of it.** A reverse proxy (Caddy/nginx/Traefik) with
   auth, or a tokened entry point — never publish `:8081–8083` raw.
2. **Restrict egress.** Attach the terminal containers to an `internal` Docker network
   or firewall their outbound traffic so a shell can't pivot.
3. **Terminate TLS at the edge** and run the backend with `--https` (or proxy `wss://`)
   so the served page emits `wss://`, avoiding mixed-content blocks.
4. Consider a read-only rootfs with `tmpfs` for `/tmp` and the home dir, and per-user
   resource limits, if sessions are untrusted.

The footer of the page repeats this warning so it isn't lost.

## The fix that makes the demos work

The backend spawns each shell as a session leader and makes the PTY slave its
controlling terminal (`setsid` + `TIOCSCTTY`, done in a `pre_exec` hook before
`exec`). That's what lets `Ctrl-C` deliver `SIGINT`, full-screen TUIs render, and
window resizes raise `SIGWINCH`. Without it you get a shell with no controlling
terminal — the reason naive setups have to fall back to `python3 -c 'pty.spawn(...)'`.

[xterm.rs]: https://fenrir.pro
