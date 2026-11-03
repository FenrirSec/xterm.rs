use hyper::{
    Body, Request, Response, upgrade, server::conn::AddrStream, Server, StatusCode,
};
use hyper::service::{make_service_fn, service_fn};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
use nix::pty::openpty;
use nix::fcntl::{fcntl, FcntlArg, OFlag};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use tokio::process::Command;
use std::convert::Infallible;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::Deserialize;
use log::info;
use nix::unistd::dup;
use structopt::StructOpt;
use tokio::sync::broadcast;

#[derive(StructOpt, Debug)]
#[structopt(name = "websocket-server", about = "A WebSocket server with PTY support.")]
struct Opt {
    /// Use HTTPS (wss://) instead of HTTP (ws://)
    #[structopt(long)]
    https: bool,

    /// Host to bind the server to
    #[structopt(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to bind the server to
    #[structopt(long, default_value = "8080")]
    port: u16,

    /// Command to run in the PTY (default: "bash -i")
    #[structopt(long, default_value = "bash -i")]
    cmd: String,

    /// Exit the program when the PTY process closes or when CTRL+D is received
    #[structopt(long)]
    exit_with_process: bool,

    /// Directory to append per-connection keystroke audit logs to. Omit to
    /// disable. Each WebSocket connection gets its own timestamped file holding
    /// everything the user typed. Logged in the server, so a user cannot turn it
    /// off from inside the shell.
    #[structopt(long)]
    log_dir: Option<String>,

    /// Label written into each log's name and header so the admin can tell
    /// whose session it is (e.g. the user or session id). Falls back to the
    /// HC_SESSION_ID environment variable when not given.
    #[structopt(long)]
    label: Option<String>,

    /// Stream keystrokes to the store over WebSocket instead of (or as well as)
    /// a log file, e.g. wss://store/internal/termlog or
    /// ws://store:5000/internal/termlog. Best-effort: if the store is
    /// unreachable the terminal keeps working.
    #[structopt(long)]
    log_ws_url: Option<String>,

    /// Shared secret sent as ?token= on the log WebSocket. Must match the
    /// store's TERMLOG_TOKEN.
    #[structopt(long)]
    log_ws_token: Option<String>,

    /// Accept a self-signed / internal TLS certificate on the log WebSocket
    /// (wss). Use when the store presents an internal cert on the docker net.
    #[structopt(long)]
    log_ws_insecure: bool,
}

#[derive(Deserialize)]
struct ResizeMessage {
    rows: u16,
    cols: u16,
}

/// Stream keystrokes to the store over WebSocket. Best-effort: any failure
/// logs and returns, which drops the receiver so the terminal's send side just
/// no-ops. Never blocks or kills the terminal.
async fn ws_logger(
    url: String,
    token: Option<String>,
    insecure: bool,
    user: String,
    pid: i32,
    cmd: String,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(u128, String)>,
) {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let full_url = match token {
        Some(ref t) if !t.is_empty() => {
            if url.contains('?') { format!("{}&token={}", url, t) } else { format!("{}?token={}", url, t) }
        }
        _ => url.clone(),
    };

    // wss:// support is opt-in via the `wss` cargo feature, so the default
    // build needs no TLS crate and compiles unchanged. With the feature on,
    // --log-ws-insecure accepts a self-signed/internal cert.
    let connect_res = {
        #[cfg(feature = "wss")]
        {
            if insecure && full_url.starts_with("wss") {
                match native_tls::TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true)
                    .build()
                {
                    Ok(tls) => {
                        let connector = tokio_tungstenite::Connector::NativeTls(tls);
                        tokio_tungstenite::connect_async_tls_with_config(full_url.as_str(), None, false, Some(connector)).await
                    }
                    Err(e) => {
                        eprintln!("termlog: TLS setup failed: {}", e);
                        return;
                    }
                }
            } else {
                tokio_tungstenite::connect_async(full_url.as_str()).await
            }
        }
        #[cfg(not(feature = "wss"))]
        {
            if full_url.starts_with("wss") {
                eprintln!("termlog: wss:// needs the `wss` cargo feature; \
                           build with --features wss, or use ws:// (TLS at your proxy)");
            }
            let _ = insecure;
            tokio_tungstenite::connect_async(full_url.as_str()).await
        }
    };

    let (mut ws, _resp) = match connect_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("termlog: connect to store failed: {}", e);
            return;
        }
    };

    let hello = serde_json::json!({
        "type": "hello", "user": user, "pid": pid.to_string(), "cmd": cmd
    }).to_string();
    if ws.send(Message::Text(hello)).await.is_err() {
        return;
    }
    info!("termlog: streaming to store");

    while let Some((t, data)) = rx.recv().await {
        let frame = serde_json::json!({
            "type": "input", "t": t as u64, "data": data
        }).to_string();
        if ws.send(Message::Text(frame)).await.is_err() {
            break;
        }
    }
    let _ = ws.close(None).await;
}

async fn handle_websocket(
    stream: WebSocketStream<hyper::upgrade::Upgraded>,
    cmd: String,
    exit_with_process: bool,
    shutdown_sender: Option<broadcast::Sender<()>>,
    log_dir: Option<String>,
    label: Option<String>,
    log_ws_url: Option<String>,
    log_ws_token: Option<String>,
    log_ws_insecure: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Write as _;        // writeln! on the std log File
    info!("WebSocket connection established");
    let (mut ws_sender, mut ws_receiver) = stream.split();

    // Create a PTY. Seed it with a sane 80x24 default so anything that spawns a
    // full-screen program before the client's first resize message lands does
    // not start at 0x0. The real size still arrives from the browser on connect.
    let winsize = nix::pty::Winsize {
        ws_row: 24,
        ws_col: 80,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = openpty(Some(&winsize), None)?;
    let master_fd = pty.master.as_raw_fd();
    let slave_fd = pty.slave.as_raw_fd();
    info!("PTY created (master: {}, slave: {})", master_fd, slave_fd);

    // Leave the slave in its default cooked mode. Bash/readline configure the
    // line discipline themselves; forcing raw mode here would clear ISIG and
    // turn Ctrl-C into a literal byte instead of a SIGINT.

    // Set master to non-blocking mode.
    let flags = fcntl(master_fd, FcntlArg::F_GETFL)?;
    let new_flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(master_fd, FcntlArg::F_SETFL(new_flags))?;

    // Parse the command and its arguments.
    let cmd_for_log = cmd.clone();
    let mut cmd_parts = cmd.split_whitespace();
    let cmd_name = cmd_parts.next().unwrap_or("bash");
    let cmd_args: Vec<&str> = cmd_parts.collect();

    // Build the child. All controlling-terminal setup happens in the pre_exec
    // hook, which runs in the forked child before exec. Keep it strictly
    // async-signal-safe: raw syscalls only, no allocation, no locks. This is
    // what `python3 -c 'pty.spawn(...)'` was doing for you.
    let mut command = Command::new(cmd_name);
    command.args(cmd_args).env("TERM", "xterm-256color");

    unsafe {
        command.pre_exec(move || {
            // New session => session leader with no controlling terminal.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // Claim the slave PTY as this session's controlling terminal.
            if libc::ioctl(slave_fd, libc::TIOCSCTTY, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // Wire stdin/stdout/stderr to the slave.
            if libc::dup2(slave_fd, libc::STDIN_FILENO) == -1
                || libc::dup2(slave_fd, libc::STDOUT_FILENO) == -1
                || libc::dup2(slave_fd, libc::STDERR_FILENO) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            // Drop the raw fds the child no longer needs.
            if slave_fd > libc::STDERR_FILENO {
                libc::close(slave_fd);
            }
            libc::close(master_fd);
            Ok(())
        });
    }

    let child = command.spawn()?;
    let child_pid = child.id().expect("Child process should have a PID") as i32;
    info!("Spawned '{}' with PID {}", cmd_name, child_pid);

    // The parent must release the slave end, otherwise the master never sees
    // EOF when the shell exits.
    drop(pty.slave);

    // Optional keystroke audit log: one file per connection. Everything the
    // user sends is appended as a timestamped, escaped line. It lives here in
    // the server rather than in the shell, so it cannot be disabled from inside
    // bash. Note: this captures *all* input, including anything typed at a
    // password prompt.
    let mut input_log: Option<std::fs::File> = None;
    if let Some(dir) = log_dir.as_deref() {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Who this session belongs to: --label, else HC_SESSION_ID, else blank.
        let raw_label = label.clone()
            .or_else(|| std::env::var("HC_SESSION_ID").ok())
            .unwrap_or_default();
        // Keep the label filesystem-safe (it becomes part of the file name).
        let safe_label: String = raw_label.chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' { c } else { '_' })
            .take(64)
            .collect();
        let path = if safe_label.is_empty() {
            format!("{}/session-{}-{}.log", dir, child_pid, ts)
        } else {
            format!("{}/session-{}-{}-{}.log", dir, safe_label, child_pid, ts)
        };
        match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut f) => {
                let _ = writeln!(f, "# start pid={} ts={} user={:?} cmd={:?}",
                                 child_pid, ts, raw_label, cmd_for_log);
                let _ = f.flush();
                info!("Keystroke log: {}", path);
                input_log = Some(f);
            }
            Err(e) => eprintln!("Keystroke log: cannot open {}: {}", path, e),
        }
    }

    // Optional: stream keystrokes to the store over WebSocket. Spawned as a
    // detached task so a slow or down store never blocks the terminal.
    let log_tx: Option<tokio::sync::mpsc::UnboundedSender<(u128, String)>> =
        if let Some(url) = log_ws_url {
            let user = label.clone()
                .or_else(|| std::env::var("HC_SESSION_ID").ok())
                .unwrap_or_default();
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(u128, String)>();
            tokio::spawn(ws_logger(url, log_ws_token, log_ws_insecure,
                                   user, child_pid, cmd_for_log.clone(), rx));
            Some(tx)
        } else {
            None
        };

    if exit_with_process {
        if let Some(sender) = &shutdown_sender {
            let sender = sender.clone();
            let mut child = child;
            tokio::spawn(async move {
                let _ = child.wait().await;
                let _ = sender.send(());
            });
        }
    }

    // Take sole ownership of the master fd, then build separate read/write
    // handles. into_raw_fd stops the OwnedFd and the File from both closing
    // the same descriptor.
    let master_raw = pty.master.into_raw_fd();
    let read_fd = dup(master_raw)?;
    let master_file_reader = unsafe { std::fs::File::from_raw_fd(read_fd) };
    let master_file_writer = unsafe { std::fs::File::from_raw_fd(master_raw) };

    // Wrap the files in async I/O wrappers.
    let mut pty_reader = tokio::io::BufReader::new(tokio::fs::File::from_std(master_file_reader));
    let mut pty_writer = tokio::io::BufWriter::new(tokio::fs::File::from_std(master_file_writer));

    // Task: forward data from PTY to WebSocket.
    let pty_to_ws = async move {
        info!("Pty to WS");
        let mut buf = [0u8; 1024];
        loop {
            match pty_reader.read(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    info!("Sending {} bytes to WebSocket", n);
                    if let Err(e) = ws_sender.send(tokio_tungstenite::tungstenite::Message::Binary(buf[..n].to_vec())).await {
                        eprintln!("Send error: {}", e);
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => {
                    eprintln!("PTY read error: {}", e);
                    break;
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Task: forward data (or resize commands) from WebSocket to PTY.
    let ws_to_pty = async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    if let Ok(resize) = serde_json::from_str::<ResizeMessage>(&text) {
                        let winsize = libc::winsize {
                            ws_row: resize.rows,
                            ws_col: resize.cols,
                            ws_xpixel: 0,
                            ws_ypixel: 0,
                        };
                        unsafe {
                            libc::ioctl(master_fd, libc::TIOCSWINSZ, &winsize);
                        }
                        info!("Terminal resized to {}x{}", resize.cols, resize.rows);
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                    // Audit everything the user typed, before any filtering, so
                    // the record is faithful (Ctrl-D, escape sequences and all).
                    if input_log.is_some() || log_tx.is_some() {
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0);
                        if let Some(f) = input_log.as_mut() {
                            let _ = writeln!(f, "{}\t{:?}", ts, String::from_utf8_lossy(&data));
                            let _ = f.flush();
                        }
                        if let Some(tx) = log_tx.as_ref() {
                            let _ = tx.send((ts, String::from_utf8_lossy(&data).to_string()));
                        }
                    }

                    if exit_with_process && data.contains(&0x04) {
                        if let Some(sender) = &shutdown_sender {
                            let _ = sender.send(());
                        }
                        break;
                    }

                    let filtered_data: Vec<u8> = if exit_with_process {
                        data
                    } else {
                        data.into_iter().filter(|&b| b != 0x04).collect()
                    };

                    if !filtered_data.is_empty() {
                        info!("Writing {} bytes to PTY: {:?}", filtered_data.len(), filtered_data);
                        if let Err(e) = pty_writer.write_all(&filtered_data).await {
                            eprintln!("PTY write error: {}", e);
                            break;
                        }
                        if let Err(e) = pty_writer.flush().await {
                            eprintln!("PTY flush error: {}", e);
                            break;
                        }
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Run both tasks concurrently until one completes.
    tokio::select! {
        res = pty_to_ws => res?,
        res = ws_to_pty => res?,
    };

    info!("Closing connection");
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child_pid),
        nix::sys::signal::Signal::SIGKILL,
    );
    Ok(())
}

async fn handle_request(
    req: Request<Body>,
    https: bool,
    cmd: String,
    exit_with_process: bool,
    shutdown_sender: Option<broadcast::Sender<()>>,
    log_dir: Option<String>,
    label: Option<String>,
    log_ws_url: Option<String>,
    log_ws_token: Option<String>,
    log_ws_insecure: bool,
) -> Result<Response<Body>, Infallible> {
    info!("Received request: {} {}", req.method(), req.uri().path());

    if req.uri().path() == "/ws" {
        info!("WebSocket upgrade requested");

        if req.headers().get("upgrade")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_lowercase()) != Some("websocket".to_string()) {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Missing or invalid Upgrade header"))
                .unwrap());
        }

        let key = match req.headers().get("sec-websocket-key") {
            Some(k) => k.to_str().unwrap_or(""),
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Missing Sec-WebSocket-Key"))
                    .unwrap());
            }
        };

        let accept_key = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());

        let response = Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Accept", accept_key)
            .body(Body::empty())
            .unwrap();

        tokio::spawn(async move {
            match upgrade::on(req).await {
                Ok(upgraded) => {
                    info!("Connection upgraded");
                    let ws_stream = WebSocketStream::from_raw_socket(upgraded, Role::Server, None).await;
                    if let Err(e) = handle_websocket(ws_stream, cmd, exit_with_process, shutdown_sender, log_dir, label, log_ws_url, log_ws_token, log_ws_insecure).await {
                        eprintln!("WebSocket handling error: {}", e);
                    }
                }
                Err(e) => eprintln!("Upgrade error: {}", e),
            }
        });

        return Ok(response);
    } else if req.uri().path() == "/favicon.ico" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap());
    } else {
        let ws_protocol = if https { "wss" } else { "ws" };
        let html = format!(r#"
        <!DOCTYPE html>
        <html>
        <head>
            <link rel="stylesheet" href="https://unpkg.com/@xterm/xterm/css/xterm.css">
            <script src="https://unpkg.com/@xterm/xterm/lib/xterm.js"></script>
            <script src="https://unpkg.com/@xterm/addon-fit/lib/addon-fit.js"></script>
        </head>
        <body>
            <style>
                html, div, body {{
                    padding: 0;
                    margin: 0;
                }}
            </style>
            <div id="terminal" style="height: 100vh;"></div>
            <script>
                const term = new Terminal({{
                    cursorBlink: true,
                    fontFamily: 'monospace',
                    fontSize: 14,
                    theme: {{ background: '#111314', foreground: '#ffffff' }},
                    convertEol: true,
                }});
                // @xterm/addon-fit exposes FitAddon.FitAddon; fall back to a
                // bare FitAddon global just in case the build shape differs.
                const FitAddonCtor = (window.FitAddon && window.FitAddon.FitAddon)
                    ? window.FitAddon.FitAddon
                    : window.FitAddon;
                const fitAddon = new FitAddonCtor();
                term.loadAddon(fitAddon);
                let ws = null;
                let interval;
                let ro = null;
                const el = document.getElementById('terminal');

                // Report the *current* terminal size to the server. Sent
                // explicitly (not just via onResize) because fit() only emits
                // onResize when dimensions actually change, which usually does
                // not happen on the first connect.
                function sendSize() {{
                    if (ws && ws.readyState === WebSocket.OPEN) {{
                        ws.send(JSON.stringify({{ cols: term.cols, rows: term.rows }}));
                    }}
                }}

                // Measure the container, resize the terminal, then report.
                function fitAndSend() {{
                    try {{ fitAddon.fit(); }} catch (e) {{}}
                    sendSize();
                }}

                function connect() {{
                    console.log('Connecting to WebSocket...');
                    ws = new WebSocket('{}://' + window.location.host + window.location.pathname.replace('term', 'ws'));
                    window.ws = ws;
                    ws.binaryType = "arraybuffer";
                    ws.onopen = () => {{
                        console.log('WebSocket connected');
                        term.reset();
                        // Wait for web-font metrics (no-op with plain monospace),
                        // then let layout settle one frame before measuring so
                        // the iframe has reached its final size.
                        const settle = document.fonts ? document.fonts.ready : Promise.resolve();
                        settle.then(() => requestAnimationFrame(() => {{
                            fitAndSend();
                            term.focus();
                        }}));
                        interval = setInterval(() => ws.send(''), 300);
                    }};
                    ws.onerror = (error) => {{
                        console.error('WebSocket error:', error);
                    }};
                    ws.onclose = () => {{
                        console.log('WebSocket closed');
                        clearInterval(interval);
                    }};
                    ws.onmessage = (event) => {{
                        let text;
                        if (typeof event.data === "string") {{
                            text = event.data;
                        }} else {{
                            text = new TextDecoder("utf-8").decode(event.data);
                        }}
                        term.write(text);
                    }};
                }}

                term.onData(data => {{
                    if (ws && ws.readyState === WebSocket.OPEN) {{
                        ws.send(new TextEncoder().encode(data));
                    }}
                }});

                // Fires when fit() (or anything) actually changes dimensions.
                term.onResize(() => sendSize());

                // Manual fallback via the `resize` escape-sequence round-trip.
                // No longer needed, kept for convenience/debugging.
                function resize() {{
                    setTimeout(() => {{
                        if (ws && ws.readyState === WebSocket.OPEN) {{
                            ws.send(new TextEncoder().encode("resize\n"));
                        }}
                    }}, 200);
                }}

                term.open(el);

                // Android/Gboard input hardening. Some soft keyboards honor
                // these and stop injecting predictive/duplicated input.
                const ta = document.querySelector('.xterm-helper-textarea');
                if (ta) {{
                    ta.setAttribute('autocomplete', 'off');
                    ta.setAttribute('autocorrect', 'off');
                    ta.setAttribute('autocapitalize', 'none');
                    ta.setAttribute('spellcheck', 'false');
                }}

                // The core fix: re-measure whenever the container's real size
                // changes. This fires when the injected iframe finally settles,
                // when the challenge wrapper flips from display:none to visible,
                // and on window resize -- so even if the first fit runs too
                // early at the wrong size, the next one corrects it and reports
                // the right dimensions to the PTY.
                ro = new ResizeObserver(() => fitAndSend());
                ro.observe(el);

                connect();
                window.addEventListener('resize', () => fitAndSend());
            </script>
        </body>
        </html>
        "#, ws_protocol);
        return Ok(Response::new(Body::from(html)));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt = Opt::from_args();

    let (shutdown_sender, _) = broadcast::channel(1);

    let addr = format!("{}:{}", opt.host, opt.port).parse()?;
    let service = make_service_fn(|_conn: &AddrStream| {
        let https = opt.https;
        let cmd = opt.cmd.clone();
        let exit_with_process = opt.exit_with_process;
        let log_dir = opt.log_dir.clone();
        let label = opt.label.clone();
        let log_ws_url = opt.log_ws_url.clone();
        let log_ws_token = opt.log_ws_token.clone();
        let log_ws_insecure = opt.log_ws_insecure;
        let shutdown_sender = if exit_with_process {
            Some(shutdown_sender.clone())
        } else {
            None
        };
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, https, cmd.clone(), exit_with_process, shutdown_sender.clone(), log_dir.clone(), label.clone(), log_ws_url.clone(), log_ws_token.clone(), log_ws_insecure)
            }))
        }
    });

    info!("Server starting on {}://{}", if opt.https { "https" } else { "http" }, addr);
    let server = Server::bind(&addr).serve(service);

    let graceful = server.with_graceful_shutdown(async {
        let _ = shutdown_sender.subscribe().recv().await;
        info!("Received shutdown signal, shutting down server.");
    });

    graceful.await?;
    Ok(())
}
