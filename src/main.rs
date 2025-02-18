use hyper::{
    Body, Request, Response, upgrade, server::conn::AddrStream, Server, StatusCode,
};
use hyper::service::{make_service_fn, service_fn};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
use nix::pty::openpty;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg, cfmakeraw};
use nix::fcntl::{fcntl, FcntlArg, OFlag};
use std::os::fd::{AsRawFd, FromRawFd};
use std::process::{Stdio};
use tokio::process::Command;
use std::convert::Infallible;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::Deserialize;
use log::info;
use nix::unistd::{setsid, dup, getsid};
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
}

#[derive(Deserialize)]
struct ResizeMessage {
    rows: u16,
    cols: u16,
}

async fn handle_websocket(
    stream: WebSocketStream<hyper::upgrade::Upgraded>,
    cmd: String,
    exit_with_process: bool,
    shutdown_sender: Option<broadcast::Sender<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("WebSocket connection established");
    let (mut ws_sender, mut ws_receiver) = stream.split();

    // Create a PTY.
    let pty = openpty(None, None)?;
    let master_fd = pty.master.as_raw_fd();
    let slave_fd = pty.slave.as_raw_fd();
    info!("PTY created (master: {}, slave: {})", master_fd, slave_fd);

    // Configure the PTY on the slave side.
    let mut termios = tcgetattr(slave_fd)?;
    cfmakeraw(&mut termios);

    // Re-enable echoing of input characters.
    use nix::sys::termios::LocalFlags;
    termios.local_flags |= LocalFlags::ECHO;

    // Apply the updated terminal settings.
    tcsetattr(slave_fd, SetArg::TCSANOW, &termios)?;
    info!("PTY (slave) configured in raw mode with ECHO enabled");

    // Set master to non-blocking mode.
    let flags = fcntl(master_fd, FcntlArg::F_GETFL)?;
    let new_flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(master_fd, FcntlArg::F_SETFL(new_flags))?;

    // Parse the command and its arguments.
    let mut cmd_parts = cmd.split_whitespace();
    let cmd_name = cmd_parts.next().unwrap_or("bash");
    let cmd_args: Vec<&str> = cmd_parts.collect();

    // Spawn the specified command.
    let slave_stdin = unsafe { Stdio::from_raw_fd(slave_fd) };
    let slave_stdout = unsafe { Stdio::from_raw_fd(slave_fd) };
    let slave_stderr = unsafe { Stdio::from_raw_fd(slave_fd) };

    let child = Command::new(cmd_name)
        .args(cmd_args)
        .env("TERM", "xterm-256color")
        .stdin(slave_stdin)
        .stdout(slave_stdout)
        .stderr(slave_stderr)
        .spawn()?;


        let child_pid = child.id()
        .expect("Child process should have a PID") as i32;  // Convert u32 to i32

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

    // Check if the process is already a session leader.
    if getsid(None).is_err() {
        if let Err(e) = setsid() {
            eprintln!("Failed to set session leader: {}", e);
            return Err(Box::new(e));
        }
    }

    // Duplicate the PTY master file descriptor.
    let master_fd_dup = dup(pty.master.as_raw_fd())?;

    // Create separate file handles for reading and writing.
    let master_file_reader = unsafe { std::fs::File::from_raw_fd(pty.master.as_raw_fd()) };
    let master_file_writer = unsafe { std::fs::File::from_raw_fd(master_fd_dup) };

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
                    if let Err(e) = handle_websocket(ws_stream, cmd, exit_with_process, shutdown_sender).await {
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
            <link rel="stylesheet" href="https://unpkg.com/xterm@5.1.0/css/xterm.css">
            <script src="https://unpkg.com/xterm@5.1.0/lib/xterm.js"></script>
            <script src="https://unpkg.com/xterm-addon-fit@0.7.0/lib/xterm-addon-fit.js"></script>
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
                const fitAddon = new FitAddon.FitAddon();
                term.loadAddon(fitAddon);
                let ws = null;
                let interval;

                function connect() {{
                    console.log('Connecting to WebSocket...');
                    ws = new WebSocket('{}://' + window.location.host + window.location.pathname.replace('term', 'ws'));
                    window.ws = ws;
                    ws.binaryType = "arraybuffer";
                    ws.onopen = () => {{
                        console.log('WebSocket connected');
                        term.reset();
                        fitAddon.fit();
                        term.focus();
                        interval = setInterval(() => ws.send(''), 300)
                    }};
                    ws.onerror = (error) => {{
                        console.error('WebSocket error:', error);
                    }};
                    ws.onclose = () => {{
                        console.log('WebSocket closed');
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
                        if (data === "\x03" || data === "\x04") {{
                            ws.send(new TextEncoder().encode(data));
                        }} else {{
                            ws.send(new TextEncoder().encode(data));
                        }}
                    }}
                }});
                term.onResize(({{ cols, rows }}) => {{
                    if (ws && ws.readyState === WebSocket.OPEN) {{
                        ws.send(JSON.stringify({{ cols, rows }}));
                    }}
                }});
                function resize() {{
                    setTimeout(() => {{
                        if (ws && ws.readyState === WebSocket.OPEN) {{
                            ws.send(new TextEncoder().encode("resize\n"));
                        }}
                    }}, 200)
                }}

                term.open(document.getElementById('terminal'));
                fitAddon.fit();
                connect();
                window.addEventListener('resize', () => fitAddon.fit());
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
        let shutdown_sender = if exit_with_process {
            Some(shutdown_sender.clone())
        } else {
            None
        };
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, https, cmd.clone(), exit_with_process, shutdown_sender.clone())
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