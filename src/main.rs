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
use std::process::{Command, Stdio};
use std::convert::Infallible;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::Deserialize;
use log::info;
use libc::{ioctl, TIOCSCTTY};
use nix::unistd::{setsid, setpgid, Pid, dup, getsid, getpgrp};

#[derive(Deserialize)]
struct ResizeMessage {
    rows: u16,
    cols: u16,
}

async fn handle_websocket(
    stream: WebSocketStream<hyper::upgrade::Upgraded>,
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

    // Make the PTY the controlling terminal for the shell.
    unsafe {
        ioctl(pty.master.as_raw_fd(), TIOCSCTTY, 0);
    }

    // Spawn an interactive shell.
    let mut child = Command::new("bash")
        .arg("-i")
        .arg("-c")
        .arg("python3 -c 'import pty; pty.spawn(\"/bin/bash\")'") // If I remove this, CTRL+C does not work, but resize works.
        .env("TERM", "xterm-256color")
        .stdin(unsafe { Stdio::from_raw_fd(slave_fd) })
        .stdout(unsafe { Stdio::from_raw_fd(slave_fd) })
        .stderr(unsafe { Stdio::from_raw_fd(slave_fd) })
        .spawn()?;
    info!("Spawned shell process (PID: {})", child.id());

    // Check if the process is already a session leader.
    if getsid(None).is_err() {
        if let Err(e) = setsid() {
            eprintln!("Failed to set session leader: {}", e);
            return Err(Box::new(e));
        }
    }

    // Check if the process is already in its own process group.
    if getpgrp() != Pid::from_raw(0) {
        if let Err(e) = setpgid(Pid::from_raw(0), Pid::from_raw(0)) {
            eprintln!("Failed to set process group: {}", e);
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
            info!("Received WebSocket message");
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    info!("Received text message (possibly resize): {}", text);
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
                        // let cmd = format!("stty rows {} columns {}\n", resize.rows, resize.cols);
                        // if let Err(e) = pty_writer.write_all(cmd.as_bytes()).await {
                        //     eprintln!("PTY write error: {}", e);
                        //     break;
                        // }
                        // $ stty rows 38 columns 116
                        info!("Terminal resized to {}x{}", resize.cols, resize.rows);
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                    info!("Writing {} bytes to PTY: {:?}", data.len(), data);
                    if let Err(e) = pty_writer.write_all(&data).await {
                        eprintln!("PTY write error: {}", e);
                        break;
                    }
                    if let Err(e) = pty_writer.flush().await {
                        eprintln!("PTY flush error: {}", e);
                        break;
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
    let _ = child.kill();
    Ok(())
}

async fn handle_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    info!("Received request: {} {}", req.method(), req.uri().path());

    if req.uri().path() == "/ws" {
        info!("WebSocket upgrade requested");

        // Ensure proper upgrade headers.
        if req.headers().get("upgrade")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_lowercase()) != Some("websocket".to_string()) {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Missing or invalid Upgrade header"))
                .unwrap());
        }

        // Get Sec-WebSocket-Key.
        let key = match req.headers().get("sec-websocket-key") {
            Some(k) => k.to_str().unwrap_or(""),
            None => {
                eprintln!("Missing Sec-WebSocket-Key header");
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Missing Sec-WebSocket-Key"))
                    .unwrap());
            }
        };

        // Compute accept key.
        let accept_key = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());

        // Build the 101 Switching Protocols response.
        let response = Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Accept", accept_key)
            .body(Body::empty())
            .unwrap();

        // Spawn the upgrade task, consuming the request.
        tokio::spawn(async move {
            match upgrade::on(req).await {
                Ok(upgraded) => {
                    info!("Connection upgraded");
                    let ws_stream = WebSocketStream::from_raw_socket(upgraded, Role::Server, None).await;
                    if let Err(e) = handle_websocket(ws_stream).await {
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
        info!("Serving HTML page");
        let html = r#"
        <!DOCTYPE html>
        <html>
        <head>
            <link rel="stylesheet" href="https://unpkg.com/xterm@5.1.0/css/xterm.css">
            <script src="https://unpkg.com/xterm@5.1.0/lib/xterm.js"></script>
            <script src="https://unpkg.com/xterm-addon-fit@0.7.0/lib/xterm-addon-fit.js"></script>
        </head>
        <body>
            <div id="terminal" style="height: 100vh;"></div>
            <script>
                const term = new Terminal({
                    cursorBlink: true,
                    fontFamily: 'monospace',
                    fontSize: 14,
                    theme: { background: '#1e1e1e', foreground: '#ffffff' },
                    convertEol: true, // Convert \n to \r\n
                });
                const fitAddon = new FitAddon.FitAddon();
                term.loadAddon(fitAddon);
                let ws = null;
                function connect() {
                    console.log('Connecting to WebSocket...');
                    ws = new WebSocket('ws://' + window.location.host + '/ws');
                    ws.binaryType = "arraybuffer"; // Ensure binary messages are ArrayBuffers.
                    ws.onopen = () => {
                        console.log('WebSocket connected');
                        term.reset();
                        fitAddon.fit();
                        term.focus();
                        ws.send(JSON.stringify({ cols, rows }));

                    };
                    ws.onerror = (error) => {
                        console.error('WebSocket error:', error);
                    };
                    ws.onclose = () => {
                        console.log('WebSocket closed');
                    };
                    ws.onmessage = (event) => {
                        let text;
                        if (typeof event.data === "string") {
                            text = event.data;
                        } else {
                            text = new TextDecoder("utf-8").decode(event.data);
                        }
                        term.write(text);
                    };
                }
                term.onData(data => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        // Handle CTRL+C (0x03) and CTRL+D (0x04)
                        if (data === "\x03" || data === "\x04") {
                            ws.send(new TextEncoder().encode(data));
                        } else {
                            ws.send(new TextEncoder().encode(data));
                        }
                    }
                });
                term.onResize(({ cols, rows }) => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ cols, rows }));
                    }
                });
                term.open(document.getElementById('terminal'));
                fitAddon.fit();
                connect();
                window.addEventListener('resize', () => fitAddon.fit());
            </script>
        </body>
        </html>
        "#;
        return Ok(Response::new(Body::from(html)));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = ([0, 0, 0, 0], 8080).into();
    let service = make_service_fn(|_conn: &AddrStream| async {
        Ok::<_, Infallible>(service_fn(handle_request))
    });

    info!("Server starting on http://{}", addr);
    Server::bind(&addr).serve(service).await?;
    Ok(())
}