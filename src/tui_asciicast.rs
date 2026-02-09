use anyhow::{Context, Result, anyhow, bail};
use ftui::runtime::{AsciicastRecorder, AsciicastWriter};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use std::io::{self, IsTerminal, Read, Write};
use std::path::Path;

/// Run the current `cass tui` invocation inside a PTY and mirror output to an
/// asciicast v2 file.
///
/// This records terminal output only by default. Input bytes are intentionally
/// not captured to reduce accidental secret leakage (passwords/tokens typed in
/// the terminal are not serialized into the recording stream).
pub fn run_tui_with_asciicast(recording_path: &Path, interactive: bool) -> Result<()> {
    ensure_parent_dir(recording_path)?;

    let (child_args, removed_flag) = strip_asciicast_args(std::env::args().skip(1));
    if !removed_flag {
        return Err(anyhow!(
            "internal error: --asciicast flag was not found in process arguments"
        ));
    }

    let exe_path = std::env::current_exe().context("resolve current executable path")?;
    let exe_str = exe_path
        .to_str()
        .ok_or_else(|| anyhow!("executable path is not valid UTF-8"))?;

    let (cols, rows) = crossterm::terminal::size().unwrap_or((120, 40));
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("open PTY for asciicast recording")?;

    let mut cmd = CommandBuilder::new(exe_str);
    for arg in child_args {
        cmd.arg(arg);
    }
    // Parent already handled update prompt check; avoid duplicate prompt in child.
    cmd.env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1");

    let mut child = pair
        .slave
        .spawn_command(cmd)
        .context("spawn TUI child process for asciicast recording")?;
    drop(pair.slave);

    let mut reader = pair
        .master
        .try_clone_reader()
        .context("clone PTY reader for asciicast capture")?;

    let mut writer_keepalive = Some(
        pair.master
            .take_writer()
            .context("take PTY writer for input forwarding")?,
    );
    let mut stdin_forwarder: Option<std::thread::JoinHandle<()>> = None;

    let allow_input = interactive
        && io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && dotenvy::var("TUI_HEADLESS").is_err();

    let _raw_mode = RawModeGuard::new(allow_input)?;

    if allow_input && let Some(writer) = writer_keepalive.take() {
        stdin_forwarder = Some(std::thread::spawn(move || forward_stdin(writer)));
    }

    let recorder = AsciicastRecorder::new(recording_path, cols, rows)
        .with_context(|| format!("create asciicast file at {}", recording_path.display()))?;
    let mut mirror = AsciicastWriter::new(io::stdout(), recorder);

    let mut buf = [0_u8; 8192];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                mirror
                    .write_all(&buf[..n])
                    .context("write PTY output to terminal/asciicast mirror")?;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) if is_pty_eof_error(&err) => break,
            Err(err) => return Err(err).context("read PTY output"),
        }
    }

    let _ = mirror.finish().context("finalize asciicast recording")?;
    drop(writer_keepalive);

    let status = child
        .wait()
        .context("wait for TUI child process to exit after recording")?;

    if let Some(handle) = stdin_forwarder.take()
        && handle.is_finished()
    {
        let _ = handle.join();
    }
    // If stdin is still blocked on read(), dropping the handle intentionally detaches.

    if !status.success() {
        bail!("TUI exited with non-zero status while recording: {status}");
    }
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    Ok(())
}

fn forward_stdin(mut child_writer: Box<dyn Write + Send>) {
    let stdin = io::stdin();
    let mut stdin_lock = stdin.lock();
    let mut buf = [0_u8; 256];
    loop {
        match stdin_lock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if child_writer.write_all(&buf[..n]).is_err() {
                    break;
                }
                if child_writer.flush().is_err() {
                    break;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

fn strip_asciicast_args<I>(args: I) -> (Vec<String>, bool)
where
    I: IntoIterator<Item = String>,
{
    let mut out = Vec::new();
    let mut removed = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--asciicast" {
            removed = true;
            let _ = iter.next();
            continue;
        }
        if arg.starts_with("--asciicast=") {
            removed = true;
            continue;
        }
        out.push(arg);
    }
    (out, removed)
}

fn is_pty_eof_error(err: &io::Error) -> bool {
    if matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::BrokenPipe
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        err.raw_os_error() == Some(libc::EIO)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    fn new(enabled: bool) -> Result<Self> {
        if enabled {
            crossterm::terminal::enable_raw_mode()
                .context("enable raw mode for input passthrough")?;
        }
        Ok(Self { enabled })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_pty_eof_error, strip_asciicast_args};
    use std::io;

    #[test]
    fn strips_split_asciicast_flag_and_value() {
        let input = vec![
            "tui".to_string(),
            "--asciicast".to_string(),
            "demo.cast".to_string(),
            "--once".to_string(),
        ];
        let (args, removed) = strip_asciicast_args(input);
        assert!(removed);
        assert_eq!(args, vec!["tui", "--once"]);
    }

    #[test]
    fn strips_inline_asciicast_flag() {
        let input = vec![
            "tui".to_string(),
            "--asciicast=demo.cast".to_string(),
            "--data-dir".to_string(),
            "/tmp/cass".to_string(),
        ];
        let (args, removed) = strip_asciicast_args(input);
        assert!(removed);
        assert_eq!(args, vec!["tui", "--data-dir", "/tmp/cass"]);
    }

    #[test]
    fn leaves_unrelated_args_untouched() {
        let input = vec!["tui".to_string(), "--once".to_string()];
        let (args, removed) = strip_asciicast_args(input.clone());
        assert!(!removed);
        assert_eq!(args, input);
    }

    #[test]
    fn recognizes_common_pty_eof_errors() {
        let eof = io::Error::new(io::ErrorKind::UnexpectedEof, "eof");
        assert!(is_pty_eof_error(&eof));

        let pipe = io::Error::new(io::ErrorKind::BrokenPipe, "broken");
        assert!(is_pty_eof_error(&pipe));
    }
}
