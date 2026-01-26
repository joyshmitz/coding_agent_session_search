fn main() -> anyhow::Result<()> {
    // Load .env early; ignore if missing.
    dotenvy::dotenv().ok();

    let raw_args: Vec<String> = std::env::args().collect();
    let parsed = match coding_agent_search::parse_cli(raw_args) {
        Ok(parsed) => parsed,
        Err(err) => {
            // If the message looks like JSON, output it directly (it's a pre-formatted robot error)
            if err.message.trim().starts_with('{') {
                eprintln!("{}", err.message);
            } else {
                // Otherwise wrap structured error
                let payload = serde_json::json!({
                    "error": {
                        "code": err.code,
                        "kind": err.kind,
                        "message": err.message,
                        "hint": err.hint,
                        "retryable": err.retryable,
                    }
                });
                eprintln!("{payload}");
            }
            std::process::exit(err.code);
        }
    };

    let use_current_thread = matches!(
        parsed.cli.command,
        Some(coding_agent_search::Commands::Search { .. })
    );
    let runtime = if use_current_thread {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
    };

    match runtime.block_on(coding_agent_search::run_with_parsed(parsed)) {
        Ok(()) => Ok(()),
        Err(err) => {
            // If the message looks like JSON, output it directly (it's a pre-formatted robot error)
            if err.message.trim().starts_with('{') {
                eprintln!("{}", err.message);
            } else {
                // Otherwise wrap structured error
                let payload = serde_json::json!({
                    "error": {
                        "code": err.code,
                        "kind": err.kind,
                        "message": err.message,
                        "hint": err.hint,
                        "retryable": err.retryable,
                    }
                });
                eprintln!("{payload}");
            }
            std::process::exit(err.code);
        }
    }
}
