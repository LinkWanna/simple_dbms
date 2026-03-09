use clap::Parser;
use simple_dbms::{DbEngine, DbExecutionResult, DbValue};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "simple_dbms",
    version,
    about = "A minimal single-file DBMS with a REPL interface"
)]
struct Args {
    /// Root directory where database files will be stored.
    #[arg(long, value_name = "PATH")]
    root: Option<PathBuf>,
}

/// Render a value to a human-readable string for CLI output.
fn render_value(value: &DbValue) -> String {
    match value {
        DbValue::Int(i) => i.to_string(),
        DbValue::Float(f) => f.to_string(),
        DbValue::Str(s) => s.clone(),
        DbValue::Null => "NULL".to_string(),
    }
}

fn run_repl(storage_root: impl AsRef<Path>) -> io::Result<()> {
    let mut engine = DbEngine::new(storage_root)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let interactive = io::stdin().is_terminal();
    let mut buffer = String::new();

    if interactive {
        println!("Simple-DBMS ready. End SQL with ';'. Type '\\q' on an empty prompt to leave.");
    }

    loop {
        if interactive {
            print!(
                "{}",
                if buffer.trim().is_empty() {
                    "simple-db> "
                } else {
                    "......> "
                }
            );
            io::stdout().flush()?;
        }

        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            if !buffer.trim().is_empty() {
                match engine.execute(buffer.trim()) {
                    Ok(DbExecutionResult::Message(msg)) => println!("{msg}"),
                    Ok(DbExecutionResult::Rows { columns, rows }) => {
                        println!("{}", columns.join(" | "));
                        if rows.is_empty() {
                            println!("[no rows]");
                        } else {
                            for row in &rows {
                                println!(
                                    "{}",
                                    row.iter().map(render_value).collect::<Vec<_>>().join(" | ")
                                );
                            }
                        }
                    }
                    Err(err) => eprintln!("Error: {err}"),
                }
            }
            break;
        }

        if buffer.trim().is_empty() && line.trim().is_empty() {
            continue;
        }

        if buffer.trim().is_empty() && line.trim() == "\\q" {
            break;
        }

        buffer.push_str(&line);

        while let Some(pos) = buffer.find(';') {
            let sql = buffer[..=pos].trim().to_string();
            buffer = buffer[pos + 1..].to_string();

            if sql.is_empty() {
                continue;
            }

            match engine.execute(&sql) {
                Ok(DbExecutionResult::Message(msg)) => println!("{msg}"),
                Ok(DbExecutionResult::Rows { columns, rows }) => {
                    println!("{}", columns.join(" | "));
                    if rows.is_empty() {
                        println!("[no rows]");
                    } else {
                        for row in &rows {
                            println!(
                                "{}",
                                row.iter().map(render_value).collect::<Vec<_>>().join(" | ")
                            );
                        }
                    }
                }
                Err(err) => eprintln!("Error: {err}"),
            }

            if buffer.trim().is_empty() {
                buffer.clear();
                break;
            }
        }
    }

    Ok(())
}

fn main() {
    let args = Args::parse();
    let root = args.root.unwrap_or(PathBuf::from("data"));

    if let Err(err) = run_repl(&root) {
        eprintln!("Failed to start REPL: {err}");
        std::process::exit(1);
    }
}
