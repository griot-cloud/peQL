//! `peql`: write data under parcel contracts and query it through them.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use peql::format::{ResultFormat, ResultFormatter};
use peql::{Caller, Engine, PeqlError, WriteMode};

#[derive(Parser)]
#[command(name = "peql", version, about = "Query data through parcel contracts")]
struct Cli {
    /// Workspace root: contracts, functions, budgets and the audit log live in <root>/_peql;
    /// relative bindings resolve under it.
    #[arg(long, global = true, default_value = ".")]
    root: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Register a contract: a parcel bundle (from `parcel compile -o`), or a YAML/JSON contract
    /// compiled here against a sample's schema.
    Register {
        contract: PathBuf,
        /// A Parquet or CSV file with the data's schema (for a YAML/JSON contract).
        #[arg(long)]
        schema: Option<PathBuf>,
        #[command(flatten)]
        types: TypeHints,
    },
    /// Write data under a contract: flags, layout, manifest and verdict.
    Write {
        /// A registered contract's name, or a contract file (registered from the input's schema).
        contract: String,
        /// A Parquet or CSV file.
        #[arg(long)]
        input: PathBuf,
        /// Add to the existing data instead of replacing it.
        #[arg(long)]
        append: bool,
        #[command(flatten)]
        types: TypeHints,
    },
    /// Run a contract's validation plan over its data and print the verdict.
    Validate { name: String },
    /// Run SQL in which every table is a contract.
    Query {
        sql: String,
        #[command(flatten)]
        caller: CallerArgs,
        /// Output: a table (default), or JSON lines, Arrow IPC or Parquet.
        #[arg(long, value_enum, default_value_t = Output::Table)]
        format: Output,
        /// Write the result here instead of standard output.
        #[arg(long, short)]
        out: Option<PathBuf>,
        /// Also print the result envelope (to standard error).
        #[arg(long)]
        envelope: bool,
        /// Print the physical plan instead of running the query (operators only).
        #[arg(long)]
        explain: bool,
    },
    /// The schema a caller would see.
    Describe {
        name: String,
        #[command(flatten)]
        caller: CallerArgs,
    },
    /// The contracts in the workspace and the state of their data.
    List,
    /// Share a contract with a tenant, or with everyone (`public`).
    Publish {
        name: String,
        /// A tenant, or `public`.
        #[arg(long)]
        to: String,
        /// Withdraw instead.
        #[arg(long)]
        revoke: bool,
    },
    /// Set a privacy budget's limit, in epsilon per caller.
    Budget {
        name: String,
        #[arg(long)]
        limit: f64,
    },
    /// Tenants' WebAssembly functions.
    Function {
        #[command(subcommand)]
        action: FunctionCommand,
    },
}

#[derive(Subcommand)]
enum FunctionCommand {
    /// Verify a module and register one of its functions for a tenant's contracts.
    Register {
        module: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        owner: String,
    },
    /// The functions registered in the workspace.
    List,
}

#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Table,
    Json,
    Arrow,
    Parquet,
}

/// Column types for CSV input, overriding inference: `--type msisdn=utf8`.
#[derive(Args, Clone, Default)]
struct TypeHints {
    #[arg(long = "type", value_name = "COLUMN=TYPE")]
    types: Vec<String>,
}

#[derive(Args)]
struct CallerArgs {
    /// A caller file (YAML): id, tenant, purpose, tier, clearance, classification, roles, now.
    #[arg(long)]
    caller: Option<PathBuf>,
    #[arg(long)]
    tenant: Option<String>,
    #[arg(long)]
    purpose: Option<String>,
    #[arg(long)]
    id: Option<String>,
    #[arg(long = "role")]
    roles: Vec<String>,
    #[arg(long)]
    clearance: Option<i64>,
    #[arg(long)]
    tier: Option<String>,
    #[arg(long)]
    classification: Option<String>,
    /// Query time, RFC 3339. Defaults to now.
    #[arg(long)]
    now: Option<DateTime<Utc>>,
}

impl CallerArgs {
    fn resolve(&self) -> Result<Caller, String> {
        let mut c = match &self.caller {
            Some(p) => {
                let text =
                    std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?;
                yaml_serde::from_str(&text).map_err(|e| format!("{}: {e}", p.display()))?
            }
            None => Caller::new("cli", "", ""),
        };
        if let Some(v) = &self.tenant {
            c.tenant = v.clone();
        }
        if let Some(v) = &self.purpose {
            c.purpose = v.clone();
        }
        if let Some(v) = &self.id {
            c.id = v.clone();
        }
        if !self.roles.is_empty() {
            c.roles = self.roles.clone();
        }
        if let Some(v) = self.clearance {
            c.clearance = v;
        }
        if let Some(v) = &self.tier {
            c.tier = v.clone();
        }
        if let Some(v) = &self.classification {
            c.classification = v.clone();
        }
        if let Some(v) = self.now {
            c.now = v;
        }
        Ok(c)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // Rust ignores SIGPIPE, so a closed stdout (`peql query ... | head -1`) makes every print
    // panic. Restore the default: stop quietly, as other command-line tools do.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

type R = Result<ExitCode, String>;

fn s<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

async fn run(cli: Cli) -> R {
    let engine = Engine::open(&cli.root).map_err(s)?;
    match cli.command {
        Command::Register {
            contract,
            schema,
            types,
        } => {
            let reg = register(&engine, &contract, schema.as_deref(), &types).await?;
            let cc = &reg.compilation.contract;
            println!(
                "registered {} v{} ({})",
                cc.name,
                cc.version,
                &cc.compilation_hash[..16]
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Write {
            contract,
            input,
            append,
            types,
        } => {
            let (schema, batches) = read_data(&input, &types).await?;
            let name = if Path::new(&contract).is_file() {
                let text = std::fs::read_to_string(&contract).map_err(s)?;
                engine
                    .register_contract(&text, &schema)
                    .map_err(s)?
                    .name()
                    .to_owned()
            } else {
                contract
            };
            let mode = if append {
                WriteMode::Append
            } else {
                WriteMode::Overwrite
            };
            let report = engine.write(&name, batches, mode).await.map_err(s)?;
            let v = &report.verdict;
            println!(
                "wrote {} rows to {name} ({} files): {}",
                report.rows_written,
                report.files,
                if v.valid {
                    "valid".to_owned()
                } else {
                    format!("NOT SERVABLE ({})", v.breached.join(", "))
                }
            );
            for (id, fails) in &v.failures {
                if *fails > 0 {
                    println!("  {id}: {fails} rows fail");
                }
            }
            Ok(if v.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Validate { name } => {
            let v = engine.validate(&name).await.map_err(s)?;
            println!("{}", serde_json::to_string_pretty(&v).map_err(s)?);
            Ok(if v.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Query {
            sql,
            caller,
            format,
            out,
            envelope,
            explain,
        } => {
            let caller = caller.resolve()?;
            if explain {
                println!("{}", engine.explain(&sql, &caller).await.map_err(s)?);
                return Ok(ExitCode::SUCCESS);
            }
            let res = match engine.query(&sql, &caller).await {
                Ok(r) => r,
                Err(e) if e.is_refusal() => {
                    eprintln!("refused: {e}");
                    return Ok(ExitCode::from(1));
                }
                Err(e) => return Err(e.to_string()),
            };
            emit(&res.batches, format, out.as_deref())?;
            if envelope {
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(&res.envelope).map_err(s)?
                );
            } else {
                for c in &res.envelope.contracts {
                    if !c.annotations.is_empty() {
                        eprintln!(
                            "note: `{}` guarantees not met: {}",
                            c.contract,
                            c.annotations.join(", ")
                        );
                    }
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Describe { name, caller } => match engine.describe(&name, &caller.resolve()?) {
            Ok(schema) => {
                for f in schema.fields() {
                    println!("{:<24} {}", f.name(), f.data_type());
                }
                Ok(ExitCode::SUCCESS)
            }
            Err(e @ (PeqlError::Denied { .. } | PeqlError::UnknownContract(_))) => {
                eprintln!("refused: {e}");
                Ok(ExitCode::from(1))
            }
            Err(e) => Err(e.to_string()),
        },
        Command::List => {
            for reg in engine.contracts() {
                let cc = &reg.compilation.contract;
                engine.ensure_manifest(&cc.name).await.map_err(s)?;
                let state = match engine.manifest(&cc.name).map_err(s)? {
                    Some(m) if m.valid => format!(
                        "{} rows, valid, written {}",
                        m.row_count,
                        m.written_at.format("%Y-%m-%d %H:%M")
                    ),
                    Some(m) => format!(
                        "{} rows, NOT SERVABLE ({})",
                        m.row_count,
                        m.breached.join(", ")
                    ),
                    None => "no data".into(),
                };
                let audiences: Vec<String> =
                    engine.store().audiences(&cc.name).into_iter().collect();
                let shared = if audiences.is_empty() {
                    String::new()
                } else {
                    format!("  shared with {}", audiences.join(", "))
                };
                println!(
                    "{:<28} v{:<4} owner {:<10} {state}{shared}",
                    cc.name,
                    cc.version,
                    cc.owner.as_deref().unwrap_or("-")
                );
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Publish { name, to, revoke } => {
            if revoke {
                engine.unpublish(&name, &to).map_err(s)?;
                println!("{name} is no longer shared with {to}");
            } else {
                engine.publish(&name, &to).map_err(s)?;
                println!("{name} is shared with {to}");
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Budget { name, limit } => {
            engine.budgets().set_limit(&name, limit).map_err(s)?;
            println!("budget {name}: {limit} epsilon per caller");
            Ok(ExitCode::SUCCESS)
        }
        Command::Function { action } => match action {
            FunctionCommand::Register {
                module,
                manifest,
                owner,
            } => {
                let bytes =
                    std::fs::read(&module).map_err(|e| format!("{}: {e}", module.display()))?;
                let text = std::fs::read_to_string(&manifest)
                    .map_err(|e| format!("{}: {e}", manifest.display()))?;
                let m = yaml_serde::from_str(&text)
                    .map_err(|e| format!("{}: {e}", manifest.display()))?;
                let entry = engine.register_function(&bytes, &m, &owner).map_err(s)?;
                println!(
                    "registered {} v{} for {owner} ({})",
                    entry.name,
                    entry.version,
                    &entry.hash[..16]
                );
                Ok(ExitCode::SUCCESS)
            }
            FunctionCommand::List => {
                for f in engine.functions().list() {
                    let sig = f.signatures.first().map(|sig| {
                        let args: Vec<String> = sig.args.iter().map(|t| t.to_string()).collect();
                        format!("({}) -> {}", args.join(", "), sig.ret)
                    });
                    println!(
                        "{:<20} v{:<3} {:<32} owner {:<12} {}",
                        f.name,
                        f.version,
                        sig.unwrap_or_default(),
                        f.owner,
                        &f.hash[..16]
                    );
                }
                Ok(ExitCode::SUCCESS)
            }
        },
    }
}

async fn register(
    engine: &Engine,
    path: &Path,
    schema: Option<&Path>,
    types: &TypeHints,
) -> Result<Arc<peql::store::Registered>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Ok(bundle) = parcel_runtime::bundle::Bundle::from_json(&text) {
        return engine.register_bundle(&bundle).map_err(s);
    }
    let schema_from = schema
        .ok_or("a contract document needs --schema (a Parquet or CSV sample); a bundle does not")?;
    let (schema, _) = read_data(schema_from, types).await?;
    engine.register_contract(&text, &schema).map_err(s)
}

fn emit(batches: &[RecordBatch], format: Output, out: Option<&Path>) -> Result<(), String> {
    let bytes = match format {
        Output::Table => {
            let text = pretty_format_batches(batches).map_err(s)?.to_string();
            match out {
                Some(p) => std::fs::write(p, text).map_err(s)?,
                None => println!("{text}"),
            }
            return Ok(());
        }
        Output::Json => ResultFormatter::format_results(batches, ResultFormat::Json),
        Output::Arrow => ResultFormatter::format_results(batches, ResultFormat::Arrow),
        Output::Parquet => ResultFormatter::format_results(batches, ResultFormat::Parquet),
    }
    .map_err(s)?;
    match out {
        Some(p) => std::fs::write(p, bytes).map_err(s),
        None => {
            use std::io::Write;
            std::io::stdout().write_all(&bytes).map_err(s)
        }
    }
}

/// A Parquet or CSV file into batches, with CSV column types overridden by `--type`.
async fn read_data(path: &Path, hints: &TypeHints) -> Result<(Schema, Vec<RecordBatch>), String> {
    let ctx = SessionContext::new();
    let p = path.to_str().ok_or("path is not UTF-8")?;
    let hints: Vec<(String, datafusion::arrow::datatypes::DataType)> = hints
        .types
        .iter()
        .map(|t| {
            let (c, ty) = t
                .split_once('=')
                .ok_or_else(|| format!("--type expects COLUMN=TYPE, got `{t}`"))?;
            Ok((
                c.trim().to_owned(),
                parcel_core::types::parse_type_name(ty)?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let df = if path.extension().is_some_and(|e| e == "csv") {
        let inferred = ctx
            .read_csv(p, CsvReadOptions::new())
            .await
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let fields: Vec<datafusion::arrow::datatypes::Field> = inferred
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|f| match hints.iter().find(|(c, _)| c == f.name()) {
                Some((_, t)) => f.as_ref().clone().with_data_type(t.clone()),
                None => f.as_ref().clone(),
            })
            .collect();
        let schema = Schema::new(fields);
        ctx.read_csv(p, CsvReadOptions::new().schema(&schema)).await
    } else {
        ctx.read_parquet(p, ParquetReadOptions::default()).await
    }
    .map_err(|e| format!("{}: {e}", path.display()))?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.map_err(s)?;
    Ok((schema.as_ref().clone(), batches))
}
