mod check_dates;
mod live_paper;
mod sftp_key;
mod sftp_to_s3;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mm-cli")]
#[command(about = "Market Making CLI Tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check available dates on Crypto.com SFTP server
    CheckDates {
        /// SFTP username
        #[arg(long, env = "CRYPTO_COM_SFTP_USERNAME", default_value = "user080")]
        sftp_username: String,

        /// Path to SFTP private key
        #[arg(
            long,
            env = "CRYPTO_COM_SFTP_KEY_PATH",
            default_value = "./user080 (1) 2"
        )]
        sftp_key_path: String,

        /// Base SFTP path
        #[arg(
            long,
            env = "CRYPTO_COM_SFTP_REMOTE_PATH",
            default_value = "exchange/book_l2_150_0010"
        )]
        sftp_base_path: String,

        /// Trading pair to check
        #[arg(long, env = "TRADING_PAIR", default_value = "BTC_USDT")]
        trading_pair: String,

        /// Year to check (e.g. 2025) or "all" for all available years
        #[arg(long, default_value = "2025")]
        year: String,
    },
    /// Upload data directly from Crypto.com SFTP to AWS S3
    Upload {
        /// SFTP username
        #[arg(long, env = "CRYPTO_COM_SFTP_USERNAME", default_value = "user080")]
        sftp_username: String,

        /// Path to SFTP private key
        #[arg(
            long,
            env = "CRYPTO_COM_SFTP_KEY_PATH",
            default_value = "./user080 (1) 2"
        )]
        sftp_key_path: String,

        /// SFTP remote path (base path, date will be appended)
        #[arg(
            long,
            env = "CRYPTO_COM_SFTP_REMOTE_PATH",
            default_value = "exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT"
        )]
        sftp_remote_path: String,

        /// S3 bucket name
        #[arg(long, env = "S3_BUCKET")]
        s3_bucket: String,

        /// S3 prefix/path
        #[arg(long, env = "S3_PREFIX", default_value = "BTC_USDT/")]
        s3_prefix: String,

        /// AWS region
        #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
        s3_region: String,

        /// Start date (YYYY-MM-DD)
        #[arg(long, default_value = "2023-10-25")]
        start_date: String,

        /// End date (YYYY-MM-DD)
        #[arg(long, default_value = "2023-10-31")]
        end_date: String,

        /// Trading pair (e.g., BTC_USDT)
        #[arg(long, env = "TRADING_PAIR", default_value = "BTC_USDT")]
        trading_pair: String,

        /// Maximum concurrent SFTP download workers (default: 20)
        #[arg(long, env = "MAX_CONCURRENT_UPLOADS")]
        max_concurrent: Option<usize>,

        /// Maximum concurrent S3 PutObject calls (default: 64)
        #[arg(long, env = "MAX_S3_CONCURRENT_UPLOADS")]
        max_s3_concurrent: Option<usize>,

        /// Skip checking if files already exist in S3 (faster for first-time uploads)
        #[arg(long, default_value = "false")]
        skip_s3_check: bool,
    },
    /// Run the RebateMM strategy in live paper/sim mode using Crypto.com market data
    LivePaper {
        /// Trading pair to run live paper mode on
        #[arg(long, env = "TRADING_PAIR", default_value = "ETH_USDT")]
        trading_pair: String,

        /// Book depth to subscribe to
        #[arg(long, default_value_t = 50)]
        depth: u32,

        /// Dashboard update interval in milliseconds
        #[arg(long, default_value_t = 1000)]
        dashboard_interval_ms: u64,

        /// Assumed touch queue percentage for paper fills
        #[arg(long, default_value_t = 0.5)]
        queue_depth_pct: f64,

        /// Latency profile for order placement/cancel handling
        #[arg(long, value_enum, default_value_t = live_paper::LatencyProfile::Disabled)]
        latency_profile: live_paper::LatencyProfile,

        /// Optional directory for recording public trades as jsonl
        #[arg(long)]
        record_trades_dir: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::CheckDates {
            sftp_username,
            sftp_key_path,
            sftp_base_path,
            trading_pair,
            year,
        } => {
            let year_filter = if year.eq_ignore_ascii_case("all") {
                None
            } else {
                Some(year.as_str())
            };
            check_dates::check_available_dates(
                "data.crypto.com",
                &sftp_username,
                &sftp_key_path,
                &sftp_base_path,
                &trading_pair,
                year_filter,
            )?;
        }
        Commands::Upload {
            sftp_username,
            sftp_key_path,
            sftp_remote_path,
            s3_bucket,
            s3_prefix,
            s3_region,
            start_date,
            end_date,
            trading_pair: _,
            max_concurrent,
            max_s3_concurrent,
            skip_s3_check,
        } => {
            // Ensure prefix ends with /
            let s3_prefix = if s3_prefix.ends_with('/') {
                s3_prefix
            } else {
                format!("{}/", s3_prefix)
            };

            sftp_to_s3::upload_sftp_to_s3(
                "data.crypto.com",
                &sftp_username,
                &sftp_key_path,
                &sftp_remote_path,
                &s3_bucket,
                &s3_prefix,
                &s3_region,
                &start_date,
                &end_date,
                max_concurrent,
                max_s3_concurrent,
                skip_s3_check,
            )
            .await?;
        }
        Commands::LivePaper {
            trading_pair,
            depth,
            dashboard_interval_ms,
            queue_depth_pct,
            latency_profile,
            record_trades_dir,
        } => {
            live_paper::run_live_paper(live_paper::LivePaperConfig {
                trading_pair,
                depth,
                dashboard_interval_ms,
                queue_depth_pct,
                latency_profile,
                record_trades_dir: record_trades_dir.map(Into::into),
            })
            .await?;
        }
    }

    Ok(())
}
