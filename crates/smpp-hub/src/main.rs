use clap::Parser;
use smpp_core::tcp::{Server, connect};
use smpp_hub::{
    Hub,
    model::BindRole,
    standalone::{Settings, SqliteStore},
};
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::task::JoinSet;

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let settings: Settings = serde_json::from_slice(&tokio::fs::read(&cli.config).await?)?;
    let store = Arc::new(SqliteStore::open(settings.clone()).await?);
    let hub = Arc::new(Hub::new(store, &settings.system_id));
    let listener = Server::bind(settings.listen).await?;
    tracing::info!(address=%listener.local_addr()?, "SMPP hub listening");
    let mut sessions = JoinSet::new();
    let mut dialing = HashSet::new();
    let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = &mut shutdown => { result?; break; },
            incoming = listener.accept() => {
                let (stream, address) = incoming?;
                let hub = hub.clone();
                sessions.spawn(async move {
                    if let Err(error) = hub.serve_inbound(stream).await { tracing::warn!(%address,%error,"SMPP session ended"); }
                    None
                });
            }
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Ok(Some(id)) = result { dialing.remove(&id); }
            }
            _ = sweep.tick() => {
                if let Err(error) = hub.registry.expire_messages(chrono::Utc::now()).await { tracing::error!(%error,"message expiry failed"); }
                if let Err(error) = hub.queue_terminal_receipts().await { tracing::error!(%error,"receipt queuing failed"); }
                hub.bound.nudge_all().await;
                for peer in &settings.peers {
                    if peer.site.bind_role != BindRole::HubBindsToSite || dialing.contains(&peer.site.id) { continue; }
                    let Some(address) = peer.address else { continue; };
                    dialing.insert(peer.site.id);
                    let hub = hub.clone();
                    let site = peer.site.clone();
                    sessions.spawn(async move {
                        let id = site.id;
                        match connect(address, CONNECT_TIMEOUT).await {
                            Ok(stream) => { if let Err(error) = hub.serve_outbound(stream,site).await { tracing::warn!(%id,%error,"SMPP session ended"); } }
                            Err(error) => tracing::debug!(%id,%error,"SMPP peer unavailable"),
                        }
                        Some(id)
                    });
                }
            }
        }
    }
    hub.shutdown();
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
        while sessions.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
