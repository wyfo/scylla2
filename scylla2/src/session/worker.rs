use std::{future::Future, sync::Weak, time::Duration};

use rand::seq::SliceRandom;
use scylla2_cql::event::{Event, StatusChangeEvent};
use tokio::sync::mpsc;

use crate::{
    event::{DatabaseEventHandler, SessionEventHandler},
    session::{Session, SessionInner},
    topology::node::NodeDisconnectionReason,
};

pub(super) async fn event_worker(
    inner: Weak<SessionInner>,
    mut events: mpsc::UnboundedReceiver<Event>,
) {
    loop {
        let event = events.recv().await;
        let Some(session) = inner.upgrade().map(Session) else {
            return;
        };
        match event {
            Some(Event::TopologyChange(event)) => {
                session.0.database_event_handler.topology_change(event);
                tokio::spawn(async move { session.refresh_topology().await });
            }
            Some(Event::StatusChange(StatusChangeEvent::Up(addr))) => {
                session
                    .0
                    .database_event_handler
                    .status_change(StatusChangeEvent::Up(addr));
                if let Some(node) = session.topology().nodes_by_rpc_address().get(&addr.ip()) {
                    node.try_reconnect().ok();
                } else {
                    tokio::spawn(async move { session.refresh_topology().await });
                }
            }
            Some(Event::StatusChange(event)) => {
                session.0.database_event_handler.status_change(event);
            }

            Some(Event::SchemaChange(event)) => {
                session.0.database_event_handler.schema_change(event);
            }
            Some(_) => {}
            None => {
                let (events_tx, event_rx) = mpsc::unbounded_channel();
                events = event_rx;
                tokio::spawn(async move { reopen_control_connection(&session.0, events_tx).await });
            }
        }
    }
}

async fn reopen_control_connection(inner: &SessionInner, events: mpsc::UnboundedSender<Event>) {
    for delay in inner
        .node_config_local
        .reconnection_policy
        .reconnection_delays()
    {
        let topology = inner.topology.load_full();
        let local = topology
            .local_nodes()
            .choose_multiple(&mut rand::thread_rng(), topology.local_nodes().len())
            .filter(|node| node.is_up())
            .map(|node| (node, &inner.node_config_local));
        let remote = topology
            .remote_nodes()
            .choose_multiple(&mut rand::thread_rng(), topology.remote_nodes().len())
            .filter(|node| node.is_up())
            .map(|node| (node, &inner.node_config_remote));
        for (node, node_config) in local.chain(remote) {
            let Some(address )= node.address() else {
                continue;
            };
            match inner
                .control_connection
                .open(
                    Some(node.peer().rpc_address),
                    address,
                    node_config,
                    inner.register_for_schema_event,
                    events.clone(),
                )
                .await
            {
                Ok(_) => {
                    inner.control_notify.notify_waiters();
                    return;
                }
                Err(error) => {
                    inner
                        .session_event_handler
                        .control_connection_failed(address, error);
                }
            }
        }
        tokio::time::sleep(delay).await;
    }
}

pub(super) async fn node_worker(
    inner: Weak<SessionInner>,
    mut node_disconnections: mpsc::UnboundedReceiver<NodeDisconnectionReason>,
) {
    loop {
        let reason = node_disconnections.recv().await;
        let Some(session) = inner.upgrade().map(Session) else {
            return;
        };
        if let Some(
            NodeDisconnectionReason::ShardingChanged
            | NodeDisconnectionReason::ProtocolVersionChanged
            | NodeDisconnectionReason::ExtensionsChanged,
        ) = reason
        {
            tokio::spawn(async move { session.refresh_topology().await });
        }
    }
}

pub(super) async fn loop_worker<F>(
    inner: Weak<SessionInner>,
    interval: Duration,
    task: impl (Fn(Session) -> F) + Send + Sync + 'static,
) where
    F: Future,
{
    loop {
        tokio::time::sleep(interval).await;
        let Some(session) = inner.upgrade().map(Session) else {
            return;
        };
        task(session).await;
    }
}
