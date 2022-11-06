use std::{convert::Infallible, sync::Weak, time::Duration};

use scylla2_cql::event::{Event, StatusChangeEvent};
use tokio::sync::mpsc;

use crate::{
    session::{event::SessionEventType, SessionInner},
    topology::{
        node::{NodeDisconnectionReason, NodeStatus},
        peer::NodeDistance,
    },
    Session, SessionEvent,
};

pub(super) async fn database_event_worker(
    session: Weak<SessionInner>,
    mut database_events_internal: mpsc::UnboundedReceiver<Event>,
) -> Option<Infallible> {
    loop {
        let event = database_events_internal.recv().await?;
        let session = Session(session.upgrade()?);
        match &event {
            Event::TopologyChange(_) => {
                let session = session.clone();
                tokio::spawn(async move { session.refresh_topology().await });
            }
            Event::StatusChange(StatusChangeEvent::Up(addr)) => {
                if let Some(node) = session.topology().nodes_by_rpc_address().get(&addr.ip()) {
                    node.try_reconnect().ok();
                } else {
                    let session = session.clone();
                    tokio::spawn(async move { session.refresh_topology().await });
                }
            }
            _ => {}
        }
        if event.r#type().is_none()
            || session
                .0
                .database_event_filter
                .as_ref()
                .map_or(true, |f| f.contains(&event.r#type().unwrap()))
        {
            session.0.database_events.send(event).ok();
        }
    }
}

pub(super) async fn session_event_worker(
    session: Weak<SessionInner>,
    mut session_events_internal: mpsc::UnboundedReceiver<SessionEvent>,
) -> Option<Infallible> {
    loop {
        let event = session_events_internal.recv().await?;
        let session = Session(session.upgrade()?);
        match &event {
            SessionEvent::NodeStatusUpdate { status, .. }
                if matches!(
                    status,
                    NodeStatus::Disconnected(NodeDisconnectionReason::ShardingChanged)
                ) =>
            {
                let session = session.clone();
                tokio::spawn(async move { session.refresh_topology().await });
            }
            SessionEvent::ControlConnectionClosed { .. } => {
                let session = session.clone();
                tokio::spawn(async move { session.reopen_control_connection().await });
            }
            _ => {}
        }
        if session
            .0
            .session_event_filter
            .as_ref()
            .map_or(true, |f| f.contains(&SessionEventType::from(&event)))
        {
            session.0.session_events.send(event).ok();
        }
    }
}

pub(super) async fn heartbeat_worker(
    session: Weak<SessionInner>,
    interval: Duration,
    distance: NodeDistance,
) -> Option<Infallible> {
    loop {
        tokio::time::sleep(interval).await;
        Session(session.upgrade()?).send_heartbeat(Some(distance));
    }
}

pub(super) async fn refresh_topology_worker(
    session: Weak<SessionInner>,
    interval: Duration,
) -> Option<Infallible> {
    loop {
        tokio::time::sleep(interval).await;
        Session(session.upgrade()?).refresh_topology().await.ok();
    }
}
