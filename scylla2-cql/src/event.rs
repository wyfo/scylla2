use std::{io, net::SocketAddr, str::FromStr};

use crate::{cql::ReadCql, extensions::ProtocolExtensions, utils::invalid_data, ProtocolVersion};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum EventType {
    TopologyChange,
    StatusChange,
    SchemaChange,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    TopologyChange(TopologyChangeEvent),
    StatusChange(StatusChangeEvent),
    SchemaChange(SchemaChangeEvent),
    Other(String),
}

impl Event {
    pub fn r#type(&self) -> Option<EventType> {
        match self {
            Event::TopologyChange(_) => Some(EventType::TopologyChange),
            Event::StatusChange(_) => Some(EventType::StatusChange),
            Event::SchemaChange(_) => Some(EventType::SchemaChange),
            _ => None,
        }
    }

    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let event_type = <&str>::read_cql(&mut slice)?;
        Ok(match EventType::from_str(event_type) {
            Ok(EventType::TopologyChange) => Event::TopologyChange(
                TopologyChangeEvent::deserialize(version, extensions, slice)?,
            ),
            Ok(EventType::StatusChange) => {
                Event::StatusChange(StatusChangeEvent::deserialize(version, extensions, slice)?)
            }
            Ok(EventType::SchemaChange) => {
                Event::SchemaChange(SchemaChangeEvent::deserialize(version, extensions, slice)?)
            }
            Err(_) => Event::Other(event_type.into()),
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TopologyChangeEvent {
    NewNode(SocketAddr),
    RemovedNode(SocketAddr),
    Other(String),
}

impl TopologyChangeEvent {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let kind = <&str>::read_cql(&mut slice)?;
        let addr = SocketAddr::read_cql(&mut slice)?;
        Ok(match kind {
            "NEW_NODE" => TopologyChangeEvent::NewNode(addr),
            "REMOVED_NODE" => TopologyChangeEvent::RemovedNode(addr),
            _ => TopologyChangeEvent::Other(kind.into()),
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum StatusChangeEvent {
    Up(SocketAddr),
    Down(SocketAddr),
    Other(String),
}

impl StatusChangeEvent {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let kind = <&str>::read_cql(&mut slice)?;
        let addr = SocketAddr::read_cql(&mut slice)?;
        Ok(match kind {
            "UP" => StatusChangeEvent::Up(addr),
            "DOWN" => StatusChangeEvent::Down(addr),
            _ => StatusChangeEvent::Other(kind.into()),
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SchemaChangeEvent {
    change_type: SchemaChangeType,
    keyspace: String,
    kind: SchemaChangeKind,
}

impl SchemaChangeEvent {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let change_type = SchemaChangeType::from_str(<&str>::read_cql(&mut slice)?)
            .map_err(|_| "Invalid schema change type")
            .map_err(invalid_data)?;
        let target = <&str>::read_cql(&mut slice)?;
        let keyspace = String::read_cql(&mut slice)?;
        Ok(SchemaChangeEvent {
            change_type,
            keyspace,
            kind: match target {
                "KEYSPACE" => SchemaChangeKind::Keyspace,
                "TABLE" => SchemaChangeKind::Table {
                    name: String::read_cql(&mut slice)?,
                },
                "TYPE" => SchemaChangeKind::Type {
                    name: String::read_cql(&mut slice)?,
                },
                "FUNCTION" => SchemaChangeKind::Function {
                    name: String::read_cql(&mut slice)?,
                    arg_types: ReadCql::read_cql(&mut slice)?,
                },
                "AGGREGATE" => SchemaChangeKind::Aggregate {
                    name: String::read_cql(&mut slice)?,
                    arg_types: ReadCql::read_cql(&mut slice)?,
                },
                _ => SchemaChangeKind::Other(target.into()),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum SchemaChangeKind {
    Keyspace,
    Table {
        name: String,
    },
    Type {
        name: String,
    },
    Function {
        name: String,
        arg_types: Vec<String>,
    },
    Aggregate {
        name: String,
        arg_types: Vec<String>,
    },
    Other(String),
}
