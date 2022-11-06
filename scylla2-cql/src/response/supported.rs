use std::{collections::HashMap, io, str::FromStr};

use crate::cql::ReadCql;

#[derive(Debug)]
pub struct Supported {
    pub options: HashMap<String, Vec<String>>,
}

impl Supported {
    pub fn deserialize(mut slice: &[u8]) -> io::Result<Self> {
        let options = HashMap::<String, Vec<String>>::read_cql(&mut slice)?;
        Ok(Self { options })
    }

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.options.get(key)?.first()?.parse().ok()
    }
}
