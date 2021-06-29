use anyhow::Result;
use ton_api::ton;

use super::db::*;

pub trait NodeState: serde::Serialize + serde::de::DeserializeOwned {
    fn get_key() -> &'static str;

    fn load_from_db(db: &dyn Db) -> Result<Self> {
        let value = db.load_node_state(Self::get_key())?;
        Ok(bincode::deserialize::<Self>(&value)?)
    }

    fn store_into_db(&self, db: &dyn Db) -> Result<()> {
        let value = bincode::serialize(self)?;
        db.store_node_state(Self::get_key(), value)
    }
}

macro_rules! define_node_state {
    ($ident:ident, $inner:path) => {
        #[derive(serde::Serialize, serde::Deserialize)]
        pub struct $ident(pub $inner);

        impl NodeState for $ident {
            fn get_key() -> &'static str {
                stringify!($ident)
            }
        }
    };
}

define_node_state!(LastMcBlockId, ton::ton_node::blockidext::BlockIdExt);
define_node_state!(InitMcBlockId, ton::ton_node::blockidext::BlockIdExt);
