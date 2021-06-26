pub trait Db: Send + Sync {}

pub struct InMemoryDb {}

impl Db for InMemoryDb {}
