use sqlx::{Pool, Postgres};

#[derive(Clone)]
pub struct Storage {
    pool: Pool<Postgres>,
}

impl Storage {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

