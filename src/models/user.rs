use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct User {
    pub avatar_url: String,
    pub username: String,
    pub id: i32,
}
