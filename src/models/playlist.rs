use serde::{Deserialize, Serialize};
use crate::models::track::TrackData;
use crate::models::user::User;

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)] // Try to deserialize as one of the variants
pub enum PlaylistTrack {
    Full(TrackData),     // Your original (but now fully optional) TrackData
    Partial { id: i32 }, // A struct for the minimal objects
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PlaylistData {
    pub id: i32,
    pub title: String,
    pub artwork_url: Option<String>,
    pub duration: i32,
    pub user: User,
    pub tracks: Vec<PlaylistTrack>,
}
