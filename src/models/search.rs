use serde::{Deserialize, Serialize};
use crate::models::playlist::PlaylistData;
use crate::models::track::TrackData;
use crate::models::user::User;

#[derive(Deserialize, Serialize)]
pub enum SearchItem {
    Playlist(PlaylistData),
    Track(TrackData),
    User(User),
}

#[derive(Deserialize, Serialize)]
pub struct SearchResponse {
    pub collection: Vec<SearchItem>,
}