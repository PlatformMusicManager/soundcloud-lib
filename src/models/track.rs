use serde::{Deserialize, Serialize};
use crate::models::user::User;

#[derive(Deserialize, Serialize, Clone)]
pub struct TrackData {
    pub id: i32,
    pub title: String,
    pub artwork_url: Option<String>,
    pub duration: i32,
    pub media: Media,
    pub track_authorization: String,
    pub user: User,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Media {
    pub transcodings: Vec<EncodingData>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct EncodingData {
    pub url: String,
    pub preset: Option<String>,
    pub duration: u32,
    pub snipped: bool,
    pub format: FormatData,
    pub quality: String,
    pub is_legacy_transcoding: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct FormatData {
    pub protocol: String,
    pub mime_type: String,
}