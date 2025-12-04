use domain::models::db::soundcloud::TrackInputSoundcloud;
use domain::models::music_api::services::ApiServices;
use domain::models::music_api::services::ApiServices::Soundcloud;
use domain::models::music_api::track::ApiTrack;
use serde::{Deserialize, Serialize};
use crate::models::user::User;

#[derive(Deserialize, Serialize, Clone)]
pub struct TrackData {
    pub id: i64,
    pub title: String,
    pub artwork_url: Option<String>,
    pub duration: i32,
    pub media: Media,
    pub track_authorization: String,
    pub user: User,
}

impl Into<TrackInputSoundcloud> for TrackData {
    fn into(self) -> TrackInputSoundcloud {
        TrackInputSoundcloud {
            id: self.id,
            title: self.title,
            duration: self.duration,
            img: self.artwork_url,
            author_id: self.user.id
        }
    }
}

impl Into<ApiTrack> for TrackData {
    fn into(self) -> ApiTrack {
        ApiTrack {
            id: self.id.to_string(),
            service: Soundcloud,
            title: self.title,
            artists: vec![self.user.into()],
            alb_id: None,
            alb_title: None,
            duration: 0,
            track_url: None,
            track_token: None,
        }
    }
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