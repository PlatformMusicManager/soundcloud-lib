use crate::models::user::User;
use domain::errors::music_services::soundcloud_api_error::SoundcloudApiError;
use domain::models::db::soundcloud::{AuthorInputSoundcloud, TrackInputSoundcloud};
use domain::models::music_api::services::ApiServices::Soundcloud;
use domain::models::music_api::track::ApiTrack;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum Track {
    // Serde attempts this FIRST.
    // If the JSON has `title` and `user`, this succeeds.
    Full(TrackData),

    // If the above fails, Serde attempts this.
    Stub(StubTrackData),
}

impl TryInto<TrackData> for Track {
    type Error = SoundcloudApiError;

    fn try_into(self) -> Result<TrackData, Self::Error> {
        let Track::Full(track_data) = self else {
            return Err(SoundcloudApiError::TrackDataIsNotFull);
        };

        Ok(track_data)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TrackData {
    pub id: i64,
    pub title: String,
    pub artwork_url: Option<String>,
    pub duration: i64,
    pub media: Media,
    pub track_authorization: String,
    pub user: User,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StubTrackData {
    pub id: i64,
}

impl Into<(TrackInputSoundcloud, AuthorInputSoundcloud)> for TrackData {
    fn into(self) -> (TrackInputSoundcloud, AuthorInputSoundcloud) {
        (
            TrackInputSoundcloud {
                id: self.id,
                title: self.title,
                duration: self.duration,
                img: self.artwork_url,
                author_id: self.user.id.clone(),
            },
            AuthorInputSoundcloud {
                id: self.user.id,
                title: self.user.username,
                img: self.user.avatar_url,
            },
        )
    }
}

impl Into<ApiTrack> for TrackData {
    fn into(self) -> ApiTrack {
        ApiTrack {
            id: self.id.to_string(),
            service: Soundcloud,
            title: self.title,
            picture: self.artwork_url,
            artists: vec![self.user.into()],
            alb_id: None,
            alb_title: None,
            duration: self.duration,
            track_url: self.media.get_best_media().map(|media| media.url.clone()),
            track_token: Some(self.track_authorization),
            platform: Soundcloud,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Media {
    pub transcodings: Vec<EncodingData>,
}

impl Media {
    pub fn get_best_media(&self) -> Option<&EncodingData> {
        self.transcodings.first()
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct EncodingData {
    pub url: String,
    pub preset: Option<String>,
    pub duration: u32,
    pub snipped: bool,
    pub format: FormatData,
    pub quality: String,
    pub is_legacy_transcoding: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FormatData {
    pub protocol: String,
    pub mime_type: String,
}
