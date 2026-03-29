use crate::models::track::{Track, TrackData};
use crate::models::user::User;
use domain::models::db::soundcloud::{
    AuthorInputSoundcloud, CreateReplacePlaylistInput, PlaylistInputSoundcloud,
    TrackInputSoundcloud,
};
use domain::models::music_api::playlist::ApiPlaylist;
use domain::models::music_api::track::ApiTrack;
use serde::{Deserialize, Serialize};

// #[derive(Deserialize, Serialize, Clone)]
// #[serde(untagged)] // Try to deserialize as one of the variants
// pub enum PlaylistTrack {
//     Full(TrackData),     // Your original (but now fully optional) TrackData
//     Partial { id: i64 }, // A struct for the minimal objects
// }

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PlaylistData {
    pub id: i64,
    pub title: String,
    pub artwork_url: Option<String>,
    pub duration: i32,
    pub user: User,
    pub tracks: TracksList,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TracksList(pub Vec<Track>);

impl Into<(Vec<TrackInputSoundcloud>, Vec<AuthorInputSoundcloud>)> for TracksList {
    fn into(self) -> (Vec<TrackInputSoundcloud>, Vec<AuthorInputSoundcloud>) {
        let len = self.0.len();

        let mut tracks = Vec::with_capacity(len);
        let mut authors = Vec::with_capacity(len / 2);

        self.0.into_iter().for_each(|track| {
            if let Track::Full(track) = track {
                let (track_i, author_i) = track.into();
                tracks.push(track_i);
                authors.push(author_i);
            }
        });

        (tracks, authors)
    }
}

impl Into<CreateReplacePlaylistInput> for PlaylistData {
    fn into(self) -> CreateReplacePlaylistInput {
        let (tracks, track_authors) = self.tracks.into();

        CreateReplacePlaylistInput {
            playlist: PlaylistInputSoundcloud {
                id: self.id,
                title: self.title,
                img: self.artwork_url,
                author_id: self.user.id.clone(),
            },
            playlist_author: self.user.into(),

            tracks,
            track_authors,
        }
    }
}

impl Into<ApiPlaylist> for PlaylistData {
    fn into(self) -> ApiPlaylist {
        ApiPlaylist {
            platform: domain::models::music_api::services::ApiServices::Soundcloud,
            id: self.id.to_string(),
            title: self.title,
            parent_user_id: self.user.id.to_string(),
            parent_username: self.user.username,
            parent_picture: self.user.avatar_url,
            picture: self.artwork_url,
            size: self.tracks.0.len() as u32,
            tracks: self
                .tracks
                .0
                .into_iter()
                .filter_map(|el| match el {
                    Track::Full(data) => Some(data.into()),
                    Track::Stub(_) => None,
                })
                .collect(),
        }
    }
}
