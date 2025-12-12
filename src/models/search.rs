use domain::models::music_api::artist::ApiArtist;
use domain::models::music_api::playlist::ApiPlaylist;
use domain::models::music_api::search_results::ApiSearchPage;
use domain::models::music_api::track::ApiTrack;
use serde::{Deserialize, Serialize};
use crate::models::playlist::PlaylistData;
use crate::models::track::{Track, TrackData};
use crate::models::user::User;

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SearchItem {
    Playlist(PlaylistData),
    Track(Track),
    User(User),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SearchResponse {
    pub collection: Vec<SearchItem>,
}

impl Into<ApiSearchPage> for SearchResponse {
    fn into(self) -> ApiSearchPage {
        let mut artists: Vec<ApiArtist> = Vec::new();
        let mut playlists: Vec<ApiPlaylist> = Vec::new();
        let mut tracks: Vec<ApiTrack> = Vec::new();

        self.collection.into_iter().for_each(|el| {
                match el {
                    SearchItem::Playlist(pl) => {
                        playlists.push(pl.into());
                    }
                    SearchItem::Track(Track::Full(tr)) => {
                        tracks.push(tr.into());
                    }
                    SearchItem::Track(Track::Stub(_)) => {

                    }
                    SearchItem::User(us) => {
                        artists.push(us.into());
                    }
                }
            });

        ApiSearchPage {
            artists,
            tracks,
            playlists,
            users: vec![],
            albums: vec![],
        }
    }
}