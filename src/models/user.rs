use domain::models::db::soundcloud::AuthorInputSoundcloud;
use domain::models::music_api::artist::ApiArtist;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct User {
    pub id: i64,
    pub avatar_url: Option<String>,
    pub username: String,
}

impl Into<ApiArtist> for User {
    fn into(self) -> ApiArtist {
        ApiArtist {
            id: self.id.to_string(),
            username: self.username,
            picture: self.avatar_url,
            is_dummy: false,
        }
    }
}

impl Into<AuthorInputSoundcloud> for User {
    fn into(self) -> AuthorInputSoundcloud {
        AuthorInputSoundcloud {
            id: self.id,
            title: self.username,
            img: self.avatar_url,
        }
    }
}
