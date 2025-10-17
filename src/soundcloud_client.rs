use async_stream::stream;
use futures::{Stream, StreamExt, TryStreamExt};
use regex::Regex;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;

const BASE_URL: &str = "https://api-v2.soundcloud.com";

#[derive(Deserialize, Serialize, Clone)]
pub struct FormatData {
    pub protocol: String,
    pub mime_type: String,
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
pub struct Media {
    pub transcodings: Vec<EncodingData>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct User {
    pub avatar_url: String,
    pub username: String,
    pub id: i32,
}

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

#[derive(Deserialize, Serialize)]
pub struct ChunkUrl {
    pub url: String,
}

pub struct SoundCloudApi {
    client: Client,
    client_id: String,
    url_re: Regex,
}

#[derive(Error, Debug)]
pub enum SoundcloudError {
    #[error("Invalid request to SoundCloud")]
    InvalidRequestToSoundcloud(#[from] reqwest::Error),

    #[error("Error while creating URL for SoundCloud request, invalid data was provided")]
    UrlParseError(#[from] url::ParseError),

    #[error("Error while deserialize")]
    DeserializeError(#[from] serde_json::Error),

    #[error("No data for track in response")]
    NoTrackDataInResponse(),

    #[error("No media data attached in track in response")]
    NoMediaDataInResponse(),
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>;

impl SoundCloudApi {
    pub fn new(client_id: String) -> Self {
        Self {
            client: Client::new(),
            client_id,
            url_re: Regex::new(r#"https:?:[^\s"]+"#).unwrap(),
        }
    }

    pub async fn search(
        &self,
        query: &str,
        offset: &str,
        limit: &str,
    ) -> Result<SearchResponse, SoundcloudError> {
        let url = Url::parse_with_params(
            format!("{}/search", BASE_URL).as_str(),
            &[
                ("q", query),
                ("client_id", self.client_id.as_str()),
                ("limit", limit),
                ("offset", offset),
            ],
        )?;

        let req = self.client.get(url).build()?;
        let res = self.client.execute(req).await?.text().await?;

        let search_res: SearchResponse = serde_json::from_str(&res)?;
        Ok(search_res)
    }

    pub async fn get_track_data(&self, ids: &str) -> Result<Vec<TrackData>, SoundcloudError> {
        let url = Url::parse_with_params(
            format!("{BASE_URL}/tracks").as_str(),
            &[("ids", ids), ("client_id", self.client_id.as_str())],
        )?;
        let req = self.client.get(url).build()?;
        let res = self.client.execute(req).await?.text().await?;
        let track: Vec<TrackData> = serde_json::from_str(&res)?;

        Ok(track)
    }

    pub async fn get_url_to_chunks(
        &self,
        url: &str,
        track_authorization: &str,
    ) -> Result<String, SoundcloudError> {
        let url = Url::parse_with_params(
            url,
            &[
                ("client_id", self.client_id.as_str()),
                ("track_authorization", track_authorization),
            ],
        )?;
        let req = self.client.get(url).build()?;
        let res = self.client.execute(req).await?.text().await?;
        let urls: ChunkUrl = serde_json::from_str(&res)?;

        Ok(urls.url)
    }

    pub async fn get_chunks(&self, url: &str) -> Result<Vec<String>, SoundcloudError> {
        let req = self.client.get(url).build()?;
        let res = self.client.execute(req).await?.text().await?;
        let urls: Vec<String> = self
            .url_re
            .find_iter(res.as_str())
            .map(|m| m.as_str().to_string())
            .collect();

        Ok(urls)
    }

    pub async fn stream_chunk(&self, url: String) -> Result<ByteStream, SoundcloudError> {
        let response = self.client.get(url).send().await?;

        let stream = response
            .bytes_stream()
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e)) // A better mapping
            .boxed();

        Ok(stream)
    }

    pub async fn stream(self: Arc<Self>, id: &str) -> Result<ByteStream, SoundcloudError> {
        let track_data = self.get_track_data(id).await?;

        let track = track_data
            .first()
            .ok_or_else(SoundcloudError::NoTrackDataInResponse)?;

        // Picking first available audio, first one is always highest quality
        let media_data = track
            .media
            .transcodings
            .first()
            .ok_or_else(SoundcloudError::NoMediaDataInResponse)?;

        let url_with_chunks = self
            .get_url_to_chunks(&media_data.url, &track.track_authorization)
            .await?;

        let url_chunks = self.get_chunks(&url_with_chunks).await?;

        let stream = stream! {
            // Iterate through each of your chunk URLs
            for url_chunk in url_chunks.into_iter() {
                // 1. Get the Result<ByteStream, ...> for this specific chunk
                let sub_stream_result = self.stream_chunk(url_chunk).await;

                match sub_stream_result {
                    // 2. If you successfully got a sub-stream of bytes...
                    Ok(mut sub_stream) => {
                        // 3. ...loop over it and yield its items individually.
                        while let Some(bytes_result) = sub_stream.next().await {
                            // `bytes_result` is `Result<bytes::Bytes, std::io::Error>`
                            yield bytes_result;
                        }
                    }
                    // If getting the sub-stream failed, we need to yield an error
                    // that matches the outer stream's error type (std::io::Error).
                    Err(e) => {
                        let io_error = std::io::Error::new(ErrorKind::Other, e.to_string());
                        yield Err(io_error);
                        // After a fatal error, stop processing more chunks.
                        break;
                    }
                }
            }
        };

        Ok(stream.boxed())
    }
}
