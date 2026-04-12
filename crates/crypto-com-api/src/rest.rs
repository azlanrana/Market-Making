// REST API client for Crypto.com
// TODO: Implement REST client

pub struct RestClient {
    #[allow(dead_code)]
    api_key: String,
    #[allow(dead_code)]
    api_secret: String,
    #[allow(dead_code)]
    base_url: String,
}

impl RestClient {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
            base_url: "https://api.crypto.com/v2".to_string(),
        }
    }
}

