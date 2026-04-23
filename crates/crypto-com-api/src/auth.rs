// Authentication for Crypto.com API
// TODO: Implement authentication

pub struct Auth {
    #[allow(dead_code)]
    api_key: String,
    #[allow(dead_code)]
    api_secret: String,
}

impl Auth {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
        }
    }
}
