use mm_core::config::AssetConfig;
use std::collections::HashMap;

pub struct AssetRegistry {
    assets: HashMap<String, AssetConfig>,
}

impl AssetRegistry {
    pub fn new() -> Self {
        Self {
            assets: HashMap::new(),
        }
    }

    pub fn register(&mut self, config: AssetConfig) {
        self.assets.insert(config.symbol.clone(), config);
    }

    pub fn get(&self, symbol: &str) -> Option<&AssetConfig> {
        self.assets.get(symbol)
    }
}

