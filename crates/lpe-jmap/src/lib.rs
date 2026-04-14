use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SessionCapabilities {
    pub core: bool,
    pub mail: bool,
}

pub fn default_capabilities() -> SessionCapabilities {
    SessionCapabilities {
        core: true,
        mail: false,
    }
}
