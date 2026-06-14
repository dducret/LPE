pub(crate) const PACKAGE_NAME: &str = env!("CARGO_PKG_NAME");
pub(crate) const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const GIT_COMMIT: &str = match option_env!("LPE_BUILD_GIT_COMMIT") {
    Some(value) => value,
    None => "",
};
pub(crate) const GIT_COMMIT_FULL: &str = match option_env!("LPE_BUILD_GIT_COMMIT_FULL") {
    Some(value) => value,
    None => "",
};
pub(crate) const GIT_COMMIT_TIME: &str = match option_env!("LPE_BUILD_GIT_COMMIT_TIME") {
    Some(value) => value,
    None => "",
};
pub(crate) const GIT_DIRTY: &str = match option_env!("LPE_BUILD_GIT_DIRTY") {
    Some(value) => value,
    None => "",
};
pub(crate) const BUILD_UNIX_TIME: &str = match option_env!("LPE_BUILD_UNIX_TIME") {
    Some(value) => value,
    None => "",
};
pub(crate) const TARGET: &str = match option_env!("LPE_BUILD_TARGET") {
    Some(value) => value,
    None => "",
};
pub(crate) const PROFILE: &str = match option_env!("LPE_BUILD_PROFILE") {
    Some(value) => value,
    None => "",
};
