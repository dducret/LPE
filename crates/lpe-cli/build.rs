use std::{
    env,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");

    set_git_env("LPE_BUILD_GIT_COMMIT", &["rev-parse", "--short=12", "HEAD"]);
    set_git_env("LPE_BUILD_GIT_COMMIT_FULL", &["rev-parse", "HEAD"]);
    set_git_env("LPE_BUILD_GIT_COMMIT_TIME", &["log", "-1", "--format=%cI"]);
    set_cargo_env("LPE_BUILD_TARGET", "TARGET");
    set_cargo_env("LPE_BUILD_PROFILE", "PROFILE");
    println!(
        "cargo:rustc-env=LPE_BUILD_UNIX_TIME={}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs().to_string())
            .unwrap_or_default()
    );
    println!(
        "cargo:rustc-env=LPE_BUILD_GIT_DIRTY={}",
        if git_worktree_is_dirty() {
            "true"
        } else {
            "false"
        }
    );
}

fn set_cargo_env(name: &str, source: &str) {
    if let Ok(value) = env::var(source) {
        println!("cargo:rustc-env={name}={value}");
    }
}

fn set_git_env(name: &str, args: &[&str]) {
    if let Some(value) = git_output(args) {
        println!("cargo:rustc-env={name}={value}");
    }
}

fn git_worktree_is_dirty() -> bool {
    git_output(&["status", "--porcelain"])
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}
