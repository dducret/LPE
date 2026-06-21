use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-env-changed=LPE_BUILD_UNIX_TIME");
    println!("cargo:rerun-if-env-changed=LPE_BUILD_CHECK_DIRTY");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");

    set_git_env("LPE_BUILD_GIT_COMMIT", &["rev-parse", "--short=12", "HEAD"]);
    set_git_env("LPE_BUILD_GIT_COMMIT_FULL", &["rev-parse", "HEAD"]);
    set_git_env("LPE_BUILD_GIT_COMMIT_TIME", &["log", "-1", "--format=%cI"]);
    set_cargo_env("LPE_BUILD_TARGET", "TARGET");
    set_cargo_env("LPE_BUILD_PROFILE", "PROFILE");
    set_build_unix_time();
    set_dirty_env_if_requested();
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

fn set_build_unix_time() {
    let value = env::var("LPE_BUILD_UNIX_TIME")
        .ok()
        .or_else(|| env::var("SOURCE_DATE_EPOCH").ok());
    if let Some(value) = value {
        println!("cargo:rustc-env=LPE_BUILD_UNIX_TIME={value}");
    }
}

fn set_dirty_env_if_requested() {
    if !env_truthy("LPE_BUILD_CHECK_DIRTY") {
        return;
    }

    println!("cargo:rerun-if-changed=../../.git/index");
    println!(
        "cargo:rustc-env=LPE_BUILD_GIT_DIRTY={}",
        if git_worktree_is_dirty() {
            "true"
        } else {
            "false"
        }
    );
}

fn env_truthy(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
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
