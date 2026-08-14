use std::{env, error::Error, fs, path::PathBuf};

const DEFAULT_REVISION: &str = "main";
const PROMPT_PATH: &str = "crates/codegen/xai-grok-agent/templates/prompt.md";
const RESPONSE_GUIDELINES_PATH: &str = "assets/codex-response-guidelines.md";

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={RESPONSE_GUIDELINES_PATH}");
    println!("cargo:rerun-if-env-changed=GROK_BUILD_PROMPT_REV");

    let revision =
        env::var("GROK_BUILD_PROMPT_REV").unwrap_or_else(|_| DEFAULT_REVISION.to_owned());
    validate_revision(&revision)?;
    let url =
        format!("https://raw.githubusercontent.com/xai-org/grok-build/{revision}/{PROMPT_PATH}");
    let source = reqwest::blocking::Client::builder()
        .user_agent("grok-codex-oauth-build")
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .text()?;
    let replacement = fs::read_to_string(RESPONSE_GUIDELINES_PATH)?;
    let generated = replace_tagged_section(&source, "response_guidelines", replacement.trim())
        .unwrap_or(source);

    let output = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?)
        .join("grok-system-prompt.md");
    fs::write(output, generated)?;
    println!("cargo:warning=using Grok system prompt from {url}");
    Ok(())
}

fn validate_revision(revision: &str) -> Result<(), Box<dyn Error>> {
    if revision.is_empty()
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
    {
        return Err("GROK_BUILD_PROMPT_REV contains unsupported characters".into());
    }
    Ok(())
}

fn replace_tagged_section(text: &str, tag: &str, replacement: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = text.find(&start_tag)?;
    let content_start = start + start_tag.len();
    let end = text[content_start..].find(&end_tag)? + content_start;
    Some(format!(
        "{}{}\n{}\n{}{}",
        &text[..start],
        start_tag,
        replacement,
        end_tag,
        &text[end + end_tag.len()..]
    ))
}
