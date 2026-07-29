$ErrorActionPreference = "Stop"
$bundled = Join-Path $env:GROK_PLUGIN_ROOT "bin/grok-codex-oauth.exe"
if (Test-Path $bundled) {
    & $bundled @args
    exit $LASTEXITCODE
}

& grok-codex-oauth @args
exit $LASTEXITCODE
