# Local development helpers. Run `just --list` to discover recipes.

# Rebuild, embed, install, and restart the home-machine Web UI.
recompile-webui:
    SCRY_EMBED_WEBUI=1 cargo install --path crates/scry --locked --force
    systemctl --user restart scry-webui.service
    @printf 'installed: '; "$HOME/.cargo/bin/scry" --version
    @printf 'service:   '; systemctl --user is-active scry-webui.service
