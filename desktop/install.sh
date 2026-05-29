#!/usr/bin/env bash
# ABOUTME: Installer for the scry desktop query app.
# ABOUTME: Downloads the latest GitHub release for this platform, or
#          (with --local) builds from this checkout and installs that.

set -euo pipefail

REPO="serialexp/scry"
APP_NAME="scry-desktop"          # installed binary name (server binary is `scry`)
DISPLAY_NAME="scry"              # menu / window name
TAG_PREFIX="v"                   # release tags (shared: `vX.Y.Z` builds image + app)
ICON_URL="https://raw.githubusercontent.com/${REPO}/main/desktop/src-tauri/icons/icon.png"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}warning:${NC} $1"; }
error() { echo -e "${RED}error:${NC} $1" >&2; exit 1; }

usage() {
    cat <<EOF
scry desktop installer

Usage:
  install.sh            Download + install the latest published release.
  install.sh --local    Build from this checkout and install the result.
  install.sh -h|--help  Show this help.

The default mode pulls the latest GitHub release tagged '${TAG_PREFIX}*'
from ${REPO}. Use --local on the dev box to install a freshly built
bundle before any release is published.
EOF
}

# ── Linux desktop entry (XDG) ────────────────────────────────────────────
create_desktop_entry() {
    local bin_path="$1"
    local icon_dir="${HOME}/.local/share/icons/hicolor/256x256/apps"
    local desktop_dir="${HOME}/.local/share/applications"
    local icon_path="${icon_dir}/${APP_NAME}.png"
    local desktop_path="${desktop_dir}/${APP_NAME}.desktop"
    local local_icon="${2:-}"

    if [[ ! -d "${HOME}/.local/share" ]]; then
        warn "XDG data directory not found, skipping desktop entry creation"
        return 0
    fi

    info "Creating desktop entry..."
    mkdir -p "$icon_dir" "$desktop_dir"

    if [[ -n "$local_icon" && -f "$local_icon" ]]; then
        cp "$local_icon" "$icon_path"
        info "Installed application icon from build"
    elif curl -sL "$ICON_URL" -o "$icon_path" 2>/dev/null; then
        info "Downloaded application icon"
    else
        warn "Could not obtain icon, desktop entry will use a generic one"
        icon_path=""
    fi

    cat > "$desktop_path" << EOF
[Desktop Entry]
Name=${DISPLAY_NAME}
Comment=Query a scry-queryd daemon
Exec=${bin_path}
Icon=${icon_path:-application-x-executable}
Type=Application
Categories=Development;Utility;
Terminal=false
StartupWMClass=${APP_NAME}
EOF
    chmod +x "$desktop_path"

    if command -v update-desktop-database &> /dev/null; then
        update-desktop-database "$desktop_dir" 2>/dev/null || true
    fi

    info "Desktop entry created at ${desktop_path}"
}

# Install a Linux executable (a release AppImage, or a bare Tauri binary
# for a same-machine --local install) into ~/.local/bin (or /usr/local/bin
# if writable), plus an XDG desktop entry.
install_linux_binary() {
    local src="$1"      # path to the executable (.AppImage or bare binary)
    local icon="${2:-}" # optional local icon to use for the desktop entry

    local bin_dir
    if [[ -w "/usr/local/bin" ]]; then
        bin_dir="/usr/local/bin"
    else
        bin_dir="${HOME}/.local/bin"
        mkdir -p "$bin_dir"
    fi

    info "Installing ${APP_NAME} to ${bin_dir}/..."
    install -m 0755 "$src" "${bin_dir}/${APP_NAME}"

    create_desktop_entry "${bin_dir}/${APP_NAME}" "$icon"

    info "Installation complete!"
    echo ""
    echo "  Binary installed to: ${bin_dir}/${APP_NAME}"
    echo "  Desktop entry: ~/.local/share/applications/${APP_NAME}.desktop"
    echo ""
    if [[ ":$PATH:" != *":${bin_dir}:"* ]]; then
        warn "${bin_dir} is not in your PATH"
        echo "  Add it with: export PATH=\"\$PATH:${bin_dir}\""
    fi
}

# ── Platform detection ───────────────────────────────────────────────────
detect_platform() {
    local os arch
    case "$(uname -s)" in
        Linux*)  os="linux" ;;
        Darwin*) os="macos" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        *) error "Unsupported operating system: $(uname -s)" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch="x86_64" ;;
        arm64|aarch64) arch="aarch64" ;;
        *) error "Unsupported architecture: $(uname -m)" ;;
    esac
    echo "${os}-${arch}"
}

# Latest *published* release whose tag starts with $TAG_PREFIX. We list
# releases (not /latest) so server releases under other tags don't shadow
# the desktop app.
get_latest_version() {
    local version
    version=$(curl -sL "https://api.github.com/repos/${REPO}/releases" |
              grep '"tag_name":' |
              sed -E 's/.*"tag_name": *"([^"]+)".*/\1/' |
              grep "^${TAG_PREFIX}" |
              head -1)
    if [[ -z "$version" ]]; then
        error "No published '${TAG_PREFIX}*' release found for ${REPO}.
       Publish the draft release the CI created, or run: install.sh --local"
    fi
    echo "$version"
}

asset_url() {
    local version="$1" name="$2"
    echo "https://github.com/${REPO}/releases/download/${version}/${name}"
}

# ── Release install: Linux ───────────────────────────────────────────────
install_linux() {
    local version="$1"

    info "Fetching release assets..."
    local assets_json
    assets_json=$(curl -sL "https://api.github.com/repos/${REPO}/releases/tags/${version}")

    # Prefer the AppImage (most portable), fall back to .deb.
    local asset_name
    asset_name=$(echo "$assets_json" | grep -o "\"name\": *\"[^\"]*\.AppImage\"" | sed 's/"name": *"\(.*\)"/\1/' | head -1)
    if [[ -z "$asset_name" ]]; then
        asset_name=$(echo "$assets_json" | grep -o "\"name\": *\"[^\"]*_amd64\.deb\"" | sed 's/"name": *"\(.*\)"/\1/' | head -1)
        [[ -z "$asset_name" ]] && error "Could not find a Linux AppImage or .deb in ${version}"
    fi

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' RETURN

    info "Downloading ${asset_name}..."
    curl -sL "$(asset_url "$version" "$asset_name")" -o "${tmp_dir}/${asset_name}"

    if [[ "$asset_name" == *.AppImage ]]; then
        install_linux_binary "${tmp_dir}/${asset_name}"
    else
        info "Installing .deb package..."
        command -v apt &> /dev/null || error ".deb found but apt is not available"
        sudo apt install -y "${tmp_dir}/${asset_name}"
        info "Installation complete!"
    fi
}

# ── Release install: macOS ───────────────────────────────────────────────
install_macos() {
    local version="$1" arch="$2"

    info "Fetching release assets..."
    local assets_json
    assets_json=$(curl -sL "https://api.github.com/repos/${REPO}/releases/tags/${version}")

    local asset_name
    if [[ "$arch" == "aarch64" ]]; then
        asset_name=$(echo "$assets_json" | grep -o "\"name\": *\"[^\"]*aarch64[^\"]*\.dmg\"" | sed 's/"name": *"\(.*\)"/\1/' | head -1)
    else
        asset_name=$(echo "$assets_json" | grep -o "\"name\": *\"[^\"]*\(x64\|x86_64\)[^\"]*\.dmg\"" | sed 's/"name": *"\(.*\)"/\1/' | head -1)
    fi
    [[ -z "$asset_name" ]] && error "Could not find a macOS DMG for ${arch} in ${version}"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' RETURN
    local dmg_path="${tmp_dir}/${APP_NAME}.dmg"

    info "Downloading ${asset_name}..."
    curl -sL "$(asset_url "$version" "$asset_name")" -o "$dmg_path"

    info "Mounting DMG..."
    local mount_point
    mount_point=$(hdiutil attach -nobrowse -readonly "$dmg_path" 2>/dev/null | grep "/Volumes" | cut -f3)
    [[ -z "$mount_point" ]] && error "Failed to mount DMG"

    local app_bundle
    app_bundle=$(find "$mount_point" -maxdepth 1 -name "*.app" -type d | head -1)
    if [[ -z "$app_bundle" ]]; then
        hdiutil detach "$mount_point" -quiet
        error "Could not find .app bundle in DMG"
    fi

    local app_path="/Applications/${DISPLAY_NAME}.app"
    info "Installing to ${app_path}..."
    [[ -d "$app_path" ]] && rm -rf "$app_path"
    cp -R "$app_bundle" "/Applications/"

    hdiutil detach "$mount_point" -quiet
    info "Installation complete! Launch '${DISPLAY_NAME}' from Applications or Spotlight."
}

# ── Release install: Windows ─────────────────────────────────────────────
install_windows() {
    local version="$1"
    local assets_json
    assets_json=$(curl -sL "https://api.github.com/repos/${REPO}/releases/tags/${version}")
    local asset_name
    asset_name=$(echo "$assets_json" | grep -o "\"name\": *\"[^\"]*\.msi\"" | sed 's/"name": *"\(.*\)"/\1/' | head -1)
    [[ -z "$asset_name" ]] && error "Could not find a Windows MSI in ${version}"

    local url
    url="$(asset_url "$version" "$asset_name")"
    echo ""
    echo "Windows installation via this script is not automated. Download:"
    echo "  $url"
    echo ""
    echo "Or in PowerShell:"
    echo "  Invoke-WebRequest -Uri '$url' -OutFile '${asset_name}'"
    echo "  Start-Process msiexec.exe -Wait -ArgumentList '/i ${asset_name} /quiet'"
}

# ── Local build + install (Linux/macOS dev box) ──────────────────────────
install_local() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    command -v bun &> /dev/null || error "bun is required for --local (https://bun.sh)"
    command -v cargo &> /dev/null || error "a Rust toolchain (cargo) is required for --local"

    info "Installing frontend dependencies..."
    ( cd "$script_dir" && bun install )

    local os
    os="$(uname -s)"

    if [[ "$os" == "Linux" ]]; then
        # Build only the executable, not a bundle. The release binary links
        # against the system webkit2gtk already present on this machine, so a
        # same-machine install needs neither the AppImage wrapper (which would
        # require librsvg2-dev at bundle time) nor root. Distributable bundles
        # (.AppImage/.deb) are what CI produces and what the release-download
        # path installs.
        info "Building the desktop binary (cargo tauri build --no-bundle)... this takes a while."
        ( cd "$script_dir" && bun run tauri build --no-bundle )

        local bin="${script_dir}/src-tauri/target/release/${APP_NAME}"
        [[ -x "$bin" ]] || error "Build finished but no binary found at ${bin}"
        install_linux_binary "$bin" "${script_dir}/src-tauri/icons/icon.png"
    elif [[ "$os" == "Darwin" ]]; then
        info "Building the .app bundle (cargo tauri build)... this takes a while."
        ( cd "$script_dir" && bun run tauri build --bundles app )

        local app_bundle
        app_bundle=$(find "${script_dir}/src-tauri/target/release/bundle/macos" -maxdepth 1 -name "*.app" -type d 2>/dev/null | head -1)
        [[ -z "$app_bundle" ]] && error "Build finished but no .app found under target/release/bundle/macos"
        local app_path="/Applications/${DISPLAY_NAME}.app"
        info "Installing to ${app_path}..."
        [[ -d "$app_path" ]] && rm -rf "$app_path"
        cp -R "$app_bundle" "/Applications/"
        info "Installation complete! Launch '${DISPLAY_NAME}' from Applications or Spotlight."
    else
        error "--local only supports Linux and macOS (got: $os)"
    fi
}

main() {
    case "${1:-}" in
        -h|--help) usage; exit 0 ;;
        --local)
            echo ""
            echo "  scry desktop — local build + install"
            echo ""
            install_local
            exit 0
            ;;
        "") ;;
        *) error "unknown argument: $1 (try --help)" ;;
    esac

    echo ""
    echo "  scry desktop installer"
    echo ""

    local platform
    platform=$(detect_platform)
    info "Detected platform: ${platform}"

    local version
    version=$(get_latest_version)
    info "Latest release: ${version}"

    case "$platform" in
        linux-x86_64|linux-aarch64) install_linux "$version" ;;
        macos-x86_64)               install_macos "$version" "x86_64" ;;
        macos-aarch64)              install_macos "$version" "aarch64" ;;
        windows-x86_64)             install_windows "$version" ;;
        *) error "No installation method for platform: ${platform}" ;;
    esac
}

main "$@"
