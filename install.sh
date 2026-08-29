#!/usr/bin/env bash
# ABOUTME: Installer for the scry CLI/server binary (`scry`).
# ABOUTME: Downloads the latest GitHub release tarball for this platform and
#          installs the `scry` binary onto PATH. Headless — no desktop entry.
#
# This installs the multicall server/CLI binary (`scry ingest`, `scry query`,
# `scry get`, `scry agent`, `scry gateway`, `scry replay-opensearch`, …), NOT
# the desktop GUI query app — for that, see desktop/install.sh.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/serialexp/scry/main/install.sh | sh
#   ./install.sh                 # from a checkout
#   ./install.sh -h | --help

set -euo pipefail

REPO="serialexp/scry"
BIN="scry"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; NC='\033[0m'
info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}warning:${NC} $1"; }
error() { echo -e "${RED}error:${NC} $1" >&2; exit 1; }

usage() {
    cat <<EOF
scry CLI installer

Usage:
  install.sh            Download + install the latest published release.
  install.sh -h|--help  Show this help.

Pulls the latest '${REPO}' release tarball for your platform and installs the
'${BIN}' binary to /usr/local/bin (if writable) or ~/.local/bin.

Linux binaries are static musl builds (run on any distro, no glibc dependency);
macOS binaries are native. Windows is not supported by this script.
EOF
}

# ── Platform detection ───────────────────────────────────────────────────
detect_platform() {
    local os arch
    case "$(uname -s)" in
        Linux*)  os="linux" ;;
        Darwin*) os="macos" ;;
        MINGW*|MSYS*|CYGWIN*) error "Windows is not supported by this installer; download a release asset manually." ;;
        *) error "Unsupported operating system: $(uname -s)" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch="x86_64" ;;
        arm64|aarch64) arch="aarch64" ;;
        *) error "Unsupported architecture: $(uname -m)" ;;
    esac
    echo "${os}-${arch}"
}

# Latest *published* release tag. The '/latest' endpoint skips drafts and
# pre-releases, which is exactly the property this relies on: the release
# workflow builds every asset into a draft and only publishes once the image
# and all four tarballs are attached, so whatever '/latest' returns here is
# guaranteed complete. Matches all first-party tags (`vX.Y.Z`).
get_latest_version() {
    local version
    version=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" |
              grep '"tag_name":' |
              sed -E 's/.*"tag_name": *"([^"]+)".*/\1/' |
              head -1)
    [[ -n "$version" ]] || error "Failed to fetch the latest release for ${REPO}.
       Is a release published yet? Otherwise build from source: cargo build --release -p scry"
    echo "$version"
}

# ── sha256 verification (portable across coreutils / macOS) ──────────────
verify_sha256() {
    local file="$1" sums="$2"   # sums = the '<hex>  <name>' line file
    if command -v sha256sum &>/dev/null; then
        ( cd "$(dirname "$file")" && sha256sum -c "$(basename "$sums")" >/dev/null )
    elif command -v shasum &>/dev/null; then
        ( cd "$(dirname "$file")" && shasum -a 256 -c "$(basename "$sums")" >/dev/null )
    else
        warn "no sha256sum/shasum found; skipping checksum verification"
        return 0
    fi
}

main() {
    case "${1:-}" in
        -h|--help) usage; exit 0 ;;
        "") ;;
        *) error "unknown argument: $1 (try --help)" ;;
    esac

    echo ""
    echo "  scry CLI installer"
    echo ""

    local platform version tag ver_num asset url
    platform=$(detect_platform)
    info "Detected platform: ${platform}"

    tag=$(get_latest_version)
    ver_num="${tag#v}"
    info "Latest release: ${tag}"

    asset="${BIN}-${ver_num}-${platform}.tar.gz"
    url="https://github.com/${REPO}/releases/download/${tag}/${asset}"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Downloading ${asset}..."
    curl -fsSL "$url" -o "${tmp_dir}/${asset}" \
        || error "Could not download ${url}
       (no asset for ${platform} in ${tag}?)"

    # The .sha256 sidecar is best-effort — verify if present.
    if curl -fsSL "${url}.sha256" -o "${tmp_dir}/${asset}.sha256" 2>/dev/null; then
        info "Verifying checksum..."
        verify_sha256 "${tmp_dir}/${asset}" "${tmp_dir}/${asset}.sha256" \
            || error "checksum verification failed for ${asset}"
    else
        warn "no .sha256 sidecar published for ${asset}; skipping verification"
    fi

    info "Extracting..."
    tar -xzf "${tmp_dir}/${asset}" -C "${tmp_dir}"

    local bin_src
    bin_src=$(find "${tmp_dir}" -name "$BIN" -type f 2>/dev/null | head -1)
    [[ -n "$bin_src" ]] || error "Could not find the ${BIN} binary in ${asset}"

    local bin_dir
    if [[ -w "/usr/local/bin" ]]; then
        bin_dir="/usr/local/bin"
    else
        bin_dir="${HOME}/.local/bin"
        mkdir -p "$bin_dir"
    fi

    info "Installing ${BIN} to ${bin_dir}/..."
    install -m 0755 "$bin_src" "${bin_dir}/${BIN}"

    info "Installation complete!"
    echo ""
    echo "  Installed: ${bin_dir}/${BIN} (${tag})"
    echo "  Try:       ${BIN} --help"
    echo ""
    if [[ ":$PATH:" != *":${bin_dir}:"* ]]; then
        warn "${bin_dir} is not in your PATH"
        echo "  Add it with: export PATH=\"\$PATH:${bin_dir}\""
    fi
}

main "$@"
