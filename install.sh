#!/usr/bin/env bash
set -euo pipefail
umask 022
shopt -s lastpipe 2>/dev/null || true

VERSION="${VERSION:-}"
OWNER="${OWNER:-Dicklesworthstone}"
REPO="${REPO:-coding_agent_session_search}"
DEST_DEFAULT="$HOME/.local/bin"
DEST="${DEST:-$DEST_DEFAULT}"
EASY=0
QUIET=0
VERIFY=0
QUICKSTART=0
FROM_SOURCE=0
CHECKSUM="${CHECKSUM:-}"
CHECKSUM_URL="${CHECKSUM_URL:-}"
ARTIFACT_URL="${ARTIFACT_URL:-}"
LOCK_FILE="/tmp/coding-agent-search-install.$$.lock"
SYSTEM=0

log() { [ "$QUIET" -eq 1 ] && return 0; echo -e "$@"; }
info() { log "\033[0;34m→\033[0m $*"; }
ok() { log "\033[0;32m✓\033[0m $*"; }
warn() { log "\033[1;33m⚠\033[0m $*"; }
err() { log "\033[0;31m✗\033[0m $*"; }

resolve_version() {
  if [ -n "$VERSION" ]; then return 0; fi

  info "Resolving latest version..."
  local latest_url="https://api.github.com/repos/${OWNER}/${REPO}/releases/latest"
  local tag
  if ! tag=$(curl -fsSL -H "Accept: application/vnd.github.v3+json" "$latest_url" 2>/dev/null | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'); then
    tag=""
  fi

  if [ -n "$tag" ]; then
    VERSION="$tag"
    info "Resolved latest version: $VERSION"
  else
    VERSION="v0.1.0"
    warn "Could not resolve latest version from GitHub API; defaulting to $VERSION"
  fi
}

maybe_add_path() {
  case ":$PATH:" in
    *:"$DEST":*) return 0;;
    *)
      if [ "$EASY" -eq 1 ]; then
        if [ -w "$HOME/.bashrc" ]; then echo "export PATH=\"$DEST:\$PATH\"" >> "$HOME/.bashrc"; fi
        warn "PATH updated in ~/.bashrc; restart shell to use coding-agent-search"
      else
        warn "Add $DEST to PATH to use coding-agent-search"
      fi
    ;;
  esac
}

ensure_rust() {
  if [ "${RUSTUP_INIT_SKIP:-0}" != "0" ]; then
    info "Skipping rustup install (RUSTUP_INIT_SKIP set)"
    return 0
  fi
  if command -v cargo >/dev/null 2>&1 && rustc --version 2>/dev/null | grep -q nightly; then return 0; fi
  if [ "$EASY" -ne 1 ]; then
    if [ -t 0 ]; then
      echo -n "Install Rust nightly via rustup? (y/N): "
      read -r ans
      case "$ans" in y|Y) :;; *) warn "Skipping rustup install"; return 0;; esac
    fi
  fi
  info "Installing rustup (nightly)"
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly --profile minimal
  export PATH="$HOME/.cargo/bin:$PATH"
  rustup component add rustfmt clippy || true
}

usage() {
  cat <<EOFU
Usage: install.sh [--version vX.Y.Z] [--dest DIR] [--system] [--easy-mode] [--verify] [--quickstart] \
                  [--artifact-url URL] [--checksum HEX] [--checksum-url URL] [--quiet]
EOFU
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2;;
    --dest) DEST="$2"; shift 2;;
    --system) SYSTEM=1; DEST="/usr/local/bin"; shift;;
    --easy-mode) EASY=1; shift;;
    --verify) VERIFY=1; shift;;
    --quickstart) QUICKSTART=1; shift;;
    --artifact-url) ARTIFACT_URL="$2"; shift 2;;
    --checksum) CHECKSUM="$2"; shift 2;;
    --checksum-url) CHECKSUM_URL="$2"; shift 2;;
    --from-source) FROM_SOURCE=1; shift;;
    --quiet|-q) QUIET=1; shift;;
    -h|--help) usage; exit 0;;
    *) shift;;
  esac
done

resolve_version

mkdir -p "$DEST"
OS=$(uname -s | tr 'A-Z' 'a-z')
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *) warn "Unknown arch $ARCH, using as-is" ;;
esac
TAR="coding-agent-search-${VERSION}-${OS}-${ARCH}.tar.gz"
URL="https://github.com/${OWNER}/${REPO}/releases/download/${VERSION}/${TAR}"
[ -n "$ARTIFACT_URL" ] && URL="$ARTIFACT_URL"

exec 9>"$LOCK_FILE" || true
LOCKED=0
if flock -n 9; then LOCKED=1; else err "Another installer is running (lock $LOCK_FILE)"; exit 1; fi

cleanup() {
  rm -rf "$TMP"
  if [ "$LOCKED" -eq 1 ]; then rm -f "$LOCK_FILE"; fi
}

TMP=$(mktemp -d)
trap cleanup EXIT

info "Downloading $URL"
if [ "$FROM_SOURCE" -eq 0 ]; then
  if ! curl -fsSL "$URL" -o "$TMP/$TAR"; then
    warn "Artifact download failed; falling back to build-from-source"
    FROM_SOURCE=1
  fi
fi

if [ "$FROM_SOURCE" -eq 1 ]; then
  info "Building from source (requires git, rust nightly)"
  ensure_rust
  git clone --depth 1 "https://github.com/${OWNER}/${REPO}.git" "$TMP/src"
  (cd "$TMP/src" && cargo build --release)
  BIN="$TMP/src/target/release/coding-agent-search"
  [ -x "$BIN" ] || { err "Build failed"; exit 1; }
  install -m 0755 "$BIN" "$DEST"
  ok "Installed to $DEST/coding-agent-search (source build)"
  maybe_add_path
  if [ "$VERIFY" -eq 1 ]; then "$DEST/coding-agent-search" --version || true; ok "Self-test complete"; fi
  if [ "$QUICKSTART" -eq 1 ]; then info "Running index --full (quickstart)"; "$DEST/coding-agent-search" index --full || warn "index --full failed"; fi
  ok "Done. Run: coding-agent-search tui"
  exit 0
fi

if [ -z "$CHECKSUM" ]; then
  [ -z "$CHECKSUM_URL" ] && CHECKSUM_URL="${URL}.sha256"
  info "Fetching checksum from ${CHECKSUM_URL}"
  CHECKSUM_FILE="$TMP/checksum.sha256"
  if ! curl -fsSL "$CHECKSUM_URL" -o "$CHECKSUM_FILE"; then
    err "Checksum required and could not be fetched"; exit 1;
  fi
  # Extract just the hash (first field) from the file
  CHECKSUM=$(awk '{print $1}' "$CHECKSUM_FILE")
  if [ -z "$CHECKSUM" ]; then err "Empty checksum file"; exit 1; fi
fi

echo "$CHECKSUM  $TMP/$TAR" | sha256sum -c - || { err "Checksum mismatch"; exit 1; }
ok "Checksum verified"

info "Extracting"
tar -xzf "$TMP/$TAR" -C "$TMP"
BIN="$TMP/coding-agent-search"
[ -x "$BIN" ] || { err "Binary not found in tar"; exit 1; }
install -m 0755 "$BIN" "$DEST"
ok "Installed to $DEST/coding-agent-search"
maybe_add_path


if [ "$VERIFY" -eq 1 ]; then
  "$DEST/coding-agent-search" --version || true
  ok "Self-test complete"
fi

if [ "$QUICKSTART" -eq 1 ]; then
  info "Running index --full (quickstart)"
  "$DEST/coding-agent-search" index --full || warn "index --full failed"
fi

ok "Done. Run: coding-agent-search tui"
