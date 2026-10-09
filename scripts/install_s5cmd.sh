#!/usr/bin/env bash
(
  set -e
  S5CMD_VERSION=$(
    curl -s https://api.github.com/repos/peak/s5cmd/releases/latest \
    | jq -r '.tag_name | .[1:]'
  )
  BIN_DIR=$([[ "$USER" == "root" ]] && echo /usr/local/bin || echo ~/.local/bin)
  mkdir -p "$BIN_DIR"
  # Architecture discovery
  case "$(uname -m)" in
    x86_64) S5CMD_ARCH="64bit" ;;
    aarch64|arm64) S5CMD_ARCH="arm64" ;;
    *) echo "System architecture $(uname -m) not supported."; exit 1 ;;
  esac
  # Binary and checksums download
  S5CMD_FILE="s5cmd_${S5CMD_VERSION}_Linux-${S5CMD_ARCH}.tar.gz"
  S5CMD_BASE_URL="https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}"
  TMP_DIR=$(mktemp -d)
  cd "$TMP_DIR"
  curl -fsSLO "$S5CMD_BASE_URL/$S5CMD_FILE"
  curl -fsSLO "$S5CMD_BASE_URL/s5cmd_checksums.txt"
  # Checksum validation and installation
  grep " ${S5CMD_FILE}$" s5cmd_checksums.txt | sha256sum -c
  tar -xf "$S5CMD_FILE" -C "$BIN_DIR" s5cmd
  chmod +x "$BIN_DIR/s5cmd"
  # Cleanup
  rm -rf "$TMP_DIR"
)
