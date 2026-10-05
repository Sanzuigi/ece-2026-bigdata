#!/usr/bin/env bash
set -euo pipefail

version=$(curl -fsS https://api.github.com/repos/peak/s5cmd/releases/latest \
  | jq -er '.tag_name | ltrimstr("v")')

case "$(uname -m)" in
  x86_64) architecture="64bit" ;;
  aarch64|arm64) architecture="arm64" ;;
  *) echo "Unsupported architecture"; exit 1 ;;
esac

filename="s5cmd_${version}_Linux-${architecture}.tar.gz"
base_url="https://github.com/peak/s5cmd/releases/download/v${version}"
temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT

mkdir -p "$HOME/.local/bin"
cd "$temporary_directory"
curl -fsSLO "$base_url/$filename"
curl -fsSLO "$base_url/s5cmd_checksums.txt"
grep " ${filename}$" s5cmd_checksums.txt | sha256sum -c
tar -xzf "$filename" -C "$HOME/.local/bin" s5cmd
chmod +x "$HOME/.local/bin/s5cmd"
