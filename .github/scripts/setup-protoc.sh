#!/bin/bash
set -euo pipefail

PROTOC_VERSION=$(cat .protoc-version)
OS=$(uname -s)
ARCH=$(uname -m)

if [ "$OS" = "Linux" ] && [ "$ARCH" = "x86_64" ]; then
	PROTOC_ARCH="linux-x86_64"
elif [ "$OS" = "Linux" ] && [ "$ARCH" = "aarch64" ]; then
	PROTOC_ARCH="linux-aarch_64"
elif [ "$OS" = "Darwin" ] && [ "$ARCH" = "x86_64" ]; then
	PROTOC_ARCH="osx-x86_64"
elif [ "$OS" = "Darwin" ] && [ "$ARCH" = "arm64" ]; then
	PROTOC_ARCH="osx-aarch_64"
elif echo "$OS" | grep -qi "mingw\|msys\|cygwin\|windows_nt"; then
	PROTOC_ARCH="win64"
else
	echo "::error::Unsupported platform: ${OS} ${ARCH}"
	exit 1
fi

ZIPFILE="protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip"
curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${ZIPFILE}"

if echo "$OS" | grep -qi "mingw\|msys\|cygwin\|windows_nt"; then
	unzip "${ZIPFILE}" -d "$HOME/.local"
	cygpath -w "$HOME/.local/bin" >>"$GITHUB_PATH"
else
	sudo unzip "${ZIPFILE}" -d /usr/local
fi

rm "${ZIPFILE}"
