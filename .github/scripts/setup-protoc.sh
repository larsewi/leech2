#!/bin/bash
set -euo pipefail

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

# The committed .protoc-checksums file is the single source of truth for both
# the protoc version and the expected archive checksums. Each line is in the
# "sha256sum" format: "<sha256>  protoc-<version>-<arch>.zip".
LINE=$(awk -v arch="${PROTOC_ARCH}" '$2 ~ ("-" arch "\\.zip$")' .protoc-checksums)
if [ -z "$LINE" ]; then
    echo "::error::No checksum entry for arch ${PROTOC_ARCH} in .protoc-checksums"
    exit 1
fi
EXPECTED=${LINE%% *}
ZIPFILE=${LINE##* }

# Derive the release tag (vX.Y.Z) from the archive filename.
PROTOC_VERSION=${ZIPFILE#protoc-}
PROTOC_VERSION=${PROTOC_VERSION%-"${PROTOC_ARCH}".zip}

curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${ZIPFILE}"

# Verify the download before trusting it. Linux and Git Bash on Windows ship
# sha256sum (coreutils); macOS only ships shasum.
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "${ZIPFILE}" | awk '{print $1}')
else
    ACTUAL=$(shasum -a 256 "${ZIPFILE}" | awk '{print $1}')
fi
if [ "$ACTUAL" != "$EXPECTED" ]; then
    echo "::error::Checksum mismatch for ${ZIPFILE}: expected ${EXPECTED}, got ${ACTUAL}"
    exit 1
fi

if echo "$OS" | grep -qi "mingw\|msys\|cygwin\|windows_nt"; then
    unzip "${ZIPFILE}" -d "$HOME/.local"
    cygpath -w "$HOME/.local/bin" >>"$GITHUB_PATH"
else
    sudo unzip "${ZIPFILE}" -d /usr/local
fi

rm "${ZIPFILE}"
