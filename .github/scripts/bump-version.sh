#!/bin/bash
set -euo pipefail

COMPONENT="$1"
CARGO_TOML="$2"

# Extract current version from Cargo.toml
CURRENT=$(grep '^version = ' "$CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')

IFS='.' read -r MAJOR MINOR PATCH <<<"$CURRENT"

case "$COMPONENT" in
major)
    MAJOR=$((MAJOR + 1))
    MINOR=0
    PATCH=0
    ;;
minor)
    MINOR=$((MINOR + 1))
    PATCH=0
    ;;
patch)
    PATCH=$((PATCH + 1))
    ;;
*)
    echo "::error::Invalid component: $COMPONENT (must be major, minor, or patch)"
    exit 1
    ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"

# Check that the tag does not already exist
if git tag -l | grep -qx "v${NEW_VERSION}"; then
    echo "::error::Tag v${NEW_VERSION} already exists"
    exit 1
fi

# Update version in Cargo.toml
sed -i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" "$CARGO_TOML"

echo "Bumped version: ${CURRENT} -> ${NEW_VERSION}"
echo "old_version=${CURRENT}" >>"$GITHUB_OUTPUT"
echo "new_version=${NEW_VERSION}" >>"$GITHUB_OUTPUT"
