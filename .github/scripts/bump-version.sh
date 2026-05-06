#!/bin/bash
set -euo pipefail

COMPONENT="$1"
CARGO_TOML="$2"

# Resolve "auto" by inspecting labels on PRs merged since the last version tag.
if [ "$COMPONENT" = "auto" ]; then
    LAST_TAG=$(git tag --list 'v*' --sort=-v:refname | head -1)
    if [ -z "$LAST_TAG" ]; then
        echo "::error::No prior version tag found; cannot resolve auto bump"
        exit 1
    fi
    SINCE=$(git log -1 --format=%cI "$LAST_TAG")
    echo "Last tag: $LAST_TAG ($SINCE)" >&2
    LABELS=$(gh pr list \
        --state merged \
        --base master \
        --search "merged:>=$SINCE" \
        --json labels \
        --jq '.[].labels[].name' | sort -u)
    echo "Labels on PRs merged since $LAST_TAG:" >&2
    echo "$LABELS" >&2
    if echo "$LABELS" | grep -qx "breaking"; then
        COMPONENT="major"
    elif echo "$LABELS" | grep -qx "feature"; then
        COMPONENT="minor"
    else
        COMPONENT="patch"
    fi
    echo "Resolved bump: $COMPONENT" >&2
fi

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
    echo "::error::Invalid component: $COMPONENT (must be auto, major, minor, or patch)"
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
