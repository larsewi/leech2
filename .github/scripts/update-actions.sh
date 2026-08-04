#!/bin/bash
set -euo pipefail

# Pins look like "uses: owner/repo@v6". Local actions ("uses: ./.github/...")
# track no upstream release and are excluded by requiring an owner and an "@".
PINS=$(find .github -name '*.yml' -exec grep -hoE 'uses: [^./[:space:]]+/[^@[:space:]]+@[^[:space:]]+' {} + |
    sed 's/^uses: //' | sort -u)

echo "$PINS" | while read -r PIN; do
    REPOSITORY=${PIN%@*}
    CURRENT=${PIN##*@}

    # Only bare major tags are tracked. A pin to a full version or a commit SHA
    # is deliberate and left alone.
    if ! [[ $CURRENT =~ ^v?[0-9]+$ ]]; then
        echo "Skipping ${REPOSITORY}: ${CURRENT} is not a major tag"
        continue
    fi

    if ! LATEST=$(gh api "repos/${REPOSITORY}/releases/latest" --jq .tag_name 2>/dev/null); then
        echo "Skipping ${REPOSITORY}: no latest release found"
        continue
    fi

    MAJOR=${LATEST%%.*}
    if ! [[ $MAJOR =~ ^v?[0-9]+$ ]]; then
        echo "Skipping ${REPOSITORY}: cannot read a major tag from ${LATEST}"
        continue
    fi

    if [ "$CURRENT" = "$MAJOR" ]; then
        echo "${REPOSITORY}@${CURRENT} is up to date"
        continue
    fi

    echo "Updating ${REPOSITORY} from ${CURRENT} to ${MAJOR} (latest release ${LATEST})"
    # The pin ends the line, so anchor the match: an unanchored "@v6" would also
    # rewrite the prefix of a "@v6.1.0" pin.
    find .github -name '*.yml' -exec \
        sed -i "s|uses: ${REPOSITORY}@${CURRENT}\$|uses: ${REPOSITORY}@${MAJOR}|" {} +
done
