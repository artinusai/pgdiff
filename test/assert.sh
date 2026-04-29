#!/bin/bash

set -euo pipefail

clean_pgdiff_output() {
    sed '/^-- /d;/^$/d'
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq -- "$needle" <<<"$haystack"; then
        echo "Expected output to contain: $needle" >&2
        echo "Actual output:" >&2
        echo "$haystack" >&2
        exit 1
    fi
}

assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    if grep -Fq -- "$needle" <<<"$haystack"; then
        echo "Expected output to omit: $needle" >&2
        echo "Actual output:" >&2
        echo "$haystack" >&2
        exit 1
    fi
}
