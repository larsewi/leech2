#!/bin/sh

set -e
echo "$0: Running autoreconf ..."
autoreconf --force --install -I m4 || exit
