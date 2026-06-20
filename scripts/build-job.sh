#!/usr/bin/env bash
# Build the Flink fat jar using Gradle in a container (no host Gradle required).
# The Gradle cache is a named volume so repeat builds don't re-download deps.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . docker/.env; set +a
: "${GRADLE_IMAGE:?GRADLE_IMAGE not set in docker/.env}"

echo "== building spaas-processor fat jar with ${GRADLE_IMAGE} =="
docker run --rm \
  -v "$PWD/processor":/work \
  -v spaas-gradle-cache:/home/gradle/.gradle \
  -w /work \
  "$GRADLE_IMAGE" gradle --no-daemon shadowJar "$@"

echo "== artifacts =="
ls -lh processor/build/libs/
