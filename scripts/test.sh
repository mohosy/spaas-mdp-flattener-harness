#!/usr/bin/env bash
# Run the processor test suite with Gradle in a container.
#   scripts/test.sh             -> fast unit tests (default)
#   scripts/test.sh integration -> include the Testcontainers end-to-end test
#                                  (needs the Docker socket; see -Pintegration)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . docker/.env; set +a
: "${GRADLE_IMAGE:?GRADLE_IMAGE not set}"

mode="${1:-unit}"
if [ "$mode" = "integration" ]; then
  # Docker-out-of-Docker: Testcontainers in the build container talks to the host docker.
  docker run --rm \
    -v "$PWD/processor":/work \
    -v spaas-gradle-cache:/home/gradle/.gradle \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
    --add-host host.docker.internal:host-gateway \
    -w /work \
    "$GRADLE_IMAGE" gradle --no-daemon test -Pintegration
else
  docker run --rm \
    -v "$PWD/processor":/work \
    -v spaas-gradle-cache:/home/gradle/.gradle \
    -w /work \
    "$GRADLE_IMAGE" gradle --no-daemon test
fi
