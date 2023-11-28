#!/usr/bin/env bash

set -e

BUILD_IMAGE=${PROJECT_NAME}-build
BUILD_ARGS=()

# Compose image name and build arguments
# Example of image name "clickhouse-tools-build-linux-amd64-linux-bionic"		
if [[ -n "${DEB_TARGET_PLATFORM}" ]]; then
    BUILD_ARGS+=(--platform=${DEB_TARGET_PLATFORM})
    BUILD_IMAGE="${BUILD_IMAGE}-${DEB_TARGET_PLATFORM}"
fi
if [[ -n "${DEB_BUILD_DISTRIBUTION}" ]]; then
    BUILD_ARGS+=(--build-arg BASE_IMAGE=${DEB_BUILD_DISTRIBUTION})
    BUILD_IMAGE="${BUILD_IMAGE}-${DEB_BUILD_DISTRIBUTION}"
fi
# Normalize docker image name
BUILD_IMAGE=$(echo ${BUILD_IMAGE} | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/-/g')

RUN_ARGS=( \
    -v ${PWD}:/src \
    --env BUILD_DEB_OUTPUT_DIR="${BUILD_DEB_OUTPUT_DIR}" \
    --env DEB_SIGN_KEY="${DEB_SIGN_KEY}" \
    --env DEB_SIGN_KEY_ID="${DEB_SIGN_KEY_ID}" \
)
# Mount signing key file if its path is provided
if [[ -n "${DEB_SIGN_KEY_PATH}" ]]; then
    RUN_ARGS+=( \
        -v ${DEB_SIGN_KEY_PATH}:/signing_key \
        --env DEB_SIGN_KEY_PATH=/signing_key \
    )
fi

docker build "${BUILD_ARGS[@]}" -t "${BUILD_IMAGE}" -f Dockerfile-deb-build .
docker run "${RUN_ARGS[@]}" "${BUILD_IMAGE}"
