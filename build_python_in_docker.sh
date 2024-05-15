#!/usr/bin/env bash

set -e

BUILD_IMAGE=${PYTHON_BUILD_NAME}-build
BUILD_ARGS=()

# Normalize docker image name
BUILD_IMAGE=$(echo ${BUILD_IMAGE} | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/-/g')

if [[ -n "${DEB_TARGET_PLATFORM}" ]]; then
    BUILD_ARGS+=(--platform=${DEB_TARGET_PLATFORM})
fi
if [[ -n "${DEB_BUILD_DISTRIBUTION}" ]]; then
    BUILD_ARGS+=(--build-arg BASE_IMAGE=${DEB_BUILD_DISTRIBUTION})
fi
if [[ -n "${TARGET_PYTHON_VERSION}" ]]; then
    BUILD_ARGS+=(--build-arg PYTHON_VERSION=${TARGET_PYTHON_VERSION})
fi
if [[ -n "${PYTHON_INSTALL_PREFIX}" ]]; then
    BUILD_ARGS+=(--build-arg PYTHON_INSTALL_PREFIX=${PYTHON_INSTALL_PREFIX})
fi

docker build "${BUILD_ARGS[@]}" -t "${BUILD_IMAGE}" -f Dockerfile-python-build .

echo "${BUILD_IMAGE} was built."
