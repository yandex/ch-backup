#!/usr/bin/env bash

set -e

# Sanitize package signing options
COUNT=0
for sign_param in DEB_SIGN_KEY DEB_SIGN_KEY_ID DEB_SIGN_KEY_PATH; do
    if [[ -n "${!sign_param}" ]]; then ((COUNT+=1)); fi
done
if (( COUNT > 1 )); then
    echo "Error: At most one of DEB_SIGN_KEY or DEB_SIGN_KEY_ID or DEB_SIGN_KEY_PATH vars must be defined " >&2
    exit 1
fi

# Import GPG signing private key if it is provided
if [[ -n "${DEB_SIGN_KEY_ID}" ]]; then
    # Check if gpg knows about this key id
    if [[ $(gpg --list-keys ${DEB_SIGN_KEY_ID} 2>&1) =~ "No public key" ]]; then
        echo "Error: No public key ${DEB_SIGN_KEY_ID}" >&2
        exit 1
    else
        SIGN_ARGS="-k${DEB_SIGN_KEY_ID}"
    fi
elif [[ -n "${DEB_SIGN_KEY}" ]]; then
    echo "${DEB_SIGN_KEY}" | gpg --import
    KEY_ID=$(gpg --list-keys --with-colon | awk -F: '/^fpr/ {print $10;exit}')
    if [[ -z ${KEY_ID} ]]; then
        echo "Error: Unable to import signing key from var DEB_SIGN_KEY" >&2
        exit 1
    fi
    SIGN_ARGS="-k${KEY_ID}"
elif [[ -n "${DEB_SIGN_KEY_PATH}" ]]; then
    gpg --import --with-colons "${DEB_SIGN_KEY_PATH}"
    KEY_ID=$(gpg --list-keys --with-colon | awk -F: '/^fpr/ {print $10;exit}')
    if [[ -z ${KEY_ID} ]]; then
        echo "Error: Unable to import signing key from path: ${DEB_SIGN_KEY_PATH}" >&2
        exit 1
    fi
    SIGN_ARGS="-k${KEY_ID}"
else
    # Do not sign debian package
    SIGN_ARGS="-us -uc"
fi

# Build package
(cd debian && debuild --preserve-env --check-dirname-level 0 ${SIGN_ARGS})

# Move debian package and signed metadata files to the output dir
DEB_FILES=$(echo ../${PROJECT_NAME}*.{deb,dsc,changes,buildinfo,tar.*})
mkdir -p ${BUILD_DEB_OUTPUT_DIR} && mv $DEB_FILES ${BUILD_DEB_OUTPUT_DIR}
