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

# Return the fingerprint of the first available secret key.
get_secret_key_id() {
    gpg --batch --list-secret-keys --with-colons "$@" 2>/dev/null \
        | awk -F: '$1 == "fpr" {print $10; exit}'
}

# Use an isolated keyring for private keys supplied as data or as a file.
setup_signing_keyring() {
    SIGNING_GNUPGHOME=$(mktemp -d) || {
        echo "Error: unable to create temporary GNUPGHOME" >&2
        exit 1
    }
    chmod 700 "${SIGNING_GNUPGHOME}"
    export GNUPGHOME="${SIGNING_GNUPGHOME}"
    trap 'rm -rf -- "${SIGNING_GNUPGHOME}"' EXIT
}

# Import GPG signing private key if it is provided
if [[ -n "${DEB_SIGN_KEY_ID}" ]]; then
    # Check that gpg has the secret part of this key.
    KEY_ID=$(get_secret_key_id "${DEB_SIGN_KEY_ID}")
    if [[ -z ${KEY_ID} ]]; then
        echo "Error: No secret key ${DEB_SIGN_KEY_ID}" >&2
        exit 1
    fi
    SIGN_ARGS="-k${KEY_ID}"
elif [[ -n "${DEB_SIGN_KEY}" ]]; then
    setup_signing_keyring
    printf '%s\n' "${DEB_SIGN_KEY}" | gpg --batch --import
    KEY_ID=$(get_secret_key_id)
    if [[ -z ${KEY_ID} ]]; then
        echo "Error: Unable to import signing key from var DEB_SIGN_KEY" >&2
        exit 1
    fi
    SIGN_ARGS="-k${KEY_ID}"
elif [[ -n "${DEB_SIGN_KEY_PATH}" ]]; then
    setup_signing_keyring
    gpg --batch --import "${DEB_SIGN_KEY_PATH}"
    KEY_ID=$(get_secret_key_id)
    if [[ -z ${KEY_ID} ]]; then
        echo "Error: Unable to import signing key from path: ${DEB_SIGN_KEY_PATH}" >&2
        exit 1
    fi
    SIGN_ARGS="-k${KEY_ID}"
else
    # Do not sign debian package
    SIGN_ARGS="-us -uc"
fi

if [[ -n "${KEY_ID}" ]]; then
    echo "Using GPG secret key: ${KEY_ID}"
fi

# Build package
(cd debian && debuild --preserve-env --check-dirname-level 0 ${SIGN_ARGS})

# Move debian package and signed metadata files to the output dir
DEB_FILES=$(echo ../${PROJECT_NAME}*.{deb,dsc,changes,buildinfo,tar.*})
mkdir -p ${BUILD_DEB_OUTPUT_DIR} && mv $DEB_FILES ${BUILD_DEB_OUTPUT_DIR}
