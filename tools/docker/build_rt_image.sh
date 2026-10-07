#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}"  )" >/dev/null 2>&1 && pwd )"

report_error() {
    RED=$(tput setaf 1)
    NO_COLOR=$(tput sgr0)

    cat <<EOF
${RED}error:${NO_COLOR} input is invalid

build_image
Build an Occlum Docker runtime image for a specific OS

USAGE:
    build_rt_image.sh <OCCLUM_VERSION> <OS_NAME> [<SGX_PSW_VERSION> <SGX_DCAP_VERSION>]

<OCCLUM_VERSION>:
    For ubuntu20.04, the version of the Occlum debian packages to install, e.g "0.29.7".
    Make sure this Occlum version debian packages are available in advance.
    For ubuntu22.04 and ubuntu24.04, it is only used in the label of the image, e.g
    "sk13". The Occlum runtime is copied from the Occlum development image given by the
    OCCLUM_DEV_IMAGE environment variable, docker.io/sk13sk13/occlum:sk13-ubuntu22.04 and
    docker.io/sk13sk13/occlum:sk13-ubuntu24.04 by default.

<OS_NAME>:
    The name of the OS distribution that the Docker image is based on. Currently, <OS_NAME> must be one of the following values:
        ubuntu20.04         Use Ubuntu 20.04 as the base image
        ubuntu22.04         Use Ubuntu 22.04 as the base image
        ubuntu24.04         Use Ubuntu 24.04 as the base image

<SGX_PSW_VERSION>:
    The SGX PSW version libraries expected to be installed in the runtime docker image.
    Required for ubuntu20.04. Not for ubuntu22.04 and ubuntu24.04, where the runtime
    image gets the SGX packages in the versions that are installed in the Occlum
    development image.

<SGX_DCAP_VERSION>:
    The SGX DCAP version libraries expected to be installed in the runtime docker image.
    Required for ubuntu20.04. Not for ubuntu22.04 and ubuntu24.04, see above.


Note: <OCCLUM_VERSION>, <SGX_PSW_VERSION> and <SGX_DCAP_VERSION> have dependencies. Details
please refer to Dockerfile.ubuntu20.04.

The resulting Docker image will have "occlum/occlum:<OCCLUM_VERSION>-rt-<OS_NAME>" as its label.
EOF
    exit 1
}

set -e

if [[ ( "$#" < 2 ) || ( "$#" > 4 ) ]] ; then
    report_error
fi

occlum_version=$1
os_name=$2
sgx_psw_version=$3
sgx_dcap_version=$4

function check_item_in_list() {
    item=$1
    list=$2
    [[ $list =~ (^|[[:space:]])$item($|[[:space:]]) ]]
}

check_item_in_list "$os_name" "ubuntu20.04 ubuntu22.04 ubuntu24.04" || report_error

extra_build_args=()
if [[ "$os_name" == "ubuntu20.04" ]]; then
    # The Dockerfiles of ubuntu22.04 and ubuntu24.04 take the versions from the development image
    [[ "$#" == 4 ]] || report_error
    extra_build_args+=(--build-arg PSW_VERSION="$sgx_psw_version" --build-arg DCAP_VERSION="$sgx_dcap_version")
fi
if [ -n "$OCCLUM_DEV_IMAGE" ]; then
    extra_build_args+=(--build-arg OCCLUM_DEV_IMAGE="$OCCLUM_DEV_IMAGE")
fi

cd "$script_dir"
docker build -f "$script_dir/Dockerfile.$os_name-rt" \
    -t "occlum/occlum:$occlum_version-rt-$os_name" \
    --build-arg OCCLUM_VERSION=$occlum_version \
    "${extra_build_args[@]}" \
    .
