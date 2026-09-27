# Building Occlum Docker images

This folder contains scripts and Dockerfiles for users to build the Docker images
for Occlum. An Occlum Docker image sets up the development environment for
Occlum and also gets Occlum preinstalled.


## How to Build

### Docker image for development

Currently, three Linux OS distributions are supported: Ubuntu 20.04, aliyunlinux3 and anolis8.8.

To build an Occlum Docker image, run the following command
```
./build_image.sh <OCCLUM_LABEL> <OS_NAME> <OCCLUM_BRANCH>
```
where `<OCCLUM_LABEL>` is an arbitrary string chosen by the user to
describe the version of Occlum preinstalled in the Docker image
(e.g., "latest", "0.24.0", and "prerelease") and `<OS_NAME>` is the
name of the OS distribution that the Docker image is based on.
Currently, `<OS_NAME>` must be one of the following values:
`ubuntu20.04`, `aliyunlinux3` and `anolis8.8`.
`<OCCLUM_BRANCH>` indicates which the docker image is built on, e.g "0.24.0".
It is optional, if not provided, "master" branch will be used.

The resulting Docker image will have `occlum/occlum:<OCCLUM_LABEL>-<OS_NAME>` as its label.

### Docker image for runtime

Currently, two Linux OS distributions are supported for runtime docker image: Ubuntu 20.04 and Ubuntu 22.04.

The Occlum runtime docker image has the smallest size, plus supports running prebuilt Occlum instance.

To build an Occlum runtime Docker image, run the following command
```
./build_rt_image.sh <OCCLUM_VERSION> <OS_NAME> <SGX_PSW_VERSION> <SGX_DCAP_VERSION>

<OCCLUM_VERSION>:
    For ubuntu20.04, the version of the Occlum debian packages to install, e.g "0.29.7".
    Make sure this Occlum version debian packages are available in advance.
    For ubuntu22.04, it is only used in the label of the image, e.g "sk13".

<OS_NAME>:
    The name of the OS distribution that the Docker image is based on. Currently, <OS_NAME> must be one of the following values:
        ubuntu20.04         Use Ubuntu 20.04 as the base image
        ubuntu22.04         Use Ubuntu 22.04 as the base image

<SGX_PSW_VERSION>:
    The SGX PSW version libraries expected to be installed in the runtime docker image.

<SGX_DCAP_VERSION>:
    The SGX DCAP version libraries expected to be installed in the runtime docker image.
```

The resulting Docker image will have `occlum/occlum:<OCCLUM_VERSION>-rt-<OS_NAME>` as its label.

Just note, that the **<OCCLUM_VERSION>**, **<SGX_PSW_VERSION>** and **<SGX_DCAP_VERSION>** have dependencies. Details please refer to Dockerfile.ubuntu20.04.

For example, building Occlum runtime docker image for version 0.29.7.
```
./build_rt_image.sh 0.29.7 ubuntu20.04 2.17.100.3 1.14.100.3
```

#### Ubuntu 22.04

The Ubuntu 22.04 runtime image does not install the Occlum debian packages.
Instead, it copies the files of the `occlum-runtime` package (`occlum`,
`occlum_build.mk`, `occlum-run`, `occlum_exec_client` and `occlum_exec_server`
in `/opt/occlum/build/bin`) from an Occlum development image. By default, this
is `docker.io/sk13sk13/occlum:sk13-ubuntu22.04`, which is built with
`Dockerfile.ubuntu22.04` from the `0.31.0-dev` branch of the sk13/occlum fork.
To use another development image, set the `OCCLUM_DEV_IMAGE` environment
variable, which is passed to the `OCCLUM_DEV_IMAGE` build argument of
`Dockerfile.ubuntu22.04-rt`.

Use the SGX PSW and DCAP versions of the development image, see
`Dockerfile.ubuntu22.04`. For example, the following command builds
`occlum/occlum:sk13-rt-ubuntu22.04`:
```
./build_rt_image.sh sk13 ubuntu22.04 2.21.100.1 1.18.100.1
```

The runtime image only provides the `occlum` command line tool. `occlum run`
starts the `occlum-run` of the Occlum instance, and the Occlum LibOS and PAL
that run the enclave are part of the instance too. So build and package the
instance (`occlum build` and `occlum package`) with the same development image.
