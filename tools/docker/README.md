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
`ubuntu20.04`, `ubuntu22.04`, `ubuntu24.04`, `aliyunlinux3` and `anolis8.8`.
`<OCCLUM_BRANCH>` indicates which the docker image is built on, e.g "0.24.0".
It is optional, if not provided, "0.31.0-dev" branch will be used.

The Ubuntu 22.04 and 24.04 images are built from the `OCCLUM_REPO` repository, by default
https://github.com/sk13/occlum.git. `Dockerfile.ubuntu22.04` and `Dockerfile.ubuntu24.04` add it with `ADD`
from Git, which needs BuildKit (the default builder since Docker 23.0).
`ADD` resolves the branch on every build, so a rebuild reuses the cache only for
the steps whose files have not changed: the toolchains are rebuilt only when
`tools/toolchains` has changed, Occlum itself whenever the branch has changed.

The Ubuntu 24.04 image differs from the 22.04 one only in the base image, the
`noble` repository and package versions of the Intel SGX packages, and `git`
instead of `git-core`. It is built with the same glibc 2.39 as the Ubuntu 22.04
image, which is the glibc of Ubuntu 24.04, so that programs built on Ubuntu 24.04
(whose libstdc++ needs `GLIBC_2.38`) run in Occlum. Programs built on 24.04 must
bring their own libstdc++ and libgcc_s, as the toolchain libraries of Occlum are the
ones of the build image. There is no runtime image for Ubuntu 24.04 yet.

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
        ubuntu24.04         Use Ubuntu 24.04 as the base image

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

The SGX PSW and DCAP packages of the runtime image are the ones of the
development image, in exactly the versions installed there (read from its
package database), so no versions are given. For example, the following command
builds `occlum/occlum:sk13-rt-ubuntu22.04`:
```
./build_rt_image.sh sk13 ubuntu22.04
```

#### Ubuntu 24.04

The Ubuntu 24.04 runtime image is built the same way, with
`Dockerfile.ubuntu24.04-rt`: it copies the `occlum-runtime` files from the
development image built with `Dockerfile.ubuntu24.04` (by default
`docker.io/sk13sk13/occlum:sk13-ubuntu24.04`; set `OCCLUM_DEV_IMAGE` to use
another one) and installs the SGX PSW and DCAP packages of the `noble`
repository in the versions of the development image. For example, the following
command builds `occlum/occlum:sk13-rt-ubuntu24.04`:
```
./build_rt_image.sh sk13 ubuntu24.04
```

The runtime image only provides the `occlum` command line tool. `occlum run`
starts the `occlum-run` of the Occlum instance, and the Occlum LibOS and PAL
that run the enclave are part of the instance too. So build and package the
instance (`occlum build` and `occlum package`) with the same development image.
