#!/bin/bash
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}"  )" >/dev/null 2>&1 && pwd )"
BUILD_DIR=/tmp/occlum_gcc_toolchain
INSTALL_DIR=/opt/occlum/toolchains/gcc

# Exit if any command fails
set -e

# Clean previous build and installation if any
rm -rf ${BUILD_DIR}
rm -rf ${INSTALL_DIR}

# Create the build directory
mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}

# Download musl-cross-make project
git clone https://github.com/occlum/musl-cross-make
cd musl-cross-make
git checkout 0.9.9.hotfix

# Let musl-cross-make build for x86-64 Linux
TARGET=x86_64-linux-musl
# We will check out the branch ${MUSL_VER} from ${MUSL_REPO}
MUSL_REPO=https://github.com/occlum/musl
MUSL_VER=1.1.24
# We will use this version of GCC
GCC_VER=8.3.0

# This patch replaces syscall instruction with libc's syscall wrapper
cp ${THIS_DIR}/0014-libgomp-*.diff patches/gcc-${GCC_VER}/

# musl-cross-make downloads config.sub from git.savannah.gnu.org, which is
# often unreachable. Download it from the freedesktop-sdk GitLab mirror of the
# same config.git repository instead; musl-cross-make still checks its hash.
SAVANNAH_CONFIG_SUB='http://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.sub;hb=$(CONFIG_SUB_REV)'
MIRROR_CONFIG_SUB='https://gitlab.com/freedesktop-sdk/mirrors/savannah/config/-/raw/$(CONFIG_SUB_REV)/config.sub'
grep -qF "${SAVANNAH_CONFIG_SUB}" Makefile
sed -i "s|${SAVANNAH_CONFIG_SUB}|${MIRROR_CONFIG_SUB}|" Makefile
grep -qF "${MIRROR_CONFIG_SUB}" Makefile

# musl-cross-make downloads the sources of GCC, binutils, GMP, MPC and MPFR from
# ftp.gnu.org, which is often unreachable, and has been down completely at times
# (the whole of gnu.org). It checks the SHA-1 of every source (hashes/), so any
# mirror of the GNU archive can serve them: use the first one that has GCC. The
# list can be replaced with GNU_MIRRORS in the environment.
GNU_MIRRORS=${GNU_MIRRORS:-"https://mirrors.kernel.org/gnu https://ftp.fau.de/gnu https://mirror.ibcp.fr/pub/gnu https://mirrors.ocf.berkeley.edu/gnu https://mirrors.tuna.tsinghua.edu.cn/gnu https://ftp.gnu.org/pub/gnu"}
GNU_SITE=
for mirror in ${GNU_MIRRORS}; do
    if wget -q --spider --timeout=20 --tries=1 "${mirror}/gcc/gcc-${GCC_VER}/gcc-${GCC_VER}.tar.xz"; then
        GNU_SITE=${mirror}
        break
    fi
    echo "The GNU mirror ${mirror} is not reachable"
done
if [ -z "${GNU_SITE}" ]; then
    echo "None of the GNU mirrors is reachable: ${GNU_MIRRORS}" >&2
    exit 1
fi
echo "Downloading the sources of the GNU tools from ${GNU_SITE}"

# Build musl-gcc toolchain for Occlum
cat > config.mak <<EOF
TARGET = ${TARGET}
OUTPUT = ${INSTALL_DIR}
COMMON_CONFIG += CFLAGS="-fPIC" CXXFLAGS="-fPIC" LDFLAGS="-pie"

GCC_VER = ${GCC_VER}

MUSL_VER = git-${MUSL_VER}
MUSL_REPO = ${MUSL_REPO}

GNU_SITE = ${GNU_SITE}
DL_CMD = wget -c --tries=5 --waitretry=5 --timeout=30 -O
EOF
make -j$(nproc)
make install

# Remove all source code and build files
rm -rf ${BUILD_DIR}

# Generate the wrappers for executables
cat > ${INSTALL_DIR}/bin/occlum-gcc <<EOF
#!/bin/bash
${INSTALL_DIR}/bin/${TARGET}-gcc -fPIC -pie -Wl,-rpath,${INSTALL_DIR}/${TARGET}/lib "\$@"
EOF

cat > ${INSTALL_DIR}/bin/occlum-g++ <<EOF
#!/bin/bash
${INSTALL_DIR}/bin/${TARGET}-g++ -fPIC -pie -Wl,-rpath,${INSTALL_DIR}/${TARGET}/lib "\$@"
EOF

cat > ${INSTALL_DIR}/bin/occlum-ld <<EOF
#!/bin/bash
${INSTALL_DIR}/bin/${TARGET}-ld -pie -rpath ${INSTALL_DIR}/${TARGET}/lib "\$@"
EOF

chmod +x ${INSTALL_DIR}/bin/occlum-gcc
chmod +x ${INSTALL_DIR}/bin/occlum-g++
chmod +x ${INSTALL_DIR}/bin/occlum-ld

# Make symbolic links
ln -sf ${INSTALL_DIR}/${TARGET}/lib/libc.so /lib/ld-musl-x86_64.so.1
ln -sf ${INSTALL_DIR} /usr/local/occlum
ln -sf ${INSTALL_DIR}/bin/x86_64-linux-musl-gcc-ar ${INSTALL_DIR}/bin/occlum-ar
ln -sf ${INSTALL_DIR}/bin/x86_64-linux-musl-strip ${INSTALL_DIR}/bin/occlum-strip
