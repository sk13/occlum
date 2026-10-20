# Tests for Occlum

## Unit Tests

There is a set of unit tests in the source [**test**](https://github.com/occlum/occlum/tree/master/test). It includes almost all the syscall (Occlum supported) test cases. Every PR will run this unit tests to make sure of no failures introduced on basic functions.

Users could run the unit test manually as well.

```
// run all the unit test cases for musl-libc
# make test

// run all the unit test cases for glibc
# make test-glibc

// run only specified test case, timerfd for example
# TESTS=timerfd make test

// run test cases for 100 times
# make test times=100

// run test without rebuilding Occlum, using binaries installed already
# OCCLUM_BIN_PATH=/opt/occlum/build/bin make test

// run the unit tests of the LibOS code that needs no enclave, on the host (so far
// the queue of the channels of pipes and unix sockets, and the entries of the control
// messages of unix sockets)
# make test-host

// run a test that is not in the default list, since it drives the kernel heap to its
// limit and may take the enclave down on a busy machine: run it by itself, on a quiet machine
# TESTS=unix_socket_memory_exhaust make test
```

## Gvisor Tests
<To be added>

## LTP Tests

Linux [LTP](https://github.com/linux-test-project/ltp) is the most popular test suite for Linux. Occlum could also apply the LTP to verify the stability and compatibility to Linux app. For detail, please refer to [**linux-ltp**](https://github.com/occlum/occlum/tree/master/demos/linux-ltp).
