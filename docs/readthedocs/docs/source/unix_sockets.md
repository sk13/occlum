# Unix Domain Sockets and Pipes

Occlum supports Unix domain sockets of the type `SOCK_STREAM` (also as `socketpair()`), and pipes. They share the queue of the bytes (`src/libos/src/fs/channel.rs`). `SOCK_DGRAM`, `SOCK_SEQPACKET` and raw sockets of the domain `AF_UNIX` are not supported.

## Conformance with Linux

`test/uds_conf` is a probe with 1,619 checks that compares what the LibOS does with the results of Linux. It is a test, but not one of the default tests since it takes about a minute:

```
# make -C test test TESTS=uds_conf
# OCCLUM_TEST_GLIBC=1 make -C test test-glibc TESTS=uds_conf
```

All results are those of Linux, but the ones that `test/uds_conf/uds_conf.known` lists. They are differences of design or of other parts of the LibOS:

* A Unix socket that binds to a path makes a socket file there, as on Linux. The LibOS releases the name when the last socket that has it is closed, so that a program can bind to the file that is left without unlinking it first, where Linux fails with `EADDRINUSE`. The redirects of the network policy depend on that.
* The queue accounts bytes, where Linux accounts the memory of the messages. A socket is writable as soon as one byte is free (Linux waits until a good part of the buffer is free), and a blocked writer is woken when the peer has read one byte.
* A call that a signal interrupts fails with `EINTR` also if the handler has `SA_RESTART`, as everywhere in the LibOS.
* The epoll file of the LibOS finds a file by its descriptor number: it forgets a file when the descriptor is closed, also if another descriptor for the file is open, where Linux goes on monitoring it.
* `SO_PEERPIDFD` and `SO_PEERSEC` of the end of a `socketpair()` are not supported.
* The LibOS does not enforce `RLIMIT_NOFILE` for descriptors: the probe opens more than 1,900 sockets, where Linux stops at the limit of the process (1,021 sockets with the limit of 1,024) with `EMFILE`. So the receiver of descriptors never fails with `EMFILE`.

## Behavior that programs may notice

* A write to a pipe or a Unix socket whose reader is gone raises `SIGPIPE` in the calling thread, as on Linux, unless the call has `MSG_NOSIGNAL`. A program that does not ignore `SIGPIPE` and relied on getting only `EPIPE` is terminated, as it is on Linux.
* A socket whose peer closes with bytes that nobody has read fails the next read with `ECONNRESET` once, which `SO_ERROR` shows, too, and a writer that waits on a full buffer is woken with `ECONNRESET`, as on Linux. A pipe has no such error.
* `SO_SNDBUF` limits the bytes that a socket has in flight to its peer. The size is twice what the program sets and at most twice 212992 bytes (`net.core.wmem_max` of Linux by default), unless the program uses `SO_SNDBUFFORCE`, as the bytes in flight take the kernel heap of the enclave. `SO_RCVBUF` is stored, and limits nothing.
* The queues allocate their memory when bytes are queued, in chunks: a socket pair takes about 4 kB of the kernel heap, a connection that was accepted about 4 kB, a pipe about 1 kB, and what is queued comes on top. A write that cannot get memory fails with `ENOMEM`.
* Descriptors that are sent with `SCM_RIGHTS` are held by the queue of the receiving socket from the send on, so a sender can close them right after it has sent them, and the receiver finds them if the sender has exited. A queue holds at most 4,096 messages with descriptors, and a sender waits for room (or fails with `EAGAIN` if it does not block). A socket that is sent to itself, or sockets that hold each other through their queues, are not freed (Linux finds them): each such cycle keeps about 3.6 kB of the kernel heap until the enclave ends, and some thousand of them use up the kernel heap of the default configuration.
* The file of a pathname socket is a socket file in the file systems that have that type, with the permissions of the program's `umask`, and `open()` and `truncate()` refuse it. See [the file systems](filesystem/fs_overview.md) for the HostFS and for hard links.
