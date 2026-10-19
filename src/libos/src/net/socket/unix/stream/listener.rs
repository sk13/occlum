use super::endpoint::{Endpoint, RelayNotifier};
use super::*;
use fs::channel::Channel;
use fs::IoEvents;
use std::sync::Arc;

/// The listener status of a stream unix socket.
/// It contains a channel holding incoming connections.
/// The nonblocking status of the reader end keeps the same with the socket.
/// The writer end is always non-blocking. The connect function returns
/// ECONNREFUSED rather than block when the channel is full.
pub struct Listener {
    channel: RwLock<Channel<Endpoint>>,
    pub(super) notifier: Arc<RelayNotifier>,
}

impl Listener {
    pub(super) fn new(
        capacity: usize,
        nonblocking: bool,
        notifier: Arc<RelayNotifier>,
    ) -> Result<Self> {
        let channel = Channel::new(capacity)?;
        channel.producer().set_nonblocking(true);
        channel.consumer().set_nonblocking(nonblocking);

        Ok(Self {
            channel: RwLock::new(channel),
            notifier,
        })
    }

    pub fn capacity(&self) -> usize {
        let channel = self.channel.read().unwrap();
        channel.capacity()
    }

    // TODO: when pop_incoming is blocked somewhere, the resize operation will blockingly wait for
    // the block to end. This is a rare scenario, so we will fix it in the future.
    pub fn resize(&self, capacity: usize) {
        if self.capacity() == capacity {
            return;
        }

        let mut channel = self.channel.write().unwrap();
        let new_channel = Channel::new(capacity).unwrap();
        new_channel.producer().set_nonblocking(true);
        new_channel
            .consumer()
            .set_nonblocking(channel.consumer().is_nonblocking());

        let remaining = channel.items_to_consume();
        for i in 0..std::cmp::min(remaining, capacity) {
            new_channel.push(channel.pop().unwrap().unwrap()).unwrap();
        }

        *channel = new_channel;
    }

    pub fn push_incoming(&self, stream_socket: Endpoint) -> Result<()> {
        let channel = self.channel.read().unwrap();
        channel.push(stream_socket)
    }

    pub fn pop_incoming(&self) -> Result<Endpoint> {
        let channel = self.channel.read().unwrap();
        // Keep the errors of the channel: EAGAIN if the socket is non-blocking
        // and EINTR if a signal interrupts it. No item means that the channel
        // is shut down.
        channel
            .pop()?
            .ok_or_else(|| errno!(EAGAIN, "No connection is incoming"))
    }

    pub fn remaining(&self) -> usize {
        let channel = self.channel.read().unwrap();
        channel.items_to_consume()
    }

    pub fn nonblocking(&self) -> bool {
        let channel = self.channel.read().unwrap();
        channel.consumer().is_nonblocking()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) {
        let channel = self.channel.read().unwrap();
        channel.consumer().set_nonblocking(nonblocking);
    }

    pub fn shutdown(&self) {
        let channel = self.channel.read().unwrap();
        channel.shutdown();
    }

    pub fn poll_new(&self) -> IoEvents {
        let mut events = IoEvents::empty();
        let channel = self.channel.read().unwrap();
        let item_num = channel.items_to_consume();
        if item_num > 0 {
            events |= IoEvents::IN;
        }
        events
    }
}
