use super::*;

impl_ioctl_cmd! {
    pub struct GetWriteBufLen<Input=(), Output=i32> {}
}
