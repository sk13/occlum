use super::*;

impl_ioctl_cmd! {
    pub struct GetAtMark<Input=(), Output=i32> {}
}
