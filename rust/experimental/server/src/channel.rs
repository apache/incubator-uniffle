use std::sync::Arc;
use crossbeam_channel::{Receiver, Sender, unbounded};
use crate::app::{AppManager, WritingViewContext};
use crate::error::WorkerError;
use crate::metric::TOTAL_RECEIVED_DATA;
use crate::runtime::manager::RuntimeManager;

#[derive(Clone)]
pub struct Channel {
    app_manager: Arc<AppManager>,
    runtime_manager: RuntimeManager,

    sender: Sender<WritingViewContext>,
    receiver: Receiver<WritingViewContext>,
}

impl Channel {
    pub fn new(app_manager: Arc<AppManager>, runtime_manager: RuntimeManager) -> Self {
        let (s, r) = unbounded();
        Self {
            app_manager,
            runtime_manager,
            sender: s,
            receiver: r,
        }
    }

    pub fn send(&self, ctx: WritingViewContext) -> anyhow::Result<i32, WorkerError> {
        let len: i32 = ctx.data_blocks.iter().map(|block| block.length).sum();
        TOTAL_RECEIVED_DATA.inc_by(len as u64);

        self.sender.send(ctx).map_err(|_| WorkerError::WRITING_TO_CHANNEL_FAIL)?;
        Ok(len)
    }
}