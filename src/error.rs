use thiserror::Error;

#[derive(Debug, Error)]
pub enum BPlusTreeError {
    #[error("No data recieved.")]
    BPlusTreeBuildError,
}