use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum BPlusTreeError {
    #[error("No data recieved.")]
    BPlusTreeBuildError,
    #[error("Insertion failed.")]
    BPlusTreeInsertError,
}