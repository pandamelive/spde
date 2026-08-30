//! 磁盘IO实现

pub mod file_writer;
pub mod resume_bitmap;

pub use file_writer::FileChunkWriter;
pub use resume_bitmap::{bitmap_path_for, ResumeBitmap};
