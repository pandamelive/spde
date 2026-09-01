//! 磁盘IO实现

pub mod file_writer;
pub mod null_writer;
pub mod resume_bitmap;
pub mod writer_factory;

pub use file_writer::FileChunkWriter;
pub use null_writer::NullChunkWriter;
pub use resume_bitmap::{bitmap_path_for, ResumeBitmap};
pub use writer_factory::{create_writer, MemoryWriter, WriterType};
