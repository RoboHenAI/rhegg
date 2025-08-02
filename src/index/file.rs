use std::io::Write;

use dhfarm_engine::db::field::{Record, Value};
use anyhow::{bail, Result};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum FileType {
    File,
    SymbolicLink,
    HardLink,
    CharacterDevice,
    BlockDevice,
    FIFO,
    Socket,
    Unknown(u8)
}

impl From<u8> for FileType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::File,
            1 => Self::SymbolicLink,
            2 => Self::HardLink,
            3 => Self::CharacterDevice,
            4 => Self::BlockDevice,
            5 => Self::FIFO,
            6 => Self::Socket,
            _ => Self::Unknown(value)
        }
    }
}

impl From<FileType> for u8 {
    fn from(value: FileType) -> Self {
        match value {
            FileType::File => 0,
            FileType::SymbolicLink => 1,
            FileType::HardLink => 2,
            FileType::CharacterDevice => 3,
            FileType::BlockDevice => 4,
            FileType::FIFO => 5,
            FileType::Socket => 6,
            FileType::Unknown(v) => v
        }
    }
}

impl From<FileType> for Value {
    fn from(value: FileType) -> Self {
        Value::from(u8::from(value))
    }
}

impl TryFrom<Value> for FileType {
    type Error = anyhow::Error;
    
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::U8(v) => Ok(Self::from(v)),
            _ => bail!("expected 't' field (file type)")
        }
    }
}

impl TryFrom<&Value> for FileType {
    type Error = anyhow::Error;
    
    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::U8(v) => Ok(Self::from(*v)),
            _ => bail!("expected 't' field (file type)")
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct FileMeta {
    pub offset: u64,
    pub path: String,
    pub file_type: FileType,
    pub size: u64,
    pub used: u64
}

impl FileMeta {
    /// Copies the values from another file meta into this one.
    /// 
    /// # Arguments
    /// 
    /// * `meta`: The file meta to copy from.
    pub fn copy_from(&mut self, meta: &FileMeta) {
        self.offset = meta.offset;
        self.path = meta.path.clone();
        self.file_type = meta.file_type;
        self.size = meta.size;
        self.used = meta.used;
    }

    /// Copies the values from this file meta into a record.
    /// 
    /// # Arguments
    /// 
    /// * `record`: The record to copy into.
    pub fn copy_to_record(&self, record: &mut Record) {
        record.set("o", self.offset.into());
        record.set("n", self.path.as_str().into());
        record.set("t", self.file_type.into());
        record.set("s", self.size.into());
        record.set("u", self.used.into());
    }

    /// Creates a file meta from a record.
    /// 
    /// # Arguments
    /// 
    /// * `record`: The record to create the file meta from.
    pub fn from_record(record: &Record) -> Result<Self> {
        Ok(Self {
            offset: match record.get("o") {
                Some(v) => v.try_into()?,
                None => bail!("expected 'o' field (offset)")
            },
            path: match record.get("n") {
                Some(v) => v.try_into()?,
                None => bail!("expected 'n' field (path)")
            },
            file_type: match record.get("t") {
                Some(v) => v.try_into()?,
                None => bail!("expected 't' field (file type)")
            },
            size: match record.get("s") {
                Some(v) => v.try_into()?,
                None => bail!("expected 's' field (size)")
            },
            used: match record.get("u") {
                Some(v) => v.try_into()?,
                None => bail!("expected 'u' field (used)")
            }
        })
    }

    /// Creates a new file meta.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - The writer to use for creating the file.
    fn create_as_file(self, writer: &mut impl Write) -> Result<()> {
        // write a default file size
        if self.size > 0 {
            let buf = [0u8; 512];
            let blocks = self.size / 512 + if self.size % 512 > 0 { 1 } else { 0 };
            for _ in 0..blocks {
                writer.write_all(&buf)?;
            }
        }
        Ok(())
    }

    /// Creates a new file into writer from meta.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - The writer to use for creating the file.
    pub fn create_file(self, writer: &mut impl Write) -> Result<()> {
        match self.file_type {
            FileType::File => self.create_as_file(writer),
            _ => unimplemented!()
        }
    }
}

impl Default for FileMeta {
    fn default() -> Self {
        Self {
            offset: 0,
            path: String::new(),
            file_type: FileType::Unknown(0),
            size: 0,
            used: 0
        }
    }
}

/// Represents a file partition metadata.
#[derive(Clone, PartialEq, Debug)]
pub struct FilePartition {
    pub prev: usize,
    pub next: usize
}

impl FilePartition {
    /// Returns true if the file is parted.
    pub fn is_parted(&self) -> bool {
        self.next > 0 || self.prev > 0
    }
}

impl Default for FilePartition {
    fn default() -> Self {
        Self {
            prev: 0,
            next: 0
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct FileEntry {
    pub meta: FileMeta,
    pub partition: FilePartition
}

impl FileEntry {
    /// Copies the values from another file entry into this one.
    /// 
    /// # Arguments
    /// 
    /// * `entry`: The file entry to copy from.
    pub fn copy_from(&mut self, entry: &FileEntry) {
        self.meta.copy_from(&entry.meta);
        self.partition.next = entry.partition.next;
        self.partition.prev = entry.partition.prev;
    }

    /// Copies the values from this file entry into a record.
    /// 
    /// # Arguments
    /// 
    /// * `table`: The table to use for creating the record.
    /// * `copy_meta`: Whether to copy the meta values.
    pub fn copy_to_record(&self, record: &mut Record, copy_meta: bool) {
        if copy_meta {
            self.meta.copy_to_record(record);
        }
        record.set("a", (self.partition.next as u8).into());
        record.set("b", (self.partition.prev as u8).into());
    }

    /// Creates a file entry from a record.
    /// 
    /// # Arguments
    /// 
    /// * `record`: The record to create the file entry from.
    pub fn from_record(record: &Record) -> Result<Self> {
        let meta = FileMeta::from_record(record)?;
        let next_part: u8 = match record.get("a") {
            Some(v) => v.try_into()?,
            None => bail!("expected 'a' field (next part)")
        };
        let prev_part: u8 = match record.get("b") {
            Some(v) => v.try_into()?,
            None => bail!("expected 'b' field (prev part)")
        };
        Ok(Self {
            meta,
            partition: FilePartition {
                next: next_part.into(),
                prev: prev_part.into()
            }
        })
    }

    /// Returns true if the file is parted.
    pub fn is_parted(&self) -> bool {
        self.partition.is_parted()
    }
}

impl Default for FileEntry {
    fn default() -> Self {
        Self {
            meta: FileMeta::default(),
            partition: FilePartition::default()
        }
    }
}

#[cfg(test)]
mod test_helper {
    use super::*;
    use dhfarm_engine::db::field::{Record, Value};

    pub fn create_file_meta() -> FileMeta {
        FileMeta {
            offset: 12,
            path: "test".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 5
        }
    }

    pub fn create_file_entry() -> FileEntry {
        FileEntry {
            meta: create_file_meta(),
            partition: FilePartition {
                next: 3,
                prev: 7
            }
        }
    }

    pub fn create_record() -> Record {
        let mut record = Record::new();
        record.add("o", Value::from(12u64)).unwrap();
        record.add("n", Value::from("test")).unwrap();
        record.add("t", Value::from(u8::from(FileType::File))).unwrap();
        record.add("s", Value::from(10u64)).unwrap();
        record.add("u", Value::from(5u64)).unwrap();
        record.add("a", Value::from(3u8)).unwrap();
        record.add("b", Value::from(7u8)).unwrap();
        record
    }
}

#[cfg(test)]
mod tests {
    use dhfarm_engine::db::field::Value;
    use super::test_helper::*;
    use super::*;

    #[test]
    fn file_meta_copy_from() {
        let meta = create_file_meta();
        let mut meta2 = FileMeta::default();
        meta2.copy_from(&meta);
        assert_eq!(meta2, meta);
    }

    #[test]
    fn file_meta_copy_to_record() {
        let meta = create_file_meta();
        let mut record = create_record();
        meta.copy_to_record(&mut record);
        assert_eq!(record.get("o").unwrap(), &Value::from(12u64));
        assert_eq!(record.get("n").unwrap(), &Value::from("test"));
        assert_eq!(record.get("t").unwrap(), &Value::from(FileType::File));
        assert_eq!(record.get("s").unwrap(), &Value::from(10u64));
        assert_eq!(record.get("u").unwrap(), &Value::from(5u64));
    }

    #[test]
    fn file_meta_from_record() {
        let record = create_record();
        let meta = match FileMeta::from_record(&record) {
            Ok(meta) => meta,
            Err(e) => {
                assert!(false, "Failed to create file meta: {}", e);
                return;
            }
        };
        assert_eq!(meta, create_file_meta());
    }

    #[test]
    fn file_entry_copy_from() {
        let entry = create_file_entry();
        let mut entry2 = FileEntry::default();
        entry2.copy_from(&entry);
        assert_eq!(entry2, entry);
    }

    #[test]
    fn file_entry_copy_to_record() {
        let entry = create_file_entry();
        let mut record = create_record();
        entry.copy_to_record(&mut record, true);
        assert_eq!(record.get("o").unwrap(), &Value::from(12u64));
        assert_eq!(record.get("n").unwrap(), &Value::from("test"));
        assert_eq!(record.get("t").unwrap(), &Value::from(FileType::File));
        assert_eq!(record.get("s").unwrap(), &Value::from(10u64));
        assert_eq!(record.get("u").unwrap(), &Value::from(5u64));
        assert_eq!(record.get("a").unwrap(), &Value::from(3u8));
        assert_eq!(record.get("b").unwrap(), &Value::from(7u8));
    }

    #[test]
    fn file_entry_from_record() {
        let record = create_record();
        let entry = match FileEntry::from_record(&record) {
            Ok(entry) => entry,
            Err(e) => {
                assert!(false, "Failed to create file entry: {}", e);
                return;
            }
        };
        assert_eq!(entry, create_file_entry());
    }

    #[test]
    fn create_as_file() {
        let mut data = Vec::new();
        let meta = FileMeta {
            offset: 12,
            path: "/test".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 5
        };
        if let Err(e) = meta.create_file(&mut data) {
            assert!(false, "Failed to create file: {}", e);
            return;
        }
        assert_eq!(data.len(), 512);
        assert_eq!(data, vec![0u8; 512]);
    }

    #[test]
    fn file_partition_is_parted() {
        let mut partition = FilePartition::default();
        assert!(!partition.is_parted());
        partition.next = 1;
        assert!(partition.is_parted());
        partition.next = 0;
        partition.prev = 1;
        assert!(partition.is_parted());
        partition.prev = 2;
        assert!(partition.is_parted());
    }

    #[test]
    fn file_entry_is_parted() {
        let mut entry = FileEntry {
            meta: FileMeta::default(),
            partition: FilePartition::default()
        };
        assert!(!entry.is_parted());
        entry.partition.next = 1;
        assert!(entry.is_parted());
        entry.partition.next = 0;
        entry.partition.prev = 1;
        assert!(entry.is_parted());
        entry.partition.prev = 2;
        assert!(entry.is_parted());
    }
}