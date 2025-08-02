use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Seek, SeekFrom, Write};

use dhfarm_engine::{Segment, SegmentMeta};
use anyhow::{bail, Result};

use crate::Index;

#[derive(PartialEq, Clone, Debug)]
pub struct Part {
    pub start: u64,
    pub index: usize,
    pub segment_meta: Option<SegmentMeta>
}

#[derive(PartialEq, Debug)]
pub struct Multipart<'index, 'data, T: Read + Write +Seek> {
    /// Data index to be used by the multipart.
    data_index: &'index mut Index,
    
    /// Parts of the multipart.
    parts: Vec<Part>,
    
    /// Current segment of the multipart.
    segment: Option<Segment<'data, T>>,
    
    /// Current part index of the multipart.
    current: usize,

    /// Whether to allow growing the segment size (if current segment isn't static).
    allow_grow: bool
}

impl<'index, 'data, T: Read + Write +Seek> Multipart<'index, 'data, T> {
    /// Creates a new multipart.
    /// 
    /// # Arguments
    /// 
    /// * `data_index` - Data index to be used by the multipart.
    /// * `path` - Path of the file.
    /// * `data` - Data to be used by the multipart.
    pub fn create(data_index: &'index mut Index, path: &str, data: &'data mut T) -> Result<Self> {
        // create new file into the data index
        let offset = data.seek(SeekFrom::End(0))?;
        let index = data_index.append(crate::FileMeta {
            offset,
            path: path.to_string(),
            file_type: crate::FileType::File,
            size: 0,
            used: 0
        })?;
        data_index.flush(data)?;

        // create the current segment and self
        let segment = Segment::new(data, offset, 0, true)?;
        Ok(Self {
            data_index,
            parts: vec![Part {
                start: 0,
                index,
                segment_meta: None
            }],
            allow_grow: true,
            segment: Some(segment),
            current: 0
        })
    }

    /// Calculates the part index based on the position.
    /// 
    /// # Arguments
    /// 
    /// * `parts` - Parts of the multipart.
    /// * `pos` - Position to calculate the part index for.
    pub fn calc_part_index(parts: &Vec<Part>, pos: u64) -> usize {
        for i in (0..parts.len()).rev() {
            if parts[i].start <= pos {
                return i;
            }
        }
        0
    }

    /// Opens an existing multipart.
    /// 
    /// # Arguments
    /// 
    /// * `data_index` - Data index to be used by the multipart.
    /// * `path` - Path of the file.
    /// * `pos` - Position to open the multipart at.
    /// * `data` - Data to be used by the multipart.
    pub fn open(data_index: &'index mut Index, path: &str, pos: u64, data: &'data mut T) -> Result<Self> {
        let (index, mut entry) = match data_index.get(path) {
            Some(v) => v,
            None => return Err(anyhow::anyhow!(IoError::new(IoErrorKind::NotFound, "file not found")))
        };

        // detect file parts
        let last_entry_index = data_index.len() - 1;
        let last_entry = match data_index.get_index(last_entry_index) {
            Some(v) => v,
            None => bail!("last entry not found")
        };
        let real_size = last_entry.meta.offset + last_entry.meta.size;
        let mut parts = vec![Part{
            start: 0,
            index,
            segment_meta: Some(SegmentMeta {
                start: entry.meta.offset,
                size: entry.meta.size,
                pos: entry.meta.offset,
                real_size,
                allow_grow: last_entry_index == index
            })
        }];
        let mut prev_pos = entry.meta.offset;
        let mut next_index = entry.partition.next;
        while next_index > 0 {
            entry = match data_index.get_index(next_index) {
                Some(v) => v,
                None => return Err(anyhow::anyhow!(IoError::new(
                    IoErrorKind::NotFound,
                    format!("file partition not found at index {}", next_index)
                )))
            };
            parts.push(Part{
                start: entry.meta.offset - prev_pos,
                index: next_index,
                segment_meta: Some(SegmentMeta {
                    start: entry.meta.offset,
                    size: entry.meta.size,
                    pos: entry.meta.offset,
                    real_size,
                    allow_grow: last_entry_index == next_index
                })
            });
            prev_pos = entry.meta.offset;
            next_index = entry.partition.next;
        }

        // calculate the current part index based on the position
        let current = Self::calc_part_index(&parts, pos);

        // assume the last entry can grow then create the segment
        let allow_grow = last_entry_index == parts[current].index;
        let segment_meta = match parts[current].segment_meta.take() {
            Some(v) => v,
            None => bail!("segment meta not found")
        };
        data.seek(SeekFrom::Start(segment_meta.pos))?;
        let segment = Segment::implode(segment_meta, data);
        Ok(Self {
            data_index,
            parts,
            segment: Some(segment),
            current,
            allow_grow
        })
    }

    /// Adds a new part to the multipart.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the multipart.
    pub fn add_part(&mut self) -> Result<()> {
        // create the new part
        let (segment_meta, data) = self.segment.take().unwrap().explode();
        let offset = match data.seek(SeekFrom::End(0)) {
            Ok(offset) => offset,
            Err(e) => {
                self.segment = Some(Segment::implode(segment_meta, data));
                return Err(e.into());
            }
        };
        let index = self.data_index.append(crate::FileMeta {
            offset,
            path: String::default(),
            file_type: crate::FileType::File,
            size: 0,
            used: 0
        })?;
        if let Err(e) = self.data_index.flush(data) {
            self.segment = Some(Segment::implode(segment_meta, data));
            return Err(e.into());
        }

        // Rust shenanigans don't allow me to handle errors here and restore the segment's data
        self.segment = Some(Segment::new(data, offset, 0, true).unwrap());
        let last_start = match self.parts.last() {
            Some(v) => v.start,
            None => bail!("couldn't get the last part! the parts should never be empty")
        };
        let entry_size = match self.data_index.get_index(index) {
            Some(v) => v.meta.size,
            None => bail!("couldn't get the entry size! the entry should exist")
        };
        self.parts.push(Part{
            start: last_start + entry_size,
            index,
            segment_meta: None
        });
        self.current = self.parts.len() - 1;
        self.allow_grow = true;
        Ok(())
    }
}

impl<'data, 'index, T: Read + Write +Seek> Read for Multipart<'data, 'index, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // segment is always Some
        self.segment.as_mut().unwrap().read(buf)
        // TODO: handle end of file as next part if any
    }
}

impl<'data, 'index, T: Read + Write +Seek> Write for Multipart<'data, 'index, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // segment is always Some
        self.segment.as_mut().unwrap().write(buf)
        // TODO: handle end of file as next part if any
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // segment is always Some
        self.segment.as_mut().unwrap().flush()
    }
}

impl<'data, 'index, T: Read + Write +Seek> Seek for Multipart<'data, 'index, T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // segment is always Some
        self.segment.as_mut().unwrap().seek(pos)
        // TODO: handle file switching and segment meta position update on implode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_create_success() {
        // create index
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();

        // create multipart
        let path = "testfile";
        let multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // validate
        assert_eq!(multipart.parts.len(), 1);
        assert_eq!(multipart.current, 0);
        assert!(multipart.allow_grow);
        assert_eq!(multipart.parts[0].index, 1);
        assert_eq!(multipart.parts[0].start, 0);
        assert!(multipart.parts[0].segment_meta.is_none());
        match multipart.segment {
            Some(segment) => {
                let (meta, extracted_data) = segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 0);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1596);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Segment should be initialized");
                return;
            }
        }
    }

    #[test]
    fn test_create_with_nonempty_data() {
        // create index
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();
        data.seek(SeekFrom::End(0)).unwrap();
        data.write_all(b"hello").unwrap();
        data.flush().unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();

        // create multipart
        let path = "testfile";
        let multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // validate
        assert_eq!(multipart.parts.len(), 1);
        assert_eq!(multipart.current, 0);
        assert!(multipart.allow_grow);
        assert_eq!(multipart.parts[0].index, 1);
        assert_eq!(multipart.parts[0].start, 0);
        assert!(multipart.parts[0].segment_meta.is_none());
        match multipart.segment {
            Some(segment) => {
                let (meta, extracted_data) = segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1601);
                assert_eq!(meta.size, 0);
                assert_eq!(meta.pos, 1601);
                assert_eq!(meta.real_size, 1601);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Segment should be initialized");
                return;
            }
        }
    }

    #[test]
    fn test_open_single_part() {
        // create index
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();

        // write data to the cursor and create file into the index
        data.seek(SeekFrom::End(0)).unwrap();
        let offset = data.stream_position().unwrap();
        data.write_all(b"hello").unwrap();
        data.flush().unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        let path = "testfile";
        let index = data_index.append(crate::FileMeta {
            offset,
            path: path.to_string(),
            file_type: crate::FileType::File,
            size: 5,
            used: 5
        }).unwrap();
        data_index.flush(&mut data).unwrap();


        // create multipart
        let multipart = match Multipart::open(&mut data_index, path, 0, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // validate
        assert_eq!(multipart.parts.len(), 1);
        assert_eq!(multipart.current, 0);
        assert!(multipart.allow_grow);
        assert_eq!(multipart.parts[0].index, index);
        assert_eq!(multipart.parts[0].start, 0);
        assert!(multipart.parts[0].segment_meta.is_none());
        match multipart.segment {
            Some(segment) => {
                let (meta, extracted_data) = segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 5);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1601);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Segment should be initialized");
                return;
            }
        }
    }

    #[test]
    fn test_open_multiple_parts() {
        // create index
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();

        // write the first part and create file into the index
        data.seek(SeekFrom::End(0)).unwrap();
        let offset = data.stream_position().unwrap();
        data.write_all(b"hello").unwrap();
        data.flush().unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        let path = "testfile";
        let first_index = data_index.append(crate::FileMeta {
            offset,
            path: path.to_string(),
            file_type: crate::FileType::File,
            size: 5,
            used: 5
        }).unwrap();

        // write a second part and add it to the index
        data.seek(SeekFrom::End(0)).unwrap();
        let offset = data.stream_position().unwrap();
        data.write_all(b" world").unwrap();
        data.flush().unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        let second_index = data_index.append(crate::FileMeta {
            offset,
            path: String::default(),
            file_type: crate::FileType::File,
            size: 6,
            used: 6
        }).unwrap();
        data_index.link_next(first_index, second_index).unwrap();
        data_index.flush(&mut data).unwrap();

        // open multipart and move to the first part
        let multipart = match Multipart::open(&mut data_index, path, 3, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // validate first part
        assert_eq!(multipart.parts.len(), 2);
        assert_eq!(multipart.current, 0);
        assert!(!multipart.allow_grow);
        assert_eq!(multipart.parts[0].index, first_index);
        assert_eq!(multipart.parts[0].start, 0);
        assert!(multipart.parts[0].segment_meta.is_none());
        match multipart.segment {
            Some(segment) => {
                let (meta, extracted_data) = segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 5);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1607);
                assert!(!meta.allow_grow);
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Segment should be initialized");
                return;
            }
        }

        // open multipart and move to the second part
        let multipart = match Multipart::open(&mut data_index, path, 8, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // validate second part
        assert_eq!(multipart.parts.len(), 2);
        assert_eq!(multipart.current, 1);
        assert!(multipart.allow_grow);
        assert_eq!(multipart.parts[1].index, second_index);
        assert_eq!(multipart.parts[1].start, 5);
        assert!(multipart.parts[1].segment_meta.is_none());
        match multipart.segment {
            Some(segment) => {
                let (meta, extracted_data) = segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1601);
                assert_eq!(meta.size, 6);
                assert_eq!(meta.pos, 1601);
                assert_eq!(meta.real_size, 1607);
                assert!(meta.allow_grow);
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Segment should be initialized");
                return;
            }
        }
    }

    #[test]
    fn test_open_not_found() {
        use std::io::Cursor;
        let mut data = Cursor::new(Vec::new());
        let mut index = crate::Index::new(&mut data, Some(5)).unwrap();
        let path = "nonexistent";
        match Multipart::open(&mut index, path, 0, &mut data) {
            Ok(_) => {
                assert!(false, "Opening a nonexistent file should return an error");
            },
            Err(e) => {
                match e.downcast::<std::io::Error>() {
                    Ok(e) => {
                        assert!(e.kind() == std::io::ErrorKind::NotFound, "expected 'not found' error but got: {}", e);
                    },
                    Err(e) => {
                        assert!(false, "expected 'not found' error but got: {}", e);
                    }
                }
            }
        }
    }

    #[test]
    fn test_calc_part_index_basic() {
        let parts = vec![
            Part { start: 0, index: 1, segment_meta: None },
            Part { start: 100, index: 2, segment_meta: None },
            Part { start: 200, index: 3, segment_meta: None },
        ];
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 0), 0);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 99), 0);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 100), 1);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 150), 1);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 199), 1);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 200), 2);
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 250), 2);
    }

    #[test]
    fn test_calc_part_index_out_of_bounds() {
        let parts = vec![
            Part { start: 0, index: 1, segment_meta: None },
            Part { start: 100, index: 2, segment_meta: None },
        ];
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 300), 1);
    }

    #[test]
    fn test_calc_part_index_empty_parts() {
        let parts: Vec<Part> = vec![];
        Multipart::<Cursor<Vec<u8>>>::calc_part_index(&parts, 0);
    }
}