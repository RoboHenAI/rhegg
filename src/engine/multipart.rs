mod switch;
mod part;
mod source;

pub use switch::*;
pub use part::*;
pub use source::*;

use std::io::{Result as IoResult, Error as IoError, ErrorKind as IoErrorKind, Read, Seek, SeekFrom, Write};

use dhfarm_engine::{Segment, SegmentMeta};
use anyhow::{bail, anyhow, Result};

use crate::Index;

#[derive(PartialEq, Debug)]
pub struct Multipart<'index, 'data, T: Read + Write +Seek> {
    /// Source of the multipart.
    source: Option<Source<'index, 'data, T>>,
    
    /// Parts of the multipart.
    parts: Vec<Part>,
    
    /// Current part index of the multipart.
    current: usize,

    /// Whether to allow growing the segment size (if current segment isn't static).
    allow_grow: bool
}

impl<'index, 'data, T: Read + Write +Seek> Multipart<'index, 'data, T> {
    /// Checks if the multipart is active.
    /// 
    /// # Returns
    /// 
    /// Whether the multipart is active.
    fn is_active(&self) -> bool {
        self.source.is_some()
    }

    /// Calculates the offset of the new part to be appended.
    /// 
    /// # Arguments
    /// 
    /// * `data_index` - Data index to be used by the multipart.
    /// 
    /// # Returns
    /// 
    /// The offset of the new part to be appended.
    fn new_page_append_offset(data_index: &Index) -> u64 {
        let last_page_index = data_index.calc_page_index(data_index.len() - 1);
        let append_page_index = data_index.calc_page_index(data_index.len());

        // add the index page size if appending the new entry requires a new page
        if last_page_index != append_page_index {
            return data_index.get_page_size();
        }

        // no new page, return 0
        0
    }

    /// Creates a new multipart.
    /// 
    /// # Arguments
    /// 
    /// * `data_index` - Data index to be used by the multipart.
    /// * `path` - Path of the file.
    /// * `data` - Data to be used by the multipart.
    pub fn create(data_index: &'index mut Index, path: &str, data: &'data mut T) -> Result<Self> {
        // create new file into the data index
        let offset = data.seek(SeekFrom::End(0))? + Self::new_page_append_offset(data_index);
        let index = data_index.append(crate::FileMeta {
            offset,
            path: path.to_string(),
            file_type: crate::FileType::File,
            size: 0,
            used: 0
        })?;
        data_index.flush(data)?;

        // create the current segment and self
        let segment = Segment::new_unsafe(data, offset, 0, offset, true)?;
        Ok(Self {
            source: Some(Source {
                index: data_index,
                segment
            }),
            parts: vec![Part {
                start: 0,
                index,
                segment_meta: None
            }],
            allow_grow: true,
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
            None => bail!(IoError::new(IoErrorKind::NotFound, "file not found"))
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
                None => bail!(IoError::new(
                    IoErrorKind::NotFound,
                    format!("file partition not found at index {}", next_index)
                ))
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
            source: Some(Source {
                index: data_index,
                segment
            }),
            parts,
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
        // ensure the multipart is active
        if !self.is_active() {
            bail!("multipart is not active");
        }

        // create the new part
        let source = self.source.take().unwrap();
        let (mut segment_meta, data) = source.segment.explode();
        let data_index = source.index;
        let offset = match data.seek(SeekFrom::End(0)) {
            Ok(offset) => offset + Self::new_page_append_offset(data_index),
            Err(e) => {
                self.source = Some(Source {
                    index: data_index,
                    segment: Segment::implode(segment_meta, data)
                });
                return Err(e.into());
            }
        };
        let index = data_index.append(crate::FileMeta {
            offset,
            path: String::default(),
            file_type: crate::FileType::File,
            size: 0,
            used: 0
        })?;
        if let Err(e) = data_index.flush(data) {
            self.source = Some(Source {
                index: data_index,
                segment: Segment::implode(segment_meta, data)
            });
            return Err(e.into());
        }
        if segment_meta.allow_grow {
            segment_meta.allow_grow = false;
        }
        self.parts[self.current].segment_meta = Some(segment_meta);

        // Rust compiler shenanigans don't allow me to handle errors here and restore the segment's data
        // TODO: find a way to handle errors here and restore the segment's data
        let segment = Segment::new(data, offset, 0, true).unwrap();
        let last_start = match self.parts.last() {
            Some(v) => v.start,
            None => return Err(anyhow!("couldn't get the last part! the parts should never be empty"))
        };
        let entry_size = match data_index.get_index(index) {
            Some(v) => v.meta.size,
            None => return Err(anyhow!("couldn't get the entry size! the entry should exist"))
        };
        self.source = Some(Source {
            index: data_index,
            segment
        });
        self.parts.push(Part{
            start: last_start + entry_size,
            index,
            segment_meta: None
        });
        self.current = self.parts.len() - 1;
        self.allow_grow = true;
        Ok(())
    }

    /// Switches to the specified part by position.
    /// **WARNING:** this function assumes the multipart is active, use with caution.
    /// 
    /// # Arguments
    /// 
    /// * `switch` - Switch to the specified part by position.
    fn switch_part(&mut self, switch: SwitchPart) -> IoResult<u64> {
        // switch to the new part
        let source = self.source.as_ref().unwrap();
        let old_pos = source.segment.position() + self.parts[self.current].start;
        let (mut pos, index) = match switch {
            SwitchPart::Pos(pos) => {
                // do nothing if already in the current position
                if pos == old_pos {
                    return Ok(old_pos);
                }

                // get the new index starting position
                (pos, Self::calc_part_index(&self.parts, pos))
            },
            SwitchPart::Index(index) => {
                // do nothing if already in the current index
                if index == self.current {
                    return Ok(old_pos);
                }

                // get the new index starting position
                let pos = self.parts[index].segment_meta.as_ref().unwrap().start;
                (pos, index)
            }
        };

        // save the old segment data and extract the 
        let mut source = self.source.take().unwrap();
        let (old_meta, data) = source.segment.explode();
        let old_pos = old_meta.pos;
        self.parts[self.current].segment_meta = Some(old_meta);

        // switch to the new part and move if the position is different from the old one
        let mut new_meta = self.parts[index].segment_meta.take().unwrap();
        if pos != old_pos {
            pos = data.seek(SeekFrom::Start(pos))?;
        }
        new_meta.pos = pos;
        source.segment = Segment::implode(new_meta, data);
        self.current = index;
        self.allow_grow = self.parts[index].index + 1 == source.index.len();
        self.source = Some(source);
        Ok(pos)
    }
}

impl<'data, 'index, T: Read + Write +Seek> Read for Multipart<'data, 'index, T> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        // ensure multipart is active
        if !self.is_active() {
            return Err(IoError::new(IoErrorKind::Other, "multipart isn't active"));
        }

        // assume segment to always be Some
        let buf_len = buf.len();
        let mut buf = buf;
        let mut remaining = buf_len;
        let last_part_index = self.parts.len() - 1;
        while remaining > 0 {
            let read = self.source.as_mut().unwrap().segment.read(buf)?;
            remaining -= read;
            if remaining > 0 {
                if last_part_index < self.current {
                    break;
                }
                buf = &mut buf[read..];
                let next_index = self.current + 1;
                self.switch_part(SwitchPart::Index(next_index))?;
            }
        }
        Ok(buf_len - remaining)
    }
}

impl<'data, 'index, T: Read + Write +Seek> Write for Multipart<'data, 'index, T> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        // ensure multipart is active
        if !self.is_active() {
            return Err(IoError::new(IoErrorKind::Other, "multipart isn't active"));
        }

        // calc bytes to write within the current segment
        let buf_len = buf.len();
        let mut buf = buf;
        let mut remaining = buf_len;
        let last_part_index = self.parts.len() - 1;

        // start writting
        while remaining > 0 {
            let source = self.source.as_mut().unwrap();
            let written = source.segment.write(buf)?;
            remaining -= written;
            if remaining < 1 {
                break;
            }

            // flush part before moving to the next one
            source.segment.flush()?;

            // use remaining buffer
            buf = &buf[written..];

            // move to the next part and create a new one if the last part is reached
            let next_index = self.current + 1;
            if next_index > last_part_index {
                if let Err(e) = self.add_part() {
                    return Err(IoError::new(
                        IoErrorKind::Other,
                        e.to_string()
                    ));
                }
            }
            if let None = &self.source {
                return Err(IoError::new(
                    IoErrorKind::Other,
                    "multipart isn't active"
                ));
            }
            self.switch_part(SwitchPart::Index(next_index))?;
        }
        Ok(buf_len - remaining)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // segment is always Some
        self.source.as_mut().unwrap().segment.flush()
    }
}

impl<'data, 'index, T: Read + Write +Seek> Seek for Multipart<'data, 'index, T> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        // ensure multipart is active
        if !self.is_active() {
            return Err(IoError::new(IoErrorKind::Other, "multipart isn't active"));
        }

        // calculate the new position from start
        let pos = match pos {
            SeekFrom::Start(pos) => pos,
            SeekFrom::Current(pos_from_current) => {
                let source = self.source.as_ref().unwrap();
                let current = source.segment.real_pos() - source.segment.start_pos();
                let new_pos = current as i128 + pos_from_current as i128;
                if new_pos < 0 {
                    return Err(IoError::new(IoErrorKind::InvalidInput, "invalid seek to a negative or overflowing position"));
                }
                new_pos as u64
            },
            SeekFrom::End(pos_from_end) => {
                // get the new index starting position
                let pos = (self.parts[self.current].segment_meta.as_ref().unwrap().start + self.source.as_ref().unwrap().segment.size()) as i128 + pos_from_end as i128;
                if pos < 0 {
                    return Err(IoError::new(IoErrorKind::InvalidInput, "invalid seek to a negative or overflowing position"));
                }
                let pos = pos as u64;
                pos
            }
        };

        // switch to part and position
        self.switch_part(SwitchPart::Pos(pos))?;
        Ok(pos)
    }
}

#[cfg(test)]
mod tests {
    use serde::de::Expected;

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
        match multipart.source {
            Some(source) => {
                let (meta, extracted_data) = source.segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 0);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1596);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Source should be initialized");
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
        match multipart.source {
            Some(source) => {
                let (meta, extracted_data) = source.segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1601);
                assert_eq!(meta.size, 0);
                assert_eq!(meta.pos, 1601);
                assert_eq!(meta.real_size, 1601);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Source should be initialized");
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
        match multipart.source {
            Some(source) => {
                let (meta, extracted_data) = source.segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 5);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1601);
                assert!(meta.allow_grow, "Segment allow grow should be true");
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Source should be initialized");
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
        match multipart.source {
            Some(source) => {
                let (meta, extracted_data) = source.segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1596);
                assert_eq!(meta.size, 5);
                assert_eq!(meta.pos, 1596);
                assert_eq!(meta.real_size, 1607);
                assert!(!meta.allow_grow);
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Source should be initialized");
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
        match multipart.source {
            Some(source) => {
                let (meta, extracted_data) = source.segment.explode();
                let new_data = extracted_data.clone();
                assert_eq!(meta.start, 1601);
                assert_eq!(meta.size, 6);
                assert_eq!(meta.pos, 1601);
                assert_eq!(meta.real_size, 1607);
                assert!(meta.allow_grow);
                assert_eq!(data, new_data);
            },
            None => {
                assert!(false, "Source should be initialized");
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

    #[test]
    fn test_new_page_append_offset() {
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(2)).unwrap();
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::new_page_append_offset(&data_index), 0);
        data_index.append(crate::FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::new_page_append_offset(&data_index), 0);
        data_index.append(crate::FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::new_page_append_offset(&data_index), data_index.get_page_size());
        data_index.append(crate::FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::new_page_append_offset(&data_index), 0);
        data_index.append(crate::FileMeta { path: "d".to_string(), ..Default::default() }).unwrap();
        assert_eq!(Multipart::<Cursor<Vec<u8>>>::new_page_append_offset(&data_index), data_index.get_page_size());
    }

    #[test]
    fn test_add_part() {
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(2)).unwrap();
        assert_eq!(data.stream_position().unwrap(), 510);
        let path = "testfile";

        // create multipart and its first part
        let mut multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        let mut source = multipart.source.take().unwrap();
        source.segment.write_all(b"I say").unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(meta, SegmentMeta {
            start: 1053,
            pos: 1058,
            size: 5,
            real_size: 1058,
            allow_grow: true
        });
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });
        assert_eq!(multipart.current, 0);

        // add part 1
        if let Err(e) = multipart.add_part() {
            assert!(false, "expected to succeed but failed with error: {}", e);
            return;
        }
        assert_eq!(multipart.parts[0].segment_meta.as_ref().unwrap().allow_grow, false);
        let mut source = multipart.source.take().unwrap();
        source.segment.write_all(b" hello").unwrap();
        source.segment.flush().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(data.stream_position().unwrap(), 1064);
        assert_eq!(meta, SegmentMeta {
            start: 1058,
            pos: 1064,
            size: 6,
            real_size: 1064,
            allow_grow: true
        });
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });

        // add part 2
        multipart.add_part().unwrap();
        assert_eq!(multipart.parts[1].segment_meta.as_ref().unwrap().allow_grow, false);
        let mut source = multipart.source.take().unwrap();
        source.segment.write_all(b" world").unwrap();
        source.segment.flush().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(data.stream_position().unwrap(), 2123);
        assert_eq!(meta, SegmentMeta {
            start: 2117,
            pos: 2123,
            size: 6,
            real_size: 2123,
            allow_grow: true
        });
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });

        // add part 3
        multipart.add_part().unwrap();
        assert_eq!(multipart.parts[2].segment_meta.as_ref().unwrap().allow_grow, false);
        let mut source = multipart.source.take().unwrap();
        source.segment.write_all(b" foo").unwrap();
        source.segment.flush().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(data.stream_position().unwrap(), 2127);
        assert_eq!(meta, SegmentMeta {
            start: 2123,
            pos: 2127,
            size: 4,
            real_size: 2127,
            allow_grow: true
        });
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });
    }

    #[test]
    fn test_switch_part_by_index() {
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();
        assert_eq!(data.stream_position().unwrap(), 510);
        let path = "testfile";

        // create multipart and its first part
        let mut multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        let mut source = multipart.source.take().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(data.stream_position().unwrap(), 1596);
        source.segment = Segment::implode(meta, data);
        multipart.source = Some(source);
        assert_eq!(multipart.current, 0);
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b"I say").unwrap();

        // add part 1
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" hello").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();
        assert_eq!(multipart.current, 1);
        assert!(multipart.allow_grow);

        // add part 2
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" world").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();
        assert_eq!(multipart.current, 2);
        assert!(multipart.allow_grow);

        // add part 3
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" foo").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();
        assert_eq!(multipart.current, 3);
        assert!(multipart.allow_grow);

        // validate part 3 meta before switch
        let source = multipart.source.take().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(meta.start, 1613);
        assert_eq!(meta.size, 4);
        assert_eq!(meta.pos, 1617);
        assert_eq!(meta.real_size, 1617);
        assert!(meta.allow_grow);
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });

        // switch then validate part meta
        multipart.switch_part(SwitchPart::Index(1));
        assert_eq!(multipart.current, 1);
        assert!(!multipart.allow_grow);
        let source = multipart.source.take().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(meta.start, 1601);
        assert_eq!(meta.size, 6);
        assert_eq!(meta.pos, 1601);
        assert_eq!(meta.real_size, 1607);
        assert!(!meta.allow_grow);

        // validate data
        let expected = b"I say hello world foo";
        let mut buf = Vec::new();
        data.seek(SeekFrom::Start(source.index.get_page_size())).unwrap();
        data.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, expected);
        multipart.source = Some(Source {
            index: source.index,
            segment: Segment::implode(meta, data)
        });
    }

    #[test]
    fn test_read() {
        let mut data = Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(2)).unwrap();
        let path = "testfile";

        // create multipart and its first part
        let mut multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        let mut source = multipart.source.take().unwrap();
        let (meta, data) = source.segment.explode();
        assert_eq!(data.stream_position().unwrap(), 1053);
        source.segment = Segment::implode(meta, data);
        multipart.source = Some(source);
        assert_eq!(multipart.current, 0);
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b"I say").unwrap();

        // add part 1
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" hello").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();

        // add part 2
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" world").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();

        // add part 3
        multipart.add_part().unwrap();
        assert_eq!(multipart.source.as_mut().unwrap().segment.stream_position().unwrap(), 0);
        multipart.source.as_mut().unwrap().segment.write_all(b" foo").unwrap();
        multipart.source.as_mut().unwrap().segment.flush().unwrap();

        // switch to part 0
        multipart.switch_part(SwitchPart::Index(0));
        let mut source = multipart.source.take().unwrap();
        let (meta, data) = source.segment.explode();
        source.segment = Segment::implode(meta, data);
        multipart.source = Some(source);

        // validate data
        let expected = b"I say hello world foo";
        let mut buf = vec![0u8; expected.len()];
        let read = match multipart.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(read, expected.len());
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_write() {
        // create index
        let mut data = std::io::Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();
        let parts_start_pos = data.seek(SeekFrom::End(0)).unwrap();

        // create multipart
        let path = "testfile";
        let mut multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // write data
        let data = b"hello";
        let written = match multipart.write(data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(written, data.len());

        // write more data
        let data = b" woaaa";
        let written = match multipart.write(data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(written, data.len());

        // move back 3 bytes
        multipart.source.as_mut().unwrap().segment.flush().unwrap();
        multipart.source.as_mut().unwrap().segment.seek(SeekFrom::Current(-3)).unwrap();
        
        // disable growth for it to create a new part
        multipart.source.as_mut().unwrap().segment.disable_grow();

        // write new data
        let data = b"rld foo";
        let written = match multipart.write(data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(written, data.len());

        // validate data
        assert_eq!(multipart.parts.len(), 2);
        assert_eq!(multipart.current, 1);
        let source = multipart.source.as_ref().unwrap();
        assert_eq!(source.segment.position(), 4);
        assert!(source.segment.is_grow_allowed());
        let source = multipart.source.take().unwrap();
        let (_, data) = source.segment.explode();
        let expected = b"hello world foo";
        let mut buf = vec![0u8; expected.len()];
        let data_len = data.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(data_len, parts_start_pos + expected.len() as u64);
        data.seek(SeekFrom::Start(parts_start_pos)).unwrap();
        let readed = data.read(&mut buf).unwrap();
        assert_eq!(readed, expected.len());
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_seek_from_start() {
        // create index
        let mut data = std::io::Cursor::new(Vec::new());
        let mut data_index = crate::Index::new(&mut data, Some(5)).unwrap();
        let parts_start_pos = data.seek(SeekFrom::End(0)).unwrap();

        // create multipart
        let path = "testfile";
        let mut multipart = match Multipart::create(&mut data_index, path, &mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };

        // write data
        let data = b"hello world foo";
        let written = match multipart.source.as_mut().unwrap().segment.write(data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(written, data.len());

        // seek from start
        let pos = match multipart.seek(SeekFrom::Start(5)) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected to succeed but failed with error: {}", e);
                return;
            }
        };
        assert_eq!(pos, 5);

        // validate real data
        let (_, data) = multipart.source.take().unwrap().segment.explode();
        assert_eq!(data.stream_position().unwrap(), parts_start_pos + 5);
    }
}