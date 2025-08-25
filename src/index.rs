mod file;
mod page;

pub use file::*;
pub use page::*;

use anyhow::{bail, Result};
use std::collections::{BTreeSet, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use dhfarm_engine::db::field::Record;
use dhfarm_engine::db::table::traits::TableTrait;
use dhfarm_engine::Segment;
use radix_trie::Trie;

pub const INTERNAL_PATH_SEPARATOR: char = '/';

/// Index structure.
#[derive(PartialEq, Clone, Debug)]
pub struct Index {
    /// Pages.
    pub pages: Vec<Page>,

    /// Files in the page.
    entries: Vec<FileEntry>,

    /// Path tree.
    path_tree: Trie<String, usize>,

    /// Modified entries.
    modified: BTreeSet<usize>,

    /// Records per page ignoring the first record used for pointing to the next page.
    records_per_page: u8,

    /// Page size.
    page_size: u64,

    /// Empty record used to erase entries from the page.
    empty_record: Record
}

impl Index {
    /// Creates an index instance with a single page.
    /// 
    /// # Arguments
    /// 
    /// * `data` - The data to use for creating the index.
    /// * `records_per_page` - The number of records per page.
    pub fn new(data: &mut (impl Read + Seek + Write), records_per_page: Option<u8>) -> Result<Self> {
        let offset = data.stream_position()?;

        // create an empty entry as the first entry to ease the next_part and prev_part management
        let mut entries = Vec::new();
        entries.push(FileEntry::default());

        // add an empty record to store the next page offset
        let records_per_page = match records_per_page {
            Some(v) => Some((v as u64) + 1),
            None => None
        };

        // create an empty page as the first page
        let mut page = Page::new(data, records_per_page)?;
        page.offset = offset;
        let empty_record = page.table.header.record.new_record()?;
        let real_records_per_page = (page.table.header.meta.record_count - 1) as u8;
        let page_size = page.calc_page_size();
        Ok(Self {
            pages: vec![page],
            entries,
            path_tree: Trie::new(),
            modified: BTreeSet::new(),
            records_per_page: real_records_per_page,
            page_size,
            empty_record
        })
    }

    /// Gets the page size.
    pub fn get_page_size(&self) -> u64 {
        self.page_size
    }

    /// Gets the records per page.
    pub fn get_records_per_page(&self) -> u8 {
        self.records_per_page
    }

    /// Reads an index file and loads all pages and entries into memory.
    ///
    /// # Arguments
    ///
    /// * `data`: The data to read the index file from.
    pub fn load(data: &mut (impl Read + Seek + Write)) -> Result<Self> {
        let mut offset= data.stream_position()?;
        let mut pages = Vec::new();
        let mut entries = vec![FileEntry::default()];
        let mut path_tree = Trie::new();

        // read first page and take its records count as records per page
        let mut page = Page::load(data)?;
        let empty_record = page.table.header.record.new_record()?;
        let records_per_page = (page.table.header.meta.record_count - 1) as u8;
        let page_size = page.calc_page_size();
        let mut segment = Segment::new_unsafe(data, offset, page_size, offset + page_size, false)?;

        // read pages
        loop {
            // record page offset
            page.offset = offset;

            // add page records to the index
            let iter = page.iter(&mut segment)?;
            let mut is_first = true;
            for record in iter {
                // handle the first entry, this one contains the offset of the next page
                if is_first {
                    offset = match record.get("o") {
                        Some(v) => v.try_into()?,
                        None => bail!("expected record 0 to contain 'o' field (offset)")
                    };
                    is_first = false;
                    continue;
                }

                // handle the other entries
                let entry = FileEntry::from_record(&record)?;
                if entry.meta.offset < 1 {
                    // exit whenever the offset is 0, this will mark us the first empty record
                    break;
                }

                // add entry to the path tree and entries vector
                path_tree.insert(entry.meta.path.clone(), entries.len());
                entries.push(entry);
            }

            // save page
            pages.push(page);

            // exit when offset is 0
            if offset < 1 {
                break;
            }

            // load next page
            segment = Segment::new_unsafe(data, offset, page_size, offset + page_size, false)?;
            page = match Page::load(&mut segment) {
                Ok(v) => v,
                Err(_) => {
                    // exit as error when the index positions are corrupted
                    bail!("page not found, the index is corrupted, please fallback to scan mode");
                },
            };
        }

        Ok(Self{
            pages,
            entries,
            path_tree,
            modified: BTreeSet::new(),
            records_per_page,
            page_size,
            empty_record
        })
    }

    /// Adds a new page to the index.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Stream to write the page into.
    pub fn add_page(&mut self, data: &mut (impl Read + Seek + Write)) -> Result<&mut Page> {
        // save new page
        let new_page_offset = data.seek(SeekFrom::End(0))?;
        let mut segment = Segment::new_unsafe(data, new_page_offset, self.page_size, new_page_offset, false)?;
        let mut page = Page::new(&mut segment, Some(self.records_per_page as u64 + 1))?;
        page.offset = new_page_offset;
        data.flush()?;

        // update the last page to point to the new page
        let page_count = self.pages.len();
        if page_count > 0 {
            let last_page = &mut self.pages[page_count - 1];
            let mut record = last_page.table.header.record.new_record()?;
            record.set("o", new_page_offset.into());
            let mut last_segment = Segment::new_unsafe(data, last_page.offset, self.page_size, last_page.offset + self.page_size, false)?;
            last_page.table.save_record_into(&mut last_segment, 0, &record)?;
            data.flush()?;
        }

        // save new page into the page array
        self.pages.push(page);
        Ok(self.pages.last_mut().unwrap())
    }

    /// Calculates the page index for a given entry index.
    /// 
    /// # Arguments
    /// 
    /// * `entry_index` - The index of the entry.
    pub fn calc_page_index(&self, entry_index: usize) -> usize {
        if entry_index < 1 {
            return 0;
        }
        (entry_index - 1) / self.records_per_page as usize
    }

    /// Gets the number of entries in the page.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Remove an entry from the page.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to remove.
    pub fn remove(&mut self, index: usize) -> Result<FileEntry> {
        // validate index
        if index < 1 {
            bail!("first entry is a placeholder and can't be removed");
        }
        let len = self.entries.len();
        if !(index < len) {
            bail!("index out of bounds");
        }

        // validate the entry is not parted
        if self.entries[index].is_parted() {
            bail!("can't remove a parted entry, must unlink it first");
        }

        // rearrange when entry to be removed is not the last one
        let last_index = len - 1;
        if index < last_index {
            let entry = self.entries.swap_remove(index);
            self.modified.insert(index);
            self.modified.insert(last_index);
            self.path_tree.remove(&entry.meta.path);
            match self.path_tree.get_mut(&self.entries[index].meta.path) {
                Some(v) => {
                    *v = index;
                },
                None => {
                    bail!("entry not found, this shouldn't happen!");
                }
            }
            return Ok(entry)
        }

        // remove when last entry
        let entry = match self.entries.pop() {
            Some(v) => v,
            None => bail!("entry not found, this shouldn't happen!")
        };
        self.modified.insert(index);
        self.path_tree.remove(&entry.meta.path);
        Ok(entry)
    }

    /// Calculates the table index for a given entry index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry.
    pub fn calc_table_index(&self, index: usize) -> u64 {
        if index == 0 {
            return 0;
        }
        ((index as u64 - 1) % self.records_per_page as u64) + 1
    }

    /// Flushes the modified entries to the writer.
    /// 
    /// # Arguments
    /// 
    /// * `data` - The data to use for writing the page.
    pub fn flush(&mut self, data: &mut (impl Read + Seek + Write)) -> Result<()> {
        // exit when no modified entries
        if self.modified.is_empty() {
            return Ok(());
        }

        // create missing pages
        let pages_count = self.pages.len();
        let total_pages = self.calc_page_index(self.entries.len() - 1) + 1;
        if pages_count < total_pages {
            for _ in 0..(total_pages - pages_count) {
                self.add_page(data)?;
            }
        }

        // grab the first modified page segment
        let mut last_index = 0;
        let mut current_page = self.calc_page_index(*self.modified.first().unwrap_or(&0));
        let mut segment = Segment::new_unsafe(data, self.pages[current_page].offset, self.page_size, self.pages[current_page].offset + self.page_size, false)?;
        let length = self.entries.len();
        for index in self.modified.iter() { // MUST BE SORTED
            // grab the next page when index moves to a different page
            let index = *index;
            let page_index = self.calc_page_index(index);
            if page_index != current_page {
                data.flush()?;
                segment = Segment::new_unsafe(data, self.pages[page_index].offset, self.page_size, self.pages[page_index].offset + self.page_size, false)?;
                current_page = page_index;
            }

            // flush when the next index is not consecutive, then record the last_index
            if index != last_index + 1 {
                segment.flush()?;
            }
            last_index = index;

            // update existing entries
            if index < length {
                let table_index = self.calc_table_index(index);
                let mut record = self.empty_record.clone();
                self.entries[index].copy_to_record(&mut record, true);
                self.pages[page_index].table.save_record_into(&mut segment, table_index, &record)?;
                continue
            }

            // handle soft deleted entries
            let table_index = self.calc_table_index(index);
            self.pages[page_index].table.save_record_into(&mut segment, table_index, &self.empty_record)?;
        }
        data.flush()?;
        Ok(())
    }

    /// Appends a file entry to the page.
    /// 
    /// # Arguments
    /// 
    /// * `meta` - The meta to append.
    pub fn append(&mut self, meta: FileMeta) -> Result<usize> {
        // check if entry already exists
        if self.path_tree.get(&meta.path).is_some() {
            return Err(anyhow::anyhow!("file entry already exists"));
        }

        // create file entry
        let new_index = self.entries.len();
        let entry = FileEntry {
            meta: meta,
            partition: FilePartition::default()
        };

        // add entry
        if !entry.meta.path.is_empty() {
            // avoid empty paths, these are meant to be partitions
            self.path_tree.insert(entry.meta.path.clone(), new_index);
        }
        self.entries.push(entry);
        self.modified.insert(new_index);
        Ok(new_index)
    }

    /// Links an entry to a target entry as next part and will return the old next part index.
    /// If the next part is 0, then the entry will be unlinked from its next part.
    /// If the next part already has a previous part, then it will error. Only one entry can
    /// linked at a time, you must unlink using unlink_prev(next_part) first.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to link.
    /// * `next_part` - The next part index.
    pub fn link_next(&mut self, index: usize, next_part: usize) -> Result<usize> {
        // validate next_part is within the entries range
        if !(next_part < self.entries.len()) {
            bail!("next part out of bounds");
        }

        // unlink next when next_part is 0
        if next_part < 1 {
            self.unlink_next(index)?;
        }

        // validate next index to not be the same
        let old_next = self.entries[index].partition.next;
        if old_next == next_part {
            return Ok(old_next)
        }

        // check that the next entry doesn't have a previous part already
        let next_part_prev = self.entries[next_part].partition.prev;
        if next_part_prev > 0 && next_part_prev != index {
            bail!("next part already has a previous part, you must unlink it first");
        }

        // check for loops
        let mut trace = HashSet::new();
        trace.insert(index);
        let mut next_index = next_part;
        while next_index != 0 {
            if !trace.insert(next_index) {
                bail!("loop detected");
            }
            next_index = self.entries[next_index].partition.next;
        }

        // unlink entry's next part if any, this will add the entry to the modified set
        if old_next > 0 {
            self.unlink_next(index)?;
        }

        // update entry next part
        self.entries[index].partition.next = next_part;
        self.entries[next_part].partition.prev = index;
        self.modified.insert(index);
        self.modified.insert(next_part);
        Ok(old_next)
    }

    /// Links an entry to a target entry as previous part and will return the old previous part index.
    /// If the previous part is 0, then the entry will be unlinked from its previous part.
    /// If the previous part already has a next part, then it will error. Only one entry can
    /// linked at a time, you must unlink using unlink_next(prev_part) first.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to link.
    /// * `prev_part` - The previous part index.
    pub fn link_prev(&mut self, index: usize, prev_part: usize) -> Result<usize> {
        // validate prev_part is within the entries range
        if !(prev_part < self.entries.len()) {
            bail!("prev part out of bounds");
        }

        // unlink prev when prev_part is 0
        if prev_part < 1 {
            self.unlink_prev(index)?;
        }

        // validate prev index to not be the same
        let old_prev = self.entries[index].partition.prev;
        if old_prev == prev_part {
            return Ok(old_prev)
        }

        // check that the previous entry doesn't have a next part already
        let prev_part_next = self.entries[prev_part].partition.next;
        if prev_part_next > 0 && prev_part_next != index {
            bail!("previous part already has a next part, you must unlink it first");
        }

        // check for loops
        let mut trace = HashSet::new();
        trace.insert(index);
        let mut prev_index = prev_part;
        while prev_index != 0 {
            if !trace.insert(prev_index) {
                bail!("loop detected");
            }
            prev_index = self.entries[prev_index].partition.prev;
        }

        // unlink entry's prev part if any, this will add the entry to the modified set
        if old_prev > 0 {
            self.unlink_prev(index)?;
        }

        // update entry prev part
        self.entries[index].partition.prev = prev_part;
        self.entries[prev_part].partition.next = index;
        self.modified.insert(index);
        self.modified.insert(prev_part);
        Ok(old_prev)
    }

    /// Links an entry to other entries as previous and next parts.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to link.
    /// * `prev_part` - The previous part index.
    /// * `next_part` - The next part index.
    /// 
    /// # Returns
    /// 
    /// * Ok(old_prev_part, old_next_part) - A tuple with the old previous and next parts.
    pub fn link(&mut self, index: usize, prev_part: usize, next_part: usize) -> Result<(usize, usize)> {
        let old_prev_part = self.link_prev(index, prev_part)?;
        let old_next_part = self.link_next(index, next_part)?;
        Ok((old_prev_part, old_next_part))
    }

    /// Unlinks an entry from its next part.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to unlink.
    pub fn unlink_next(&mut self, index: usize) -> Result<usize> {
        // exit if the entry doesn't have a next part
        let next_part = self.entries[index].partition.next;
        if next_part < 1 {
            return Ok(0)
        }

        // handle next part unlink
        self.entries[index].partition.next = 0;
        self.entries[next_part].partition.prev = 0;
        self.modified.insert(index);
        self.modified.insert(next_part);
        Ok(next_part)
    }

    /// Unlinks an entry from its previous part.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to unlink.
    pub fn unlink_prev(&mut self, index: usize) -> Result<usize> {
        // exit if the entry doesn't have a previous part
        let prev_part = self.entries[index].partition.prev;
        if prev_part < 1 {
            return Ok(0)
        }

        // handle prev part unlink
        self.entries[index].partition.prev = 0;
        self.entries[prev_part].partition.next = 0;
        self.modified.insert(index);
        self.modified.insert(prev_part);
        Ok(prev_part)
    }

    /// Unlinks an entry from its previous and next parts if any.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to unlink.
    /// * `link_parts` - Whether to link the previous and next parts together.
    /// 
    /// # Returns
    /// 
    /// * Ok(prev_part, next_part) - A tuple of the unlinked previous and next parts.
    pub fn unlink(&mut self, index: usize, link_parts: bool) -> Result<(usize, usize)> {
        let prev_part = self.unlink_prev(index)?;
        let next_part = self.unlink_next(index)?;
        if link_parts {
            self.entries[prev_part].partition.next = next_part;
            self.entries[next_part].partition.prev = prev_part;
        }
        Ok((prev_part, next_part))
    }

    /// Gets an entry by path.
    /// 
    /// # Arguments
    /// 
    /// * `path` - The path of the entry to get.
    /// 
    /// # Returns
    /// 
    /// * `Option<(usize, &FileEntry)>` - The entry and its index if found, otherwise None.
    pub fn get(&self, path: &str) -> Option<(usize, &FileEntry)> {
        match self.path_tree.get(path) {
            Some(&index) => match self.entries.get(index) {
                Some(entry) => Some((index, entry)),
                None => None
            },
            None => None
        }
    }

    /// Gets a mutable entry by path.
    /// 
    /// # Arguments
    /// 
    /// * `path` - The path of the entry to get.
    /// 
    /// # Returns
    /// 
    /// * `Option<&mut FileEntry>` - The entry and its index if found, otherwise None.
    pub fn get_mut(&mut self, path: &str) -> Option<(usize, &mut FileEntry)> {
        match self.path_tree.get(path) {
            Some(&index) => match self.entries.get_mut(index) {
                Some(entry) => Some((index, entry)),
                None => None
            },
            None => None
        }
    }

    /// Gets an entry by index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to get.
    /// 
    /// # Returns
    /// 
    /// * `Option<&FileEntry>` - The entry if found, otherwise None.
    pub fn get_index(&self, index: usize) -> Option<&FileEntry> {
        self.entries.get(index)
    }

    /// Gets a mutable entry by index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to get.
    /// 
    /// # Returns
    /// 
    /// * `Option<&mut FileEntry>` - The entry if found, otherwise None.
    pub fn get_index_mut(&mut self, index: usize) -> Option<&mut FileEntry> {
        self.entries.get_mut(index)
    }

    /// Mark an entry as modified, so it can be flushed when flush is called.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The index of the entry to save.
    pub fn save_entry(&mut self, index: usize) -> Result<()> {
        if index < self.entries.len() {
            self.modified.insert(index);
            return Ok(())
        }
        bail!("index out of bounds");
    }
}

#[cfg(test)]
mod test_helper {
    use crate::index::file::FileType;

    use super::*;

    pub fn create_index(data: &mut (impl Read + Seek + Write)) -> Result<Index> {
        let mut index = Index::new(data, None)?;
        index.append(FileMeta{
            offset: 10,
            path: "/path/to/recordA.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();
        index.append(FileMeta{
            offset: 20,
            path: "/path/to/recordB.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();
        index.append(FileMeta{
            offset: 30,
            path: "/path/to/recordC.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();
        index.append(FileMeta{
            offset: 40,
            path: "/path/to/recordD.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::index::file::FileType;
    use crate::index::page::DEFAULT_RECORD_COUNT;

    use super::test_helper::*;
    use super::*;
    use radix_trie::TrieCommon;
    use std::io::Cursor;

    #[test]
    fn new() {
        let mut data = Cursor::new(Vec::new());
        let index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.entries.len());
        assert_eq!(FileEntry::default(), index.entries[0]);
        assert_eq!(1, index.pages.len());
        assert_eq!(51, index.pages[0].table.header.meta.record_count);
        assert_eq!(50, index.records_per_page);
        data.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(index.page_size, data.stream_position().unwrap());
        assert_eq!(9741, index.page_size);
    }

    #[test]
    fn get_records_per_page() {
        // test default record count
        let mut data = Cursor::new(Vec::new());
        let index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(DEFAULT_RECORD_COUNT - 1, index.records_per_page as u64);

        // test custom record count
        let mut data = Cursor::new(Vec::new());
        let index = match Index::new(&mut data, Some(10)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(10, index.records_per_page);
    }

    #[test]
    fn load() {
        // create entries
        let mut data = Cursor::new(Vec::new());
        let mut expected_index = match Index::new(&mut data, Some(3)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };

        // insert record A
        let meta_a = FileMeta{
            path: "/path/to/recordA".to_string(),
            offset: 10,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        expected_index.append(meta_a.clone()).unwrap();

        // insert record B
        let meta_b = FileMeta{
            path: "/path/to/recordB".to_string(),
            offset: 20,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        expected_index.append(meta_b.clone()).unwrap();

        // insert record C
        let meta_c = FileMeta{
            path: "/path/to/recordC".to_string(),
            offset: 30,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        expected_index.append(meta_c.clone()).unwrap();

        // insert record D
        let meta_d = FileMeta{
            path: "/path/to/recordD".to_string(),
            offset: 40,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        expected_index.append(meta_d.clone()).unwrap();

        // flush
        expected_index.flush(&mut data).unwrap();
        assert_eq!(expected_index.entries.len(), 5);
        assert_eq!(expected_index.pages.len(), 2);

        // load
        data.rewind().unwrap();
        let index = Index::load(&mut data).unwrap();
        assert_eq!(index.entries.len(), 5);
        assert_eq!(index.pages.len(), 2);

        // assert_entries
        assert_eq!(expected_index.entries, index.entries);
        assert_eq!(expected_index.pages, index.pages);
    }

    #[test]
    fn add_page() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.pages.len());
        assert_eq!(1, index.entries.len());
        let pos = data.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(index.page_size, pos);
        let page = match index.add_page(&mut data) {
            Ok(page) => page.clone(),
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        let pos = data.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(index.page_size*2, pos);
        assert_eq!(page, index.pages[1]);
        assert_eq!(2, index.pages.len());
        assert_eq!(1, index.entries.len());
    }

    #[test]
    fn calc_page_index() {
        let index = Index::new(&mut Cursor::new(Vec::new()), Some(5)).unwrap();
        assert_eq!(0, index.calc_page_index(0));
        assert_eq!(0, index.calc_page_index(1));
        assert_eq!(0, index.calc_page_index(2));
        assert_eq!(0, index.calc_page_index(3));
        assert_eq!(0, index.calc_page_index(4));
        assert_eq!(0, index.calc_page_index(5));
        assert_eq!(1, index.calc_page_index(6));
        assert_eq!(1, index.calc_page_index(7));
        assert_eq!(1, index.calc_page_index(8));
        assert_eq!(1, index.calc_page_index(9));
        assert_eq!(1, index.calc_page_index(10));
        assert_eq!(2, index.calc_page_index(11));
        assert_eq!(2, index.calc_page_index(12));
        assert_eq!(2, index.calc_page_index(13));
        assert_eq!(2, index.calc_page_index(14));
        assert_eq!(2, index.calc_page_index(15));
        assert_eq!(3, index.calc_page_index(16));
        assert_eq!(3, index.calc_page_index(17));
        assert_eq!(3, index.calc_page_index(18));
        assert_eq!(3, index.calc_page_index(19));
        assert_eq!(3, index.calc_page_index(20));
    }

    #[test]
    fn len() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.entries.len());
        assert_eq!(1, index.len());
        index.entries.push(FileEntry::default());
        assert_eq!(2, index.entries.len());
        assert_eq!(2, index.len());
        index.entries.push(FileEntry::default());
        assert_eq!(3, index.entries.len());
        assert_eq!(3, index.len());
    }

    #[test]
    fn remove() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };

        // validate appends
        assert_eq!(5, index.entries.len());
        assert_eq!(4, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordA.0", index.entries[1].meta.path);
        assert_eq!("/path/to/recordB.0", index.entries[2].meta.path);
        assert_eq!("/path/to/recordC.0", index.entries[3].meta.path);
        assert_eq!("/path/to/recordD.0", index.entries[4].meta.path);
        match index.path_tree.get("/path/to/recordA.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordB.0") {
            Some(v) => assert_eq!(2, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordC.0") {
            Some(v) => assert_eq!(3, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordD.0") {
            Some(v) => assert_eq!(4, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove recordB.0
        match index.remove(2) {
            Ok(entry) => assert_eq!("/path/to/recordB.0", entry.meta.path),
            Err(e) => assert!(false, "Failed to remove entry: {}", e)
        }
        assert_eq!(4, index.entries.len());
        assert_eq!(3, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordA.0", index.entries[1].meta.path);
        assert_eq!("/path/to/recordD.0", index.entries[2].meta.path);
        assert_eq!("/path/to/recordC.0", index.entries[3].meta.path);
        match index.path_tree.get("/path/to/recordA.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordD.0") {
            Some(v) => assert_eq!(2, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordC.0") {
            Some(v) => assert_eq!(3, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove recordA.0
        match index.remove(1) {
            Ok(entry) => assert_eq!("/path/to/recordA.0", entry.meta.path),
            Err(e) => assert!(false, "Failed to remove entry: {}", e)
        }
        assert_eq!(3, index.entries.len());
        assert_eq!(2, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordC.0", index.entries[1].meta.path);
        assert_eq!("/path/to/recordD.0", index.entries[2].meta.path);
        match index.path_tree.get("/path/to/recordC.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }
        match index.path_tree.get("/path/to/recordD.0") {
            Some(v) => assert_eq!(2, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove recordD.0
        match index.remove(2) {
            Ok(entry) => assert_eq!("/path/to/recordD.0", entry.meta.path),
            Err(e) => assert!(false, "Failed to remove entry: {}", e)
        }
        assert_eq!(2, index.entries.len());
        assert_eq!(1, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordC.0", index.entries[1].meta.path);
        match index.path_tree.get("/path/to/recordC.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove recordC.0
        match index.remove(1) {
            Ok(entry) => assert_eq!("/path/to/recordC.0", entry.meta.path),
            Err(e) => assert!(false, "Failed to remove entry: {}", e)
        }
        assert_eq!(1, index.entries.len());
        assert_eq!(0, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
    }

    #[test]
    fn remove_out_of_bounds() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        index.append(FileMeta{
            offset: 10,
            path: "/path/to/recordA.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();

        // validate appends
        assert_eq!(2, index.entries.len());
        assert_eq!(1, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordA.0", index.entries[1].meta.path);
        match index.path_tree.get("/path/to/recordA.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove out of bounds
        match index.remove(2) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "index out of bounds")
        }
    }

    #[test]
    fn remove_invalid_placeholder_removal() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        index.append(FileMeta{
            offset: 10,
            path: "/path/to/recordA.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();

        // validate appends
        assert_eq!(2, index.entries.len());
        assert_eq!(1, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordA.0", index.entries[1].meta.path);
        match index.path_tree.get("/path/to/recordA.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove invalid placeholder
        match index.remove(0) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "first entry is a placeholder and can't be removed")
        }
    }

    #[test]
    fn remove_parted_entry() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        index.append(FileMeta{
            offset: 10,
            path: "/path/to/recordA.0".to_string(),
            file_type: FileType::File,
            size: 10,
            used: 10
        }).unwrap();
        index.entries[1].partition.next = 1;

        // validate appends
        assert_eq!(2, index.entries.len());
        assert_eq!(1, index.path_tree.len());
        assert_eq!("", index.entries[0].meta.path);
        assert_eq!("/path/to/recordA.0", index.entries[1].meta.path);
        match index.path_tree.get("/path/to/recordA.0") {
            Some(v) => assert_eq!(1, *v),
            None => assert!(false, "expected value but got none")
        }

        // remove parted entry
        match index.remove(1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "can't remove a parted entry, must unlink it first")
        }
    }

    #[test]
    fn calc_table_index() {
        let mut data = Cursor::new(Vec::new());
        let index = match Index::new(&mut data, Some(5)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(0, index.calc_table_index(0));
        assert_eq!(1, index.calc_table_index(1));
        assert_eq!(2, index.calc_table_index(2));
        assert_eq!(3, index.calc_table_index(3));
        assert_eq!(4, index.calc_table_index(4));
        assert_eq!(5, index.calc_table_index(5));
        assert_eq!(1, index.calc_table_index(6));
        assert_eq!(2, index.calc_table_index(7));
        assert_eq!(3, index.calc_table_index(8));
        assert_eq!(4, index.calc_table_index(9));
        assert_eq!(5, index.calc_table_index(10));
        assert_eq!(1, index.calc_table_index(11));
        assert_eq!(2, index.calc_table_index(12));
        assert_eq!(3, index.calc_table_index(13));
        assert_eq!(4, index.calc_table_index(14));
        assert_eq!(5, index.calc_table_index(15));
    }

    #[test]
    fn flush_remove_new_entries() {
        // create entries
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, Some(3)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.pages.len());
        assert_eq!(1, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // validate empty record
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);

        // insert record A and test flush
        let meta_a = FileMeta{
            path: "/path/to/recordA".to_string(),
            offset: 10,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_a.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record B and flush
        let meta_b = FileMeta{
            path: "/path/to/recordB".to_string(),
            offset: 20,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_b.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to B
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty record
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record A
        let mut expected_record_a = index.empty_record.clone();
        meta_a.copy_to_record(&mut expected_record_a);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);
        // validate record B
        let mut expected_record_b = index.empty_record.clone();
        meta_b.copy_to_record(&mut expected_record_b);
        let record_b = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);

        // remove record A
        index.remove(1).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to B
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty records
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        let record_empty = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record B
        let mut expected_record_b = index.empty_record.clone();
        meta_b.copy_to_record(&mut expected_record_b);
        let record_b = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);
    }

    #[test]
    fn flush_append_new_entries() {
        // create entries
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, Some(3)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.pages.len());
        assert_eq!(1, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // validate empty record
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);

        // insert record A and test flush
        let meta_a = FileMeta{
            path: "/path/to/recordA".to_string(),
            offset: 10,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_a.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to A
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty record
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record A
        let mut expected_record_a = index.empty_record.clone();
        meta_a.copy_to_record(&mut expected_record_a);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);

        // insert record B and flush
        let meta_b = FileMeta{
            path: "/path/to/recordB".to_string(),
            offset: 20,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_b.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to B
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty record
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record A
        let mut expected_record_a = index.empty_record.clone();
        meta_a.copy_to_record(&mut expected_record_a);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);
        // validate record B
        let mut expected_record_b = index.empty_record.clone();
        meta_b.copy_to_record(&mut expected_record_b);
        let record_b = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);
    }

    #[test]
    fn flush_append_with_new_page() {
        // create entries
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, Some(3)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.pages.len());
        assert_eq!(1, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record A
        let meta_a = FileMeta{
            path: "/path/to/recordA".to_string(),
            offset: 10,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_a.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record B
        let meta_b = FileMeta{
            path: "/path/to/recordB".to_string(),
            offset: 20,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_b.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record C
        let meta_c = FileMeta{
            path: "/path/to/recordC".to_string(),
            offset: 30,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_c.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(4, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record D
        let meta_d = FileMeta{
            path: "/path/to/recordD".to_string(),
            offset: 40,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_d.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(5, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // test flush
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(2, index.pages.len());
        assert_eq!(5, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        assert_eq!(4, index.pages[1].table.header.meta.record_count);

        // check saved entries up to D
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate offset_record on page 0
        let offset_meta = FileMeta{
            path: "".to_string(),
            offset: 1234,
            file_type: FileType::Unknown(0),
            size: 0u64,
            used: 0u64
        };
        let mut expected_offset_record = index.empty_record.clone();
        offset_meta.copy_to_record(&mut expected_offset_record);
        let offset_record = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_offset_record, offset_record);
        // validate record A on page 0
        let mut expected_record_a = index.empty_record.clone();
        meta_a.copy_to_record(&mut expected_record_a);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);
        // validate record B on page 0
        let mut expected_record_b = index.empty_record.clone();
        meta_b.copy_to_record(&mut expected_record_b);
        let record_b = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);
        // validate record C on page 0
        let mut expected_record_c = index.empty_record.clone();
        meta_c.copy_to_record(&mut expected_record_c);
        let record_c = match index.pages[0].table.record_from(&mut segment, 3).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_c, record_c);
        // validate empty record on page 1
        let mut segment = Segment::new_unsafe(&mut data, index.pages[1].offset, index.page_size, index.pages[1].offset + index.page_size, false).unwrap();
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[1].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record D on page 1
        let mut expected_record_d = index.empty_record.clone();
        meta_d.copy_to_record(&mut expected_record_d);
        let record_d = match index.pages[1].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_d, record_d);
    }

    #[test]
    fn flush_modify_entries() {
        // create entries
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, Some(3)) {
            Ok(index) => index,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(1, index.pages.len());
        assert_eq!(1, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // validate empty record
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);

        // insert record A and test flush
        let meta_a = FileMeta{
            path: "/path/to/recordA".to_string(),
            offset: 10,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_a.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(2, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // insert record B and flush
        let meta_b = FileMeta{
            path: "/path/to/recordB".to_string(),
            offset: 20,
            file_type: FileType::File,
            size:10,
            used: 10
        };
        index.append(meta_b.clone()).unwrap();
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to B
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty record
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record A
        let mut expected_record_a = index.empty_record.clone();
        meta_a.copy_to_record(&mut expected_record_a);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);
        // validate record B
        let mut expected_record_b = index.empty_record.clone();
        meta_b.copy_to_record(&mut expected_record_b);
        let record_b = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);

        // modify record A and B
        let entry_a = index.entries.get_mut(1).unwrap();
        entry_a.meta.path = "/path/to/recordA_modified".to_string();
        entry_a.meta.offset = 10;
        entry_a.meta.size = 15;
        entry_a.meta.file_type = FileType::HardLink;
        entry_a.partition.prev = 3;
        entry_a.partition.next = 4;
        let entry_a = entry_a.clone();
        index.save_entry(1).unwrap();
        let entry_b = index.entries.get_mut(2).unwrap();
        entry_b.meta.path = "/path/to/recordB_modified".to_string();
        entry_b.meta.offset = 25;
        entry_b.meta.size = 20;
        entry_b.meta.file_type = FileType::SymbolicLink;
        entry_b.partition.prev = 5;
        entry_b.partition.next = 6;
        let entry_b = entry_b.clone();
        index.save_entry(2).unwrap();
        assert_eq!(2, index.modified.len());
        assert!(index.modified.contains(&1));
        assert!(index.modified.contains(&2));
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);
        if let Err(e) = index.flush(&mut data) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(1, index.pages.len());
        assert_eq!(3, index.entries.len());
        assert_eq!(4, index.pages[0].table.header.meta.record_count);

        // check saved entries up to B
        let mut segment = Segment::new_unsafe(&mut data, index.pages[0].offset, index.page_size, index.pages[0].offset + index.page_size, false).unwrap();
        // validate empty records
        let expected_empty = index.empty_record.clone();
        let record_empty = match index.pages[0].table.record_from(&mut segment, 0).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record but got none");
                return;
            }
        };
        assert_eq!(expected_empty, record_empty);
        // validate record A
        let mut expected_record_a = index.empty_record.clone();
        entry_a.copy_to_record(&mut expected_record_a, true);
        let record_a = match index.pages[0].table.record_from(&mut segment, 1).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record A but got none");
                return;
            }
        };
        assert_eq!(expected_record_a, record_a);
        // validate record B
        let mut expected_record_b = index.empty_record.clone();
        entry_b.copy_to_record(&mut expected_record_b, true);
        let record_b = match index.pages[0].table.record_from(&mut segment, 2).unwrap() {
            Some(v) => v,
            None => {
                assert!(false, "expected record B but got none");
                return;
            }
        };
        assert_eq!(expected_record_b, record_b);
    }

    #[test]
    fn append() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "Failed to create index: {}", e);
                return;
            }
        };
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.path_tree.len(), 0);
        let meta = FileMeta {
            path: "/path/to/recordA".to_string(),
            offset: 200,
            size: 0,
            used: 0,
            file_type: FileType::File
        };
        if let Err(e) = index.append(meta.clone()) {
            assert!(false, "Failed to append entry: {}", e);
            return;
        }
        assert_eq!(index.modified.len(), 1);
        assert!(index.modified.contains(&1));
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.entries[1].meta, meta);
        assert_eq!(index.entries[1].partition.next, 0);
        assert_eq!(index.entries[1].partition.prev, 0);
        assert_eq!(index.path_tree.len(), 1);
        let meta = FileMeta {
            path: "/path/to/recordB".to_string(),
            offset: 200,
            size: 0,
            used: 0,
            file_type: FileType::File
        };
        if let Err(e) = index.append(meta.clone()) {
            assert!(false, "Failed to append entry: {}", e);
            return;
        }
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&1));
        assert!(index.modified.contains(&2));
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.entries[2].meta, meta);
        assert_eq!(index.entries[2].partition.next, 0);
        assert_eq!(index.entries[2].partition.prev, 0);
        assert_eq!(index.path_tree.len(), 2);
    }

    #[test]
    fn append_without_path() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match Index::new(&mut data, None) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "Failed to create index: {}", e);
                return;
            }
        };
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.path_tree.len(), 0);
        let meta = FileMeta {
            path: "".to_string(),
            offset: 200,
            size: 0,
            used: 0,
            file_type: FileType::File
        };
        if let Err(e) = index.append(meta.clone()) {
            assert!(false, "Failed to append entry: {}", e);
            return;
        }
        assert_eq!(index.modified.len(), 1);
        assert!(index.modified.contains(&1));
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.entries[1].meta, meta);
        assert_eq!(index.entries[1].partition.next, 0);
        assert_eq!(index.entries[1].partition.prev, 0);
        assert_eq!(index.path_tree.len(), 0);
        let meta = FileMeta {
            path: "".to_string(),
            offset: 200,
            size: 0,
            used: 0,
            file_type: FileType::File
        };
        if let Err(e) = index.append(meta.clone()) {
            assert!(false, "Failed to append entry: {}", e);
            return;
        }
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&1));
        assert!(index.modified.contains(&2));
        assert_eq!(index.pages.len(), 1);
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.entries[2].meta, meta);
        assert_eq!(index.entries[2].partition.next, 0);
        assert_eq!(index.entries[2].partition.prev, 0);
        assert_eq!(index.path_tree.len(), 0);
    }

    #[test]
    fn get() {
        let mut data = Cursor::new(Vec::new());
        let index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };

        match index.get("/path/to/recordA.0") {
            Some(v) => assert_eq!((1, &index.entries[1]), v),
            None => assert!(false, "expected entry recordA.0 but got not found")
        }
        match index.get("/path/to/recordB.0") {
            Some(v) => assert_eq!((2, &index.entries[2]), v),
            None => assert!(false, "expected entry recordB.0 but got not found")
        }
        match index.get("/path/to/recordC.0") {
            Some(v) => assert_eq!((3, &index.entries[3]), v),
            None => assert!(false, "expected entry recordC.0 but got not found")
        }
        match index.get("/path/to/recordD.0") {
            Some(v) => assert_eq!((4, &index.entries[4]), v),
            None => assert!(false, "expected entry recordD.0 but got not found")
        }
    }

    #[test]
    fn get_mut() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };

        let mut expected = index.entries[1].clone();
        match index.get_mut("/path/to/recordA.0") {
            Some(v) => assert_eq!((1, &mut expected), v),
            None => assert!(false, "expected entry recordA.0 but got not found")
        }
        let mut expected = index.entries[2].clone();
        match index.get_mut("/path/to/recordB.0") {
            Some(v) => assert_eq!((2, &mut expected), v),
            None => assert!(false, "expected entry recordB.0 but got not found")
        }
        let mut expected = index.entries[3].clone();
        match index.get_mut("/path/to/recordC.0") {
            Some(v) => assert_eq!((3, &mut expected), v),
            None => assert!(false, "expected entry recordC.0 but got not found")
        }
        let mut expected = index.entries[4].clone();
        match index.get_mut("/path/to/recordD.0") {
            Some(v) => assert_eq!((4, &mut expected), v),
            None => assert!(false, "expected entry recordD.0 but got not found")
        }
    }

    #[test]
    fn get_index() {
        let mut data = Cursor::new(Vec::new());
        let index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        match index.get_index(0) {
            Some(v) => assert_eq!(&index.entries[0], v),
            None => assert!(false, "expected entry recordA.0 but got not found")
        }
        match index.get_index(1) {
            Some(v) => assert_eq!(&index.entries[1], v),
            None => assert!(false, "expected entry recordA.0 but got not found")
        }
        match index.get_index(2) {
            Some(v) => assert_eq!(&index.entries[2], v),
            None => assert!(false, "expected entry recordB.0 but got not found")
        }
        match index.get_index(3) {
            Some(v) => assert_eq!(&index.entries[3], v),
            None => assert!(false, "expected entry recordC.0 but got not found")
        }
        match index.get_index(4) {
            Some(v) => assert_eq!(&index.entries[4], v),
            None => assert!(false, "expected entry recordD.0 but got not found")
        }
    }

    #[test]
    fn get_index_mut() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };

        let mut expected = index.entries[1].clone();
        match index.get_index_mut(1) {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected entry recordA.0 but got not found")
        }
        let mut expected = index.entries[2].clone();
        match index.get_index_mut(2) {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected entry recordB.0 but got not found")
        }
        let mut expected = index.entries[3].clone();
        match index.get_index_mut(3) {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected entry recordC.0 but got not found")
        }
        let mut expected = index.entries[4].clone();
        match index.get_index_mut(4) {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected entry recordD.0 but got not found")
        }
    }

    #[test]
    fn save_entry() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(index.entries.len(), 5);
        index.modified.clear();
        assert_eq!(index.modified.len(), 0);
        if let Err(e) = index.save_entry(1) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        if let Err(e) = index.save_entry(3) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        }
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&1));
        assert!(index.modified.contains(&3));
    }

    #[test]
    fn save_entry_out_of_bounds() {
        let mut data = Cursor::new(Vec::new());
        let mut index = match create_index(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        match index.save_entry(5) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => {
                let msg = e.to_string();
                assert_eq!("index out of bounds", msg);
            }
        }
    }

    #[test]
    fn link_next() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        // Create two entries
        let meta1 = FileMeta { path: "a".to_string(), ..Default::default() };
        let meta2 = FileMeta { path: "b".to_string(), ..Default::default() };
        let i1 = index.append(meta1).unwrap();
        let i2 = index.append(meta2).unwrap();

        // Link i1 -> i2
        index.modified.clear();
        let prev = index.link_next(i1, i2).unwrap();
        assert_eq!(prev, 0);
        assert_eq!(index.entries[i1].partition.next, i2);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, i1);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
    }

    #[test]
    fn link_prev() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        // Create two entries
        let meta1 = FileMeta { path: "a".to_string(), ..Default::default() };
        let meta2 = FileMeta { path: "b".to_string(), ..Default::default() };
        let i1 = index.append(meta1).unwrap();
        let i2 = index.append(meta2).unwrap();

        // Link i2 -> i1 (should update prev_part)
        index.modified.clear();
        let next = index.link_prev(i2, i1).unwrap();
        assert_eq!(next, 0);
        assert_eq!(index.entries[i2].partition.prev, i1);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i1].partition.next, i2);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i1));
    }

    #[test]
    fn link_next_loop() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.link_next(i1, i2).unwrap();
        index.link_next(i2, i3).unwrap();
        match index.link_next(i3, i1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "loop detected"),
        }
    }

    #[test]
    fn link_prev_loop() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.link_prev(i1, i2).unwrap();
        index.link_prev(i2, i3).unwrap();
        match index.link_prev(i3, i1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "loop detected"),
        }
    }

    #[test]
    fn link_next_same_loop() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        match index.link_next(i1, i1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "loop detected"),
        }
    }

    #[test]
    fn link_prev_same_loop() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        match index.link_prev(i1, i1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "loop detected"),
        }
    }

    #[test]
    fn link_next_zero() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        index.entries[i2].partition.prev = i1;
        index.entries[i1].partition.next = i2;
        index.modified.clear();
        if let Err(e) = index.link_next(i1, 0) {
            assert!(false, "expected success but got error: {}", e.to_string());
        }
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
    }

    #[test]
    fn link_prev_zero() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        index.entries[i2].partition.prev = i1;
        index.entries[i1].partition.next = i2;
        index.modified.clear();
        if let Err(e) = index.link_prev(i2, 0) {
            assert!(false, "expected success but got error: {}", e.to_string());
        }
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
    }

    #[test]
    fn link_next_out_of_bounds() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        match index.link_next(i1, 10) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "next part out of bounds"),
        }
    }

    #[test]
    fn link_prev_out_of_bounds() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        match index.link_prev(i1, 10) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "prev part out of bounds"),
        }
    }

    #[test]
    fn link_next_same_next_index() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        index.entries[i2].partition.prev = i1;
        index.entries[i1].partition.next = i2;
        index.modified.clear();
        match index.link_next(i1, i2) {
            Ok(old_next) => assert_eq!(old_next, i2),
            Err(e) => assert!(false, "expected success but got error: {}", e.to_string()),
        }
        assert_eq!(index.modified.len(), 0);
    }

    #[test]
    fn link_prev_same_prev_index() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        index.entries[i2].partition.prev = i1;
        index.entries[i1].partition.next = i2;
        index.modified.clear();
        match index.link_prev(i2, i1) {
            Ok(old_prev) => assert_eq!(old_prev, i1),
            Err(e) => assert!(false, "expected success but got error: {}", e.to_string()),
        }
        assert_eq!(index.modified.len(), 0);
    }

    #[test]
    fn link_next_already_with_prev() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.entries[i2].partition.next = i3;
        index.entries[i3].partition.prev = i2;
        match index.link_next(i1, i3) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "next part already has a previous part, you must unlink it first"),
        }
    }

    #[test]
    fn link_prev_already_with_next() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.entries[i1].partition.next = i2;
        index.entries[i2].partition.prev = i1;
        match index.link_prev(i3, i1) {
            Ok(_) => assert!(false, "expected error but got success"),
            Err(e) => assert_eq!(e.to_string(), "previous part already has a next part, you must unlink it first"),
        }
    }

    #[test]
    fn link_next_unlink_old_next() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.entries[i1].partition.next = i2;
        index.entries[i2].partition.prev = i1;
        index.modified.clear();
        index.link_next(i1, i3).unwrap();
        assert_eq!(index.entries[i1].partition.next, i3);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, i1);
        assert_eq!(index.modified.len(), 3);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));
    }

    #[test]
    fn link_prev_unlink_old_prev() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();
        index.entries[i1].partition.next = i2;
        index.entries[i2].partition.prev = i1;
        index.modified.clear();
        index.link_prev(i2, i3).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, i3);
        assert_eq!(index.entries[i3].partition.next, i2);
        assert_eq!(index.entries[i3].partition.prev, 0);
        assert_eq!(index.modified.len(), 3);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));
    }

    #[test]
    fn unlink_next() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();

        // chain i1 -> i2 -> i3
        index.entries[i1].partition.next = i2;
        index.entries[i1].partition.prev = 0;
        index.entries[i2].partition.next = i3;
        index.entries[i2].partition.prev = i1;
        index.entries[i3].partition.next = 0;
        index.entries[i3].partition.prev = i2;

        // Unlink i2 from i3
        index.modified.clear();
        index.unlink_next(i2).unwrap();
        assert_eq!(index.entries[i1].partition.next, i2);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, i1);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, 0);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));

        // relink i2 -> i3
        index.entries[i2].partition.next = i3;
        index.entries[i3].partition.prev = i2;

        // Unlink i1 from i2
        index.modified.clear();
        index.unlink_next(i1).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, i3);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, i2);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
    }

    #[test]
    fn unlink_next_zero() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();

        // Unlink i1 without previous part
        index.modified.clear();
        assert_eq!(index.entries[i1].partition.next, 0);
        index.unlink_next(i1).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.modified.len(), 0);
    }

    #[test]
    fn unlink_prev() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();

        // chain i1 -> i2 -> i3
        index.entries[i1].partition.next = i2;
        index.entries[i1].partition.prev = 0;
        index.entries[i2].partition.next = i3;
        index.entries[i2].partition.prev = i1;
        index.entries[i3].partition.next = 0;
        index.entries[i3].partition.prev = i2;

        // Unlink i1 from i2
        index.modified.clear();
        index.unlink_prev(i2).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, i3);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, i2);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));

        // relink i1 -> i2
        index.entries[i1].partition.next = i2;
        index.entries[i2].partition.prev = i1;

        // Unlink i2 from i3
        index.modified.clear();
        index.unlink_prev(i3).unwrap();
        assert_eq!(index.entries[i1].partition.next, i2);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, i1);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, 0);
        assert_eq!(index.modified.len(), 2);
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));
    }

    #[test]
    fn unlink_prev_zero() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();

        // Unlink i1 without previous part
        index.modified.clear();
        assert_eq!(index.entries[i1].partition.prev, 0);
        index.unlink_prev(i1).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.modified.len(), 0);
    }

    #[test]
    fn unlink_both_without_link_parts() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();

        // chain i1 -> i2 -> i3
        index.entries[i1].partition.next = i2;
        index.entries[i1].partition.prev = 0;
        index.entries[i2].partition.next = i3;
        index.entries[i2].partition.prev = i1;
        index.entries[i3].partition.next = 0;
        index.entries[i3].partition.prev = i2;

        // Unlink i1 from i2 (link_parts=false)
        index.modified.clear();
        index.unlink(i2, false).unwrap();
        assert_eq!(index.entries[i1].partition.next, 0);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, 0);
        assert_eq!(index.modified.len(), 3);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));
    }

    #[test]
    fn unlink_both_with_link_parts() {
        let mut data = Cursor::new(Vec::new());
        let mut index = Index::new(&mut data, None).unwrap();
        let i1 = index.append(FileMeta { path: "a".to_string(), ..Default::default() }).unwrap();
        let i2 = index.append(FileMeta { path: "b".to_string(), ..Default::default() }).unwrap();
        let i3 = index.append(FileMeta { path: "c".to_string(), ..Default::default() }).unwrap();

        // chain i1 -> i2 -> i3
        index.entries[i1].partition.next = i2;
        index.entries[i1].partition.prev = 0;
        index.entries[i2].partition.next = i3;
        index.entries[i2].partition.prev = i1;
        index.entries[i3].partition.next = 0;
        index.entries[i3].partition.prev = i2;

        // Unlink i1 from i2 (link_parts=true)
        index.modified.clear();
        index.unlink(i2, true).unwrap();
        assert_eq!(index.entries[i1].partition.next, i3);
        assert_eq!(index.entries[i1].partition.prev, 0);
        assert_eq!(index.entries[i2].partition.next, 0);
        assert_eq!(index.entries[i2].partition.prev, 0);
        assert_eq!(index.entries[i3].partition.next, 0);
        assert_eq!(index.entries[i3].partition.prev, i1);
        assert_eq!(index.modified.len(), 3);
        assert!(index.modified.contains(&i1));
        assert!(index.modified.contains(&i2));
        assert!(index.modified.contains(&i3));
    }

    #[test]
    fn get_page_size() {
        let mut data = Cursor::new(Vec::new());
        let index = Index::new(&mut data, None).unwrap();
        assert_eq!(index.get_page_size(), 9741);
        let index = Index::new(&mut data, Some(5)).unwrap();
        assert_eq!(index.get_page_size(), 1596);
        let index = Index::new(&mut data, Some(10)).unwrap();
        assert_eq!(index.get_page_size(), 2501);
        let index = Index::new(&mut data, Some(1)).unwrap();
        assert_eq!(index.get_page_size(), 872);
    }
}