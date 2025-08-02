use anyhow::{bail, Result};
use std::collections::HashSet;
use std::io::{Read, Seek, Write};
use dhfarm_engine::db::field::FieldType;
use dhfarm_engine::db::table::traits::TableTrait;
use dhfarm_engine::db::table::{IterRecord, Table};
use dhfarm_engine::traits::ByteSized;
use dhfarm_engine::uuid::Uuid;

pub const DEFAULT_RECORD_COUNT: u64 = 51;

/// Represents a page of the index.
#[derive(PartialEq, Clone, Debug)]
pub struct Page {
    /// Page offset.
    pub offset: u64,
    
    /// Table used to store the file entries.
    pub table: Table
}

impl Page {
    /// Creates a new page and initializes the files array with a single empty entry
    /// to avoid the use of Option for next_part and prev_part so whenever it has a
    /// value of 0, it means it is empty. This first records will be used to store
    /// the next page offset within the file.
    /// 
    /// # Arguments:
    /// 
    /// * `segment` - The segment to use for creating the page.
    /// 
    /// # Returns:
    /// 
    /// * `Result<Self>` - The created page.
    pub fn new(segment: &mut (impl Read + Seek + Write), record_count: Option<u64>) -> Result<Self> {
        let mut table = Table::new("page", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        // offset
        table.header_mut().record.add("o", FieldType::U64).unwrap();
        // path
        table.header_mut().record.add("n", FieldType::Str(150)).unwrap();
        // size
        table.header_mut().record.add("s", FieldType::U64).unwrap();
        // used
        table.header_mut().record.add("u", FieldType::U64).unwrap();
        // type
        table.header_mut().record.add("t", FieldType::U8).unwrap();
        // next_part
        table.header_mut().record.add("a", FieldType::U8).unwrap();
        // prev_part
        table.header_mut().record.add("b", FieldType::U8).unwrap();
        table.save_headers_into(segment)?;
        let record_count = match record_count {
            Some(v) => v,
            None => DEFAULT_RECORD_COUNT
        };
        if record_count < 2 {
            bail!("You must use at least 2 record per page (first record always stores the next page offset)")
        }

        // fill table records
        table.fill_records_into(segment, record_count.into())?;
        Ok(Self {
            table,
            offset: 0
        })
    }

    /// Loads a page from a reader.
    /// 
    /// # Arguments:
    /// 
    /// * `reader` - The reader to use for loading the page.
    /// 
    /// # Returns:
    /// 
    /// * `Result<Self>` - The loaded page.
    pub fn load(reader: &mut (impl Read + Seek)) -> Result<Self> {
        let table = Table::load(reader)?;

        // validate fields
        let keys = HashSet::from(["o", "n", "s", "u", "t", "a", "b"]);
        if keys.len() != table.header.record.len() {
            bail!("invalid number of fields");
        }
        for (key, _) in table.header.record.iter() {
            if !keys.contains(key.as_str()) {
                bail!("invalid field: {}", key);
            }
        }

        // validate record count
        if table.header.meta.record_count < 2 {
            bail!("You must use at least 2 record per page (first records always stores the next page offset)")
        }

        Ok(Self {
            table,
            offset: 0
        })
    }

    /// Return the table record iterator.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Result<IterRecord<'reader, 'table, impl Read + Seek>>` - The iterator of the table records.
    pub fn iter<'reader, 'table>(&'table self, reader: &'reader mut (impl Read + Seek)) -> Result<IterRecord<'reader, 'table, impl Read + Seek>> {
        self.table.iter(reader, None, None)
    }

    /// Returns the page size.
    pub fn calc_page_size(&self) -> u64 {
        self.table.calc_record_pos(self.table.header.meta.record_count)
    }
}

#[cfg(test)]
mod test_helper {
    use dhfarm_engine::db::field::{Record, Value};

    use crate::index::file::FileType;

    use super::*;

    /// Adds records to the table.
    /// 
    /// # Arguments
    /// 
    /// * `table` - The table to add records to.
    /// * `writer` - The writer to use for writing the records.
    /// 
    /// # Returns
    /// 
    /// * `Result<Vec<Record>>` - The result of the add operation.
    pub fn add_records(table: &mut Table, writer: &mut (impl Read + Write + Seek)) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut offset = 0;

        // add first record
        let mut  record = table.header.record.new_record()?;
        record.set("o", Value::U64(offset));
        record.set("n", Value::Str("path/to/recordA.0".to_string()));
        record.set("s", Value::U64(10));
        record.set("u", Value::U64(8));
        record.set("t", FileType::File.into());
        record.set("a", Value::U8(3));
        record.set("b", Value::U8(0));
        table.append_record_into(writer, &record, false)?;
        records.push(record);
        offset += 512;

        // add second record
        let mut record = table.header.record.new_record()?;
        record.set("o", Value::U64(offset));
        record.set("n", Value::Str("path/to/recordB".to_string()));
        record.set("s", Value::U64(516));
        record.set("u", Value::U64(200));
        record.set("t", FileType::File.into());
        record.set("a", Value::U8(0));
        record.set("b", Value::U8(0));
        table.append_record_into(writer, &record, false)?;
        records.push(record);
        offset += 1024;

        // add third record
        let mut record = table.header.record.new_record()?;
        record.set("o", Value::U64(offset));
        record.set("n", Value::Str("path/to/recordA.1".to_string()));
        record.set("s", Value::U64(5));
        record.set("u", Value::U64(5));
        record.set("t", FileType::File.into());
        record.set("a", Value::U8(0));
        record.set("b", Value::U8(1));
        table.append_record_into(writer, &record, true)?;
        records.push(record);

        Ok(records)
    }

    pub fn create_fake_table(writer: &mut (impl Read + Write + Seek), record_count: u64) -> Result<Table> {
        let mut table = Table::new("page", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        table.header.record.add("o", FieldType::U64)?;
        table.header.record.add("n", FieldType::Str(150))?;
        table.header.record.add("s", FieldType::U64)?;
        table.header.record.add("u", FieldType::U64)?;
        table.header.record.add("t", FieldType::U8)?;
        table.header.record.add("a", FieldType::U8)?;
        table.header.record.add("b", FieldType::U8)?;
        table.save_headers_into(writer)?;
        table.fill_records_into(writer, record_count)?;
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dhfarm_engine::traits::DataTrait;
    use dhfarm_engine::Data;
    use std::io::Cursor;

    #[test]
    fn new() {
        let mut data = Cursor::new(Vec::new());
        let page = match Page::new(&mut data, None) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "Failed to create page: {}", e);
                return;
            }
        };
        assert_eq!(page.table.header.meta.record_count, DEFAULT_RECORD_COUNT);
        assert_eq!(page.table.header.record.len(), 7);
        assert_eq!(page.table.header.record.get("o").unwrap().get_type(), &FieldType::U64);
        assert_eq!(page.table.header.record.get("n").unwrap().get_type(), &FieldType::Str(150));
        assert_eq!(page.table.header.record.get("s").unwrap().get_type(), &FieldType::U64);
        assert_eq!(page.table.header.record.get("u").unwrap().get_type(), &FieldType::U64);
        assert_eq!(page.table.header.record.get("t").unwrap().get_type(), &FieldType::U8);
        assert_eq!(page.table.header.record.get("a").unwrap().get_type(), &FieldType::U8);
        assert_eq!(page.table.header.record.get("b").unwrap().get_type(), &FieldType::U8);
    }

    #[test]
    fn load() {
        let mut data = Data::new(Cursor::new(Vec::new()), false);
        let mut table = test_helper::create_fake_table(&mut data, 1).unwrap();
        let records = test_helper::add_records(&mut table, &mut data).unwrap();
        data.flush().unwrap();
        let page = match Page::load(&mut data) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "Failed to load page: {}", e);
                return;
            }
        };
        assert_eq!(4, page.table.header.meta.record_count);
        match page.table.record_from(&mut data, 1) {
            Ok(opt) => match opt {
                Some(record) => assert_eq!(records[0], record),
                None => assert!(false, "expected record 1 to exists")
            },
            Err(e) => {
                assert!(false, "Failed to load record: {}", e);
                return;
            }
        }
        match page.table.record_from(&mut data, 2) {
            Ok(opt) => match opt {
                Some(record) => assert_eq!(records[1], record),
                None => assert!(false, "expected record 2 to exists")
            },
            Err(e) => {
                assert!(false, "Failed to load record: {}", e);
                return;
            }
        }
        match page.table.record_from(&mut data, 3) {
            Ok(opt) => match opt {
                Some(record) => assert_eq!(records[2], record),
                None => assert!(false, "expected record 3 to exists")
            },
            Err(e) => {
                assert!(false, "Failed to load record: {}", e);
                return;
            }
        }
    }
}