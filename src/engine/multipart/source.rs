use crate::Index;
use dhfarm_engine::Segment;
use std::io::{Read, Write, Seek};

/// Source of the multipart.
#[derive(PartialEq, Debug)]
pub struct Source<'index, 'data, T: Read + Write +Seek> {
    /// Data index.
    pub index: &'index mut Index,
    
    /// Active segment.
    pub segment: Segment<'data, T>
}