use dhfarm_engine::SegmentMeta;

/// Part of the multipart.
#[derive(PartialEq, Clone, Debug)]
pub struct Part {
    pub start: u64,
    pub index: usize,
    pub segment_meta: Option<SegmentMeta>
}