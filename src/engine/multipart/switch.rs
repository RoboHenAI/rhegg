/// Switch part.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum SwitchPart {
    Pos(u64),
    Index(usize)
}