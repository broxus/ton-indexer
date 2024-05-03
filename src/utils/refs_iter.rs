use ton_types::Cell;

pub trait CellExt {
    fn into_references(self) -> RefsIter;
}

impl CellExt for Cell {
    fn into_references(self) -> RefsIter {
        RefsIter {
            max: self.references_count() as u8,
            cell: self,
            index: 0,
        }
    }
}

pub struct RefsIter {
    cell: Cell,
    max: u8,
    index: u8,
}

impl Iterator for RefsIter {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.max {
            None
        } else {
            let child = self.cell.reference(self.index as usize).ok();
            self.index += 1;
            child
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.max.saturating_sub(self.index) as usize;
        (remaining, Some(remaining))
    }
}
