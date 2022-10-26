pub enum MergingFilesType {
    SingleSortedFileMerge,
    MultipleSortedFileWithSameSchemaMerge,
    MultipleSortedFileWithDifferentSchemaMerge,
}

pub enum MergingLogicType {
    UseLast,
    UseAssociativeExpr,
    UseJavaMergeOp,
}