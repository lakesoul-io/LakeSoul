//! 用于 ANN-Benchmarks 文件的最小化、无依赖 HDF5 读取器。
//!
//! 仅支持 `ann_benchmarks.datasets` 生成的以下格式：
//!   * superblock 版本 0
//!   * object header 版本 1（包括 continuation block）
//!   * 旧式符号表分组（`SNOD`、本地堆和 v1 B-tree）
//!   * 连续、未压缩、小端序的 f32 / i32 数据集
//!
//! 其他布局会返回明确错误，避免读取无效数据。

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DType {
    F32,
    I32,
}

struct DatasetMeta {
    shape: Vec<usize>,
    dtype: DType,
    address: u64,
}

pub struct Hdf5File {
    file: File,
    off_size: usize,
    len_size: usize,
    datasets: Vec<(String, DatasetMeta)>,
}

impl Hdf5File {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut signature = [0u8; 8];
        file.read_exact(&mut signature)?;
        if &signature != b"\x89HDF\r\n\x1a\n" {
            return Err(invalid("bad file signature"));
        }

        let superblock = read_at(&mut file, 8, 64)?;
        if superblock[0] != 0 {
            return Err(invalid("only superblock v0 is supported"));
        }
        let off_size = superblock[5] as usize;
        let len_size = superblock[6] as usize;
        if off_size != 8 || len_size != 8 {
            return Err(invalid("only 8-byte offsets/lengths are supported"));
        }

        // Root group symbol table entry starts after 4 addresses.
        let entry_offset = 8 + 16 + 4 * off_size as u64;
        let entry = read_at(&mut file, entry_offset, (2 * off_size + 24) as u64)?;
        let root_oh = read_uint(&entry, off_size, off_size);

        let mut hdf5 = Hdf5File {
            file,
            off_size,
            len_size,
            datasets: Vec::new(),
        };
        let mut datasets = Vec::new();
        hdf5.collect_group(root_oh, "", &mut datasets)?;
        hdf5.datasets = datasets;
        Ok(hdf5)
    }

    pub fn read_f32_rows(
        &mut self,
        name: &str,
        limit: usize,
    ) -> io::Result<Vec<Vec<f32>>> {
        let meta = self.meta(name, DType::F32)?;
        let rows = meta.shape[0].min(if limit == 0 { meta.shape[0] } else { limit });
        self.read_f32_rows_range(name, 0, rows)
    }

    pub fn f32_shape(&self, name: &str) -> io::Result<(usize, usize)> {
        let meta = self.meta(name, DType::F32)?;
        Ok((meta.shape[0], meta.shape[1]))
    }

    pub fn i32_shape(&self, name: &str) -> io::Result<(usize, usize)> {
        let meta = self.meta(name, DType::I32)?;
        Ok((meta.shape[0], meta.shape[1]))
    }

    pub fn read_f32_rows_range(
        &mut self,
        name: &str,
        start: usize,
        count: usize,
    ) -> io::Result<Vec<Vec<f32>>> {
        let meta = self.meta(name, DType::F32)?;
        let total = meta.shape[0];
        let end = start.saturating_add(count).min(total);
        if start >= end {
            return Ok(Vec::new());
        }
        let cols = meta.shape[1];
        let row_bytes = cols * 4;
        self.file
            .seek(SeekFrom::Start(meta.address + (start * row_bytes) as u64))?;
        let mut buffer = vec![0u8; (end - start) * row_bytes];
        self.file.read_exact(&mut buffer)?;
        let mut out = Vec::with_capacity(end - start);
        for row in buffer.chunks_exact(row_bytes) {
            let mut values = Vec::with_capacity(cols);
            for chunk in row.chunks_exact(4) {
                values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            out.push(values);
        }
        Ok(out)
    }

    pub fn read_f32_flat_range(
        &mut self,
        name: &str,
        start: usize,
        count: usize,
    ) -> io::Result<(Vec<f32>, usize)> {
        let meta = self.meta(name, DType::F32)?;
        let total = meta.shape[0];
        let end = start.saturating_add(count).min(total);
        if start >= end {
            return Ok((Vec::new(), meta.shape[1]));
        }
        let cols = meta.shape[1];
        let row_bytes = cols * 4;
        self.file
            .seek(SeekFrom::Start(meta.address + (start * row_bytes) as u64))?;
        let value_count = (end - start) * cols;
        #[cfg(target_endian = "little")]
        let values = {
            let mut values = vec![0.0f32; value_count];
            // benchmark HDF5 数据集是连续的小端序 f32。
            // 直接读入最终缓冲区，避免中间字节缓冲、逐元素端序转换
            // 和第二次整块复制。
            let bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    values.as_mut_ptr().cast::<u8>(),
                    value_count * std::mem::size_of::<f32>(),
                )
            };
            self.file.read_exact(bytes)?;
            values
        };
        #[cfg(not(target_endian = "little"))]
        let values = {
            let mut bytes = vec![0u8; value_count * 4];
            self.file.read_exact(&mut bytes)?;
            bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect()
        };
        Ok((values, cols))
    }

    pub fn read_i32_rows(
        &mut self,
        name: &str,
        limit: usize,
    ) -> io::Result<Vec<Vec<i32>>> {
        let meta = self.meta(name, DType::I32)?;
        self.read_rows(&meta, limit, |bytes| {
            i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        })
    }

    fn meta(&self, name: &str, expect: DType) -> io::Result<DatasetMeta> {
        let meta = self
            .datasets
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, m)| m)
            .ok_or_else(|| invalid(&format!("dataset `{name}` not found")))?;
        if meta.dtype != expect {
            return Err(invalid(&format!(
                "dataset `{name}` has unexpected element type"
            )));
        }
        if meta.shape.len() != 2 {
            return Err(invalid(&format!("dataset `{name}` is not 2-dimensional")));
        }
        Ok(DatasetMeta {
            shape: meta.shape.clone(),
            dtype: meta.dtype,
            address: meta.address,
        })
    }

    fn read_rows<T: Copy>(
        &mut self,
        meta: &DatasetMeta,
        limit: usize,
        convert: impl Fn([u8; 4]) -> T,
    ) -> io::Result<Vec<Vec<T>>> {
        let rows = meta.shape[0].min(if limit == 0 { meta.shape[0] } else { limit });
        let cols = meta.shape[1];
        let row_bytes = cols * 4;
        self.file.seek(SeekFrom::Start(meta.address))?;
        let mut buffer = vec![0u8; rows * row_bytes];
        self.file.read_exact(&mut buffer)?;

        let mut out = Vec::with_capacity(rows);
        for row in buffer.chunks_exact(row_bytes) {
            let mut values = Vec::with_capacity(cols);
            for chunk in row.chunks_exact(4) {
                values.push(convert([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            out.push(values);
        }
        Ok(out)
    }

    fn collect_group(
        &mut self,
        oh_address: u64,
        prefix: &str,
        out: &mut Vec<(String, DatasetMeta)>,
    ) -> io::Result<()> {
        let messages = self.object_header_messages(oh_address)?;
        for (msg_type, data) in &messages {
            match *msg_type {
                // Symbol table message: v1 B-tree of SNODs + local heap.
                0x0011 => {
                    let btree = read_uint(data, 0, self.off_size);
                    let heap = read_uint(data, self.off_size, self.off_size);
                    let symbols = self.symbol_table(btree, heap)?;
                    for (name, child_oh) in symbols {
                        let full = if prefix.is_empty() {
                            name
                        } else {
                            format!("{prefix}/{name}")
                        };
                        self.collect_group(child_oh, &full, out)?;
                    }
                }
                _ => {}
            }
        }

        if let Some(meta) = self.dataset_meta(&messages)? {
            out.push((prefix.to_string(), meta));
        }
        Ok(())
    }

    fn dataset_meta(
        &self,
        messages: &[(u16, Vec<u8>)],
    ) -> io::Result<Option<DatasetMeta>> {
        let mut shape = None;
        let mut dtype = None;
        let mut address = None;

        for (msg_type, data) in messages {
            match *msg_type {
                // Dataspace: v1 -> version, rank, flags, reserved(4), dims.
                0x0001 => {
                    if data[0] != 1 {
                        return Err(invalid("only dataspace v1 is supported"));
                    }
                    let rank = data[1] as usize;
                    let mut dims = Vec::with_capacity(rank);
                    for i in 0..rank {
                        dims.push(read_uint(data, 8 + i * self.len_size, self.len_size)
                            as usize);
                    }
                    shape = Some(dims);
                }
                // Datatype: class/version, bit fields(3), size(4).
                0x0003 => {
                    if data.len() < 12 {
                        return Err(invalid("truncated datatype message"));
                    }
                    if data[1] & 1 != 0 {
                        return Err(invalid(
                            "only little-endian element types are supported",
                        ));
                    }
                    if read_uint(data, 8, 2) != 0 || read_uint(data, 10, 2) != 32 {
                        return Err(invalid(
                            "only full-width 32-bit element types are supported",
                        ));
                    }
                    let class = data[0] & 0x0f;
                    let size = read_uint(data, 4, 4);
                    if size != 4 {
                        return Err(invalid("only 4-byte element types are supported"));
                    }
                    dtype = Some(match class {
                        0 => DType::I32, // fixed-point (signed/unsigned int)
                        1 => DType::F32, // floating point
                        other => {
                            return Err(invalid(&format!(
                                "unsupported datatype class {other}"
                            )));
                        }
                    });
                }
                // Data layout: contiguous.
                0x0008 => {
                    if data[0] != 3 || data[1] != 1 {
                        return Err(invalid("only contiguous data layout is supported"));
                    }
                    address = Some(read_uint(data, 2, self.off_size));
                }
                _ => {}
            }
        }

        match (shape, dtype, address) {
            (Some(shape), Some(dtype), Some(address)) => Ok(Some(DatasetMeta {
                shape,
                dtype,
                address,
            })),
            _ => Ok(None),
        }
    }

    /// Walk the v1 group B-tree and return `(name, object_header_address)` pairs.
    fn symbol_table(&mut self, btree: u64, heap: u64) -> io::Result<Vec<(String, u64)>> {
        let heap_data = {
            let header = read_at(
                &mut self.file,
                heap,
                (8 + 2 * self.len_size + self.off_size) as u64,
            )?;
            read_uint(&header, 8 + 2 * self.len_size, self.off_size)
        };
        let mut out = Vec::new();
        self.walk_btree(btree, heap_data, &mut out)?;
        Ok(out)
    }

    fn walk_btree(
        &mut self,
        node: u64,
        heap_data: u64,
        out: &mut Vec<(String, u64)>,
    ) -> io::Result<()> {
        let head = read_at(&mut self.file, node, 8)?;
        if &head[0..4] != b"TREE" {
            return Err(invalid("bad group B-tree signature"));
        }
        let level = head[5];
        let entries = read_uint(&head, 6, 2) as usize;
        let header_len = 8 + 2 * self.off_size;
        let body = read_at(
            &mut self.file,
            node + header_len as u64,
            ((self.len_size + self.off_size) * entries) as u64,
        )?;

        let mut pos = 0usize;
        for _ in 0..entries {
            let child = read_uint(&body, pos + self.len_size, self.off_size);
            pos += self.len_size + self.off_size;
            if level == 0 {
                let snod = read_at(&mut self.file, child, 8)?;
                if &snod[0..4] != b"SNOD" {
                    return Err(invalid("bad symbol table node signature"));
                }
                let symbols = read_uint(&snod, 6, 2) as usize;
                let entry_size = 2 * self.off_size + 24;
                let entries_buf =
                    read_at(&mut self.file, child + 8, (entry_size * symbols) as u64)?;
                for i in 0..symbols {
                    let base = i * entry_size;
                    let name_offset = read_uint(&entries_buf, base, self.off_size);
                    let oh_address =
                        read_uint(&entries_buf, base + self.off_size, self.off_size);
                    let name = self.read_heap_string(heap_data, name_offset)?;
                    out.push((name, oh_address));
                }
            } else {
                self.walk_btree(child, heap_data, out)?;
            }
        }
        Ok(())
    }

    fn read_heap_string(&mut self, heap_data: u64, offset: u64) -> io::Result<String> {
        let mut bytes = Vec::new();
        let mut cursor = heap_data + offset;
        let mut buf = [0u8; 1];
        loop {
            self.file.seek(SeekFrom::Start(cursor))?;
            self.file.read_exact(&mut buf)?;
            if buf[0] == 0 {
                break;
            }
            bytes.push(buf[0]);
            cursor += 1;
        }
        String::from_utf8(bytes).map_err(|_| invalid("non-UTF8 link name"))
    }

    /// Parse an object header v1, following continuation blocks.
    fn object_header_messages(
        &mut self,
        address: u64,
    ) -> io::Result<Vec<(u16, Vec<u8>)>> {
        let header = read_at(&mut self.file, address, 16)?;
        if header[0] != 1 {
            return Err(invalid("only object header v1 is supported"));
        }
        let header_size = read_uint(&header, 8, 4) as u64;
        let mut messages = Vec::new();
        let mut pending = vec![(address + 16, header_size)];
        let mut visited = 0usize;

        while let Some((start, size)) = pending.pop() {
            visited += 1;
            if visited > 1_000 {
                return Err(invalid("object header continuation loop"));
            }
            let block = read_at(&mut self.file, start, size)?;
            let mut pos = 0usize;
            while pos + 8 <= block.len() {
                let msg_type = read_uint(&block, pos, 2) as u16;
                let msg_size = read_uint(&block, pos + 2, 2) as usize;
                let data_start = pos + 8;
                if msg_type == 0 {
                    break;
                }
                if data_start + msg_size > block.len() {
                    return Err(invalid("truncated header message"));
                }
                let data = block[data_start..data_start + msg_size].to_vec();
                if msg_type == 0x0010 {
                    let next = read_uint(&data, 0, self.off_size);
                    let next_len = read_uint(&data, self.off_size, self.len_size);
                    pending.push((next, next_len));
                }
                messages.push((msg_type, data));
                pos = data_start + ((msg_size + 7) / 8) * 8;
            }
        }
        Ok(messages)
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("HDF5: {msg}"))
}

fn read_uint(buf: &[u8], offset: usize, size: usize) -> u64 {
    let mut value = 0u64;
    for i in 0..size {
        value |= (buf[offset + i] as u64) << (8 * i);
    }
    value
}

fn read_at(file: &mut File, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}
