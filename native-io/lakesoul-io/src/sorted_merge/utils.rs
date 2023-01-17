use arrow_buffer::MutableBuffer;
use arrow_buffer::bit_util;
use arrow_schema::DataType;

pub(crate) fn resize_for_bits(buffer: &mut MutableBuffer, len: usize) {
    let needed_bytes = bit_util::ceil(len, 8);
    if buffer.len() < needed_bytes {
        buffer.resize(needed_bytes, 0);
    }
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    std::slice::from_raw_parts(
        (p as *const T) as *const u8,
        ::std::mem::size_of::<T>(),
    )
}

pub(crate) fn get_default_value(dt: &DataType) -> &[u8] {
    let item = match dt {
        DataType::UInt8 => unsafe{ any_as_u8_slice(&(0 as u8)) },
        DataType::UInt16 => unsafe{ any_as_u8_slice(&(0 as u16)) },
        DataType::UInt32 => unsafe{ any_as_u8_slice(&(0 as u32)) },
        DataType::UInt64 => unsafe{ any_as_u8_slice(&(0 as u64)) },
        DataType::Int8 => unsafe{ any_as_u8_slice(&(0 as i8)) },
        DataType::Int16 => unsafe{ any_as_u8_slice(&(0 as i16)) },
        DataType::Int32 => unsafe{ any_as_u8_slice(&(0 as i32)) },
        DataType::Int64 => unsafe{ any_as_u8_slice(&(0 as i64)) },
        _ => panic!("Unsupported DataType: {}", dt)
    };
    item
}
