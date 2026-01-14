use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{collections::VecDeque, hint::black_box};

fn buf_with_deque(data: &[u8], capacity: usize, chunk_size: usize) {
    let mut buf: VecDeque<u8> = VecDeque::with_capacity(capacity);
    let mut res = Vec::new();
    for c in data.chunks(chunk_size) {
        buf.extend(c);
        let b = Bytes::from(buf.drain(..).collect::<Vec<u8>>());
        res.push(b);
    }
}

fn buf_with_bytes_mut(data: &[u8], capacity: usize, chunk_size: usize) {
    let mut buf = BytesMut::with_capacity(capacity);
    let mut res = Vec::new();
    for c in data.chunks(chunk_size) {
        buf.extend_from_slice(c);
        let b = buf.split().freeze();
        res.push(b);
    }
}

fn bench_buffer(c: &mut Criterion) {
    let data = vec![8u8; 102400]; // 1KB 数据块
    let mut group = c.benchmark_group("Buffer_Comparison");

    // 测试 VecDeque
    group.bench_function("VecDeque", |b| {
        b.iter(|| buf_with_deque(black_box(&data), black_box(4096), black_box(1024)))
    });

    // 测试 BytesMut
    group.bench_function("BytesMut", |b| {
        b.iter(|| buf_with_bytes_mut(black_box(&data), black_box(4096), black_box(1024)))
    });

    group.finish();
}

criterion_group!(benches, bench_buffer);
criterion_main!(benches);
