use arrow::array::BinaryArray;
fn main(){
    let values: Vec<&[u8]> =
    vec![b"one", b"two", b"", b"three"];
    let array = BinaryArray::from_vec(values);
    println!("{:?}",array);
}