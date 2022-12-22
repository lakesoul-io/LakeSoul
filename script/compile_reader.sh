cd /home/huazeng/Git/LakeSoul/native-io/lakesoul-io-c
git pull
cargo +nightly build --release
cp ../target/release/liblakesoul_io_c.so $LakeSoulLib