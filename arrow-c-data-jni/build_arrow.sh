cd arrow/cpp
mkdir build   # from inside the `cpp` subdirectory
cd build
cmake .. --preset ninja-debug-minimal
cmake --build .
