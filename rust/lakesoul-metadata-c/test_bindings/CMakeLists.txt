cmake_minimum_required(VERSION 3.14)
project(lakesoul-c-bindings)

# GoogleTest requires at least C++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
FetchContent_MakeAvailable(googletest)

link_directories("../../target/debug")


enable_testing()

add_executable(
  binding_test
  binding_test.cc
)
target_link_libraries(
  binding_test
  GTest::gtest_main
  lakesoul_metadata_c
)

include(GoogleTest)
gtest_discover_tests(binding_test)
