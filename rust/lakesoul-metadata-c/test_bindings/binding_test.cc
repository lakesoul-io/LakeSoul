#include "binding.h"
#include "gtest/gtest.h"
#include <iostream>

TEST(TokioRuntimeTest, GlobalTest) {
  rust_logger_init();
  char *s = debug();
  std::cout << s;
}

/* TEST(SplitDescArrayTest, HeapUseAfterFree) { */
/*   ASSERT_DEATH( */
/*       { */
/*         auto *json = create_split_desc_array(nullptr, nullptr, nullptr); */
/*         std::string js(json); */
/*         free_split_desc_array(json); */
/*         auto _x = *json; */
/*       }, */
/*       "heap-use-after-free"); */
/* } */
