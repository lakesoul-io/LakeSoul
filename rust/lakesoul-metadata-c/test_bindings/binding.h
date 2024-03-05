#include <cstdint>

template <typename OpaqueT> struct Result {
  OpaqueT *ptr;
  const char *err;
};

struct TokioRuntime {
  uint8_t private_[0];
};

extern "C" {
char *create_split_desc_array(const char *, const char *,
                              Result<TokioRuntime> *runtime);

void free_split_desc_array(char *);

char *debug();

void rust_logger_init();
}
