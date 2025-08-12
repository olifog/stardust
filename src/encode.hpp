#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace stardust {

inline void put_be64(std::string& s, uint64_t x) {
  for (int i = 7; i >= 0; --i) s.push_back(char((x >> (i*8)) & 0xff));
}

inline std::string key_node(uint64_t id) {
  std::string k; k.reserve(1+8);
  k.push_back('N'); put_be64(k, id); return k;
}

inline std::string key_vec(uint64_t id) {
  std::string k; k.reserve(1+8);
  k.push_back('V'); put_be64(k, id); return k;
}

inline std::string key_edge_out(uint64_t src, uint64_t dst) {
  std::string k; k.reserve(1+16);
  k.push_back('E'); put_be64(k, src); put_be64(k, dst); return k;
}

inline std::string key_edge_in(uint64_t dst, uint64_t src) {
  std::string k; k.reserve(1+16);
  k.push_back('R'); put_be64(k, dst); put_be64(k, src); return k;
}

inline std::string key_counter_nodes() {
  return std::string("C:N");
}

} // namespace stardust
