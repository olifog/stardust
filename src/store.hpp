#pragma once
#include "env.hpp"
#include "encode.hpp"
#include <vector>
#include <optional>

namespace stardust
{

  struct EdgeMeta
  {
    uint32_t typeId{0};
    float weight{1.0f};
  };

  class Store
  {
  public:
    explicit Store(Env &e) : env_(e) {}

    uint64_t createNode(std::optional<std::pair<uint16_t, std::string_view>> vectorOpt);
    void addEdge(uint64_t src, uint64_t dst, EdgeMeta m);
    std::vector<uint64_t> neighbors(uint64_t node, bool in, uint32_t limit);

  private:
    Env &env_;
  };

} // namespace stardust
