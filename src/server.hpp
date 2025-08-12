#pragma once
#include "store.hpp"
#include "schemas/graph.capnp.h"
#include <capnp/ez-rpc.h>
#include <kj/async-io.h>

namespace stardust::rpc {

class GraphDbImpl final : public GraphDb::Server {
public:
  explicit GraphDbImpl(stardust::Store& s);

  kj::Promise<void> createNode(CreateNodeContext ctx) override;
  kj::Promise<void> addEdge(AddEdgeContext ctx) override;
  kj::Promise<void> neighbors(NeighborsContext ctx) override;
  kj::Promise<void> knn(KnnContext ctx) override;

private:
  stardust::Store& store_;
};

} // namespace stardust::rpc


