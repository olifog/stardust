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
  kj::Promise<void> upsertNodeProps(UpsertNodePropsContext ctx) override;
  kj::Promise<void> setNodeLabels(SetNodeLabelsContext ctx) override;
  kj::Promise<void> upsertVector(UpsertVectorContext ctx) override;
  kj::Promise<void> deleteVector(DeleteVectorContext ctx) override;
  kj::Promise<void> addEdge(AddEdgeContext ctx) override;
  kj::Promise<void> updateEdgeProps(UpdateEdgePropsContext ctx) override;
  kj::Promise<void> neighbors(NeighborsContext ctx) override;
  kj::Promise<void> knn(KnnContext ctx) override;
  kj::Promise<void> writeBatch(WriteBatchContext ctx) override;

  kj::Promise<void> getNode(GetNodeContext ctx) override;
  kj::Promise<void> getNodeProps(GetNodePropsContext ctx) override;
  kj::Promise<void> getVectors(GetVectorsContext ctx) override;
  kj::Promise<void> getEdge(GetEdgeContext ctx) override;

  kj::Promise<void> deleteNode(DeleteNodeContext ctx) override;
  kj::Promise<void> deleteEdge(DeleteEdgeContext ctx) override;

private:
  stardust::Store& store_;
};

} // namespace stardust::rpc


