#include "server.hpp"
#include "encode.hpp"
#include <kj/debug.h>

namespace stardust::rpc {

GraphDbImpl::GraphDbImpl(stardust::Store& s) : store_(s) {}

  kj::Promise<void> GraphDbImpl::createNode(CreateNodeContext ctx) {
    auto p = ctx.getParams();
    auto params = p.getParams();
    KJ_LOG(INFO, "rpc.createNode: hasVector=", params.hasVector());

    std::optional<std::pair<uint16_t, std::string_view>> vec;
    if (params.hasVector()) {
      auto v = params.getVector();
      auto data = v.getData();
      KJ_LOG(INFO, "rpc.createNode.vector: dim=", v.getDim(), ", bytes=", data.size());

      vec = std::make_pair(
        v.getDim(),
        std::string_view(reinterpret_cast<const char*>(data.begin()), data.size())
      );
    }

    auto id = store_.createNode(vec);
    KJ_LOG(INFO, "rpc.createNode: created id=", id);

    auto res = ctx.getResults();
    auto out = res.initResult();
    auto node = out.initNode();
    node.setId(id);

    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::addEdge(AddEdgeContext ctx) {
    auto p = ctx.getParams();
    auto params = p.getParams();

    stardust::EdgeMeta meta{ params.getMeta().getTypeId(), params.getMeta().getWeight() };
    KJ_LOG(INFO, "rpc.addEdge: src=", params.getSrc(), ", dst=", params.getDst(), 
           ", typeId=", meta.typeId, ", weight=", meta.weight);
    store_.addEdge(params.getSrc(), params.getDst(), meta);
  
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::neighbors(NeighborsContext ctx) {
    auto p = ctx.getParams();
    auto params = p.getParams();
    bool in = (params.getDirection() == 1);
    KJ_LOG(INFO, "rpc.neighbors: node=", params.getNode(), 
           ", direction=", (in ? "in" : "out"), ", limit=", params.getLimit());
    
    auto vec = store_.neighbors(params.getNode(), in, params.getLimit());
    KJ_LOG(INFO, "rpc.neighbors: resultCount=", vec.size());

    auto res = ctx.getResults();
    auto out = res.initResult();
    auto list = out.initNeighbors(vec.size());
    for (size_t i = 0; i < vec.size(); ++i) list.set(i, vec[i]);

    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::knn(KnnContext ctx) {
    KJ_LOG(INFO, "rpc.knn: called");
    // TODO: brute-force scan, empty result for now
    auto res = ctx.getResults();
    auto out = res.initResult();
    out.initHits(0);
    return kj::READY_NOW;
  }

} // namespace stardust::rpc
