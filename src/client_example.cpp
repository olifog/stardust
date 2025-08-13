#include "schemas/graph.capnp.h"
#include <capnp/ez-rpc.h>
#include <kj/debug.h>
#include <iostream>
#include <vector>
#include <cstring>
#include <sstream>

static std::vector<float> demoVec(int dim)
{
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i)
    v[i] = 0.001f * i;
  return v;
}

static std::string vecToString(const std::vector<float>& v)
{
  std::ostringstream oss;
  oss << "[";
  for (size_t i = 0; i < v.size(); ++i)
  {
    if (i) oss << ", ";
    oss << v[i];
  }
  oss << "]";
  return oss.str();
}

int main(int argc, char **argv)
{
  const char *addr = (argc > 1) ? argv[1] : "unix:/tmp/stardust.sock";
  capnp::EzRpcClient client(addr);
  auto cap = client.getMain<stardust::rpc::GraphDb>();

  // create two nodes
  uint64_t idA = 0;
  {
    auto req = cap.createNodeRequest();
    auto params = req.initParams();
    auto v = demoVec(8);
    auto vecs = params.initVectors(1);
    auto tagged = vecs[0];
    tagged.setTagId(0);
    auto vec = tagged.initVector();
    vec.setDim(8);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    auto resp = req.send().wait(client.getWaitScope());
    idA = resp.getResult().getNode().getId();
    std::cout << "node A\n";
    std::cout << "\tid=" << idA << "\n";
    std::cout << "\tvector=" << vecToString(v) << "\n";
  }
  uint64_t idB = 0;
  {
    auto req = cap.createNodeRequest();
    auto resp = req.send().wait(client.getWaitScope());
    idB = resp.getResult().getNode().getId();
    std::cout << "node B\n";
    std::cout << "\tid=" << idB << "\n";

    // Add edge A->B
    auto add = cap.addEdgeRequest();
    auto ap = add.initParams();
    ap.setSrc(idA);
    ap.setDst(idB);
    auto meta = ap.initMeta();
    meta.setTypeId(0);
    add.send().wait(client.getWaitScope());
  }

  // neighbors of A
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(client.getWaitScope());
    auto list = resp.getResult().getNeighbors();
    std::cout << "neighbors of " << idA << ": ";
    for (uint32_t i = 0; i < list.size(); ++i)
      std::cout << list[i] << " ";
    std::cout << "\n";
  }
}
