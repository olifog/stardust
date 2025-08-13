#include "schemas/graph.capnp.h"
#include "server.hpp"
#include "env.hpp"
#include "store.hpp"
#include <capnp/ez-rpc.h>
#include <gtest/gtest.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <filesystem>
#include <thread>
#include <chrono>
#include <cstring>
#include <atomic>
#include <unistd.h>

namespace
{

  std::vector<float> makeDemoVec(int dim)
  {
    std::vector<float> v(dim);
    for (int i = 0; i < dim; ++i)
      v[i] = 0.001f * i;
    return v;
  }

  std::string uniqueTempPath(const std::string &stem, const std::string &ext = "")
  {
    auto base = std::filesystem::temp_directory_path() / (stem + std::to_string(::getpid()) + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto s = base.string() + ext;
    return s;
  }

  struct RpcServerThread
  {
    std::string dataDir;
    std::string bind;
    std::thread th;

    explicit RpcServerThread(std::string dataDir_, std::string bind_)
        : dataDir(std::move(dataDir_)), bind(std::move(bind_))
    {
      th = std::thread([this]()
                       {
      try {
        kj::_::Debug::setLogLevel(kj::LogSeverity::INFO);

        std::filesystem::create_directories(std::filesystem::path(dataDir));
        stardust::Env env{std::filesystem::path(dataDir)};
        stardust::Store store(env);

        const char* bindC = bind.c_str();
        if (std::strncmp(bindC, "unix:", 5) == 0) {
          const char* path = bindC + 5;
          ::unlink(path);
        }

        capnp::EzRpcServer server(kj::heap<stardust::rpc::GraphDbImpl>(store), bindC);
        auto& waitScope = server.getWaitScope();
        kj::NEVER_DONE.wait(waitScope);
      } catch (...) {
        // I LITCH DONT CARE
        KJ_LOG(ERROR, "RpcServerThread failed");
      } });
      th.detach();
    }
  };

  void waitForUnixSocketReady(const std::string &path, int maxMs = 2000)
  {
    using namespace std::chrono_literals;
    for (int i = 0; i < maxMs / 10; ++i)
    {
      if (std::filesystem::exists(path))
        return;
      std::this_thread::sleep_for(10ms);
    }
  }

} // namespace

TEST(Integration, RpcEndToEnd)
{
  const std::string dataDir = uniqueTempPath("stardust-test-db-");
  const std::string sockPath = uniqueTempPath("stardust-test-sock-", ".sock");
  const std::string bind = std::string("unix:") + sockPath;

  RpcServerThread server(dataDir, bind);
  waitForUnixSocketReady(sockPath);

  capnp::EzRpcClient client(bind.c_str());
  auto cap = client.getMain<stardust::rpc::GraphDb>();
  auto &ws = client.getWaitScope();

  // create node A with a vector, labels and props (hot + cold)
  uint64_t idA = 0;
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    // labels
    auto labels = p.initLabels();
    auto lid = labels.initLabelIds(2);
    lid.set(0, 1u);
    lid.set(1, 3u);
    // hot props
    auto hot = p.initHotProps(2);
    {
      auto pr = hot[0];
      pr.setKeyId(10u);
      auto v = pr.initVal();
      v.setI64(42);
    }
    {
      auto pr = hot[1];
      pr.setKeyId(11u);
      auto v = pr.initVal();
      v.setBoolv(true);
    }
    // cold props
    auto cold = p.initColdProps(1);
    {
      auto pr = cold[0];
      pr.setKeyId(12u);
      auto v = pr.initVal();
      const char bytes[] = "hello";
      v.setBytes(capnp::Data::Reader(reinterpret_cast<const capnp::byte *>(bytes), sizeof(bytes) - 1));
    }
    // vector
    auto v = makeDemoVec(8);
    auto vecs = p.initVectors(1);
    auto tagged = vecs[0];
    tagged.setTagId(0);
    auto vec = tagged.initVector();
    vec.setDim(8);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    auto resp = req.send().wait(ws);
    idA = resp.getResult().getNode().getId();
    ASSERT_GT(idA, 0u);
  }

  // create node B (no vectors initially)
  uint64_t idB = 0;
  {
    auto req = cap.createNodeRequest();
    auto resp = req.send().wait(ws);
    idB = resp.getResult().getNode().getId();
    ASSERT_GT(idB, 0u);
  }

  // Add edge A->B type 4.
  uint64_t edge1 = 0;
  {
    auto add = cap.addEdgeRequest();
    auto ap = add.initParams();
    ap.setSrc(idA);
    ap.setDst(idB);
    auto meta = ap.initMeta();
    meta.setTypeId(4u);
    auto resp = add.send().wait(ws);
    auto e = resp.getEdge();
    edge1 = e.getId();
    ASSERT_GT(edge1, 0u);
    EXPECT_EQ(e.getSrc(), idA);
    EXPECT_EQ(e.getDst(), idB);
  }

  // neighbors of A (OUT) should include B.
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idB)
        found = true;
    EXPECT_TRUE(found);
  }

  // neighbors of B (IN) should include A.
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idB);
    np.setDirection(stardust::rpc::Direction::IN);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idA)
        found = true;
    EXPECT_TRUE(found);
  }

  // Upsert node A props: set another hot, set cold, unset one hot.
  {
    auto req = cap.upsertNodePropsRequest();
    auto p = req.initParams();
    p.setId(idA);
    auto setHot = p.initSetHot(1);
    {
      auto pr = setHot[0];
      pr.setKeyId(13u);
      auto v = pr.initVal();
      v.setF64(3.14);
    }
    auto setCold = p.initSetCold(1);
    {
      auto pr = setCold[0];
      pr.setKeyId(14u);
      auto v = pr.initVal();
      v.setTextId(123u);
    }
    auto unset = p.initUnsetKeys(1);
    unset.set(0, 11u); // remove the bool hot prop
    req.send().wait(ws);
  }

  // Get node A header and props; check labels and hot prop presence.
  {
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idA);
    auto resp = gn.send().wait(ws);
    auto hdr = resp.getResult().getHeader();
    EXPECT_EQ(hdr.getId(), idA);
    EXPECT_EQ(hdr.getLabels().getLabelIds().size(), 2u);
    // hot props should be 2 now (10,13)
    EXPECT_EQ(hdr.getHotProps().size(), 2u);
  }
  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);
    // empty keyIds => all props (hot + cold)
    auto resp = gp.send().wait(ws);
    auto all = resp.getResult().getProps();
    ASSERT_GE(all.size(), 3u); // 10,12,13,14 present (order unspecified)
  }
  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);
    auto ks = pp.initKeyIds(2);
    ks.set(0, 10u);
    ks.set(1, 14u);
    auto resp = gp.send().wait(ws);
    auto some = resp.getResult().getProps();
    EXPECT_EQ(some.size(), 2u);
  }

  // Set labels on B and verify.
  {
    auto sl = cap.setNodeLabelsRequest();
    auto sp = sl.initParams();
    sp.setId(idB);
    auto add = sp.initAddLabels(2);
    add.set(0, 7u);
    add.set(1, 9u);
    sl.send().wait(ws);
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idB);
    auto resp = gn.send().wait(ws);
    auto hdr = resp.getResult().getHeader();
    EXPECT_EQ(hdr.getLabels().getLabelIds().size(), 2u);
  }

  // Upsert vector on B tag 1, then getVectors (all and specific), then delete.
  {
    auto up = cap.upsertVectorRequest();
    auto p = up.initParams();
    p.setId(idB);
    p.setTagId(1u);
    auto v = makeDemoVec(4);
    auto vec = p.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    up.send().wait(ws);

    // all vectors
    {
      auto gv = cap.getVectorsRequest();
      auto gp = gv.initParams();
      gp.setId(idB);
      auto resp = gv.send().wait(ws);
      auto res = resp.getResult().getVectors();
      ASSERT_EQ(res.size(), 1u);
      EXPECT_EQ(res[0].getTagId(), 1u);
      EXPECT_EQ(res[0].getVector().getData().size(), v.size() * 4);
    }
    // specific tagIds
    {
      auto gv = cap.getVectorsRequest();
      auto gp = gv.initParams();
      gp.setId(idB);
      auto tags = gp.initTagIds(1);
      tags.set(0, 1u);
      auto resp = gv.send().wait(ws);
      auto res = resp.getResult().getVectors();
      ASSERT_EQ(res.size(), 1u);
      EXPECT_EQ(res[0].getTagId(), 1u);
    }
    // delete
    {
      auto del = cap.deleteVectorRequest();
      auto dp = del.initParams();
      dp.setId(idB);
      dp.setTagId(1u);
      del.send().wait(ws);
      auto gv = cap.getVectorsRequest();
      gv.initParams().setId(idB);
      auto resp = gv.send().wait(ws);
      auto res = resp.getResult().getVectors();
      EXPECT_EQ(res.size(), 0u);
    }
  }

  // Add second edge A->B type 7. Test relType filter and Both dedupe.
  uint64_t edge2 = 0;
  {
    auto add = cap.addEdgeRequest();
    auto ap = add.initParams();
    ap.setSrc(idA);
    ap.setDst(idB);
    ap.initMeta().setTypeId(7u);
    auto resp = add.send().wait(ws);
    edge2 = resp.getEdge().getId();
    ASSERT_GT(edge2, 0u);
  }
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto rel = np.initRelTypeIn(1);
    rel.set(0, 7u);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idB)
        found = true;
    EXPECT_TRUE(found);
  }
  // neighborHas filter: require label 9 (present) and 99 (absent).
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto has = np.initNeighborHas();
    auto ids = has.initLabelIds(1);
    ids.set(0, 9u);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idB)
        found = true;
    EXPECT_TRUE(found);
  }
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto has = np.initNeighborHas();
    auto ids = has.initLabelIds(1);
    ids.set(0, 99u);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idB)
        found = true;
    EXPECT_FALSE(found);
  }

  // Update edge props on edge1 (no readback API, just exercise path).
  {
    auto upd = cap.updateEdgePropsRequest();
    auto up = upd.initParams();
    up.setEdgeId(edge1);
    auto set = up.initSetProps(1);
    set[0].setKeyId(55u);
    set[0].initVal().setI64(7);
    auto unset = up.initUnsetKeys(1);
    unset.set(0, 56u);
    upd.send().wait(ws);
  }

  // Batch write: create node C, label it, add vector, add edge B->C.
  uint64_t idC = 0;
  {
    auto wr = cap.writeBatchRequest();
    auto batch = wr.initBatch();
    auto ops = batch.initOps(4);
    // 0: createNode
    {
      auto op = ops[0].initCreateNode();
      auto ls = op.initLabels();
      auto ids = ls.initLabelIds(1);
      ids.set(0, 21u);
      auto v = makeDemoVec(2);
      auto vecs = op.initVectors(1);
      auto tv = vecs[0];
      tv.setTagId(2u);
      auto vv = tv.initVector();
      vv.setDim(2);
      capnp::Data::Builder data = vv.initData(v.size() * 4);
      std::memcpy(data.begin(), v.data(), v.size() * 4);
    }
    // 1: upsertNodeProps (on A)
    {
      auto op = ops[1].initUpsertNodeProps();
      op.setId(idA);
      auto set = op.initSetHot(1);
      set[0].setKeyId(99u);
      set[0].initVal().setBoolv(false);
    }
    // 2: setNodeLabels (on B)
    {
      auto op = ops[2].initSetNodeLabels();
      op.setId(idB);
      auto add = op.initAddLabels(1);
      add.set(0, 22u);
    }
    // 3: addEdge (B->C) type 8
    {
      auto op = ops[3].initAddEdge();
      op.setSrc(idB);
      // We don't know idC yet; it will be created by op0. After batch, we'll discover C by scanning neighbors of B.
      // Instead, add the edge after the batch once we know C.
      op.setDst(0);
      op.initMeta().setTypeId(8u);
    }
    wr.send().wait(ws);
  }
  // Create C now properly and connect B->C.
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    auto ls = p.initLabels();
    auto ids = ls.initLabelIds(1);
    ids.set(0, 21u);
    auto resp = req.send().wait(ws);
    auto node = resp.getResult().getNode();
    idC = node.getId();
    ASSERT_GT(idC, 0u);

    auto add = cap.addEdgeRequest();
    auto ap = add.initParams();
    ap.setSrc(idB);
    ap.setDst(idC);
    ap.initMeta().setTypeId(8u);
    add.send().wait(ws);
  }
  // Verify neighbors of B include C.
  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idB);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idC)
        found = true;
    EXPECT_TRUE(found);
  }

  // Delete edges A->B and verify neighbors update.
  {
    auto del = cap.deleteEdgeRequest();
    del.initParams().setEdgeId(edge1);
    del.send().wait(ws);
    auto del2 = cap.deleteEdgeRequest();
    del2.initParams().setEdgeId(edge2);
    del2.send().wait(ws);
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    bool found = false;
    for (auto x : list)
      if (x == idB)
        found = true;
    EXPECT_FALSE(found);
  }

  // Delete node B and ensure neighbors from B are empty.
  {
    auto dn = cap.deleteNodeRequest();
    dn.initParams().setId(idB);
    dn.send().wait(ws);
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idB);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();
    EXPECT_EQ(list.size(), 0u);
  }

  // KNN currently returns empty. Query against tag 0.
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTagId(0u);
    auto q = makeDemoVec(8);
    auto vec = kp.initQuery();
    vec.setDim(8);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(3);
    auto resp = knn.send().wait(ws);
    auto res = resp.getResult();
    EXPECT_EQ(res.getHits().size(), 0u);
  }
}
