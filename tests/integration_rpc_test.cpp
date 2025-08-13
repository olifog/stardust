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

class IntegrationRpc : public ::testing::Test
{
protected:
  static std::string dataDir;
  static std::string sockPath;
  static std::string bind;
  static std::unique_ptr<RpcServerThread> server;
  static std::unique_ptr<capnp::EzRpcClient> client;
  static uint64_t idA, idB, idC, edge1, edge2;

  static void SetUpTestSuite()
  {
    dataDir = uniqueTempPath("stardust-test-db-");
    sockPath = uniqueTempPath("stardust-test-sock-", ".sock");
    bind = std::string("unix:") + sockPath;
    server = std::make_unique<RpcServerThread>(dataDir, bind);
    waitForUnixSocketReady(sockPath);
    client = std::make_unique<capnp::EzRpcClient>(bind.c_str());
  }

  static void TearDownTestSuite()
  {
    client.reset();
    server.reset();
  }
};

std::string IntegrationRpc::dataDir;
std::string IntegrationRpc::sockPath;
std::string IntegrationRpc::bind;
std::unique_ptr<RpcServerThread> IntegrationRpc::server;
std::unique_ptr<capnp::EzRpcClient> IntegrationRpc::client;
uint64_t IntegrationRpc::idA = 0;
uint64_t IntegrationRpc::idB = 0;
uint64_t IntegrationRpc::idC = 0;
uint64_t IntegrationRpc::edge1 = 0;
uint64_t IntegrationRpc::edge2 = 0;

TEST_F(IntegrationRpc, Step01_CreateNodeA)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

  auto req = cap.createNodeRequest();
  auto p = req.initParams();
  auto labels = p.initLabels();
  auto lid = labels.initLabelIds(2);
  lid.set(0, 1u);
  lid.set(1, 3u);

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

  auto cold = p.initColdProps(1);
  {
    auto pr = cold[0];
    pr.setKeyId(12u);
    auto v = pr.initVal();
    const char bytes[] = "hello";
    v.setBytes(capnp::Data::Reader(reinterpret_cast<const capnp::byte *>(bytes), sizeof(bytes) - 1));
  }

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

TEST_F(IntegrationRpc, Step02_CreateNodeB)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();
  auto req = cap.createNodeRequest();

  auto resp = req.send().wait(ws);
  idB = resp.getResult().getNode().getId();

  ASSERT_GT(idB, 0u);
}

TEST_F(IntegrationRpc, Step03_AddEdge_A_to_B_Type4)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

TEST_F(IntegrationRpc, Step04_NeighborsOfA_OUT_IncludeB)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
  EXPECT_EQ(list.size(), 1u);
}

TEST_F(IntegrationRpc, Step05_NeighborsOfB_IN_IncludeA)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
  EXPECT_EQ(list.size(), 1u);
}

TEST_F(IntegrationRpc, Step06_UpsertNodeAProps)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
  unset.set(0, 11u);

  req.send().wait(ws);
}

TEST_F(IntegrationRpc, Step07_GetNodeAHeaderAndProps)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

  {
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idA);

    auto resp = gn.send().wait(ws);
    auto hdr = resp.getResult().getHeader();

    EXPECT_EQ(hdr.getId(), idA);
    EXPECT_EQ(hdr.getLabels().getLabelIds().size(), 2u);
    EXPECT_EQ(hdr.getHotProps().size(), 2u);

    {
      auto lids = hdr.getLabels().getLabelIds();
      bool has1 = false, has3 = false;
      for (auto v : lids) { if (v == 1u) has1 = true; if (v == 3u) has3 = true; }

      EXPECT_TRUE(has1);
      EXPECT_TRUE(has3);
    }

    {
      auto hp = hdr.getHotProps();
      bool has10 = false, has13 = false;
      for (auto p : hp) { if (p.getKeyId() == 10u) has10 = true; if (p.getKeyId() == 13u) has13 = true; }

      EXPECT_TRUE(has10);
      EXPECT_TRUE(has13);
    }
  }

  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);

    auto resp = gp.send().wait(ws);
    auto all = resp.getResult().getProps();

    ASSERT_GE(all.size(), 3u);

    bool has10 = false, has12 = false, has13 = false, has14 = false, has11 = false;
    for (auto p : all) {
      switch (p.getKeyId()) {
        case 10u: has10 = true; break;
        case 11u: has11 = true; break;
        case 12u: has12 = true; break;
        case 13u: has13 = true; break;
        case 14u: has14 = true; break;
        default: break;
      }
    }
    EXPECT_TRUE(has10);
    EXPECT_TRUE(has12);
    EXPECT_TRUE(has13);
    EXPECT_TRUE(has14);
    EXPECT_FALSE(has11);
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

    bool has10 = false, has14 = false;
    for (auto p : some) { if (p.getKeyId() == 10u) has10 = true; if (p.getKeyId() == 14u) has14 = true; }
    EXPECT_TRUE(has10);
    EXPECT_TRUE(has14);
  }
}

TEST_F(IntegrationRpc, Step08_SetLabelsOnBAndVerify)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

  {
    auto lids = hdr.getLabels().getLabelIds();
    bool has7 = false, has9 = false;
    for (auto v : lids) { if (v == 7u) has7 = true; if (v == 9u) has9 = true; }
    EXPECT_TRUE(has7);
    EXPECT_TRUE(has9);
  }
}

TEST_F(IntegrationRpc, Step09_VectorsOnB_AddGetDelete)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

  {
    auto gv = cap.getVectorsRequest();
    auto gp = gv.initParams();
    gp.setId(idB);

    auto resp = gv.send().wait(ws);
    auto res = resp.getResult().getVectors();

    ASSERT_EQ(res.size(), 1u);
    EXPECT_EQ(res[0].getTagId(), 1u);
    EXPECT_EQ(res[0].getVector().getData().size(), v.size() * 4);

    auto data = res[0].getVector().getData();
    ASSERT_EQ(data.size(), v.size() * 4);
    EXPECT_EQ(std::memcmp(data.begin(), v.data(), v.size() * 4), 0);
  }

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

TEST_F(IntegrationRpc, Step10_AddSecondEdgeAtoB_Type7_AndFilters)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
    {
      auto req = cap.neighborsRequest();
      auto np = req.initParams();
      np.setNode(idA);
      np.setDirection(stardust::rpc::Direction::OUT);
      np.setLimit(16);

      auto resp = req.send().wait(ws);
      EXPECT_EQ(resp.getResult().getNeighbors().size(), 2u);
    }

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

  {
    auto req = cap.neighborsRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto rel = np.initRelTypeIn(1);
    rel.set(0, 4u);
    auto resp = req.send().wait(ws);
    auto list = resp.getResult().getNeighbors();

    bool found = false;
    for (auto x : list) if (x == idB) found = true;
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
}

TEST_F(IntegrationRpc, Step11_UpdateEdgeProps_OnEdge1)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

  auto upd = cap.updateEdgePropsRequest();
  auto up = upd.initParams();
  up.setEdgeId(edge1);
  auto set = up.initSetProps(1);
  set[0].setKeyId(55u);
  set[0].initVal().setI64(7);
  auto unset = up.initUnsetKeys(1);
  unset.set(0, 56u);

  upd.send().wait(ws);

  {
    auto ge = cap.getEdgeRequest();
    ge.initParams().setEdgeId(edge1);
    auto resp = ge.send().wait(ws);
    auto e = resp.getEdge();

    EXPECT_EQ(e.getId(), edge1);
    EXPECT_EQ(e.getSrc(), idA);
    EXPECT_EQ(e.getDst(), idB);
  }
}

TEST_F(IntegrationRpc, Step12_BatchWrite)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

  auto wr = cap.writeBatchRequest();
  auto batch = wr.initBatch();
  auto ops = batch.initOps(4);

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

  {
    auto op = ops[1].initUpsertNodeProps();
    op.setId(idA);
    auto set = op.initSetHot(1);
    set[0].setKeyId(99u);
    set[0].initVal().setBoolv(false);
  }

  {
    auto op = ops[2].initSetNodeLabels();
    op.setId(idB);
    auto add = op.initAddLabels(1);
    add.set(0, 22u);
  }

  {
    auto op = ops[3].initAddEdge();
    op.setSrc(idB);
    op.setDst(0);
    op.initMeta().setTypeId(8u);
  }

  wr.send().wait(ws);

  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);
    auto keys = pp.initKeyIds(1);
    keys.set(0, 99u);
    auto r = gp.send().wait(ws);
    auto ps = r.getResult().getProps();

    EXPECT_EQ(ps.size(), 1u);
    EXPECT_EQ(ps[0].getKeyId(), 99u);
  }
  {
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idB);
    auto r = gn.send().wait(ws);
    auto lids = r.getResult().getHeader().getLabels().getLabelIds();

    bool has22 = false;
    for (auto v : lids) if (v == 22u) has22 = true;
    EXPECT_TRUE(has22);
  }
}

TEST_F(IntegrationRpc, Step13_CreateC_AndConnect_B_to_C)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

TEST_F(IntegrationRpc, Step14_VerifyNeighborsOfBIncludeC)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

  {
    auto rq = cap.neighborsRequest();
    auto np2 = rq.initParams();
    np2.setNode(idB);
    np2.setDirection(stardust::rpc::Direction::OUT);
    np2.setLimit(16);
    auto rel = np2.initRelTypeIn(1);
    rel.set(0, 8u);
    auto r = rq.send().wait(ws);

    bool hasC = false; for (auto x : r.getResult().getNeighbors()) if (x == idC) hasC = true;
    EXPECT_TRUE(hasC);
  }
  {
    auto rq = cap.neighborsRequest();
    auto np2 = rq.initParams();
    np2.setNode(idB);
    np2.setDirection(stardust::rpc::Direction::OUT);
    np2.setLimit(16);
    auto rel = np2.initRelTypeIn(1);
    rel.set(0, 7u);
    auto r = rq.send().wait(ws);
    
    bool hasC = false; for (auto x : r.getResult().getNeighbors()) if (x == idC) hasC = true;
    EXPECT_FALSE(hasC);
  }
}

TEST_F(IntegrationRpc, Step15_DeleteEdgesAtoB_AndVerify)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
  EXPECT_EQ(list.size(), 0u);
}

TEST_F(IntegrationRpc, Step16_DeleteNodeB_AndEnsureNoNeighbors)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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

TEST_F(IntegrationRpc, Step17_Knn_Empty)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::GraphDb>();

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
