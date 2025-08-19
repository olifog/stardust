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

        capnp::EzRpcServer server(kj::heap<stardust::rpc::StardustImpl>(store), bindC);
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
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.createNodeRequest();
  auto p = req.initParams();
  auto labels = p.initLabels();
  auto lnames = labels.initNames(2);
  lnames.set(0, "nodelabel-a");
  lnames.set(1, "nodelabel-c");

  auto hot = p.initHotProps(2);
  {
    auto pr = hot[0];
    pr.setKey("hotprop-a");
    auto v = pr.initVal();
    v.setI64(42);
  }
  {
    auto pr = hot[1];
    pr.setKey("hotprop-b");
    auto v = pr.initVal();
    v.setBoolv(true);
  }

  auto cold = p.initColdProps(1);
  {
    auto pr = cold[0];
    pr.setKey("coldprop-c");
    auto v = pr.initVal();
    const char bytes[] = "hello";
    v.setBytes(capnp::Data::Reader(reinterpret_cast<const capnp::byte *>(bytes), sizeof(bytes) - 1));
  }

  auto v = makeDemoVec(8);
  auto vecs = p.initVectors(1);
  auto tagged = vecs[0];
  tagged.setTag("vec-a");
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
  auto cap = client->getMain<stardust::rpc::Stardust>();
  auto req = cap.createNodeRequest();

  auto resp = req.send().wait(ws);
  idB = resp.getResult().getNode().getId();

  ASSERT_GT(idB, 0u);
}

TEST_F(IntegrationRpc, Step03_AddEdge_A_to_B_Type4)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto add = cap.addEdgeRequest();
  auto ap = add.initParams();
  ap.setSrc(idA);
  ap.setDst(idB);

  auto meta = ap.initMeta();
  meta.setType("edgetype-a");

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
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.listAdjacencyRequest();
  auto np = req.initParams();
  np.setNode(idA);
  np.setDirection(stardust::rpc::Direction::OUT);
  np.setLimit(16);

  auto resp = req.send().wait(ws);
  auto items = resp.getResult().getItems();

  bool found = false;
  for (auto row : items)
    if (row.getNeighbor() == idB)
      found = true;
  EXPECT_TRUE(found);
  EXPECT_EQ(items.size(), 1u);
}

TEST_F(IntegrationRpc, Step05_NeighborsOfB_IN_IncludeA)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.listAdjacencyRequest();
  auto np = req.initParams();
  np.setNode(idB);
  np.setDirection(stardust::rpc::Direction::IN);
  np.setLimit(16);

  auto resp = req.send().wait(ws);
  auto items = resp.getResult().getItems();

  bool found = false;
  for (auto row : items)
    if (row.getNeighbor() == idA)
      found = true;
  EXPECT_TRUE(found);
  EXPECT_EQ(items.size(), 1u);
}

TEST_F(IntegrationRpc, Step06_UpsertNodeAProps)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.upsertNodePropsRequest();
  auto p = req.initParams();
  p.setId(idA);

  auto setHot = p.initSetHot(2);
  {
    auto pr = setHot[0];
    pr.setKey("hotprop-a");
    auto v = pr.initVal();
    v.setF64(3.14);
  }
  {
    auto pr = setHot[1];
    pr.setKey("hotprop-c");
    auto v = pr.initVal();
    v.setBoolv(false);
  }

  auto setCold = p.initSetCold(2);
  {
    auto pr = setCold[0];
    pr.setKey("coldprop-c");
    auto v = pr.initVal();
    v.setText("cold-text-c");
  }
  {
    auto pr = setCold[1];
    pr.setKey("bin-prop");
    auto v = pr.initVal();
    const capnp::byte raw[] = {0xff, 0xfe, 0x00, 0xff};
    v.setBytes(capnp::Data::Reader(raw, sizeof(raw)));
  }

  auto unset = p.initUnsetKeys(1);
  unset.set(0, "hotprop-b");

  req.send().wait(ws);
}

TEST_F(IntegrationRpc, Step07_GetNodeAHeaderAndProps)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  {
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idA);

    auto resp = gn.send().wait(ws);
    auto hdr = resp.getResult().getHeader();

    EXPECT_EQ(hdr.getId(), idA);
    EXPECT_EQ(hdr.getLabels().getNames().size(), 2u);
    EXPECT_EQ(hdr.getHotProps().size(), 2u);

    {
      auto lnames = hdr.getLabels().getNames();

      bool hasA = false, hasC = false;
      for (auto v : lnames) {
        if (strcmp(v.cStr(), "nodelabel-a") == 0)
          hasA = true;
        if (strcmp(v.cStr(), "nodelabel-c") == 0)
          hasC = true;
      }

      EXPECT_TRUE(hasA);
      EXPECT_TRUE(hasC);
    }

    {
      auto hp = hdr.getHotProps();
      bool hasA = false, hasB = false;
      for (auto p : hp) {
        if (strcmp(p.getKey().cStr(), "hotprop-a") == 0)
          hasA = true;
        if (strcmp(p.getKey().cStr(), "hotprop-b") == 0)
          hasB = true;
      }

      EXPECT_TRUE(hasA);
      EXPECT_FALSE(hasB);
    }
  }

  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);

    auto resp = gp.send().wait(ws);
    auto all = resp.getResult().getProps();

    ASSERT_GE(all.size(), 4u);

    bool hasA = false, hasB = false, hasC = false, hasColdC = false, hasRandom = false, hasBin = false;
    bool hotAF64Ok = false, hotCBoolOk = false, coldCTextOk = false, binBytesOk = false;
    for (auto p : all)
    {
      const auto key = p.getKey().cStr();
      if (strcmp(key, "hotprop-a") == 0)
      {
        hasA = true;
        auto val = p.getVal();
        EXPECT_EQ(val.which(), stardust::rpc::Value::F64);
        if (val.which() == stardust::rpc::Value::F64) hotAF64Ok = (val.getF64() == 3.14);
      }
      else if (strcmp(key, "hotprop-b") == 0)
        hasB = true;
      else if (strcmp(key, "coldprop-c") == 0)
      {
        hasC = true;
        auto val = p.getVal();
        EXPECT_EQ(val.which(), stardust::rpc::Value::TEXT);
        if (val.which() == stardust::rpc::Value::TEXT)
          coldCTextOk = (strcmp(val.getText().cStr(), "cold-text-c") == 0);
      }
      else if (strcmp(key, "hotprop-c") == 0)
      {
        hasColdC = true;
        auto val = p.getVal();
        EXPECT_EQ(val.which(), stardust::rpc::Value::BOOLV);
        if (val.which() == stardust::rpc::Value::BOOLV) hotCBoolOk = (val.getBoolv() == false);
      }
      else if (strcmp(key, "bin-prop") == 0)
      {
        hasBin = true;
        auto val = p.getVal();
        EXPECT_EQ(val.which(), stardust::rpc::Value::BYTES);
        if (val.which() == stardust::rpc::Value::BYTES)
        {
          auto d = val.getBytes();
          const capnp::byte exp[] = {0xff, 0xfe, 0x00, 0xff};
          binBytesOk = (d.size() == sizeof(exp) && std::memcmp(d.begin(), exp, sizeof(exp)) == 0);
        }
      }
      else if (strcmp(key, "hotprop-random-other-key") == 0)
        hasRandom = true;
    }
    EXPECT_TRUE(hasA);
    EXPECT_TRUE(hasC);
    EXPECT_TRUE(hasColdC);
    EXPECT_TRUE(hasBin);
    EXPECT_FALSE(hasB);
    EXPECT_FALSE(hasRandom);
    EXPECT_TRUE(hotAF64Ok);
    EXPECT_TRUE(hotCBoolOk);
    EXPECT_TRUE(coldCTextOk);
    EXPECT_TRUE(binBytesOk);
  }

  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();

    pp.setId(idA);
    auto ks = pp.initKeys(2);
    ks.set(0, "hotprop-a");
    ks.set(1, "coldprop-c");

    auto resp = gp.send().wait(ws);
    auto some = resp.getResult().getProps();

    EXPECT_EQ(some.size(), 2u);

    bool hasA = false, hasC = false;
    for (auto p : some)
    {
      if (strcmp(p.getKey().cStr(), "hotprop-a") == 0)
        hasA = true;
      if (strcmp(p.getKey().cStr(), "coldprop-c") == 0)
        hasC = true;
    }
    EXPECT_TRUE(hasA);
    EXPECT_TRUE(hasC);
  }
}

TEST_F(IntegrationRpc, Step08_SetLabelsOnBAndVerify)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto sl = cap.setNodeLabelsRequest();
  auto sp = sl.initParams();
  sp.setId(idB);
  auto add = sp.initAddLabels(2);
  add.set(0, "nodelabel-a");
  add.set(1, "nodelabel-b");

  sl.send().wait(ws);
  auto gn = cap.getNodeRequest();

  gn.initParams().setId(idB);
  auto resp = gn.send().wait(ws);
  auto hdr = resp.getResult().getHeader();

  EXPECT_EQ(hdr.getLabels().getNames().size(), 2u);

  {
    auto lnames = hdr.getLabels().getNames();
    bool hasA = false, hasB = false;
    for (uint32_t i = 0; i < lnames.size(); ++i)
    {
      auto v = lnames[i];
      if (strcmp(v.cStr(), "nodelabel-a") == 0)
        hasA = true;
      if (strcmp(v.cStr(), "nodelabel-b") == 0)
        hasB = true;
    }
    EXPECT_TRUE(hasA);
    EXPECT_TRUE(hasB);
  }
}

TEST_F(IntegrationRpc, Step09_VectorsOnB_AddGetDelete)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto up = cap.upsertVectorRequest();
  auto p = up.initParams();
  p.setId(idB);
  p.setTag("vec-b");

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
    EXPECT_EQ(strcmp(res[0].getTag().cStr(), "vec-b"), 0);
    EXPECT_EQ(res[0].getVector().getData().size(), v.size() * 4);

    auto data = res[0].getVector().getData();
    ASSERT_EQ(data.size(), v.size() * 4);
    EXPECT_EQ(std::memcmp(data.begin(), v.data(), v.size() * 4), 0);
  }

  {
    auto gv = cap.getVectorsRequest();
    auto gp = gv.initParams();
    gp.setId(idB);
    auto tags = gp.initTags(1);
    tags.set(0, "vec-b");

    auto resp = gv.send().wait(ws);
    auto res = resp.getResult().getVectors();

    ASSERT_EQ(res.size(), 1u);
    EXPECT_EQ(strcmp(res[0].getTag().cStr(), "vec-b"), 0);
  }

  {
    auto del = cap.deleteVectorRequest();
    auto dp = del.initParams();
    dp.setId(idB);
    dp.setTag("vec-b");

    del.send().wait(ws);
    auto gv = cap.getVectorsRequest();
    gv.initParams().setId(idB);

    auto resp = gv.send().wait(ws);
    auto res = resp.getResult().getVectors();

    EXPECT_EQ(res.size(), 0u);
  }
}

TEST_F(IntegrationRpc, Step10_AddSecondEdgeAtoB_AndFilters)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  {
    auto add = cap.addEdgeRequest();
    auto ap = add.initParams();
    ap.setSrc(idA);
    ap.setDst(idB);
    ap.initMeta().setType("edgetype-b");

    auto resp = add.send().wait(ws);
    edge2 = resp.getEdge().getId();

    ASSERT_GT(edge2, 0u);
  }

  {
    {
      auto req = cap.listAdjacencyRequest();
      auto np = req.initParams();
      np.setNode(idA);
      np.setDirection(stardust::rpc::Direction::OUT);
      np.setLimit(16);

      auto resp = req.send().wait(ws);
      EXPECT_EQ(resp.getResult().getItems().size(), 2u);
    }

    auto req = cap.listAdjacencyRequest();
    auto np = req.initParams();

    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);

    auto resp = req.send().wait(ws);
    auto items = resp.getResult().getItems();

    bool found = false;
    size_t count = 0;
    for (auto row : items) {
      if (strcmp(row.getType().cStr(), "edgetype-b") == 0) {
        ++count;
        if (row.getNeighbor() == idB) found = true;
      }
    }
    EXPECT_EQ(count, 1u);
    EXPECT_TRUE(found);
  }

  {
    auto req = cap.listAdjacencyRequest();
    auto np = req.initParams();
    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);
    auto resp = req.send().wait(ws);
    auto items = resp.getResult().getItems();

    bool found = false;
    size_t count = 0;
    for (auto row : items) {
      if (strcmp(row.getType().cStr(), "edgetype-a") == 0) {
        ++count;
        if (row.getNeighbor() == idB) found = true;
      }
    }
    EXPECT_EQ(count, 1u);
    EXPECT_TRUE(found);
  }

  {
    auto req = cap.listAdjacencyRequest();
    auto np = req.initParams();

    np.setNode(idA);
    np.setDirection(stardust::rpc::Direction::OUT);
    np.setLimit(16);

    auto resp = req.send().wait(ws);
    auto items = resp.getResult().getItems();

    bool found = false;
    for (auto row : items) {
      if (row.getNeighbor() == idB) {
        // verify neighbor has label "nodelabel-b"
        auto gn = cap.getNodeRequest();
        gn.initParams().setId(row.getNeighbor());
        auto r = gn.send().wait(ws);
        auto lnames = r.getResult().getHeader().getLabels().getNames();
        bool hasLabel = false;
        for (uint32_t i = 0; i < lnames.size(); ++i) {
          if (strcmp(lnames[i].cStr(), "nodelabel-b") == 0) {
            hasLabel = true;
          }
        }
        if (hasLabel) found = true;
      }
    }
    EXPECT_TRUE(found);
  }
}

TEST_F(IntegrationRpc, Step11_UpdateEdgeProps_OnEdge1)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto upd = cap.updateEdgePropsRequest();
  auto up = upd.initParams();
  up.setEdgeId(edge1);
  auto set = up.initSetProps(1);
  set[0].setKey("hotprop-a");
  set[0].initVal().setI64(7);
  auto unset = up.initUnsetKeys(1);
  unset.set(0, "hotprop-b");

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
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto wr = cap.writeBatchRequest();
  auto batch = wr.initBatch();
  auto ops = batch.initOps(4);

  {
    auto op = ops[0].initCreateNode();
    auto ls = op.initLabels();
    auto names = ls.initNames(1);
    names.set(0, "nodelabel-d");

    auto v = makeDemoVec(2);
    auto vecs = op.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("vec-d");
    auto vv = tv.initVector();
    vv.setDim(2);
    capnp::Data::Builder data = vv.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
  }

  {
    auto op = ops[1].initUpsertNodeProps();
    op.setId(idA);
    auto set = op.initSetHot(1);
    set[0].setKey("a");
    set[0].initVal().setBoolv(false);
  }

  {
    auto op = ops[2].initSetNodeLabels();
    op.setId(idB);
    auto add = op.initAddLabels(1);
    add.set(0, "nodelabel-d");
  }

  {
    auto op = ops[3].initAddEdge();
    op.setSrc(idB);
    op.setDst(0);
    op.initMeta().setType("edgetype-d");
  }

  wr.send().wait(ws);

  {
    auto gp = cap.getNodePropsRequest();
    auto pp = gp.initParams();
    pp.setId(idA);
    auto keys = pp.initKeys(1);
    keys.set(0, "a");
    auto r = gp.send().wait(ws);
    auto ps = r.getResult().getProps();

    EXPECT_EQ(ps.size(), 1u);
    EXPECT_EQ(strcmp(ps[0].getKey().cStr(), "a"), 0);
  }
  {
    auto gn = cap.getNodeRequest();
    gn.initParams().setId(idB);
    auto r = gn.send().wait(ws);
    auto lnames = r.getResult().getHeader().getLabels().getNames();

    bool has22 = false;
    for (uint32_t i = 0; i < lnames.size(); ++i)
    {
      auto v = lnames[i];
      if (strcmp(v.cStr(), "nodelabel-d") == 0)
        has22 = true;
    }
    EXPECT_TRUE(has22);
  }
}

TEST_F(IntegrationRpc, Step13_CreateC_AndConnect_B_to_C)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.createNodeRequest();
  auto p = req.initParams();
  auto ls = p.initLabels();
  auto names = ls.initNames(1);
  names.set(0, "nodelabel-d");

  auto resp = req.send().wait(ws);
  auto node = resp.getResult().getNode();

  idC = node.getId();
  ASSERT_GT(idC, 0u);

  auto add = cap.addEdgeRequest();
  auto ap = add.initParams();
  ap.setSrc(idB);
  ap.setDst(idC);
  ap.initMeta().setType("edgetype-c");
  add.send().wait(ws);
}

TEST_F(IntegrationRpc, Step14_VerifyNeighborsOfBIncludeC)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto req = cap.listAdjacencyRequest();
  auto np = req.initParams();
  np.setNode(idB);
  np.setDirection(stardust::rpc::Direction::OUT);
  np.setLimit(16);

  auto resp = req.send().wait(ws);
  auto items = resp.getResult().getItems();

  bool found = false;
  for (auto row : items)
    if (row.getNeighbor() == idC)
      found = true;
  EXPECT_TRUE(found);

  {
    auto rq = cap.listAdjacencyRequest();
    auto np2 = rq.initParams();
    np2.setNode(idB);
    np2.setDirection(stardust::rpc::Direction::OUT);
    np2.setLimit(16);
    auto r = rq.send().wait(ws);

    bool hasC = false;
    for (auto row : r.getResult().getItems())
      if (strcmp(row.getType().cStr(), "edgetype-c") == 0 && row.getNeighbor() == idC)
        hasC = true;
    EXPECT_TRUE(hasC);
  }
  {
    auto rq = cap.listAdjacencyRequest();
    auto np2 = rq.initParams();
    np2.setNode(idB);
    np2.setDirection(stardust::rpc::Direction::OUT);
    np2.setLimit(16);
    auto r = rq.send().wait(ws);

    bool hasC = false;
    for (auto row : r.getResult().getItems())
      if (strcmp(row.getType().cStr(), "edgetype-b") == 0 && row.getNeighbor() == idC)
        hasC = true;
    EXPECT_FALSE(hasC);
  }
}

TEST_F(IntegrationRpc, Step15_DeleteEdgesAtoB_AndVerify)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto del = cap.deleteEdgeRequest();
  del.initParams().setEdgeId(edge1);
  del.send().wait(ws);
  auto del2 = cap.deleteEdgeRequest();
  del2.initParams().setEdgeId(edge2);
  del2.send().wait(ws);
  auto req = cap.listAdjacencyRequest();
  auto np = req.initParams();
  np.setNode(idA);
  np.setDirection(stardust::rpc::Direction::OUT);
  np.setLimit(16);

  auto resp = req.send().wait(ws);
  auto items = resp.getResult().getItems();

  bool found = false;
  for (auto row : items)
    if (row.getNeighbor() == idB)
      found = true;
  EXPECT_FALSE(found);
  EXPECT_EQ(items.size(), 0u);
}

TEST_F(IntegrationRpc, Step16_DeleteNodeB_AndEnsureNoNeighbors)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  auto dn = cap.deleteNodeRequest();
  dn.initParams().setId(idB);
  dn.send().wait(ws);
  auto req = cap.listAdjacencyRequest();
  auto np = req.initParams();
  np.setNode(idB);
  np.setDirection(stardust::rpc::Direction::OUT);
  np.setLimit(16);

  auto resp = req.send().wait(ws);
  auto items = resp.getResult().getItems();

  EXPECT_EQ(items.size(), 0u);
}

TEST_F(IntegrationRpc, Step18_CreateNodesWithVectorsForKnn)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();

  // 5 nodes with vectors of dimension 4 and tag "knn-test"
  std::vector<uint64_t> nodeIds;
  
  // node 1: [1.0, 0.0, 0.0, 0.0] - unit vector in first dimension
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    
    std::vector<float> v = {1.0f, 0.0f, 0.0f, 0.0f};
    auto vecs = p.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("knn-test");
    auto vec = tv.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    
    auto resp = req.send().wait(ws);
    nodeIds.push_back(resp.getResult().getNode().getId());
  }
  
  // node 2: [0.0, 1.0, 0.0, 0.0] - unit vector in second dimension
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    
    std::vector<float> v = {0.0f, 1.0f, 0.0f, 0.0f};
    auto vecs = p.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("knn-test");
    auto vec = tv.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    
    auto resp = req.send().wait(ws);
    nodeIds.push_back(resp.getResult().getNode().getId());
  }
  
  // node 3: [0.7071, 0.7071, 0.0, 0.0] - 45 degrees between first two dimensions
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    
    std::vector<float> v = {0.7071f, 0.7071f, 0.0f, 0.0f};
    auto vecs = p.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("knn-test");
    auto vec = tv.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    
    auto resp = req.send().wait(ws);
    nodeIds.push_back(resp.getResult().getNode().getId());
  }
  
  // node 4: [0.5, 0.5, 0.5, 0.5] - equal in all dimensions
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    
    std::vector<float> v = {0.5f, 0.5f, 0.5f, 0.5f};
    auto vecs = p.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("knn-test");
    auto vec = tv.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    
    auto resp = req.send().wait(ws);
    nodeIds.push_back(resp.getResult().getNode().getId());
  }
  
  // node 5: [-1.0, 0.0, 0.0, 0.0] - opposite of node 1
  {
    auto req = cap.createNodeRequest();
    auto p = req.initParams();
    
    std::vector<float> v = {-1.0f, 0.0f, 0.0f, 0.0f};
    auto vecs = p.initVectors(1);
    auto tv = vecs[0];
    tv.setTag("knn-test");
    auto vec = tv.initVector();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(v.size() * 4);
    std::memcpy(data.begin(), v.data(), v.size() * 4);
    
    auto resp = req.send().wait(ws);
    nodeIds.push_back(resp.getResult().getNode().getId());
  }
  
  EXPECT_EQ(nodeIds.size(), 5u);
  for (auto id : nodeIds) {
    EXPECT_GT(id, 0u);
  }
}

TEST_F(IntegrationRpc, Step19_KnnQueries)
{
  auto &ws = client->getWaitScope();
  auto cap = client->getMain<stardust::rpc::Stardust>();
  
  // test 1: Query with [1.0, 0.0, 0.0, 0.0], k=3
  // Should find node 1 as exact match (score ~1.0), then node 3 (has 0.7071 in first dim)
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTag("knn-test");
    
    std::vector<float> q = {1.0f, 0.0f, 0.0f, 0.0f};
    auto vec = kp.initQuery();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(5);
    
    auto resp = knn.send().wait(ws);
    auto hits = resp.getResult().getHits();
    
    ASSERT_EQ(hits.size(), 5u);
    
    EXPECT_NEAR(hits[0].getScore(), 1.0f, 0.0001f);
    
    for (size_t i = 1; i < hits.size(); ++i) {
      EXPECT_LE(hits[i].getScore(), hits[i-1].getScore());
    }
    
    // last result should be the opposite vector with score -1.0
    EXPECT_NEAR(hits[4].getScore(), -1.0f, 0.0001f);
  }
  
  // test 2: Query with [0.0, 1.0, 0.0, 0.0], k=2
  // Should find node 2 as exact match, then node 3
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTag("knn-test");
    
    std::vector<float> q = {0.0f, 1.0f, 0.0f, 0.0f};
    auto vec = kp.initQuery();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(2);
    
    auto resp = knn.send().wait(ws);
    auto hits = resp.getResult().getHits();
    
    ASSERT_EQ(hits.size(), 2u);
    
    EXPECT_NEAR(hits[0].getScore(), 1.0f, 0.0001f);
    EXPECT_NEAR(hits[1].getScore(), 0.7071f, 0.01f);
  }
  
  // test 3: Query with [0.25, 0.25, 0.25, 0.25]
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTag("knn-test");
    
    std::vector<float> q = {0.25f, 0.25f, 0.25f, 0.25f};
    auto vec = kp.initQuery();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(5);
    
    auto resp = knn.send().wait(ws);
    auto hits = resp.getResult().getHits();
    
    ASSERT_EQ(hits.size(), 5u);
    
    EXPECT_NEAR(hits[0].getScore(), 1.0f, 0.0001f);
    
    for (size_t i = 1; i < hits.size(); ++i) {
      EXPECT_LE(hits[i].getScore(), hits[i-1].getScore());
    }
  }
  
  // test 4: Query with zero vector [0, 0, 0, 0], k=3
  // with zero query vector, all scores should be 0 (as per the implementation)
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTag("knn-test");
    
    std::vector<float> q = {0.0f, 0.0f, 0.0f, 0.0f};
    auto vec = kp.initQuery();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(3);
    
    auto resp = knn.send().wait(ws);
    auto hits = resp.getResult().getHits();
    
    ASSERT_EQ(hits.size(), 3u);
    
    for (const auto& hit : hits) {
      EXPECT_NEAR(hit.getScore(), 0.0f, 0.0001f);
    }
  }
  
  // test 5: Query with k=0
  // should return empty results
  {
    auto knn = cap.knnRequest();
    auto kp = knn.initParams();
    kp.setTag("knn-test");
    
    std::vector<float> q = {1.0f, 0.0f, 0.0f, 0.0f};
    auto vec = kp.initQuery();
    vec.setDim(4);
    capnp::Data::Builder data = vec.initData(q.size() * 4);
    std::memcpy(data.begin(), q.data(), q.size() * 4);
    kp.setK(0);
    
    auto resp = knn.send().wait(ws);
    auto hits = resp.getResult().getHits();
    
    EXPECT_EQ(hits.size(), 0u);
  }
}
