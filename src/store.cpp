#include "store.hpp"
#include <cstring>
#include <lmdb.h>

namespace stardust
{

  static uint64_t incr_counter(MDB_txn *tx, MDB_dbi ctrs, std::string_view key)
  {
    MDB_val k{key.size(), const_cast<char *>(key.data())};
    MDB_val v{};
    int rc = mdb_get(tx, ctrs, &k, &v);
    uint64_t x = 0;
    if (rc == 0 && v.mv_size == 8)
      std::memcpy(&x, v.mv_data, 8);
    x += 1;
    MDB_val nv{8, &x};
    rc = mdb_put(tx, ctrs, &k, &nv, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    return x;
  }

  uint64_t Store::createNode(std::optional<std::pair<uint16_t, std::string_view>> vectorOpt)
  {
    Txn tx(env_.raw(), true);
    auto id = incr_counter(tx.get(), env_.ctrs(), key_counter_nodes());
    auto nk = key_node(id);
    MDB_val k{nk.size(), nk.data()};

    uint32_t header_stub[2] = {0, 0}; // label/degree placeholder for v0
    MDB_val val{sizeof(header_stub), header_stub};

    int rc = mdb_put(tx.get(), env_.nodes(), &k, &val, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    if (vectorOpt)
    {
      auto [dim, bytes] = *vectorOpt;
      auto vk = key_vec(id);
      MDB_val vk0{vk.size(), vk.data()};
      MDB_val vv{bytes.size(), const_cast<char *>(bytes.data())};
      
      rc = mdb_put(tx.get(), env_.vecs(), &vk0, &vv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }
    tx.commit();
    return id;
  }

  void Store::addEdge(uint64_t src, uint64_t dst, EdgeMeta m)
  {
    Txn tx(env_.raw(), true);
    auto k1s = key_edge_out(src, dst);
    MDB_val k1{k1s.size(), k1s.data()};
    MDB_val v1{sizeof(m), &m};
    int rc = mdb_put(tx.get(), env_.eout(), &k1, &v1, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    auto k2s = key_edge_in(dst, src);
    MDB_val k2{k2s.size(), k2s.data()};
    rc = mdb_put(tx.get(), env_.ein(), &k2, &v1, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    tx.commit();
  }

  std::vector<uint64_t> Store::neighbors(uint64_t node, bool in, uint32_t limit)
  {
    std::vector<uint64_t> out;
    out.reserve(limit);
    Txn tx(env_.raw(), false);
    MDB_cursor *cur{};
    auto dbi = in ? env_.ein() : env_.eout();
    int rc = mdb_cursor_open(tx.get(), dbi, &cur);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    std::string prefix;
    if (in)
    {
      prefix = key_edge_in(node, 0);
    }
    else
    {
      prefix = key_edge_out(node, 0);
    }

    MDB_val k{prefix.size(), prefix.data()}, v{};
    rc = mdb_cursor_get(cur, &k, &v, MDB_SET_RANGE);
    while (rc == 0 && out.size() < limit)
    {
      auto *p = static_cast<const unsigned char *>(k.mv_data);
      if (k.mv_size < 1 + 16 || p[0] != (in ? 'R' : 'E'))
        break;
      uint64_t major = 0;
      for (int i = 0; i < 8; ++i)
        major = (major << 8) | p[1 + i];
      if (major != node)
        break;
      uint64_t minor = 0;
      for (int i = 0; i < 8; ++i)
        minor = (minor << 8) | p[1 + 8 + i];
      out.push_back(minor);
      rc = mdb_cursor_get(cur, &k, &v, MDB_NEXT);
    }
    mdb_cursor_close(cur);
    // read txn auto-aborts
    return out;
  }

} // namespace stardust
