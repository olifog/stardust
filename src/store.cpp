#include "store.hpp"
#include "encode.hpp"
#include <lmdb.h>
#include <cstring>
#include <string_view>
#include <algorithm>
#include <unordered_set>

namespace stardust
{
  // -------------------- meta helpers (schema, sequences) --------------------

  static inline MDB_val make_val_from_str(const std::string &s)
  {
    return MDB_val{s.size(), const_cast<char *>(s.data())};
  }

  static bool mdb_get_val(MDB_txn *tx, DbHandle dbi, std::string_view key, MDB_val &out)
  {
    MDB_val k{key.size(), const_cast<char *>(key.data())};
    int rc = mdb_get(tx, dbi, &k, &out);
    if (rc == MDB_NOTFOUND)
      return false;
    if (rc)
      throw MdbError(mdb_strerror(rc));
    return true;
  }

  static uint64_t read_u64_or(MDB_txn *tx, DbHandle dbi, std::string_view key, uint64_t fallback)
  {
    MDB_val v{};
    if (!mdb_get_val(tx, dbi, key, v) || v.mv_size != 8)
      return fallback;
    uint64_t x = 0;
    std::memcpy(&x, v.mv_data, 8);
    return x;
  }

  static void write_u64(MDB_txn *tx, DbHandle dbi, std::string_view key, uint64_t value)
  {
    MDB_val k{key.size(), const_cast<char *>(key.data())};
    MDB_val v{8, &value};
    int rc = mdb_put(tx, dbi, &k, &v, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

  static uint32_t read_u32_or(MDB_txn *tx, DbHandle dbi, std::string_view key, uint32_t fallback)
  {
    MDB_val v{};
    if (!mdb_get_val(tx, dbi, key, v) || v.mv_size != 4)
      return fallback;
    uint32_t x = 0;
    std::memcpy(&x, v.mv_data, 4);
    return x;
  }

  static void write_u32(MDB_txn *tx, DbHandle dbi, std::string_view key, uint32_t value)
  {
    MDB_val k{key.size(), const_cast<char *>(key.data())};
    MDB_val v{4, &value};
    int rc = mdb_put(tx, dbi, &k, &v, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

  static void ensure_schema_version(Txn &tx, Env &env)
  {
    auto key = key_meta_schema_version();
    MDB_val v{};
    if (!mdb_get_val(tx.get(), env.meta(), key, v))
    {
      uint32_t version = 1u; // default initial schema version 1
      write_u32(tx.get(), env.meta(), key, version);
    }
  }

  static uint64_t incr_meta_seq(Txn &tx, Env &env, const std::string &key, uint64_t initial = 0)
  {
    ensure_schema_version(tx, env);
    uint64_t current = read_u64_or(tx.get(), env.meta(), key, initial);
    uint64_t next = current + 1;
    write_u64(tx.get(), env.meta(), key, next);
    return next;
  }

  static uint64_t next_node_id(Txn &tx, Env &env)
  {
    return incr_meta_seq(tx, env, key_meta_node_seq(), 0);
  }

  static uint64_t next_edge_id(Txn &tx, Env &env)
  {
    return incr_meta_seq(tx, env, key_meta_edge_seq(), 0);
  }

  // -------------------- value/header encoding --------------------

  enum class ValueTag : uint8_t
  {
    I64 = 0,
    F64 = 1,
    Bool = 2,
    TextId = 3,
    Bytes = 4,
    Null = 5
  };

  static void encode_value(std::string &out, const Value &v)
  {
    if (std::holds_alternative<int64_t>(v))
    {
      out.push_back(char(ValueTag::I64));
      int64_t x = std::get<int64_t>(v);
      uint64_t ux = static_cast<uint64_t>(x);
      put_be64(out, ux);
      return;
    }
    if (std::holds_alternative<double>(v))
    {
      out.push_back(char(ValueTag::F64));
      double d = std::get<double>(v);
      static_assert(sizeof(double) == 8, "double not 8 bytes");
      uint64_t ux;
      std::memcpy(&ux, &d, 8);
      put_be64(out, ux);
      return;
    }
    if (std::holds_alternative<bool>(v))
    {
      out.push_back(char(ValueTag::Bool));
      out.push_back(std::get<bool>(v) ? 1 : 0);
      return;
    }
    if (std::holds_alternative<uint32_t>(v))
    {
      out.push_back(char(ValueTag::TextId));
      put_be32(out, std::get<uint32_t>(v));
      return;
    }
    if (std::holds_alternative<std::string>(v))
    {
      out.push_back(char(ValueTag::Bytes));
      const auto &s = std::get<std::string>(v);
      put_be32(out, static_cast<uint32_t>(s.size()));
      out.append(s);
      return;
    }
    out.push_back(char(ValueTag::Null));
  }

  static const unsigned char *decode_value(const unsigned char *p, const unsigned char *end, Value &out)
  {
    if (p >= end)
      throw MdbError("corrupt value: empty");
    auto tag = static_cast<ValueTag>(*p++);
    switch (tag)
    {
    case ValueTag::I64:
    {
      if (end - p < 8)
        throw MdbError("corrupt i64");
      uint64_t ux = read_be64(p);
      p += 8;
      out = static_cast<int64_t>(ux);
      return p;
    }
    case ValueTag::F64:
    {
      if (end - p < 8)
        throw MdbError("corrupt f64");
      uint64_t ux = read_be64(p);
      p += 8;
      double d;
      std::memcpy(&d, &ux, 8);
      out = d;
      return p;
    }
    case ValueTag::Bool:
    {
      if (end - p < 1)
        throw MdbError("corrupt bool");
      out = bool(*p++ != 0);
      return p;
    }
    case ValueTag::TextId:
    {
      if (end - p < 4)
        throw MdbError("corrupt textId");
      out = read_be32(p);
      p += 4;
      return p;
    }
    case ValueTag::Bytes:
    {
      if (end - p < 4)
        throw MdbError("corrupt bytes len");
      uint32_t len = read_be32(p);
      p += 4;
      if (end - p < static_cast<std::ptrdiff_t>(len))
        throw MdbError("corrupt bytes data");
      out = std::string(reinterpret_cast<const char *>(p), len);
      p += len;
      return p;
    }
    case ValueTag::Null:
      out = std::monostate{};
      return p;
    default:
      throw MdbError("unknown value tag");
    }
  }

  static void encode_property(std::string &out, const Property &p)
  {
    put_be32(out, p.keyId);
    encode_value(out, p.val);
  }

  static const unsigned char *decode_property(const unsigned char *p, const unsigned char *end, Property &out)
  {
    if (end - p < 4)
      throw MdbError("corrupt prop keyId");
    out.keyId = read_be32(p);
    p += 4;
    p = decode_value(p, end, out.val);
    return p;
  }

  static void encode_label_set(std::string &out, const LabelSet &ls)
  {
    put_be32(out, static_cast<uint32_t>(ls.labelIds.size()));
    for (uint32_t id : ls.labelIds)
      put_be32(out, id);
  }

  static const unsigned char *decode_label_set(const unsigned char *p, const unsigned char *end, LabelSet &out)
  {
    if (end - p < 4)
      throw MdbError("corrupt labels count");
    uint32_t n = read_be32(p);
    p += 4;
    out.labelIds.clear();
    out.labelIds.reserve(n);
    for (uint32_t i = 0; i < n; ++i)
    {
      if (end - p < 4)
        throw MdbError("corrupt label id");
      out.labelIds.push_back(read_be32(p));
      p += 4;
    }
    return p;
  }

  static std::string encode_node_header(const NodeHeader &h)
  {
    std::string s;
    s.reserve(8 + 4 + h.labels.labelIds.size() * 4 + 4 + h.hotProps.size() * 16);
    put_be64(s, h.id);
    encode_label_set(s, h.labels);
    put_be32(s, static_cast<uint32_t>(h.hotProps.size()));
    for (const auto &p : h.hotProps)
      encode_property(s, p);
    return s;
  }

  static NodeHeader decode_node_header(std::string_view bytes)
  {
    const unsigned char *p = reinterpret_cast<const unsigned char *>(bytes.data());
    const unsigned char *end = p + bytes.size();
    if (end - p < 8)
      throw MdbError("corrupt node header id");
    NodeHeader h{};
    h.id = read_be64(p);
    p += 8;
    p = decode_label_set(p, end, h.labels);
    if (end - p < 4)
      throw MdbError("corrupt hotProps count");
    uint32_t n = read_be32(p);
    p += 4;
    h.hotProps.clear();
    h.hotProps.reserve(n);
    for (uint32_t i = 0; i < n; ++i)
    {
      Property pr{};
      p = decode_property(p, end, pr);
      h.hotProps.push_back(std::move(pr));
    }
    if (p != end)
      throw MdbError("trailing data in node header");
    return h;
  }

  static bool labels_contains_all(const std::vector<uint32_t> &have, const std::vector<uint32_t> &need)
  {
    if (need.empty())
      return true;
    size_t i = 0, j = 0;
    while (i < have.size() && j < need.size())
    {
      if (have[i] < need[j])
      {
        ++i;
      }
      else if (have[i] == need[j])
      {
        ++i;
        ++j;
      }
      else
      {
        return false;
      }
    }
    return j == need.size();
  }

  static void sort_unique(std::vector<uint32_t> &v)
  {
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
  }

  // -------------------- api ---------------------------

  CreateNodeResult Store::createNode(const CreateNodeParams &params)
  {
    Txn tx(env_.raw(), true);
    uint64_t id = next_node_id(tx, env_);

    NodeHeader hdr{};
    hdr.id = id;
    hdr.labels = params.labels;
    hdr.hotProps = params.hotProps;
    sort_unique(hdr.labels.labelIds);

    auto key = key_nodes_be(id);
    auto valBytes = encode_node_header(hdr);
    MDB_val k{key.size(), const_cast<char *>(key.data())};
    MDB_val v{valBytes.size(), const_cast<char *>(valBytes.data())};
    int rc = mdb_put(tx.get(), env_.nodes(), &k, &v, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    // cold props
    for (const auto &p : params.coldProps)
    {
      auto cpk = key_node_cold_prop_be(id, p.keyId);
      std::string pv;
      pv.reserve(16);
      encode_value(pv, p.val);
      MDB_val ck{cpk.size(), const_cast<char *>(cpk.data())};
      MDB_val cv{pv.size(), const_cast<char *>(pv.data())};
      rc = mdb_put(tx.get(), env_.nodeColdProps(), &ck, &cv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }

    // vectors
    for (const auto &tv : params.vectors)
    {
      auto vk = key_node_vector_be(id, tv.tagId);
      MDB_val ck{vk.size(), const_cast<char *>(vk.data())};
      const auto &data = tv.vector.data;
      MDB_val cv{data.size(), const_cast<char *>(data.data())};
      rc = mdb_put(tx.get(), env_.nodeVectors(), &ck, &cv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }

    // label index
    for (uint32_t labelId : hdr.labels.labelIds)
    {
      auto lk = key_label_index_be(labelId, id);
      MDB_val lk0{lk.size(), const_cast<char *>(lk.data())};
      MDB_val lv{0, nullptr};
      rc = mdb_put(tx.get(), env_.labelIndex(), &lk0, &lv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }

    tx.commit();
    CreateNodeResult out{};
    out.id = id;
    out.header = std::move(hdr);
    return out;
  }

  void Store::upsertNodeProps(const UpsertNodePropsParams &params)
  {
    Txn tx(env_.raw(), true);
    // load header
    auto nk = key_nodes_be(params.id);
    MDB_val k{nk.size(), const_cast<char *>(nk.data())}, v{};
    int rc = mdb_get(tx.get(), env_.nodes(), &k, &v);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    NodeHeader hdr = decode_node_header(std::string_view(reinterpret_cast<const char *>(v.mv_data), v.mv_size));

    // unset from hot
    if (!params.unsetKeys.empty())
    {
      std::vector<uint32_t> unset = params.unsetKeys;
      sort_unique(unset);
      hdr.hotProps.erase(
          std::remove_if(hdr.hotProps.begin(), hdr.hotProps.end(), [&](const Property &p)
                         { return std::binary_search(unset.begin(), unset.end(), p.keyId); }),
          hdr.hotProps.end());
    }

    // set hot: replace or add
    for (const auto &p : params.setHot)
    {
      bool found = false;
      for (auto &hp : hdr.hotProps)
      {
        if (hp.keyId == p.keyId)
        {
          hp.val = p.val;
          found = true;
          break;
        }
      }
      if (!found)
        hdr.hotProps.push_back(p);
    }

    // rewrite header
    auto newVal = encode_node_header(hdr);
    MDB_val nv{newVal.size(), const_cast<char *>(newVal.data())};
    rc = mdb_put(tx.get(), env_.nodes(), &k, &nv, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    // set cold
    for (const auto &p : params.setCold)
    {
      auto cpk = key_node_cold_prop_be(params.id, p.keyId);
      std::string pv;
      encode_value(pv, p.val);
      MDB_val ck{cpk.size(), const_cast<char *>(cpk.data())};
      MDB_val cv{pv.size(), const_cast<char *>(pv.data())};
      rc = mdb_put(tx.get(), env_.nodeColdProps(), &ck, &cv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }

    // unset cold
    for (uint32_t keyId : params.unsetKeys)
    {
      auto cpk = key_node_cold_prop_be(params.id, keyId);
      MDB_val ck{cpk.size(), const_cast<char *>(cpk.data())};
      rc = mdb_del(tx.get(), env_.nodeColdProps(), &ck, nullptr);
      if (rc != 0 && rc != MDB_NOTFOUND)
        throw MdbError(mdb_strerror(rc));
    }

    tx.commit();
  }

  void Store::setNodeLabels(const SetNodeLabelsParams &params)
  {
    Txn tx(env_.raw(), true);
    // load header
    auto nk = key_nodes_be(params.id);
    MDB_val k{nk.size(), const_cast<char *>(nk.data())}, v{};
    int rc = mdb_get(tx.get(), env_.nodes(), &k, &v);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    NodeHeader hdr = decode_node_header(std::string_view(reinterpret_cast<const char *>(v.mv_data), v.mv_size));

    // current labels set
    sort_unique(hdr.labels.labelIds);
    std::vector<uint32_t> add = params.addLabels;
    std::vector<uint32_t> rem = params.removeLabels;
    sort_unique(add);
    sort_unique(rem);

    // remove
    for (uint32_t id : rem)
    {
      auto it = std::lower_bound(hdr.labels.labelIds.begin(), hdr.labels.labelIds.end(), id);
      if (it != hdr.labels.labelIds.end() && *it == id)
        hdr.labels.labelIds.erase(it);
    }
    // add
    for (uint32_t id : add)
    {
      auto it = std::lower_bound(hdr.labels.labelIds.begin(), hdr.labels.labelIds.end(), id);
      if (it == hdr.labels.labelIds.end() || *it != id)
        hdr.labels.labelIds.insert(it, id);
    }

    // write header
    auto newVal = encode_node_header(hdr);
    MDB_val nv{newVal.size(), const_cast<char *>(newVal.data())};
    rc = mdb_put(tx.get(), env_.nodes(), &k, &nv, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    // update label index
    for (uint32_t id : add)
    {
      auto lk = key_label_index_be(id, params.id);
      MDB_val lk0{lk.size(), const_cast<char *>(lk.data())};
      MDB_val lv{0, nullptr};
      rc = mdb_put(tx.get(), env_.labelIndex(), &lk0, &lv, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }
    for (uint32_t id : rem)
    {
      auto lk = key_label_index_be(id, params.id);
      MDB_val lk0{lk.size(), const_cast<char *>(lk.data())};
      rc = mdb_del(tx.get(), env_.labelIndex(), &lk0, nullptr);
      if (rc != 0 && rc != MDB_NOTFOUND)
        throw MdbError(mdb_strerror(rc));
    }

    tx.commit();
  }

  void Store::upsertVector(const UpsertVectorParams &params)
  {
    Txn tx(env_.raw(), true);

    const auto &data = params.vector.data;
    if ((data.size() % 4) != 0)
      throw MdbError("vector byte length must be a multiple of 4");
    uint32_t dimFromBytes = static_cast<uint32_t>(data.size() / 4);
    if (params.vector.dim != 0 && params.vector.dim != dimFromBytes)
      throw MdbError("provided dim does not match data length");

    uint32_t enforcedDim = dimFromBytes;
    {
      auto mk = key_vec_tag_meta_be(params.tagId);
      MDB_val mk0{mk.size(), const_cast<char *>(mk.data())}, mv{};
      int mrc = mdb_get(tx.get(), env_.vecTagMeta(), &mk0, &mv);
      if (mrc == 0)
      {
        if (mv.mv_size < 4)
          throw MdbError("corrupt vecTagMeta entry");
        uint32_t storedDim = read_be32(static_cast<const unsigned char *>(mv.mv_data));
        if (storedDim != enforcedDim)
          throw MdbError("vector dim does not match tagId meta");
      }
      else if (mrc == MDB_NOTFOUND)
      {
        std::string dimv;
        dimv.reserve(4);
        put_be32(dimv, enforcedDim);
        MDB_val mvk{mk.size(), const_cast<char *>(mk.data())};
        MDB_val mvv{dimv.size(), const_cast<char *>(dimv.data())};
        int prc = mdb_put(tx.get(), env_.vecTagMeta(), &mvk, &mvv, 0);
        if (prc)
          throw MdbError(mdb_strerror(prc));
      }
      else
      {
        throw MdbError(mdb_strerror(mrc));
      }
    }

    auto vk = key_node_vector_be(params.id, params.tagId);
    MDB_val k{vk.size(), const_cast<char *>(vk.data())};
    MDB_val v{data.size(), const_cast<char *>(data.data())};
    int rc = mdb_put(tx.get(), env_.nodeVectors(), &k, &v, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    tx.commit();
  }

  void Store::deleteVector(const DeleteVectorParams &params)
  {
    Txn tx(env_.raw(), true);
    auto vk = key_node_vector_be(params.id, params.tagId);
    MDB_val k{vk.size(), const_cast<char *>(vk.data())};
    int rc = mdb_del(tx.get(), env_.nodeVectors(), &k, nullptr);
    if (rc != 0 && rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));
    tx.commit();
  }

  EdgeRef Store::addEdge(const AddEdgeParams &params)
  {
    Txn tx(env_.raw(), true);
    uint64_t eid = next_edge_id(tx, env_);
    EdgeRef ref{eid, params.src, params.dst};

    // edgesById
    auto idk = key_edge_by_id_be(eid);
    std::string refv;
    refv.reserve(24);
    put_be64(refv, ref.id);
    put_be64(refv, ref.src);
    put_be64(refv, ref.dst);
    MDB_val k{idk.size(), const_cast<char *>(idk.data())};
    MDB_val v{refv.size(), const_cast<char *>(refv.data())};
    int rc = mdb_put(tx.get(), env_.edgesById(), &k, &v, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    // type indexes
    auto sk = key_edge_by_src_type_be(ref.src, params.meta.typeId, ref.dst, ref.id);
    MDB_val sk0{sk.size(), const_cast<char *>(sk.data())};
    MDB_val vv{0, nullptr};
    rc = mdb_put(tx.get(), env_.edgesBySrcType(), &sk0, &vv, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    auto dk = key_edge_by_dst_type_be(ref.dst, params.meta.typeId, ref.src, ref.id);
    MDB_val dk0{dk.size(), const_cast<char *>(dk.data())};
    rc = mdb_put(tx.get(), env_.edgesByDstType(), &dk0, &vv, 0);
    if (rc)
      throw MdbError(mdb_strerror(rc));

    // props
    for (const auto &p : params.meta.props)
    {
      auto pk = key_edge_prop_be(ref.id, p.keyId);
      std::string pv;
      encode_value(pv, p.val);
      MDB_val pk0{pk.size(), const_cast<char *>(pk.data())};
      MDB_val pv0{pv.size(), const_cast<char *>(pv.data())};
      rc = mdb_put(tx.get(), env_.edgeProps(), &pk0, &pv0, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }

    tx.commit();
    return ref;
  }

  void Store::updateEdgeProps(const UpdateEdgePropsParams &params)
  {
    Txn tx(env_.raw(), true);
    int rc = 0;
    for (const auto &p : params.setProps)
    {
      auto pk = key_edge_prop_be(params.edgeId, p.keyId);
      std::string pv;
      encode_value(pv, p.val);
      MDB_val pk0{pk.size(), const_cast<char *>(pk.data())};
      MDB_val pv0{pv.size(), const_cast<char *>(pv.data())};
      rc = mdb_put(tx.get(), env_.edgeProps(), &pk0, &pv0, 0);
      if (rc)
        throw MdbError(mdb_strerror(rc));
    }
    for (uint32_t keyId : params.unsetKeys)
    {
      auto pk = key_edge_prop_be(params.edgeId, keyId);
      MDB_val pk0{pk.size(), const_cast<char *>(pk.data())};
      rc = mdb_del(tx.get(), env_.edgeProps(), &pk0, nullptr);
      if (rc != 0 && rc != MDB_NOTFOUND)
        throw MdbError(mdb_strerror(rc));
    }
    tx.commit();
  }

  NeighborsResult Store::neighbors(const NeighborsParams &params)
  {
    NeighborsResult result{};
    std::unordered_set<uint64_t> seen;
    result.neighbors.reserve(params.limit);

    auto includeType = [&](uint32_t t) -> bool
    {
      if (params.relTypeIn.empty())
        return true;
      return std::find(params.relTypeIn.begin(), params.relTypeIn.end(), t) != params.relTypeIn.end();
    };

    auto scanDir = [&](bool outDir)
    {
      Txn tx(env_.raw(), false);
      auto passesLabelFilter = [&](uint64_t nodeId) -> bool
      {
        if (params.neighborHas.labelIds.empty())
          return true;
        auto nk = key_nodes_be(nodeId);
        MDB_val k{nk.size(), const_cast<char *>(nk.data())}, v{};
        int rc = mdb_get(tx.get(), env_.nodes(), &k, &v);
        if (rc)
          return false;
        NodeHeader hdr = decode_node_header(std::string_view(reinterpret_cast<const char *>(v.mv_data), v.mv_size));
        return labels_contains_all(hdr.labels.labelIds, params.neighborHas.labelIds);
      };
      MDB_cursor *cur{};
      auto dbi = outDir ? env_.edgesBySrcType() : env_.edgesByDstType();
      int rc = mdb_cursor_open(tx.get(), dbi, &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = outDir ? key_edge_by_src_type_be(params.node, 0, 0, 0)
                                 : key_edge_by_dst_type_be(params.node, 0, 0, 0);
      MDB_val k{start.size(), const_cast<char *>(start.data())}, v{};
      rc = mdb_cursor_get(cur, &k, &v, MDB_SET_RANGE);
      while (rc == 0 && result.neighbors.size() < params.limit)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(k.mv_data);
        if (k.mv_size < 8 + 4 + 8 + 8)
          break;
        uint64_t major = read_be64(kb + 0);
        if (major != params.node)
          break;
        uint32_t typeId = read_be32(kb + 8);
        uint64_t other = read_be64(kb + 12 + (outDir ? 0 : 8)); // dst if out, src if in
        if (includeType(typeId))
        {
          if (passesLabelFilter(other))
          {
            if (params.direction == Direction::Both)
            {
              if (seen.insert(other).second)
                result.neighbors.push_back(other);
            }
            else
            {
              result.neighbors.push_back(other);
            }
          }
        }
        rc = mdb_cursor_get(cur, &k, &v, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    };

    if (params.direction == Direction::Out || params.direction == Direction::Both)
      scanDir(true);
    if (result.neighbors.size() < params.limit && (params.direction == Direction::In || params.direction == Direction::Both))
      scanDir(false);

    if (result.neighbors.size() > params.limit)
      result.neighbors.resize(params.limit);
    return result;
  }

  KnnResult Store::knn(const KnnParams &params)
  {
    (void)params;
    KnnResult r{};
    return r; // TODO: implement scan/HNSW
  }

  GetNodeResult Store::getNode(const GetNodeParams &params)
  {
    Txn tx(env_.raw(), false);
    auto nk = key_nodes_be(params.id);
    MDB_val k{nk.size(), const_cast<char *>(nk.data())}, v{};
    int rc = mdb_get(tx.get(), env_.nodes(), &k, &v);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    GetNodeResult out{};
    out.header = decode_node_header(std::string_view(reinterpret_cast<const char *>(v.mv_data), v.mv_size));
    return out;
  }

  GetNodePropsResult Store::getNodeProps(const GetNodePropsParams &params)
  {
    GetNodePropsResult out{};
    Txn tx(env_.raw(), false);

    // load hot header
    auto nk = key_nodes_be(params.id);
    MDB_val k{nk.size(), const_cast<char *>(nk.data())}, hv{};
    int rc = mdb_get(tx.get(), env_.nodes(), &k, &hv);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    NodeHeader hdr = decode_node_header(std::string_view(reinterpret_cast<const char *>(hv.mv_data), hv.mv_size));

    auto addProp = [&](const Property &p)
    { out.props.push_back(p); };

    if (params.keyIds.empty())
    {
      // all hot
      for (const auto &p : hdr.hotProps)
        addProp(p);
      // all cold by scan
      MDB_cursor *cur{};
      rc = mdb_cursor_open(tx.get(), env_.nodeColdProps(), &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = key_node_cold_prop_be(params.id, 0);
      MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
      rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
      while (rc == 0)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
        if (ck.mv_size < 8 + 4)
          break;
        uint64_t major = read_be64(kb + 0);
        if (major != params.id)
          break;
        uint32_t keyId = read_be32(kb + 8);
        Value val{};
        decode_value(static_cast<const unsigned char *>(cv.mv_data), static_cast<const unsigned char *>(cv.mv_data) + cv.mv_size, val);
        addProp(Property{keyId, std::move(val)});
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    }
    else
    {
      for (uint32_t keyId : params.keyIds)
      {
        bool found = false;
        for (const auto &p : hdr.hotProps)
          if (p.keyId == keyId)
          {
            addProp(p);
            found = true;
            break;
          }
        if (found)
          continue;
        auto cpk = key_node_cold_prop_be(params.id, keyId);
        MDB_val ck{cpk.size(), const_cast<char *>(cpk.data())}, cv{};
        rc = mdb_get(tx.get(), env_.nodeColdProps(), &ck, &cv);
        if (rc == 0)
        {
          Value val{};
          decode_value(static_cast<const unsigned char *>(cv.mv_data), static_cast<const unsigned char *>(cv.mv_data) + cv.mv_size, val);
          addProp(Property{keyId, std::move(val)});
        }
        else if (rc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(rc));
      }
    }

    return out;
  }

  GetVectorsResult Store::getVectors(const GetVectorsParams &params)
  {
    GetVectorsResult out{};
    Txn tx(env_.raw(), false);
    if (params.tagIds.empty())
    {
      MDB_cursor *cur{};
      int rc = mdb_cursor_open(tx.get(), env_.nodeVectors(), &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = key_node_vector_be(params.id, 0);
      MDB_val k{start.size(), const_cast<char *>(start.data())}, v{};
      rc = mdb_cursor_get(cur, &k, &v, MDB_SET_RANGE);
      while (rc == 0)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(k.mv_data);
        if (k.mv_size < 8 + 4)
          break;
        uint64_t nid = read_be64(kb + 0);
        if (nid != params.id)
          break;
        uint32_t tagId = read_be32(kb + 8);
        TaggedVector tv{};
        tv.tagId = tagId;
        {
          auto mk = key_vec_tag_meta_be(tagId);
          MDB_val mk0{mk.size(), const_cast<char *>(mk.data())}, mv{};
          int mrc = mdb_get(tx.get(), env_.vecTagMeta(), &mk0, &mv);
          if (mrc == 0 && mv.mv_size >= 4)
            tv.vector.dim = static_cast<uint16_t>(read_be32(static_cast<const unsigned char *>(mv.mv_data)));
          else
            tv.vector.dim = 0;
        }
        tv.vector.data.assign(reinterpret_cast<const char *>(v.mv_data), v.mv_size);
        out.vectors.push_back(std::move(tv));
        rc = mdb_cursor_get(cur, &k, &v, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    }
    else
    {
      for (uint32_t tagId : params.tagIds)
      {
        auto kkey = key_node_vector_be(params.id, tagId);
        MDB_val k{kkey.size(), const_cast<char *>(kkey.data())}, v{};
        int rc = mdb_get(tx.get(), env_.nodeVectors(), &k, &v);
        if (rc == 0)
        {
          TaggedVector tv{};
          tv.tagId = tagId;
          {
            auto mk = key_vec_tag_meta_be(tagId);
            MDB_val mk0{mk.size(), const_cast<char *>(mk.data())}, mv{};
            int mrc = mdb_get(tx.get(), env_.vecTagMeta(), &mk0, &mv);
            if (mrc == 0 && mv.mv_size >= 4)
              tv.vector.dim = static_cast<uint16_t>(read_be32(static_cast<const unsigned char *>(mv.mv_data)));
            else
              tv.vector.dim = 0;
          }
          tv.vector.data.assign(reinterpret_cast<const char *>(v.mv_data), v.mv_size);
          out.vectors.push_back(std::move(tv));
        }
        else if (rc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(rc));
      }
    }
    return out;
  }

  EdgeRef Store::getEdge(const GetEdgeParams &params)
  {
    Txn tx(env_.raw(), false);
    auto kkey = key_edge_by_id_be(params.edgeId);
    MDB_val k{kkey.size(), const_cast<char *>(kkey.data())}, v{};
    int rc = mdb_get(tx.get(), env_.edgesById(), &k, &v);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    if (v.mv_size != 24)
      throw MdbError("corrupt edge ref");
    const unsigned char *p = static_cast<const unsigned char *>(v.mv_data);
    EdgeRef ref{};
    ref.id = read_be64(p);
    ref.src = read_be64(p + 8);
    ref.dst = read_be64(p + 16);
    return ref;
  }

  void Store::deleteNode(const DeleteNodeParams &params)
  {
    Txn tx(env_.raw(), true);
    // load header to know labels
    auto nk = key_nodes_be(params.id);
    MDB_val k{nk.size(), const_cast<char *>(nk.data())}, v{};
    int rc = mdb_get(tx.get(), env_.nodes(), &k, &v);
    if (rc == 0)
    {
      NodeHeader hdr = decode_node_header(std::string_view(reinterpret_cast<const char *>(v.mv_data), v.mv_size));
      // remove label index entries
      for (uint32_t labelId : hdr.labels.labelIds)
      {
        auto lk = key_label_index_be(labelId, params.id);
        MDB_val lk0{lk.size(), const_cast<char *>(lk.data())};
        int drc = mdb_del(tx.get(), env_.labelIndex(), &lk0, nullptr);
        if (drc != 0 && drc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(drc));
      }
    }
    else if (rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));

    // delete node record
    rc = mdb_del(tx.get(), env_.nodes(), &k, nullptr);
    if (rc != 0 && rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));

    // delete edges where node is src or dst, plus their props and id records
    {
      std::unordered_set<uint64_t> edgeIdsToDelete;

      // scan edges where this node is src
      {
        MDB_cursor *cur{};
        rc = mdb_cursor_open(tx.get(), env_.edgesBySrcType(), &cur);
        if (rc)
          throw MdbError(mdb_strerror(rc));
        std::string start = key_edge_by_src_type_be(params.id, 0, 0, 0);
        MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
        while (rc == 0)
        {
          const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
          if (ck.mv_size < 8 + 4 + 8 + 8)
            break;
          uint64_t src = read_be64(kb + 0);
          if (src != params.id)
            break;
          uint32_t typeId = read_be32(kb + 8);
          uint64_t dst = read_be64(kb + 12);
          uint64_t eid = read_be64(kb + 20);

          // delete current src index entry
          int drc = mdb_cursor_del(cur, 0);
          if (drc)
            throw MdbError(mdb_strerror(drc));

          // delete matching dst index entry
          auto dk = key_edge_by_dst_type_be(dst, typeId, src, eid);
          MDB_val dk0{dk.size(), const_cast<char *>(dk.data())};
          drc = mdb_del(tx.get(), env_.edgesByDstType(), &dk0, nullptr);
          if (drc != 0 && drc != MDB_NOTFOUND)
            throw MdbError(mdb_strerror(drc));

          edgeIdsToDelete.insert(eid);
          rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
        }
        mdb_cursor_close(cur);
      }

      // scan edges where this node is dst (remaining ones not covered above)
      {
        MDB_cursor *cur{};
        rc = mdb_cursor_open(tx.get(), env_.edgesByDstType(), &cur);
        if (rc)
          throw MdbError(mdb_strerror(rc));
        std::string start = key_edge_by_dst_type_be(params.id, 0, 0, 0);
        MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
        while (rc == 0)
        {
          const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
          if (ck.mv_size < 8 + 4 + 8 + 8)
            break;
          uint64_t dst = read_be64(kb + 0);
          if (dst != params.id)
            break;
          uint32_t typeId = read_be32(kb + 8);
          uint64_t src = read_be64(kb + 12);
          uint64_t eid = read_be64(kb + 20);

          // delete current dst index entry
          int drc = mdb_cursor_del(cur, 0);
          if (drc)
            throw MdbError(mdb_strerror(drc));

          // delete matching src index entry
          auto sk = key_edge_by_src_type_be(src, typeId, dst, eid);
          MDB_val sk0{sk.size(), const_cast<char *>(sk.data())};
          drc = mdb_del(tx.get(), env_.edgesBySrcType(), &sk0, nullptr);
          if (drc != 0 && drc != MDB_NOTFOUND)
            throw MdbError(mdb_strerror(drc));

          edgeIdsToDelete.insert(eid);
          rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
        }
        mdb_cursor_close(cur);
      }

      // delete edgesById and edgeProps for collected eids
      for (uint64_t eid : edgeIdsToDelete)
      {
        auto idk = key_edge_by_id_be(eid);
        MDB_val idk0{idk.size(), const_cast<char *>(idk.data())};
        int drc = mdb_del(tx.get(), env_.edgesById(), &idk0, nullptr);
        if (drc != 0 && drc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(drc));

        // delete edge props by range
        MDB_cursor *pcur{};
        drc = mdb_cursor_open(tx.get(), env_.edgeProps(), &pcur);
        if (drc)
          throw MdbError(mdb_strerror(drc));
        std::string start = key_edge_prop_be(eid, 0);
        MDB_val pk{start.size(), const_cast<char *>(start.data())}, pv{};
        drc = mdb_cursor_get(pcur, &pk, &pv, MDB_SET_RANGE);
        while (drc == 0)
        {
          const unsigned char *kb = static_cast<const unsigned char *>(pk.mv_data);
          if (pk.mv_size < 8 + 4)
            break;
          uint64_t major = read_be64(kb + 0);
          if (major != eid)
            break;
          int erc = mdb_cursor_del(pcur, 0);
          if (erc)
            throw MdbError(mdb_strerror(erc));
          drc = mdb_cursor_get(pcur, &pk, &pv, MDB_NEXT);
        }
        mdb_cursor_close(pcur);
      }
    }

    // delete cold props by range
    {
      MDB_cursor *cur{};
      rc = mdb_cursor_open(tx.get(), env_.nodeColdProps(), &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = key_node_cold_prop_be(params.id, 0);
      MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
      rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
      while (rc == 0)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
        if (ck.mv_size < 8 + 4)
          break;
        uint64_t major = read_be64(kb + 0);
        if (major != params.id)
          break;
        int drc = mdb_cursor_del(cur, 0);
        if (drc)
          throw MdbError(mdb_strerror(drc));
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    }

    // delete vectors by range
    {
      MDB_cursor *cur{};
      rc = mdb_cursor_open(tx.get(), env_.nodeVectors(), &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = key_node_vector_be(params.id, 0);
      MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
      rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
      while (rc == 0)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
        if (ck.mv_size < 8 + 4)
          break;
        uint64_t major = read_be64(kb + 0);
        if (major != params.id)
          break;
        int drc = mdb_cursor_del(cur, 0);
        if (drc)
          throw MdbError(mdb_strerror(drc));
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    }

    // delete node record (after edges and props)
    rc = mdb_del(tx.get(), env_.nodes(), &k, nullptr);
    if (rc != 0 && rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));

    tx.commit();
  }

  void Store::deleteEdge(const DeleteEdgeParams &params)
  {
    Txn tx(env_.raw(), true);
    // read edge to get src/dst/typeId from indexes
    auto idk = key_edge_by_id_be(params.edgeId);
    MDB_val k{idk.size(), const_cast<char *>(idk.data())}, v{};
    int rc = mdb_get(tx.get(), env_.edgesById(), &k, &v);
    if (rc == 0)
    {
      if (v.mv_size != 24)
        throw MdbError("corrupt edge ref");
      const unsigned char *p = static_cast<const unsigned char *>(v.mv_data);
      EdgeRef ref{};
      ref.id = read_be64(p);
      ref.src = read_be64(p + 8);
      ref.dst = read_be64(p + 16);
      // we don't know typeId from edgesById - need to discover from one of the indexes by scanning edgeId suffix
      // scan src index range for this src to find a record with matching edgeId
      uint32_t foundType = 0;
      {
        MDB_cursor *cur{};
        rc = mdb_cursor_open(tx.get(), env_.edgesBySrcType(), &cur);
        if (rc)
          throw MdbError(mdb_strerror(rc));
        std::string start = key_edge_by_src_type_be(ref.src, 0, 0, 0);
        MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
        while (rc == 0)
        {
          const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
          if (ck.mv_size < 8 + 4 + 8 + 8)
            break;
          uint64_t s = read_be64(kb + 0);
          if (s != ref.src)
            break;
          uint32_t typeId = read_be32(kb + 8);
          uint64_t dst = read_be64(kb + 12);
          uint64_t eid = read_be64(kb + 20);
          if (dst == ref.dst && eid == ref.id)
          {
            foundType = typeId;
            break;
          }
          rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
        }
        mdb_cursor_close(cur);
      }

      // remove from indexes if type found
      if (foundType != 0)
      {
        auto sk = key_edge_by_src_type_be(ref.src, foundType, ref.dst, ref.id);
        MDB_val sk0{sk.size(), const_cast<char *>(sk.data())};
        int drc = mdb_del(tx.get(), env_.edgesBySrcType(), &sk0, nullptr);
        if (drc != 0 && drc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(drc));

        auto dk = key_edge_by_dst_type_be(ref.dst, foundType, ref.src, ref.id);
        MDB_val dk0{dk.size(), const_cast<char *>(dk.data())};
        drc = mdb_del(tx.get(), env_.edgesByDstType(), &dk0, nullptr);
        if (drc != 0 && drc != MDB_NOTFOUND)
          throw MdbError(mdb_strerror(drc));
      }
    }
    else if (rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));

    // delete edgesById
    rc = mdb_del(tx.get(), env_.edgesById(), &k, nullptr);
    if (rc != 0 && rc != MDB_NOTFOUND)
      throw MdbError(mdb_strerror(rc));

    // delete edge props by range
    {
      MDB_cursor *cur{};
      rc = mdb_cursor_open(tx.get(), env_.edgeProps(), &cur);
      if (rc)
        throw MdbError(mdb_strerror(rc));
      std::string start = key_edge_prop_be(params.edgeId, 0);
      MDB_val ck{start.size(), const_cast<char *>(start.data())}, cv{};
      rc = mdb_cursor_get(cur, &ck, &cv, MDB_SET_RANGE);
      while (rc == 0)
      {
        const unsigned char *kb = static_cast<const unsigned char *>(ck.mv_data);
        if (ck.mv_size < 8 + 4)
          break;
        uint64_t major = read_be64(kb + 0);
        if (major != params.edgeId)
          break;
        int drc = mdb_cursor_del(cur, 0);
        if (drc)
          throw MdbError(mdb_strerror(drc));
        rc = mdb_cursor_get(cur, &ck, &cv, MDB_NEXT);
      }
      mdb_cursor_close(cur);
    }

    tx.commit();
  }
}
