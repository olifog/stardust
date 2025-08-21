#include "server.hpp"
#include "encode.hpp"
#include <kj/debug.h>
#include <kj/array.h>

namespace stardust::rpc
{

  namespace
  {

    bool isValidUtf8(const char *data, size_t size)
    {
      const unsigned char *s = reinterpret_cast<const unsigned char *>(data);
      const unsigned char *e = s + size;
      while (s < e)
      {
        unsigned char c = *s++;
        if (c < 0x80)
          continue;
        unsigned int extra = (c >= 0xF0) ? 3 : (c >= 0xE0) ? 2
                                           : (c >= 0xC0)   ? 1
                                                           : 0xFF;
        if (extra == 0xFF || s + extra > e)
          return false;
        for (unsigned int i = 0; i < extra; ++i)
        {
          if ((s[i] & 0xC0) != 0x80)
            return false;
        }
        s += extra;
      }
      return true;
    }

    stardust::Value fromRpcValue(Value::Reader v, stardust::Store &store, bool createIfMissing)
    {
      switch (v.which())
      {
      case Value::I64:
        return static_cast<int64_t>(v.getI64());
      case Value::F64:
        return static_cast<double>(v.getF64());
      case Value::BOOLV:
        return static_cast<bool>(v.getBoolv());
      case Value::TEXT:
      {
        auto t = v.getText();
        // Avoid creating text-id entries for empty strings; store as raw bytes instead
        if (t.size() == 0)
        {
          return std::string();
        }
        std::string s(t.cStr());
        uint32_t id = store.getOrCreateTextId(s, createIfMissing);
        return id;
      }
      case Value::BYTES:
      {
        auto d = v.getBytes();
        if (d.size() == 0)
        {
          // Preserve empties as bytes; do not attempt UTF-8/text-id path
          return std::string();
        }
        const char *ptr = reinterpret_cast<const char *>(d.begin());
        size_t len = d.size();
        if (isValidUtf8(ptr, len))
        {
          std::string s(ptr, len);
          uint32_t id = store.getOrCreateTextId(s, createIfMissing);
          return id;
        }
        return std::string(ptr, len);
      }
      case Value::NULLV:
      default:
        return std::monostate{};
      }
    }

    void toRpcValue(Value::Builder b, const stardust::Value &v, stardust::Store &store)
    {
      if (std::holds_alternative<int64_t>(v))
      {
        b.setI64(std::get<int64_t>(v));
        return;
      }
      if (std::holds_alternative<double>(v))
      {
        b.setF64(std::get<double>(v));
        return;
      }
      if (std::holds_alternative<bool>(v))
      {
        b.setBoolv(std::get<bool>(v));
        return;
      }
      if (std::holds_alternative<uint32_t>(v))
      {
        auto id = std::get<uint32_t>(v);
        auto s = store.getTextName(id);
        b.setText(s);
        return;
      }
      if (std::holds_alternative<std::string>(v))
      {
        const auto &s = std::get<std::string>(v);
        b.setBytes(kj::ArrayPtr<const capnp::byte>(reinterpret_cast<const capnp::byte *>(s.data()), s.size()));
        return;
      }
      b.setNullv();
    }

    stardust::Property fromRpcProperty(Property::Reader p, stardust::Store &store, bool createIfMissing)
    {
      stardust::Property out{};
      auto name = p.getKey();
      out.keyId = store.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(name.cStr()), createIfMissing});
      out.val = fromRpcValue(p.getVal(), store, createIfMissing);
      return out;
    }

    void toRpcProperty(Property::Builder b, const stardust::Property &p, stardust::Store &store)
    {
      auto keyName = store.getPropKeyName(p.keyId);
      b.setKey(keyName);
      toRpcValue(b.initVal(), p.val, store);
    }

    stardust::LabelSet fromRpcLabelSet(LabelSet::Reader ls, stardust::Store &store, bool createIfMissing)
    {
      stardust::LabelSet out{};
      auto names = ls.getNames();
      out.labelIds.reserve(names.size());
      for (auto nm : names)
        out.labelIds.push_back(store.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(nm.cStr()), createIfMissing}));
      return out;
    }

    void toRpcLabelSet(LabelSet::Builder b, const stardust::LabelSet &ls, stardust::Store &store)
    {
      auto arr = b.initNames(ls.labelIds.size());
      for (uint32_t i = 0; i < ls.labelIds.size(); ++i)
        arr.set(i, store.getLabelName(ls.labelIds[i]));
    }

    stardust::VectorF32 fromRpcVector(VectorF32::Reader v)
    {
      stardust::VectorF32 out{};
      out.dim = v.getDim();
      auto d = v.getData();
      out.data.assign(reinterpret_cast<const char *>(d.begin()), d.size());
      return out;
    }

    void toRpcVector(VectorF32::Builder b, const stardust::VectorF32 &v)
    {
      b.setDim(v.dim);
      b.setData(kj::ArrayPtr<const capnp::byte>(reinterpret_cast<const capnp::byte *>(v.data.data()), v.data.size()));
    }

    stardust::TaggedVector fromRpcTaggedVector(TaggedVector::Reader tv, stardust::Store &store, bool createIfMissing)
    {
      stardust::TaggedVector out{};
      auto nm = tv.getTag();
      out.vector = fromRpcVector(tv.getVector());
      std::optional<uint16_t> dimOpt;
      if (createIfMissing && out.vector.dim != 0)
        dimOpt = out.vector.dim;
      out.tagId = store.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(nm.cStr()), createIfMissing, dimOpt});
      return out;
    }

    void toRpcTaggedVector(TaggedVector::Builder b, const stardust::TaggedVector &tv, stardust::Store &store)
    {
      b.setTag(store.getVecTagName(tv.tagId));
      toRpcVector(b.initVector(), tv.vector);
    }

    stardust::Direction fromRpcDirection(Direction d)
    {
      switch (d)
      {
      case Direction::OUT:
        return stardust::Direction::Out;
      case Direction::IN:
        return stardust::Direction::In;
      case Direction::BOTH:
      default:
        return stardust::Direction::Both;
      }
    }

    void toRpcNodeHeader(NodeHeader::Builder b, const stardust::NodeHeader &h, stardust::Store &store)
    {
      b.setId(h.id);
      toRpcLabelSet(b.initLabels(), h.labels, store);
      auto hp = b.initHotProps(h.hotProps.size());
      for (uint32_t i = 0; i < h.hotProps.size(); ++i)
        toRpcProperty(hp[i], h.hotProps[i], store);
    }

  } // namespace

  StardustImpl::StardustImpl(stardust::Store &s) : store_(s) {}

  kj::Promise<void> StardustImpl::createNode(CreateNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();

    stardust::CreateNodeParams in{};
    in.labels = fromRpcLabelSet(params.getLabels(), store_, true);
    {
      auto hp = params.getHotProps();
      in.hotProps.reserve(hp.size());
      for (auto pr : hp)
        in.hotProps.push_back(fromRpcProperty(pr, store_, true));
    }
    {
      auto cp = params.getColdProps();
      in.coldProps.reserve(cp.size());
      for (auto pr : cp)
        in.coldProps.push_back(fromRpcProperty(pr, store_, true));
    }
    {
      auto vecs = params.getVectors();
      in.vectors.reserve(vecs.size());
      for (auto tv : vecs)
        in.vectors.push_back(fromRpcTaggedVector(tv, store_, true));
    }

    auto result = store_.createNode(in);

    auto res = ctx.getResults();
    auto out = res.initResult();
    out.initNode().setId(result.id);
    toRpcNodeHeader(out.initHeader(), result.header, store_);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::upsertNodeProps(UpsertNodePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpsertNodePropsParams in{};
    in.id = params.getId();
    {
      auto sp = params.getSetHot();
      in.setHot.reserve(sp.size());
      for (auto pr : sp)
        in.setHot.push_back(fromRpcProperty(pr, store_, true));
    }
    {
      auto sp = params.getSetCold();
      in.setCold.reserve(sp.size());
      for (auto pr : sp)
        in.setCold.push_back(fromRpcProperty(pr, store_, true));
    }
    {
      auto uk = params.getUnsetKeys();
      in.unsetKeys.reserve(uk.size());
      for (auto k : uk)
        in.unsetKeys.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
    }
    store_.upsertNodeProps(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::setNodeLabels(SetNodeLabelsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::SetNodeLabelsParams in{};
    in.id = params.getId();
    {
      auto add = params.getAddLabels();
      in.addLabels.reserve(add.size());
      for (auto nm : add)
        in.addLabels.push_back(store_.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(nm.cStr()), true}));
    }
    {
      auto rm = params.getRemoveLabels();
      in.removeLabels.reserve(rm.size());
      for (auto nm : rm)
        in.removeLabels.push_back(store_.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(nm.cStr()), false}));
    }
    store_.setNodeLabels(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::upsertVector(UpsertVectorContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpsertVectorParams in{};
    in.id = params.getId();
    in.vector = fromRpcVector(params.getVector());
    {
      std::optional<uint16_t> dimOpt;
      if (in.vector.dim != 0)
        dimOpt = in.vector.dim;
      in.tagId = store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(params.getTag().cStr()), true, dimOpt});
    }
    store_.upsertVector(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::deleteVector(DeleteVectorContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteVectorParams in{};
    in.id = params.getId();
    in.tagId = store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(params.getTag().cStr()), false});
    store_.deleteVector(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::addEdge(AddEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();

    stardust::AddEdgeParams in{};
    in.src = params.getSrc();
    in.dst = params.getDst();
    in.meta.typeId = store_.getOrCreateRelTypeId(stardust::GetOrCreateRelTypeIdParams{std::string(params.getMeta().getType().cStr()), true});
    {
      auto props = params.getMeta().getProps();
      in.meta.props.reserve(props.size());
      for (auto pr : props)
        in.meta.props.push_back(fromRpcProperty(pr, store_, true));
    }

    auto edge = store_.addEdge(in);

    auto res = ctx.getResults();
    auto out = res.initEdge();
    out.setId(edge.id);
    out.setSrc(edge.src);
    out.setDst(edge.dst);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::updateEdgeProps(UpdateEdgePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpdateEdgePropsParams in{};
    in.edgeId = params.getEdgeId();
    {
      auto sp = params.getSetProps();
      in.setProps.reserve(sp.size());
      for (auto pr : sp)
        in.setProps.push_back(fromRpcProperty(pr, store_, true));
    }
    {
      auto uk = params.getUnsetKeys();
      in.unsetKeys.reserve(uk.size());
      for (auto k : uk)
        in.unsetKeys.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
    }
    store_.updateEdgeProps(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::listAdjacency(ListAdjacencyContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::ListAdjacencyParams in{};
    in.node = params.getNode();
    in.direction = fromRpcDirection(params.getDirection());
    in.limit = params.getLimit();
    auto adj = store_.listAdjacency(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto items = out.initItems(adj.items.size());
    for (uint32_t i = 0; i < adj.items.size(); ++i)
    {
      const auto &a = adj.items[i];
      auto row = items[i];
      row.setNeighbor(a.neighborId);
      row.setEdgeId(a.edgeId);
      row.setType(store_.getRelTypeName(a.typeId));
      switch (a.direction)
      {
      case stardust::Direction::Out:
        row.setDirection(Direction::OUT);
        break;
      case stardust::Direction::In:
        row.setDirection(Direction::IN);
        break;
      case stardust::Direction::Both:
        row.setDirection(Direction::BOTH);
        break;
      }
    }
    return kj::READY_NOW;
  }

  

  kj::Promise<void> StardustImpl::getEdgeProps(GetEdgePropsContext ctx)
  {
    auto p = ctx.getParams();
    stardust::GetEdgePropsParams in{};
    in.edgeId = p.getEdgeId();
    {
      auto ks = p.getKeys();
      in.keyIds.reserve(ks.size());
      for (auto k : ks)
        in.keyIds.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
    }
    auto resv = store_.getEdgeProps(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto props = out.initProps(resv.props.size());
    for (uint32_t i = 0; i < resv.props.size(); ++i)
      toRpcProperty(props[i], resv.props[i], store_);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::scanNodesByLabel(ScanNodesByLabelContext ctx)
  {
    auto p = ctx.getParams();
    stardust::ScanNodesByLabelParams in{};
    in.labelId = store_.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(p.getLabel().cStr()), false});
    in.limit = p.getLimit();
    auto resv = store_.scanNodesByLabel(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto ids = out.initNodeIds(resv.nodeIds.size());
    for (uint32_t i = 0; i < resv.nodeIds.size(); ++i)
      ids.set(i, resv.nodeIds[i]);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::degree(DegreeContext ctx)
  {
    auto p = ctx.getParams();
    stardust::DegreeParams in{};
    in.node = p.getNode();
    in.direction = fromRpcDirection(p.getDirection());
    auto resv = store_.degree(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    out.setCount(resv.count);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::knn(KnnContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::KnnParams in{};
    in.tagId = store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(params.getTag().cStr()), false});
    in.query = fromRpcVector(params.getQuery());
    in.k = params.getK();
    auto resv = store_.knn(in);

    auto res = ctx.getResults();
    auto out = res.initResult();
    auto hits = out.initHits(resv.hits.size());
    for (uint32_t i = 0; i < resv.hits.size(); ++i)
    {
      auto h = hits[i];
      h.setId(resv.hits[i].id);
      h.setScore(resv.hits[i].score);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::writeBatch(WriteBatchContext ctx)
  {
    auto p = ctx.getParams();
    auto batch = p.getBatch();
    auto ops = batch.getOps();
    for (auto op : ops)
    {
      switch (op.which())
      {
      case WriteOp::CREATE_NODE:
      {
        stardust::CreateNodeParams in{};
        auto r = op.getCreateNode();
        in.labels = fromRpcLabelSet(r.getLabels(), store_, true);
        for (auto pr : r.getHotProps())
          in.hotProps.push_back(fromRpcProperty(pr, store_, true));
        for (auto pr : r.getColdProps())
          in.coldProps.push_back(fromRpcProperty(pr, store_, true));
        for (auto tv : r.getVectors())
          // TODO: check if tag exists,
          in.vectors.push_back(fromRpcTaggedVector(tv, store_, true));
        (void)store_.createNode(in);
        break;
      }
      case WriteOp::UPSERT_NODE_PROPS:
      {
        stardust::UpsertNodePropsParams in{};
        auto r = op.getUpsertNodeProps();
        in.id = r.getId();
        for (auto pr : r.getSetHot())
          in.setHot.push_back(fromRpcProperty(pr, store_, true));
        for (auto pr : r.getSetCold())
          in.setCold.push_back(fromRpcProperty(pr, store_, true));
        for (auto k : r.getUnsetKeys())
          in.unsetKeys.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
        store_.upsertNodeProps(in);
        break;
      }
      case WriteOp::SET_NODE_LABELS:
      {
        stardust::SetNodeLabelsParams in{};
        auto r = op.getSetNodeLabels();
        in.id = r.getId();
        for (auto nm : r.getAddLabels())
          in.addLabels.push_back(store_.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(nm.cStr()), true}));
        for (auto nm : r.getRemoveLabels())
          in.removeLabels.push_back(store_.getOrCreateLabelId(stardust::GetOrCreateLabelIdParams{std::string(nm.cStr()), false}));
        store_.setNodeLabels(in);
        break;
      }
      case WriteOp::UPSERT_VECTOR:
      {
        stardust::UpsertVectorParams in{};
        auto r = op.getUpsertVector();
        in.id = r.getId();
        in.vector = fromRpcVector(r.getVector());
        {
          std::optional<uint16_t> dimOpt;
          if (in.vector.dim != 0)
            dimOpt = in.vector.dim;
          in.tagId = store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(r.getTag().cStr()), true, dimOpt});
        }
        store_.upsertVector(in);
        break;
      }
      case WriteOp::DELETE_VECTOR:
      {
        stardust::DeleteVectorParams in{};
        auto r = op.getDeleteVector();
        in.id = r.getId();
        in.tagId = store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(r.getTag().cStr()), false});
        store_.deleteVector(in);
        break;
      }
      case WriteOp::ADD_EDGE:
      {
        stardust::AddEdgeParams in{};
        auto r = op.getAddEdge();
        in.src = r.getSrc();
        in.dst = r.getDst();
        in.meta.typeId = store_.getOrCreateRelTypeId(stardust::GetOrCreateRelTypeIdParams{std::string(r.getMeta().getType().cStr()), true});
        for (auto pr : r.getMeta().getProps())
          in.meta.props.push_back(fromRpcProperty(pr, store_, true));
        (void)store_.addEdge(in);
        break;
      }
      case WriteOp::UPDATE_EDGE_PROPS:
      {
        stardust::UpdateEdgePropsParams in{};
        auto r = op.getUpdateEdgeProps();
        in.edgeId = r.getEdgeId();
        for (auto pr : r.getSetProps())
          in.setProps.push_back(fromRpcProperty(pr, store_, true));
        for (auto k : r.getUnsetKeys())
          in.unsetKeys.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
        store_.updateEdgeProps(in);
        break;
      }
      default:
        break;
      }
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::getNode(GetNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetNodeParams in{};
    in.id = params.getId();
    auto resv = store_.getNode(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    toRpcNodeHeader(out.initHeader(), resv.header, store_);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::getNodeProps(GetNodePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetNodePropsParams in{};
    in.id = params.getId();
    {
      auto ks = params.getKeys();
      in.keyIds.reserve(ks.size());
      for (auto k : ks)
        in.keyIds.push_back(store_.getOrCreatePropKeyId(stardust::GetOrCreatePropKeyIdParams{std::string(k.cStr()), false}));
    }
    auto resv = store_.getNodeProps(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto props = out.initProps(resv.props.size());
    for (uint32_t i = 0; i < resv.props.size(); ++i)
      toRpcProperty(props[i], resv.props[i], store_);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::getVectors(GetVectorsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetVectorsParams in{};
    in.id = params.getId();
    {
      auto tags = params.getTags();
      in.tagIds.reserve(tags.size());
      for (auto nm : tags)
        in.tagIds.push_back(store_.getOrCreateVecTagId(stardust::GetOrCreateVecTagIdParams{std::string(nm.cStr()), false}));
    }
    auto resv = store_.getVectors(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto vecs = out.initVectors(resv.vectors.size());
    for (uint32_t i = 0; i < resv.vectors.size(); ++i)
      toRpcTaggedVector(vecs[i], resv.vectors[i], store_);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::getEdge(GetEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetEdgeParams in{};
    in.edgeId = params.getEdgeId();
    auto edge = store_.getEdge(in);
    auto res = ctx.getResults();
    auto outEdge = res.initEdge();
    outEdge.setId(edge.id);
    outEdge.setSrc(edge.src);
    outEdge.setDst(edge.dst);
    // Populate meta: type + props
    auto outMeta = res.initMeta();
    {
      uint32_t typeId = store_.getEdgeTypeId(edge);
      outMeta.setType(store_.getRelTypeName(typeId));
    }
    {
      auto propsRes = store_.getEdgeProps(stardust::GetEdgePropsParams{.edgeId = edge.id});
      auto props = outMeta.initProps(propsRes.props.size());
      for (uint32_t i = 0; i < propsRes.props.size(); ++i)
        toRpcProperty(props[i], propsRes.props[i], store_);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::deleteNode(DeleteNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteNodeParams in{};
    in.id = params.getId();
    store_.deleteNode(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> StardustImpl::deleteEdge(DeleteEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteEdgeParams in{};
    in.edgeId = params.getEdgeId();
    store_.deleteEdge(in);
    return kj::READY_NOW;
  }

} // namespace stardust::rpc
