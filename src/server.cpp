#include "server.hpp"
#include "encode.hpp"
#include <kj/debug.h>
#include <kj/array.h>

namespace stardust::rpc
{

  namespace
  {

    stardust::Value fromRpcValue(Value::Reader v)
    {
      switch (v.which())
      {
      case Value::I64:
        return static_cast<int64_t>(v.getI64());
      case Value::F64:
        return static_cast<double>(v.getF64());
      case Value::BOOLV:
        return static_cast<bool>(v.getBoolv());
      case Value::TEXT_ID:
        return static_cast<uint32_t>(v.getTextId());
      case Value::BYTES:
      {
        auto d = v.getBytes();
        return std::string(reinterpret_cast<const char *>(d.begin()), d.size());
      }
      case Value::NULLV:
      default:
        return std::monostate{};
      }
    }

    void toRpcValue(Value::Builder b, const stardust::Value &v)
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
        b.setTextId(std::get<uint32_t>(v));
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

    stardust::Property fromRpcProperty(Property::Reader p)
    {
      stardust::Property out{};
      out.keyId = p.getKeyId();
      out.val = fromRpcValue(p.getVal());
      return out;
    }

    void toRpcProperty(Property::Builder b, const stardust::Property &p)
    {
      b.setKeyId(p.keyId);
      toRpcValue(b.initVal(), p.val);
    }

    stardust::LabelSet fromRpcLabelSet(LabelSet::Reader ls)
    {
      stardust::LabelSet out{};
      auto ids = ls.getLabelIds();
      out.labelIds.reserve(ids.size());
      for (auto id : ids)
        out.labelIds.push_back(id);
      return out;
    }

    void toRpcLabelSet(LabelSet::Builder b, const stardust::LabelSet &ls)
    {
      auto arr = b.initLabelIds(ls.labelIds.size());
      for (uint32_t i = 0; i < ls.labelIds.size(); ++i)
        arr.set(i, ls.labelIds[i]);
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

    stardust::TaggedVector fromRpcTaggedVector(TaggedVector::Reader tv)
    {
      stardust::TaggedVector out{};
      out.tagId = tv.getTagId();
      out.vector = fromRpcVector(tv.getVector());
      return out;
    }

    void toRpcTaggedVector(TaggedVector::Builder b, const stardust::TaggedVector &tv)
    {
      b.setTagId(tv.tagId);
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

    void toRpcNodeHeader(NodeHeader::Builder b, const stardust::NodeHeader &h)
    {
      b.setId(h.id);
      toRpcLabelSet(b.initLabels(), h.labels);
      auto hp = b.initHotProps(h.hotProps.size());
      for (uint32_t i = 0; i < h.hotProps.size(); ++i)
        toRpcProperty(hp[i], h.hotProps[i]);
    }

  } // namespace

  GraphDbImpl::GraphDbImpl(stardust::Store &s) : store_(s) {}

  kj::Promise<void> GraphDbImpl::createNode(CreateNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();

    stardust::CreateNodeParams in{};
    in.labels = fromRpcLabelSet(params.getLabels());
    {
      auto hp = params.getHotProps();
      in.hotProps.reserve(hp.size());
      for (auto pr : hp)
        in.hotProps.push_back(fromRpcProperty(pr));
    }
    {
      auto cp = params.getColdProps();
      in.coldProps.reserve(cp.size());
      for (auto pr : cp)
        in.coldProps.push_back(fromRpcProperty(pr));
    }
    {
      auto vecs = params.getVectors();
      in.vectors.reserve(vecs.size());
      for (auto tv : vecs)
        in.vectors.push_back(fromRpcTaggedVector(tv));
    }

    auto result = store_.createNode(in);

    auto res = ctx.getResults();
    auto out = res.initResult();
    out.initNode().setId(result.id);
    toRpcNodeHeader(out.initHeader(), result.header);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::upsertNodeProps(UpsertNodePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpsertNodePropsParams in{};
    in.id = params.getId();
    {
      auto sp = params.getSetHot();
      in.setHot.reserve(sp.size());
      for (auto pr : sp)
        in.setHot.push_back(fromRpcProperty(pr));
    }
    {
      auto sp = params.getSetCold();
      in.setCold.reserve(sp.size());
      for (auto pr : sp)
        in.setCold.push_back(fromRpcProperty(pr));
    }
    {
      auto uk = params.getUnsetKeys();
      in.unsetKeys.reserve(uk.size());
      for (auto k : uk)
        in.unsetKeys.push_back(k);
    }
    store_.upsertNodeProps(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::setNodeLabels(SetNodeLabelsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::SetNodeLabelsParams in{};
    in.id = params.getId();
    {
      auto add = params.getAddLabels();
      in.addLabels.reserve(add.size());
      for (auto id : add)
        in.addLabels.push_back(id);
    }
    {
      auto rm = params.getRemoveLabels();
      in.removeLabels.reserve(rm.size());
      for (auto id : rm)
        in.removeLabels.push_back(id);
    }
    store_.setNodeLabels(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::upsertVector(UpsertVectorContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpsertVectorParams in{};
    in.id = params.getId();
    in.tagId = params.getTagId();
    in.vector = fromRpcVector(params.getVector());
    store_.upsertVector(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::deleteVector(DeleteVectorContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteVectorParams in{};
    in.id = params.getId();
    in.tagId = params.getTagId();
    store_.deleteVector(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::addEdge(AddEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();

    stardust::AddEdgeParams in{};
    in.src = params.getSrc();
    in.dst = params.getDst();
    in.meta.typeId = params.getMeta().getTypeId();
    {
      auto props = params.getMeta().getProps();
      in.meta.props.reserve(props.size());
      for (auto pr : props)
        in.meta.props.push_back(fromRpcProperty(pr));
    }

    auto edge = store_.addEdge(in);

    auto res = ctx.getResults();
    auto out = res.initEdge();
    out.setId(edge.id);
    out.setSrc(edge.src);
    out.setDst(edge.dst);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::updateEdgeProps(UpdateEdgePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::UpdateEdgePropsParams in{};
    in.edgeId = params.getEdgeId();
    {
      auto sp = params.getSetProps();
      in.setProps.reserve(sp.size());
      for (auto pr : sp)
        in.setProps.push_back(fromRpcProperty(pr));
    }
    {
      auto uk = params.getUnsetKeys();
      in.unsetKeys.reserve(uk.size());
      for (auto k : uk)
        in.unsetKeys.push_back(k);
    }
    store_.updateEdgeProps(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::neighbors(NeighborsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::NeighborsParams in{};
    in.node = params.getNode();
    in.direction = fromRpcDirection(params.getDirection());
    in.limit = params.getLimit();
    {
      auto r = params.getRelTypeIn();
      in.relTypeIn.reserve(r.size());
      for (auto x : r)
        in.relTypeIn.push_back(x);
    }
    in.neighborHas = fromRpcLabelSet(params.getNeighborHas());

    auto resv = store_.neighbors(in);

    auto res = ctx.getResults();
    auto out = res.initResult();
    auto list = out.initNeighbors(resv.neighbors.size());
    for (uint32_t i = 0; i < resv.neighbors.size(); ++i)
      list.set(i, resv.neighbors[i]);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::knn(KnnContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::KnnParams in{};
    in.tagId = params.getTagId();
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

  kj::Promise<void> GraphDbImpl::writeBatch(WriteBatchContext ctx)
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
        in.labels = fromRpcLabelSet(r.getLabels());
        for (auto pr : r.getHotProps())
          in.hotProps.push_back(fromRpcProperty(pr));
        for (auto pr : r.getColdProps())
          in.coldProps.push_back(fromRpcProperty(pr));
        for (auto tv : r.getVectors())
          in.vectors.push_back(fromRpcTaggedVector(tv));
        (void)store_.createNode(in);
        break;
      }
      case WriteOp::UPSERT_NODE_PROPS:
      {
        stardust::UpsertNodePropsParams in{};
        auto r = op.getUpsertNodeProps();
        in.id = r.getId();
        for (auto pr : r.getSetHot())
          in.setHot.push_back(fromRpcProperty(pr));
        for (auto pr : r.getSetCold())
          in.setCold.push_back(fromRpcProperty(pr));
        for (auto k : r.getUnsetKeys())
          in.unsetKeys.push_back(k);
        store_.upsertNodeProps(in);
        break;
      }
      case WriteOp::SET_NODE_LABELS:
      {
        stardust::SetNodeLabelsParams in{};
        auto r = op.getSetNodeLabels();
        in.id = r.getId();
        for (auto x : r.getAddLabels())
          in.addLabels.push_back(x);
        for (auto x : r.getRemoveLabels())
          in.removeLabels.push_back(x);
        store_.setNodeLabels(in);
        break;
      }
      case WriteOp::UPSERT_VECTOR:
      {
        stardust::UpsertVectorParams in{};
        auto r = op.getUpsertVector();
        in.id = r.getId();
        in.tagId = r.getTagId();
        in.vector = fromRpcVector(r.getVector());
        store_.upsertVector(in);
        break;
      }
      case WriteOp::DELETE_VECTOR:
      {
        stardust::DeleteVectorParams in{};
        auto r = op.getDeleteVector();
        in.id = r.getId();
        in.tagId = r.getTagId();
        store_.deleteVector(in);
        break;
      }
      case WriteOp::ADD_EDGE:
      {
        stardust::AddEdgeParams in{};
        auto r = op.getAddEdge();
        in.src = r.getSrc();
        in.dst = r.getDst();
        in.meta.typeId = r.getMeta().getTypeId();
        for (auto pr : r.getMeta().getProps())
          in.meta.props.push_back(fromRpcProperty(pr));
        (void)store_.addEdge(in);
        break;
      }
      case WriteOp::UPDATE_EDGE_PROPS:
      {
        stardust::UpdateEdgePropsParams in{};
        auto r = op.getUpdateEdgeProps();
        in.edgeId = r.getEdgeId();
        for (auto pr : r.getSetProps())
          in.setProps.push_back(fromRpcProperty(pr));
        for (auto k : r.getUnsetKeys())
          in.unsetKeys.push_back(k);
        store_.updateEdgeProps(in);
        break;
      }
      default:
        break;
      }
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::getNode(GetNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetNodeParams in{};
    in.id = params.getId();
    auto resv = store_.getNode(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    toRpcNodeHeader(out.initHeader(), resv.header);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::getNodeProps(GetNodePropsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetNodePropsParams in{};
    in.id = params.getId();
    {
      auto ks = params.getKeyIds();
      in.keyIds.reserve(ks.size());
      for (auto k : ks)
        in.keyIds.push_back(k);
    }
    auto resv = store_.getNodeProps(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto props = out.initProps(resv.props.size());
    for (uint32_t i = 0; i < resv.props.size(); ++i)
      toRpcProperty(props[i], resv.props[i]);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::getVectors(GetVectorsContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetVectorsParams in{};
    in.id = params.getId();
    {
      auto ids = params.getTagIds();
      in.tagIds.reserve(ids.size());
      for (auto x : ids)
        in.tagIds.push_back(x);
    }
    auto resv = store_.getVectors(in);
    auto res = ctx.getResults();
    auto out = res.initResult();
    auto vecs = out.initVectors(resv.vectors.size());
    for (uint32_t i = 0; i < resv.vectors.size(); ++i)
      toRpcTaggedVector(vecs[i], resv.vectors[i]);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::getEdge(GetEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::GetEdgeParams in{};
    in.edgeId = params.getEdgeId();
    auto edge = store_.getEdge(in);
    auto res = ctx.getResults();
    auto out = res.initEdge();
    out.setId(edge.id);
    out.setSrc(edge.src);
    out.setDst(edge.dst);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::deleteNode(DeleteNodeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteNodeParams in{};
    in.id = params.getId();
    store_.deleteNode(in);
    return kj::READY_NOW;
  }

  kj::Promise<void> GraphDbImpl::deleteEdge(DeleteEdgeContext ctx)
  {
    auto p = ctx.getParams();
    auto params = p.getParams();
    stardust::DeleteEdgeParams in{};
    in.edgeId = params.getEdgeId();
    store_.deleteEdge(in);
    return kj::READY_NOW;
  }

} // namespace stardust::rpc
