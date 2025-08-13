#include "env.hpp"
#include <lmdb.h>
#include <utility>

namespace stardust
{

  Txn::Txn(MDB_env *env, bool rw) : env_(env), rw_(rw)
  {
    int rc = mdb_txn_begin(env_, nullptr, rw_ ? 0 : MDB_RDONLY, &txn_);
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

  Txn::~Txn() noexcept
  {
    if (txn_)
      mdb_txn_abort(txn_);
  }

  Txn::Txn(Txn &&other) noexcept : env_(other.env_), txn_(other.txn_), rw_(other.rw_)
  {
    other.txn_ = nullptr;
  }

  Txn &Txn::operator=(Txn &&other) noexcept
  {
    if (this != &other)
    {
      abort();
      env_ = other.env_;
      txn_ = other.txn_;
      rw_ = other.rw_;
      other.txn_ = nullptr;
    }
    return *this;
  }

  MDB_txn *Txn::get() const { return txn_; }

  void Txn::commit()
  {
    int rc = mdb_txn_commit(txn_);
    txn_ = nullptr;
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

  void Txn::abort() noexcept
  {
    if (txn_)
    {
      mdb_txn_abort(txn_);
      txn_ = nullptr;
    }
  }

  Env::Env(const std::filesystem::path &path, size_t mapSizeBytes)
  {
    int rc = mdb_env_create(&env_);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    mdb_env_set_maxdbs(env_, 32);
    mdb_env_set_mapsize(env_, mapSizeBytes);
    rc = mdb_env_open(env_, path.c_str(), 0, 0664);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    // open main DBIs
    Txn tx(env_, true);
    open(tx.get(), nodes_, "nodes");
    open(tx.get(), nodeColdProps_, "nodeColdProps");
    open(tx.get(), nodeVectors_, "nodeVectors");

    open(tx.get(), edgesBySrcType_, "edgesBySrcType");
    open(tx.get(), edgesByDstType_, "edgesByDstType");
    open(tx.get(), edgesById_, "edgesById");
    open(tx.get(), edgeProps_, "edgeProps");

    open(tx.get(), labelIds_, "labelIds");
    open(tx.get(), labelsByName_, "labelsByName");
    open(tx.get(), relTypeIds_, "relTypeIds");
    open(tx.get(), relTypesByName_, "relTypesByName");
    open(tx.get(), propKeyIds_, "propKeyIds");
    open(tx.get(), propKeysByName_, "propKeysByName");
    open(tx.get(), vecTagIds_, "vecTagIds");
    open(tx.get(), vecTagsByName_, "vecTagsByName");
    open(tx.get(), vecTagMeta_, "vecTagMeta");

    open(tx.get(), meta_, "meta");
    open(tx.get(), labelIndex_, "labelIndex");
    tx.commit();
  }

  Env::~Env() noexcept
  {
    if (env_)
      mdb_env_close(env_);
  }

  Env::Env(Env &&other) noexcept
      : env_(other.env_),
        nodes_(other.nodes_),
        nodeColdProps_(other.nodeColdProps_),
        nodeVectors_(other.nodeVectors_),
        edgesBySrcType_(other.edgesBySrcType_),
        edgesByDstType_(other.edgesByDstType_),
        edgesById_(other.edgesById_),
        edgeProps_(other.edgeProps_),
        labelIds_(other.labelIds_),
        labelsByName_(other.labelsByName_),
        relTypeIds_(other.relTypeIds_),
        relTypesByName_(other.relTypesByName_),
        propKeyIds_(other.propKeyIds_),
        propKeysByName_(other.propKeysByName_),
        vecTagIds_(other.vecTagIds_),
        vecTagsByName_(other.vecTagsByName_),
        vecTagMeta_(other.vecTagMeta_),
        meta_(other.meta_),
        labelIndex_(other.labelIndex_)
  {
    other.env_ = nullptr;
  }

  Env &Env::operator=(Env &&other) noexcept
  {
    if (this != &other)
    {
      if (env_)
        mdb_env_close(env_);
      env_ = other.env_;
      nodes_ = other.nodes_;
      nodeColdProps_ = other.nodeColdProps_;
      nodeVectors_ = other.nodeVectors_;

      edgesBySrcType_ = other.edgesBySrcType_;
      edgesByDstType_ = other.edgesByDstType_;
      edgesById_ = other.edgesById_;
      edgeProps_ = other.edgeProps_;

      labelIds_ = other.labelIds_;
      labelsByName_ = other.labelsByName_;
      relTypeIds_ = other.relTypeIds_;
      relTypesByName_ = other.relTypesByName_;
      propKeyIds_ = other.propKeyIds_;
      propKeysByName_ = other.propKeysByName_;
      vecTagIds_ = other.vecTagIds_;
      vecTagsByName_ = other.vecTagsByName_;
      vecTagMeta_ = other.vecTagMeta_;

      meta_ = other.meta_;
      labelIndex_ = other.labelIndex_;
      other.env_ = nullptr;
    }
    return *this;
  }

  MDB_env *Env::raw() const { return env_; }
  DbHandle Env::nodes() const { return nodes_; }
  DbHandle Env::nodeColdProps() const { return nodeColdProps_; }
  DbHandle Env::nodeVectors() const { return nodeVectors_; }

  DbHandle Env::edgesBySrcType() const { return edgesBySrcType_; }
  DbHandle Env::edgesByDstType() const { return edgesByDstType_; }
  DbHandle Env::edgesById() const { return edgesById_; }
  DbHandle Env::edgeProps() const { return edgeProps_; }

  DbHandle Env::labelIds() const { return labelIds_; }
  DbHandle Env::labelsByName() const { return labelsByName_; }
  DbHandle Env::relTypeIds() const { return relTypeIds_; }
  DbHandle Env::relTypesByName() const { return relTypesByName_; }
  DbHandle Env::propKeyIds() const { return propKeyIds_; }
  DbHandle Env::propKeysByName() const { return propKeysByName_; }
  DbHandle Env::vecTagIds() const { return vecTagIds_; }
  DbHandle Env::vecTagsByName() const { return vecTagsByName_; }
  DbHandle Env::vecTagMeta() const { return vecTagMeta_; }

  DbHandle Env::meta() const { return meta_; }
  DbHandle Env::labelIndex() const { return labelIndex_; }

  void Env::open(MDB_txn *tx, DbHandle &out, const char *name)
  {
    int rc = mdb_dbi_open(tx, name, MDB_CREATE, &out);
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

} // namespace stardust
