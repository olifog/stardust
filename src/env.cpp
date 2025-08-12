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
    mdb_env_set_maxdbs(env_, 8);
    mdb_env_set_mapsize(env_, mapSizeBytes);
    rc = mdb_env_open(env_, path.c_str(), 0, 0664);
    if (rc)
      throw MdbError(mdb_strerror(rc));
    // open main DBIs
    Txn tx(env_, true);
    open(tx.get(), nodes_, "nodes");
    open(tx.get(), vecs_, "vecs");
    open(tx.get(), eout_, "edges_out");
    open(tx.get(), ein_, "edges_in");
    open(tx.get(), ctrs_, "counters");
    tx.commit();
  }

  Env::~Env() noexcept
  {
    if (env_)
      mdb_env_close(env_);
  }

  Env::Env(Env &&other) noexcept
      : env_(other.env_), nodes_(other.nodes_), vecs_(other.vecs_), eout_(other.eout_), ein_(other.ein_), ctrs_(other.ctrs_)
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
      vecs_ = other.vecs_;
      eout_ = other.eout_;
      ein_ = other.ein_;
      ctrs_ = other.ctrs_;
      other.env_ = nullptr;
    }
    return *this;
  }

  MDB_env *Env::raw() const { return env_; }
  DbHandle Env::nodes() const { return nodes_; }
  DbHandle Env::vecs() const { return vecs_; }
  DbHandle Env::eout() const { return eout_; }
  DbHandle Env::ein() const { return ein_; }
  DbHandle Env::ctrs() const { return ctrs_; }

  void Env::open(MDB_txn *tx, DbHandle &out, const char *name)
  {
    int rc = mdb_dbi_open(tx, name, MDB_CREATE, &out);
    if (rc)
      throw MdbError(mdb_strerror(rc));
  }

} // namespace stardust
