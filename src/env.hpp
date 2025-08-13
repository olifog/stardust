#pragma once
#include <filesystem>
#include <stdexcept>

struct MDB_env;
struct MDB_txn;
using DbHandle = unsigned int;

namespace stardust
{

  struct MdbError : std::runtime_error
  {
    using std::runtime_error::runtime_error;
  };

  class Txn
  {
  public:
    Txn(MDB_env *env, bool rw);
    ~Txn() noexcept;
    Txn(const Txn &) = delete;
    Txn &operator=(const Txn &) = delete;
    Txn(Txn &&other) noexcept;
    Txn &operator=(Txn &&other) noexcept;

    MDB_txn *get() const;
    void commit();
    void abort() noexcept;

  private:
    MDB_env *env_{};
    MDB_txn *txn_{};
    bool rw_{};
  };

  class Env
  {
  public:
    Env(const std::filesystem::path &path, size_t mapSizeBytes = size_t(16ull << 30));
    ~Env() noexcept;
    Env(const Env &) = delete;
    Env &operator=(const Env &) = delete;
    Env(Env &&other) noexcept;
    Env &operator=(Env &&other) noexcept;

    MDB_env *raw() const;
    DbHandle nodes() const;
    DbHandle nodeColdProps() const;
    DbHandle nodeVectors() const;

    DbHandle edgesBySrcType() const;
    DbHandle edgesByDstType() const;
    DbHandle edgesById() const;
    DbHandle edgeProps() const;

    DbHandle labelIds() const;
    DbHandle labelsByName() const;
    DbHandle relTypeIds() const;
    DbHandle relTypesByName() const;
    DbHandle propKeyIds() const;
    DbHandle propKeysByName() const;
    DbHandle vecTagIds() const;
    DbHandle vecTagsByName() const;
    DbHandle vecTagMeta() const;

    DbHandle meta() const;
    DbHandle labelIndex() const;

  private:
    static void open(MDB_txn *tx, DbHandle &out, const char *name);

    MDB_env *env_{};
    DbHandle nodes_{};
    DbHandle nodeColdProps_{};
    DbHandle nodeVectors_{};

    DbHandle edgesBySrcType_{};
    DbHandle edgesByDstType_{};
    DbHandle edgesById_{};
    DbHandle edgeProps_{};

    DbHandle labelIds_{};
    DbHandle labelsByName_{};
    DbHandle relTypeIds_{};
    DbHandle relTypesByName_{};
    DbHandle propKeyIds_{};
    DbHandle propKeysByName_{};
    DbHandle vecTagIds_{};
    DbHandle vecTagsByName_{};
    DbHandle vecTagMeta_{};
    
    DbHandle meta_{};
    DbHandle labelIndex_{};
  };

} // namespace stardust
