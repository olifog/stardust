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
    DbHandle vecs() const;
    DbHandle eout() const;
    DbHandle ein() const;
    DbHandle ctrs() const;

  private:
    static void open(MDB_txn *tx, DbHandle &out, const char *name);

    MDB_env *env_{};
    DbHandle nodes_{}, vecs_{}, eout_{}, ein_{}, ctrs_{};
  };

} // namespace stardust
