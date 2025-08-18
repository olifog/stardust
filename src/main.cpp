#include "env.hpp"
#include "store.hpp"
#include "server.hpp"
#include "http_server.hpp"
#include <capnp/ez-rpc.h>
#include <kj/async-io.h>
#include <kj/main.h>
#include <kj/debug.h>
#include <filesystem>
#include <cstring>
#include <unistd.h>

class StardustdApp
{
public:
  explicit StardustdApp(kj::ProcessContext &context) : context_(context) {}

  kj::MainFunc getMain()
  {
    return kj::MainBuilder(context_, "0.1", "Stardust vector/graph DB server using capnproto RPC")
        .addOption({'v'}, KJ_BIND_METHOD(*this, optVerbose),
                   "increase logging verbosity (INFO)")
        .addOptionWithArg({'b', "bind"}, KJ_BIND_METHOD(*this, optBind),
                          "bind", "bind address (e.g., unix:/tmp/stardust.sock or 0.0.0.0:0)")
        .addOptionWithArg({'d', "data"}, KJ_BIND_METHOD(*this, optData),
                          "dir", "data directory for LMDB (default: data)")
        .addOptionWithArg({'H', "http"}, KJ_BIND_METHOD(*this, optHttp),
                          "http", "HTTP bind (e.g., http://0.0.0.0:8080, default: disabled)")
        .callAfterParsing(KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext &context_;
  bool verbose_ = false;
  kj::String bind_ = kj::heapString("unix:/tmp/stardust.sock");
  kj::String dataDir_ = kj::heapString("data");
  size_t mapSizeBytes_ = size_t(16ull << 30);
  kj::String httpBind_ = kj::heapString("");

  kj::MainBuilder::Validity optVerbose()
  {
    verbose_ = true;
    context_.increaseLoggingVerbosity();
    return true;
  }

  kj::MainBuilder::Validity optBind(kj::StringPtr value)
  {
    bind_ = kj::heapString(value);
    return true;
  }

  kj::MainBuilder::Validity optData(kj::StringPtr value)
  {
    dataDir_ = kj::heapString(value);
    return true;
  }

  kj::MainBuilder::Validity optHttp(kj::StringPtr value)
  {
    httpBind_ = kj::heapString(value);
    return true;
  }

  kj::MainBuilder::Validity run()
  {
    try
    {
      std::filesystem::create_directories(std::filesystem::path(dataDir_.cStr()));

      stardust::Env env(std::filesystem::path(dataDir_.cStr()), mapSizeBytes_);
      stardust::Store store(env);

      const char *bindC = bind_.cStr();
      if (std::strncmp(bindC, "unix:", 5) == 0)
      {
        const char *path = bindC + 5;
        ::unlink(path);
      }

      if (httpBind_.size() > 0)
      {
        stardust::http::startHttpServer(store, std::string(httpBind_.cStr()));
        KJ_LOG(INFO, "http server listening on ", httpBind_);
      }

      capnp::EzRpcServer server(kj::heap<stardust::rpc::GraphDbImpl>(store), bindC);
      auto &waitScope = server.getWaitScope();
      if (std::strncmp(bindC, "unix:", 5) == 0)
      {
        KJ_LOG(INFO, "stardustd listening on ", bindC);
      }
      else
      {
        auto addr = server.getPort().wait(waitScope);
        KJ_LOG(INFO, "stardustd listening on ", bindC, " (port ", addr, ")");
      }
      kj::NEVER_DONE.wait(waitScope);
    }
    catch (const std::exception &e)
    {
      KJ_LOG(ERROR, "fatal: ", e.what());
      return kj::MainBuilder::Validity("fatal error");
    }
    return true;
  }
};

KJ_MAIN(StardustdApp);
