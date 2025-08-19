#pragma once
#include "store.hpp"
#include <string>

namespace stardust::http
{

  // Starts a Mongoose HTTP server in a background thread.
  // The server exposes minimal endpoints:
  // - POST /api/node?labels=LabelA,LabelB -> { "id": <u64> }
  // - POST /api/edge?src=<u64>&dst=<u64>&type=<name> -> { "id": <u64> }
  // - GET  /api/neighbors?node=<u64>&direction=out|in|both&limit=<u32> -> { "neighbors": [u64...] }
  // - GET  /api/health -> { "ok": true }
  // bind must be like "http://0.0.0.0:8080" or "http://127.0.0.1:0" (0 means ephemeral)
  void startHttpServer(stardust::Store &store, const std::string &bind);

} // namespace stardust::http
