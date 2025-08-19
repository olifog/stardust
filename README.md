
# stardust

hybrid graph/vector database!!!! WIP

built on top of LMDB, with capnproto for schema/RPC. Minimal HTTP is also available for quick testing.

## build

### requirements

- CMake >= 3.30
- a C++20 compiler
- Cap'n Proto (capnp, libcapnp-dev)
- LMDB (liblmdb-dev)

install on macos:

```bash
brew install cmake pkg-config capnp lmdb
```

### dev with docker compose

Run the server:

```bash
docker compose up dev
```

- to pass server args: `STARDUST_ARGS="-v -b unix:/tmp/stardust.sock" docker compose up dev`.

Run tests:

```bash
docker compose run --rm test
```

args:

- `-v`: verbose logging
- `-b`: bind address (default: `unix:/tmp/stardust.sock`)
- `-d`: data directory (default: `data`)
- `-H, --http`: optional HTTP bind, e.g. `http://0.0.0.0:8080` (disabled by default)

to interact with the server, you can use either the capnproto RPC server or the HTTP server.
see `src/client_example.cpp` for an example, the schema is in `schemas/graph.capnp`.

### HTTP API

when started with `--http http://0.0.0.0:8080`, a tiny HTTP server is enabled using Mongoose [link](https://github.com/cesanta/mongoose).

endpoints:

- `GET /api/health` → `{ "ok": true }`
- `POST /api/node?labels=LabelA,LabelB` → `{ "id": <u64> }`
- `POST /api/edge?src=<u64>&dst=<u64>&type=<TypeName>` → `{ "id": <u64> }`
- `GET /api/neighbors?node=<u64>&direction=out|in|both&limit=<u32>` → `{ "neighbors": [<u64> ...] }`

## TODO

- [X] actual knn
- [X] where is vecTagMeta written
- [ ] partition vectors by tagId
- [X] way more tests, make sure things are working properly
- [X] implement interning of various strings properly (label names, property key names, types, etc.)
- [X] intern text props
- [X] expand out the API, more CRUD + query filters etc.
- [X] add in http server alternative to capnproto
- [x] initial python sdk wrapper around capnproto
- [ ] tidy up http server
- [ ] add the 4 new http queries to rpc server
- [ ] initial typescript sdk wrapper around http
- [ ] add colors to explorer
- [ ] add embeddings + actual lmdb data for the demo
- [ ] add a couple graph algos, betweenness centrality, pathfinding
- [ ] expose http api as (read only) mcp server
- [ ] make custom extension to cypher, make a grammar, register queries to precompile them (?)
- [ ] hnsw index for vector search
- [ ] configurable similarity metric

longer term:

- [ ] own storage engine, optimise locality w/ graph location and vector latent space
- [ ] additional indexes on node/edge properties
- [ ] simd stuff
- [ ] think deeper about concurrency model, LMDB single-writer currently but could be multi-writer
- [ ] isolation guarantees, transactions, crash consistency, etc.
- [ ] auth
- [ ] cli installer, docker image, etc.
- [ ] inbuilt opinionated concept of versioning (?), layer on top of primitives
