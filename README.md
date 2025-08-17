
# stardust

hybrid graph/vector database!!!! WIP

built on top of LMDB, with capnproto for schema/RPC

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

### build/test commands

```bash
make build           # configure + build (default Release)
make test            # run tests
make debug           # build Debug
make release         # build Release
```

## running

```bash
make run ARGS="-v"
```

args:

- `-v`: verbose logging
- `-b`: bind address (default: `unix:/tmp/stardust.sock`)
- `-d`: data directory (default: `data`)

to interact with the server, you (currently) need to create a capnproto client.
see `src/client_example.cpp` for an example, the schema is in `schemas/graph.capnp`.

## TODO

- [X] actual knn
- [X] where is vecTagMeta written
- [ ] partition vectors by tagId
- [X] way more tests, make sure things are working properly
  - [ ] test with real embedding model
- [X] implement interning of various strings properly (label names, property key names, types, etc.)
- [ ] intern text props
- [ ] expand out the API, more CRUD + query filters etc.
- [ ] add in http server alternative to capnproto
- [ ] make example graph rag application
- [ ] hnsw index for vector search
- [ ] expose http api as (read only) mcp server
- [ ] make custom extension to cypher, make a grammar, register queries to precompile them (?)

longer term:

- [ ] own storage engine, optimise locality w/ graph location and vector latent space
- [ ] simd stuff
- [ ] think deeper about concurrency model, LMDB single-writer currently but could be multi-writer
- [ ] isolation guarantees, transactions, crash consistency, etc.
- [ ] auth
- [ ] cli installer, docker image, etc.
- [ ] inbuilt opinionated concept of versioning (?), layer on top of primitives
- [ ] python/ts sdks
