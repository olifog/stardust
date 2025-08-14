
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

- [ ] way more tests, make sure things are working properly
- [ ] actual knn
- [ ] expand out the API, more CRUD + query filters etc.
- [ ] make a wrapper http server
- [ ] make custom extension to cypher, make a grammar, register queries to precompile them (?)
