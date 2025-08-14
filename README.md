
# stardust

hybrid graph/vector database!!!! WIP

built on top of LMDB, with capnproto for schema/RPC

## Build and test

```bash
make build           # configure + build (default Release)
make test            # run tests
make debug           # build Debug
make release         # build Release
```

## Running

```bash
make run ARGS="-v"
```

Args:

- `-v`: verbose logging
- `-b`: bind address (default: `unix:/tmp/stardust.sock`)
- `-d`: data directory (default: `data`)

To interact with the server, you (currently) need to create a capnproto client.
See `src/client_example.cpp` for an example, the schema is in `schemas/graph.capnp`.

## TODO

- [ ] way more tests, make sure things are working properly
- [ ] actual knn
- [ ] expand out the API, more CRUD + query filters etc.
- [ ] make a wrapper http server
- [ ] make custom extension to cypher, make a grammar, register queries to precompile them (?)
