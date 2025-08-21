# stardust mcp server

mcp server exposing the stardust graph/vector DB for graphRAG. used the python capnp sdk to talk to the database and ollama for local embeddings (for now)

## run (stdio)

```bash
export STARDUST_URL="unix:/tmp/stardust.sock"
export OLLAMA_URL="http://localhost:11434"
export OLLAMA_MODEL="nomic-embed-text:v1.5"

uv run stardust-mcp  # runs with stdio transport
```

## Tools & Resources

- `graph_rag_search(query_text, tag, k, hops, per_node_limit, direction)` -> returns `resource_uri`
- `expand_from_seeds(seeds, hops, per_node_limit, direction)` -> returns `resource_uri`
- `stardust://node/{id}` resource
- `stardust://subgraph/{key}` resource
- `/answer_with_stardust` prompt
