from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Optional, TypedDict, Literal, cast

from ollama import AsyncClient as OllamaAsyncClient

from fastmcp import FastMCP, Context

from stardust import connect as sd_connect
import capnp

logging.basicConfig(level=logging.INFO)


class NodeOut(TypedDict):
    id: int
    labels: list[str]
    props: dict[str, Any]


class EdgeOut(TypedDict):
    id: int
    src: int
    dst: int
    type: str
    props: dict[str, Any]


class RAGResult(TypedDict):
    resource_uri: str
    seed_ids: list[int]
    k: int
    hops: int
    total_nodes: int
    total_edges: int


class Embedder:
    async def embed(self, text: str) -> list[float]:
        raise NotImplementedError


class OllamaEmbedder(Embedder):
    def __init__(self, base_url: str, model: str) -> None:
        self._model = model
        self._client = OllamaAsyncClient(host=base_url)

    async def embed(self, text: str) -> list[float]:
        resp = await self._client.embeddings(model=self._model, prompt=text)
        vec = resp.embedding
        return [float(x) for x in vec]

    async def aclose(self) -> None:
        if hasattr(self._client, "client") and hasattr(self._client.client, "aclose"):
            await self._client.client.aclose()


@dataclass
class AppCtx:
    sd: Any
    embedder: Optional[Embedder]
    rag_store: dict[str, dict[str, Any]]


def build_server() -> FastMCP:
    stardust_url = os.environ.get("STARDUST_URL", "unix:/tmp/stardust.sock")
    # default tag is read at tool invocation to support runtime overrides
    ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "nomic-embed-text:v1.5")

    from contextlib import asynccontextmanager
    from collections.abc import AsyncIterator

    @asynccontextmanager
    async def lifespan(_: FastMCP) -> AsyncIterator[AppCtx]:
        logging.info("Connecting to Stardust at %s", stardust_url)
        sd = await sd_connect(stardust_url)
        embedder: Optional[Embedder] = OllamaEmbedder(ollama_url, ollama_model)
        ctx = AppCtx(sd=sd, embedder=embedder, rag_store={})
        try:
            yield ctx
        finally:
            try:
                await sd.aclose()
            except Exception:
                pass
            if isinstance(embedder, OllamaEmbedder):
                try:
                    await embedder.aclose()
                except Exception:
                    pass

    mcp = FastMCP("stardust", lifespan=lifespan)

    def _props_list_to_dict(props_result: dict[str, Any]) -> dict[str, Any]:
        items = props_result.get("props", [])
        out: dict[str, Any] = {}
        for p in items:
            key = p.get("key")
            val = p.get("val")
            if key is not None:
                out[str(key)] = val
        return out

    async def fetch_node(sd: Any, node_id: int) -> NodeOut:
        n = await sd.get_node(node_id)
        props_res = await sd.get_node_props(node_id)
        labels = n.get("header", {}).get("labels", {}).get("names", []) or n.get(
            "node", {}
        ).get("labels", {}).get("names", [])
        node_id_out = (
            n.get("header", {}).get("id") or n.get("node", {}).get("id") or node_id
        )
        return {
            "id": int(node_id_out),
            "labels": list(labels),
            "props": _props_list_to_dict(props_res),
        }

    async def fetch_neighbors(
        sd: Any,
        node_id: int,
        limit: int,
        direction: Literal["in", "out", "both"] = "both",
    ) -> dict[str, Any]:
        res = await sd.list_adjacency(node=node_id, direction=direction, limit=limit)
        return cast(dict[str, Any], res)

    async def expand_subgraph(
        sd: Any,
        seeds: list[int],
        hops: int,
        per_node_limit: int,
        direction: Literal["in", "out", "both"],
    ) -> tuple[list[NodeOut], list[EdgeOut]]:
        seen_nodes: set[int] = set(seeds)
        edges: dict[int, EdgeOut] = {}
        frontier = list(seeds)

        for _ in range(max(0, hops)):
            next_frontier: list[int] = []
            tasks = [
                fetch_neighbors(sd, nid, per_node_limit, direction) for nid in frontier
            ]
            results: list[dict[str, Any] | Exception] = await asyncio.gather(
                *tasks, return_exceptions=True
            )  # type: ignore[assignment]
            to_fetch_eids: list[int] = []
            for res in results:
                if isinstance(res, Exception):
                    continue
                for row in res.get("items") or res.get("adjacent") or []:
                    # support either RPC schema (items) or HTTP shape (adjacent)
                    if "edge" in row:
                        e = row.get("edge", {})
                        meta = row.get("meta", {})
                        eid = int(e.get("id"))
                        if eid not in edges:
                            props = (
                                {
                                    p.get("key"): p.get("val")
                                    for p in meta.get("props", [])
                                }
                                if meta.get("props")
                                else {}
                            )
                            edges[eid] = {
                                "id": eid,
                                "src": int(e.get("src")),
                                "dst": int(e.get("dst")),
                                "type": meta.get("type", ""),
                                "props": props,
                            }
                        other = (
                            int(row.get("otherNode", 0))
                            if row.get("otherNode") is not None
                            else None
                        )
                    else:
                        eid = int(row.get("edgeId"))
                        if eid not in edges:
                            to_fetch_eids.append(eid)
                        other = int(row.get("neighbor"))

                    if other is not None and other not in seen_nodes:
                        seen_nodes.add(other)
                        next_frontier.append(other)
            # fetch missing edge details concurrently
            if to_fetch_eids:
                edge_results: list[dict[str, Any] | Exception] = await asyncio.gather(
                    *[sd.get_edge(eid) for eid in to_fetch_eids], return_exceptions=True
                )
                for er in edge_results:
                    if isinstance(er, Exception):
                        continue
                    e = er.get("edge", {})
                    meta = er.get("meta", {})
                    eid = int(e.get("id"))
                    props = (
                        {p.get("key"): p.get("val") for p in meta.get("props", [])}
                        if meta.get("props")
                        else {}
                    )
                    edges[eid] = {
                        "id": eid,
                        "src": int(e.get("src")),
                        "dst": int(e.get("dst")),
                        "type": meta.get("type", ""),
                        "props": props,
                    }
            frontier = next_frontier

        node_list = list(seen_nodes)
        chunks = [node_list[i : i + 64] for i in range(0, len(node_list), 64)]
        nodes: list[NodeOut] = []
        for c in chunks:
            nodes.extend(
                await asyncio.gather(
                    *[fetch_node(sd, nid) for nid in c], return_exceptions=False
                )
            )
        return nodes, list(edges.values())

    @mcp.resource("stardust://node/{node_id}")
    async def read_node(
        node_id: int, ctx: Context
    ) -> dict[str, Any]:
        sd = ctx.request_context.lifespan_context.sd
        node = await fetch_node(sd, node_id)
        return {"id": node["id"], "labels": node["labels"], "props": node["props"]}

    @mcp.resource("stardust://subgraph/{key}")
    async def read_subgraph(
        key: str, ctx: Context
    ) -> dict[str, Any]:
        store = ctx.request_context.lifespan_context.rag_store
        if key not in store:
            return {"error": "not found"}
        return cast(dict[str, Any], store[key])

    @mcp.tool()
    async def expand_from_seeds(
        seeds: list[int],
        ctx: Context,
        hops: int = 1,
        per_node_limit: int = 32,
        direction: Literal["in", "out", "both"] = "both",
    ) -> RAGResult:
        sd = ctx.request_context.lifespan_context.sd

        await ctx.report_progress(
            0.25, 1.0, f"Expanding {len(seeds)} seeds, {hops} hops"
        )

        nodes, edges = await expand_subgraph(
            sd, seeds, hops=hops, per_node_limit=per_node_limit, direction=direction
        )

        preview_lines: list[str] = []
        for n in nodes[:20]:
            label = f" ({', '.join(n['labels'])})" if n["labels"] else ""
            preview_lines.append(
                f"- Node {n['id']}{label}: {str(n['props'])[:300]}"
            )

        payload = {
            "type": "stardust-subgraph",
            "seeds": [int(s) for s in seeds],
            "hops": hops,
            "nodes": nodes,
            "edges": edges,
            # include topk for UI parity, set uniform score for provided seeds
            "topk": [{"id": int(s), "score": 1.0} for s in seeds],
            "preview_markdown": "# Subgraph Preview\n" + "\n".join(preview_lines),
        }
        key = str(uuid.uuid4())
        ctx.request_context.lifespan_context.rag_store[key] = payload

        return {
            "resource_uri": f"stardust://subgraph/{key}",
            "seed_ids": [int(s) for s in seeds],
            "k": len(seeds),
            "hops": hops,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
        }

    @mcp.tool()
    async def graph_rag_search(
        query_text: str,
        ctx: Context,
        tag: Optional[str] = None,
        k: int = 8,
        hops: int = 1,
        per_node_limit: int = 32,
        direction: Literal["in", "out", "both"] = "both",
    ) -> RAGResult:
        sd = ctx.request_context.lifespan_context.sd
        embedder = ctx.request_context.lifespan_context.embedder
        default_tag = os.environ.get("STARDUST_VECTOR_TAG", "text")
        use_tag = tag or default_tag

        await ctx.info("Embedding query…")
        if embedder is None:
            raise RuntimeError("No embedder configured")

        qvec = await embedder.embed(query_text)
        await ctx.report_progress(0.25, 1.0, "Running KNN")
        hits = await sd.knn(tag=use_tag, query=qvec, k=k)
        seed_ids = [int(h.id) for h in hits]

        await ctx.report_progress(
            0.55, 1.0, f"Expanding {len(seed_ids)} seeds, {hops} hops"
        )
        nodes, edges = await expand_subgraph(
            sd, seed_ids, hops=hops, per_node_limit=per_node_limit, direction=direction
        )

        id2score = {int(h.id): float(h.score) for h in hits}
        preview_lines: list[str] = []
        for n in nodes[:20]:
            s = id2score.get(int(n["id"]))
            label = f" ({', '.join(n['labels'])})" if n["labels"] else ""
            score_txt = f" [score={s:.3f}]" if s is not None else ""
            preview_lines.append(
                f"- Node {n['id']}{label}{score_txt}: {str(n['props'])[:300]}"
            )

        payload = {
            "type": "stardust-subgraph",
            "seeds": seed_ids,
            "vector_tag": use_tag,
            "hops": hops,
            "nodes": nodes,
            "edges": edges,
            "topk": [{"id": int(h.id), "score": float(h.score)} for h in hits],
            "preview_markdown": "# Subgraph Preview\n" + "\n".join(preview_lines),
        }
        key = str(uuid.uuid4())
        ctx.request_context.lifespan_context.rag_store[key] = payload

        return {
            "resource_uri": f"stardust://subgraph/{key}",
            "seed_ids": seed_ids,
            "k": k,
            "hops": hops,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
        }

    @mcp.prompt()
    def answer_with_stardust(question: str, subgraph_uri: str) -> str:
        return (
            "You are given a Stardust subgraph resource at: "
            + subgraph_uri
            + ".\n1) Read it and ground your answer ONLY on nodes/edges/props present.\n"
            "2) Cite node ids in parentheses when asserting facts, e.g., (node 123).\n"
            "3) If information is insufficient, say so and propose follow-up graph expansions.\n\n"
            "Question: " + question
        )

    return mcp


def cli() -> None:
    parser = argparse.ArgumentParser(description="Stardust MCP server")
    parser.add_argument(
        "--transport",
        default=os.environ.get("MCP_TRANSPORT", "http"),
        choices=["stdio", "http"],
        help="MCP transport (http or stdio)",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MCP_HOST", "127.0.0.1"),
        help="HTTP host to bind (for transport=http)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MCP_PORT", "8000")),
        help="HTTP port to bind (for transport=http)",
    )
    args = parser.parse_args()
    async def _main() -> None:
        server = build_server()
        if args.transport == "stdio":
            await server.run_stdio_async(show_banner=False)
        elif args.transport == "http":
            logging.info("Starting Stardust MCP HTTP server on %s:%d", args.host, args.port)
            await server.run_async(transport="http", host=args.host, port=args.port, show_banner=False)
        else:
            raise ValueError(f"Unsupported transport: {args.transport}")

    asyncio.run(capnp.run(_main()))


if __name__ == "__main__":
    cli()
