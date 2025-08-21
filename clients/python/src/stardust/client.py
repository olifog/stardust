from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Optional, cast

import capnp

_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")
_GRAPH_CAPNP_PATH = os.path.join(_SCHEMA_DIR, "graph.capnp")


def _discover_capnp_include_paths() -> list[str]:
    paths: list[str] = []
    env = os.environ.get("CAPNP_INCLUDE_DIR") or os.environ.get("CAPNP_PATH")
    if env:
        for p in env.split(os.pathsep):
            if p:
                paths.append(p)
    common_candidates = [
        "/opt/homebrew/include",
        "/usr/local/include",
        "/usr/include",
    ]
    for base in common_candidates:
        if os.path.exists(os.path.join(base, "capnp", "c++.capnp")):
            paths.append(base)
    seen: set[str] = set()
    unique_paths: list[str] = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            unique_paths.append(p)
    return unique_paths


_CAPNP_IMPORT_DIRS = _discover_capnp_include_paths()
graph_capnp = capnp.load(_GRAPH_CAPNP_PATH, imports=_CAPNP_IMPORT_DIRS)


# -------------------- Public API --------------------

def _normalize_address(url: str) -> str:
    if url.startswith("unix:"):
        return url
    if url.startswith("tcp://"):
        return url[len("tcp://") :]
    return url


async def connect(url: str) -> StardustClient:
    if url.startswith("embedded:"):
        raise RuntimeError("Embedded engine not available. Install future embedded extra.")

    addr = _normalize_address(url)

    if addr.startswith("unix:"):
        path = addr[len("unix:") :]
        connection = await capnp.AsyncIoStream.create_unix_connection(path=path)
    else:
        host, port_str = addr.split(":", 1)
        port = int(port_str)
        connection = await capnp.AsyncIoStream.create_connection(host=host, port=port)

    tp_client = capnp.TwoPartyClient(connection)

    service = tp_client.bootstrap().cast_as(graph_capnp.Stardust)
    return StardustClient(service, _async_transport=_AsyncTransport(connection, tp_client))


class _AsyncTransport:
    def __init__(self, connection: Any, capnp_client: Any) -> None:
        self.connection = connection
        self.capnp_client = capnp_client


@dataclass
class KnnHit:
    id: int
    score: float


class StardustClient:
    """Pythonic wrapper over the Cap'n Proto stardust api

    This class provides snake_case methods and Python-native types.
    """

    def __init__(self, svc: Any, _async_transport: Optional[_AsyncTransport] = None) -> None:
        self._svc = svc
        self._async_transport = _async_transport

    # -------------------- Lifecycle --------------------

    def is_async(self) -> bool:
        return self._async_transport is not None

    async def aclose(self) -> None:
        if self._async_transport is None:
            return
        try:
            self._async_transport.capnp_client.close()
        except Exception:
            pass
        try:
            self._async_transport.connection.close()
        except Exception:
            pass

    # -------------------- Writes --------------------

    async def create_node(
        self,
        labels: Iterable[str] | None = None,
        hot_props: Mapping[str, Any] | None = None,
        cold_props: Mapping[str, Any] | None = None,
        vectors: Iterable[tuple[str, list[float]]] | None = None,
    ) -> dict[str, Any]:
        """Create a node.

        Returns a dict with keys: {"node": {"id": int}, "header": {...}}.
        """
        req = self._svc.createNode_request()
        params = req.init("params")
        ls = params.init("labels")

        _names = list(labels or [])
        arr = ls.init("names", len(_names))
        for i, nm in enumerate(_names):
            arr[i] = nm

        _hot = list((hot_props or {}).items())
        hp = params.init("hotProps", len(_hot))
        for i, (k, v) in enumerate(_hot):
            item = hp[i]
            item.key = k

            _set_value(item.init("val"), v)
        _cold = list((cold_props or {}).items())
        cp = params.init("coldProps", len(_cold))
        for i, (k, v) in enumerate(_cold):
            item = cp[i]
            item.key = k
            _set_value(item.init("val"), v)

        _vecs = list(vectors or [])
        vp = params.init("vectors", len(_vecs))
        for i, (tag, vec) in enumerate(_vecs):
            item = vp[i]
            item.tag = tag
            vf = item.init("vector")
            vf.dim = len(vec)
            vf.data = _floats_to_bytes(vec)

        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def upsert_vector(self, node_id: int, tag: str, vector: list[float]) -> None:
        req = self._svc.upsertVector_request()
        params = req.init("params")
        params.id = node_id
        params.tag = tag
        vf = params.init("vector")
        vf.dim = len(vector)
        vf.data = _floats_to_bytes(vector)
        await req.send()

    async def delete_vector(self, node_id: int, tag: str) -> None:
        req = self._svc.deleteVector_request()
        params = req.init("params")
        params.id = node_id
        params.tag = tag
        await req.send()

    async def add_edge(
        self, src: int, dst: int, edge_type: str, props: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        req = self._svc.addEdge_request()
        params = req.init("params")
        params.src = src
        params.dst = dst
        meta = params.init("meta")
        meta.type = edge_type
        _props = list((props or {}).items())
        arr = meta.init("props", len(_props))
        for i, (k, v) in enumerate(_props):
            item = arr[i]
            item.key = k
            _set_value(item.init("val"), v)
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['edge'])

    # -------------------- Reads --------------------

    async def get_node(self, node_id: int) -> dict[str, Any]:
        req = self._svc.getNode_request()
        params = req.init("params")
        params.id = node_id
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def get_node_props(self, node_id: int, keys: Iterable[str] | None = None) -> dict[str, Any]:
        req = self._svc.getNodeProps_request()
        params = req.init("params")
        params.id = node_id
        _keys = list(keys or [])
        arr = params.init("keys", len(_keys))
        for i, k in enumerate(_keys):
            arr[i] = k
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def get_vectors(self, node_id: int, tags: Iterable[str] | None = None) -> dict[str, Any]:
        req = self._svc.getVectors_request()
        params = req.init("params")
        params.id = node_id
        _tags = list(tags or [])
        arr = params.init("tags", len(_tags))
        for i, t in enumerate(_tags):
            arr[i] = t
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def get_edge(self, edge_id: int) -> dict[str, Any]:
        req = self._svc.getEdge_request()
        params = req.init("params")
        params.edgeId = edge_id
        res = await req.send()
        # merged response: { edge: {id,src,dst}, meta: {type, props:[...] } }
        return cast(dict[str, Any], res.to_dict())

    async def list_adjacency(self, node: int, direction: str = "both", limit: int = 100) -> dict[str, Any]:
        req = self._svc.listAdjacency_request()
        params = req.init("params")
        params.node = node
        params.limit = limit
        params.direction = _direction_from_str(direction)
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def degree(self, node: int, direction: str = "both") -> dict[str, Any]:
        req = self._svc.degree_request()
        params = req.init("params")
        params.node = node
        params.direction = _direction_from_str(direction)
        res = await req.send()
        return cast(dict[str, Any], res.to_dict()['result'])

    async def knn(self, tag: str, query: list[float], k: int) -> list[KnnHit]:
        req = self._svc.knn_request()
        params = req.init("params")
        params.tag = tag
        vf = params.init("query")
        vf.dim = len(query)
        vf.data = _floats_to_bytes(query)
        params.k = k
        res = await req.send()
        out: list[KnnHit] = []
        for h in res.result.hits:
            out.append(KnnHit(id=int(h.id), score=float(h.score)))
        return out


# -------------------- Helpers --------------------


def _direction_from_str(s: str) -> Any:
    text = s.lower()
    if text == "out":
        return graph_capnp.Direction.out
    if text == "in":
        return getattr(graph_capnp.Direction, "in")
    return graph_capnp.Direction.both


def _set_value(builder: Any, value: Any) -> None:
    # Map Python types into Value union
    if value is None:
        builder.nullv = None
        return
    if isinstance(value, bool):
        builder.boolv = bool(value)
        return
    if isinstance(value, int):
        builder.i64 = int(value)
        return
    if isinstance(value, float):
        builder.f64 = float(value)
        return
    if isinstance(value, bytes | bytearray):
        builder.bytes = bytes(value)
        return
    if isinstance(value, str):
        builder.text = value
        return
    # Fallback: try repr as bytes
    builder.bytes = repr(value).encode("utf-8")


def _floats_to_bytes(values: Iterable[float]) -> bytes:
    # pack as float32 array
    import array

    ar = array.array("f", (float(v) for v in values))
    return ar.tobytes()
