from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, cast

import capnp

_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")
_GRAPH_CAPNP_PATH = os.path.join(_SCHEMA_DIR, "graph.capnp")
graph_capnp = capnp.load(_GRAPH_CAPNP_PATH)


# -------------------- Public API --------------------


def connect(url: str) -> StardustClient:
    """Connect to a Stardust server over Cap'n Proto.

    Supported URL forms:
    - "unix:/path/to/socket" → connect to a UNIX domain socket
    - "tcp://host:port" or "host:port" → TCP connection

    TODO(embedded): support "embedded:/path" to run the engine in-process.
    """
    if url.startswith("embedded:"):
        # TODO(embedded): delegate to an embedded engine adapter when available
        raise RuntimeError("Embedded engine not available. Install future embedded extra.")

    addr = _normalize_address(url)
    client = _make_two_party_client(addr)
    service = client.bootstrap().cast_as(graph_capnp.Stardust)
    return StardustClient(service)


def _normalize_address(url: str) -> str:
    if url.startswith("unix:"):
        return url
    if url.startswith("tcp://"):
        return url[len("tcp://") :]
    return url


def _make_two_party_client(addr: str) -> capnp.TwoPartyClient:
    if addr.startswith("unix:"):
        # pycapnp uses 'unix:/path' string directly
        return capnp.TwoPartyClient(addr)
    # TCP host:port
    return capnp.TwoPartyClient(addr)


@dataclass
class KnnHit:
    id: int
    score: float


class StardustClient:
    """Pythonic wrapper over the Cap'n Proto stardust api

    This class provides snake_case methods and Python-native types.
    """

    def __init__(self, svc: Any) -> None:
        self._svc = svc

    # -------------------- Writes --------------------

    def create_node(
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
        # labels
        ls = params.init("labels")
        _names = list(labels or [])
        arr = ls.init("names", len(_names))
        for i, nm in enumerate(_names):
            arr[i] = nm
        # hot props
        _hot = list((hot_props or {}).items())
        hp = params.init("hotProps", len(_hot))
        for i, (k, v) in enumerate(_hot):
            item = hp[i]
            item.key = k
            _set_value(item.init("val"), v)
        # cold props
        _cold = list((cold_props or {}).items())
        cp = params.init("coldProps", len(_cold))
        for i, (k, v) in enumerate(_cold):
            item = cp[i]
            item.key = k
            _set_value(item.init("val"), v)
        # vectors
        _vecs = list(vectors or [])
        vp = params.init("vectors", len(_vecs))
        for i, (tag, vec) in enumerate(_vecs):
            item = vp[i]
            item.tag = tag
            vf = item.init("vector")
            vf.dim = len(vec)
            # serialize float32 vector into bytes
            vf.data = _floats_to_bytes(vec)
        res = req.send().wait()
        # Cap'n Proto result can be accessed with .to_dict() in pycapnp
        return cast(dict[str, Any], res.to_dict())

    def upsert_vector(self, node_id: int, tag: str, vector: list[float]) -> None:
        req = self._svc.upsertVector_request()
        params = req.init("params")
        params.id = node_id
        params.tag = tag
        vf = params.init("vector")
        vf.dim = len(vector)
        vf.data = _floats_to_bytes(vector)
        req.send().wait()

    def delete_vector(self, node_id: int, tag: str) -> None:
        req = self._svc.deleteVector_request()
        params = req.init("params")
        params.id = node_id
        params.tag = tag
        req.send().wait()

    def add_edge(
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
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    # -------------------- Reads --------------------

    def get_node(self, node_id: int) -> dict[str, Any]:
        req = self._svc.getNode_request()
        params = req.init("params")
        params.id = node_id
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def get_node_props(self, node_id: int, keys: Iterable[str] | None = None) -> dict[str, Any]:
        req = self._svc.getNodeProps_request()
        params = req.init("params")
        params.id = node_id
        _keys = list(keys or [])
        arr = params.init("keys", len(_keys))
        for i, k in enumerate(_keys):
            arr[i] = k
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def get_vectors(self, node_id: int, tags: Iterable[str] | None = None) -> dict[str, Any]:
        req = self._svc.getVectors_request()
        params = req.init("params")
        params.id = node_id
        _tags = list(tags or [])
        arr = params.init("tags", len(_tags))
        for i, t in enumerate(_tags):
            arr[i] = t
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def get_edge_header(self, edge_id: int) -> dict[str, Any]:
        req = self._svc.getEdgeHeader_request()
        params = req.init("params")
        params.edgeId = edge_id
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def list_adjacency(self, node: int, direction: str = "both", limit: int = 100) -> dict[str, Any]:
        req = self._svc.listAdjacency_request()
        params = req.init("params")
        params.node = node
        params.limit = limit
        params.direction = _direction_from_str(direction)
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def degree(self, node: int, direction: str = "both") -> dict[str, Any]:
        req = self._svc.degree_request()
        params = req.init("params")
        params.node = node
        params.direction = _direction_from_str(direction)
        res = req.send().wait()
        return cast(dict[str, Any], res.to_dict())

    def knn(self, tag: str, query: list[float], k: int) -> list[KnnHit]:
        req = self._svc.knn_request()
        params = req.init("params")
        params.tag = tag
        vf = params.init("query")
        vf.dim = len(query)
        vf.data = _floats_to_bytes(query)
        params.k = k
        res = req.send().wait()
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
