const API_URL = "http://localhost:8080";

export type JsonValue = string | number | boolean | null;

interface OkResponse {
  ok: boolean;
}
interface IdResponse {
  id: number;
}

export interface NodeHeader {
  id: number;
  labels: string[];
  hotProps: Record<string, JsonValue>;
}

export interface NodeResponse {
  header: NodeHeader;
}
export interface NodePropsResponse {
  props: Record<string, JsonValue>;
}

export interface VectorDTO {
  tag: string;
  dim: number;
  data: string;
}
export interface GetVectorsResponse {
  vectors: VectorDTO[];
}

export interface Edge {
  id: number;
  src: number;
  dst: number;
}
export interface AdjacencyRow {
  neighbor: number;
  edgeId: number;
  type: string;
  direction: "out" | "in" | "both";
}
export interface AdjacencyResponse {
  items: AdjacencyRow[];
}

export interface KnnHit {
  id: number;
  score: number;
}
export interface KnnResponse {
  hits: KnnHit[];
}

const buildQuery = (params: Record<string, string | number | undefined>) => {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    usp.set(k, String(v));
  }
  return usp.toString();
};

const getJSON = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const res = await fetch(`${API_URL}${path}`, init);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return (await res.json()) as T;
};

// Health ----------------------------------------------------------------------
export const health = async () => getJSON<OkResponse>(`/api/health`);

// Nodes -----------------------------------------------------------------------
export const createNode = async (labels?: string[]) => {
  const q = labels?.length
    ? `?${buildQuery({ labels: labels.join(",") })}`
    : "";
  return getJSON<IdResponse>(`/api/node${q}`, { method: "POST" });
};

export const getNode = async (id: number) => {
  const q = buildQuery({ id });
  return getJSON<NodeResponse>(`/api/node?${q}`);
};

export const getNodeProps = async (id: number, keys?: string[]) => {
  const q = buildQuery({ id, keys: keys?.length ? keys.join(",") : undefined });
  return getJSON<NodePropsResponse>(`/api/nodeProps?${q}`);
};

export const setNodeLabels = async (params: {
  id: number;
  add?: string[];
  remove?: string[];
}) => {
  const q = buildQuery({
    id: params.id,
    add: params.add?.length ? params.add.join(",") : undefined,
    remove: params.remove?.length ? params.remove.join(",") : undefined,
  });
  return getJSON<OkResponse>(`/api/setNodeLabels?${q}`, { method: "POST" });
};

export const upsertNodeProps = async (params: {
  id: number;
  setHot?: Record<string, JsonValue>;
  setCold?: Record<string, JsonValue>;
  unset?: string[];
}) => {
  const encodeVal = (v: JsonValue): string => {
    if (v === null) return "null";
    if (typeof v === "boolean") return v ? "true" : "false";
    return String(v);
  };
  const joinKv = (obj?: Record<string, JsonValue>) =>
    obj && Object.keys(obj).length
      ? Object.entries(obj)
          .map(
            ([k, v]) =>
              `${encodeURIComponent(k)}=${encodeURIComponent(encodeVal(v))}`,
          )
          .join(",")
      : undefined;
  const q = buildQuery({
    id: params.id,
    setHot: joinKv(params.setHot),
    setCold: joinKv(params.setCold),
    unset: params.unset?.length
      ? params.unset.map(encodeURIComponent).join(",")
      : undefined,
  });
  return getJSON<OkResponse>(`/api/upsertNodeProps?${q}`, { method: "POST" });
};

export const deleteNode = async (id: number) => {
  const q = buildQuery({ id });
  return getJSON<OkResponse>(`/api/node?${q}`, { method: "DELETE" });
};

// Edges -----------------------------------------------------------------------
export const addEdge = async (params: {
  src: number;
  dst: number;
  type: string;
}) => {
  const q = buildQuery(params);
  return getJSON<IdResponse>(`/api/edge?${q}`, { method: "POST" });
};

export const getEdge = async (edgeId: number) => {
  const q = buildQuery({ edgeId });
  return getJSON<Edge>(`/api/edge?${q}`);
};

export const updateEdgeProps = async (params: {
  edgeId: number;
  set?: Record<string, JsonValue>;
  unset?: string[];
}) => {
  const encodeVal = (v: JsonValue): string => {
    if (v === null) return "null";
    if (typeof v === "boolean") return v ? "true" : "false";
    return String(v);
  };
  const joinKv = (obj?: Record<string, JsonValue>) =>
    obj && Object.keys(obj).length
      ? Object.entries(obj)
          .map(
            ([k, v]) =>
              `${encodeURIComponent(k)}=${encodeURIComponent(encodeVal(v))}`,
          )
          .join(",")
      : undefined;
  const q = buildQuery({
    edgeId: params.edgeId,
    set: joinKv(params.set),
    unset: params.unset?.length
      ? params.unset.map(encodeURIComponent).join(",")
      : undefined,
  });
  return getJSON<OkResponse>(`/api/updateEdgeProps?${q}`, { method: "POST" });
};

export const deleteEdge = async (edgeId: number) => {
  const q = buildQuery({ edgeId });
  return getJSON<OkResponse>(`/api/edge?${q}`, { method: "DELETE" });
};

// Vectors ---------------------------------------------------------------------
export const getVectors = async (id: number, tags?: string[]) => {
  const q = buildQuery({ id, tags: tags?.length ? tags.join(",") : undefined });
  return getJSON<GetVectorsResponse>(`/api/vectors?${q}`);
};

export const upsertVector = async (
  params:
    | { id: number; tag: string; data: number[] }
    | { id: number; tag: string; dataB64: string; dim: number },
) => {
  if ("data" in params) {
    const q = buildQuery({
      id: params.id,
      tag: params.tag,
      data: params.data.join(","),
    });
    return getJSON<OkResponse>(`/api/upsertVector?${q}`, { method: "POST" });
  }
  const q = buildQuery({
    id: params.id,
    tag: params.tag,
    data_b64: params.dataB64,
    dim: params.dim,
  });
  return getJSON<OkResponse>(`/api/upsertVector?${q}`, { method: "POST" });
};

export const deleteVector = async (id: number, tag: string) => {
  const q = buildQuery({ id, tag });
  return getJSON<OkResponse>(`/api/deleteVector?${q}`, { method: "POST" });
};

// Graph traversal --------------------------------------------------------------
export const adjacency = async (params: {
  node: number;
  direction?: "out" | "in" | "both";
  limit?: number;
}) => {
  const q = buildQuery({
    node: params.node,
    direction: params.direction,
    limit: params.limit,
  });
  return getJSON<AdjacencyResponse>(`/api/adjacency?${q}`);
};

export const knn = async (params: { tag: string; q: number[]; k?: number }) => {
  const q = buildQuery({ tag: params.tag, q: params.q.join(","), k: params.k });
  return getJSON<KnnResponse>(`/api/knn?${q}`);
};

// Helpers ---------------------------------------------------------------------
export interface NodeWithNeighboursResult {
  node: NodeHeader;
  neighbourHeaders: NodeHeader[];
  edges: Edge[];
}

export const getNodeWithNeighbours = async (
  id: number,
  params?: { direction?: "out" | "in" | "both"; limit?: number },
): Promise<NodeWithNeighboursResult> => {
  const [{ header }, { items }] = await Promise.all([
    getNode(id),
    adjacency({
      node: id,
      direction: params?.direction ?? "both",
      limit: params?.limit,
    }),
  ]);

  // Best-effort: fetch headers for neighbour nodes. If any fail, skip them.
  const neighbourHeaders: NodeHeader[] = [];
  await Promise.all(
    items.map(async (row) => {
      try {
        const res = await getNode(row.neighbor);
        neighbourHeaders.push(res.header);
      } catch {
        // ignore individual neighbour fetch failures
      }
    }),
  );

  const edges: Edge[] = items.map((row) => {
    const dir = row.direction;
    if (dir === "in") {
      return { id: row.edgeId, src: row.neighbor, dst: id };
    }
    return { id: row.edgeId, src: id, dst: row.neighbor };
  });

  return { node: header, neighbourHeaders, edges };
};
