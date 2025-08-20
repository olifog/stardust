import { HttpClient } from "./http";
import type {
  AdjacencyResponse,
  Edge,
  GetVectorsResponse,
  IdResponse,
  JsonValue,
  KnnResponse,
  NodeHeader,
  NodePropsResponse,
  NodeResponse,
  NodeWithNeighboursResult,
} from "./types";

export interface StardustClientOptions {
  baseUrl?: string;
}

export class StardustClient {
  private readonly http: HttpClient;

  constructor(options?: StardustClientOptions) {
    const baseUrl = options?.baseUrl ?? "http://localhost:8080";
    this.http = new HttpClient(baseUrl);
  }

  // Health --------------------------------------------------------------------
  health = async () => this.http.getJson<{ ok: boolean }>(`/api/health`);

  // Nodes ---------------------------------------------------------------------
  createNode = async (labels?: string[]) => {
    const q = this.http.buildQuery({ labels: labels?.length ? labels.join(",") : undefined });
    return this.http.getJson<IdResponse>(`/api/node${q}`, { method: "POST" });
  };

  getNode = async (id: number) => {
    const q = this.http.buildQuery({ id });
    return this.http.getJson<NodeResponse>(`/api/node${q}`);
  };

  getNodeProps = async (id: number, keys?: string[]) => {
    const q = this.http.buildQuery({ id, keys: keys?.length ? keys.join(",") : undefined });
    return this.http.getJson<NodePropsResponse>(`/api/nodeProps${q}`);
  };

  setNodeLabels = async (params: { id: number; add?: string[]; remove?: string[] }) => {
    const q = this.http.buildQuery({
      id: params.id,
      add: params.add?.length ? params.add.join(",") : undefined,
      remove: params.remove?.length ? params.remove.join(",") : undefined,
    });
    return this.http.getJson<{ ok: boolean }>(`/api/setNodeLabels${q}`, { method: "POST" });
  };

  upsertNodeProps = async (params: {
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
            .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(encodeVal(v))}`)
            .join(",")
        : undefined;
    const q = this.http.buildQuery({
      id: params.id,
      setHot: joinKv(params.setHot),
      setCold: joinKv(params.setCold),
      unset: params.unset?.length ? params.unset.map(encodeURIComponent).join(",") : undefined,
    });
    return this.http.getJson<{ ok: boolean }>(`/api/upsertNodeProps${q}`, { method: "POST" });
  };

  deleteNode = async (id: number) => {
    const q = this.http.buildQuery({ id });
    return this.http.getJson<{ ok: boolean }>(`/api/node${q}`, { method: "DELETE" });
  };

  // Edges ---------------------------------------------------------------------
  addEdge = async (params: { src: number; dst: number; type: string }) => {
    const q = this.http.buildQuery(params);
    return this.http.getJson<IdResponse>(`/api/edge${q}`, { method: "POST" });
  };

  getEdge = async (edgeId: number) => {
    const q = this.http.buildQuery({ edgeId });
    return this.http.getJson<Edge>(`/api/edge${q}`);
  };

  updateEdgeProps = async (params: {
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
            .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(encodeVal(v))}`)
            .join(",")
        : undefined;
    const q = this.http.buildQuery({
      edgeId: params.edgeId,
      set: joinKv(params.set),
      unset: params.unset?.length ? params.unset.map(encodeURIComponent).join(",") : undefined,
    });
    return this.http.getJson<{ ok: boolean }>(`/api/updateEdgeProps${q}`, { method: "POST" });
  };

  deleteEdge = async (edgeId: number) => {
    const q = this.http.buildQuery({ edgeId });
    return this.http.getJson<{ ok: boolean }>(`/api/edge${q}`, { method: "DELETE" });
  };

  // Vectors -------------------------------------------------------------------
  getVectors = async (id: number, tags?: string[]) => {
    const q = this.http.buildQuery({ id, tags: tags?.length ? tags.join(",") : undefined });
    return this.http.getJson<GetVectorsResponse>(`/api/vectors${q}`);
  };

  upsertVector = async (
    params: { id: number; tag: string; data: number[] } | { id: number; tag: string; dataB64: string; dim: number }
  ) => {
    if ("data" in params) {
      const q = this.http.buildQuery({ id: params.id, tag: params.tag, data: params.data.join(",") });
      return this.http.getJson<{ ok: boolean }>(`/api/upsertVector${q}`, { method: "POST" });
    }
    const q = this.http.buildQuery({ id: params.id, tag: params.tag, data_b64: params.dataB64, dim: params.dim });
    return this.http.getJson<{ ok: boolean }>(`/api/upsertVector${q}`, { method: "POST" });
  };

  deleteVector = async (id: number, tag: string) => {
    const q = this.http.buildQuery({ id, tag });
    return this.http.getJson<{ ok: boolean }>(`/api/deleteVector${q}`, { method: "POST" });
  };

  // Graph traversal ------------------------------------------------------------
  adjacency = async (params: { node: number; direction?: "out" | "in" | "both"; limit?: number }) => {
    const q = this.http.buildQuery({ node: params.node, direction: params.direction, limit: params.limit });
    return this.http.getJson<AdjacencyResponse>(`/api/adjacency${q}`);
  };

  knn = async (params: { tag: string; q: number[]; k?: number }) => {
    const q = this.http.buildQuery({ tag: params.tag, q: params.q.join(","), k: params.k });
    return this.http.getJson<KnnResponse>(`/api/knn${q}`);
  };

  // Helpers -------------------------------------------------------------------
  getNodeWithNeighbours = async (
    id: number,
    params?: { direction?: "out" | "in" | "both"; limit?: number }
  ): Promise<NodeWithNeighboursResult> => {
    const [{ header }, { items }] = await Promise.all([
      this.getNode(id),
      this.adjacency({ node: id, direction: params?.direction ?? "both", limit: params?.limit }),
    ]);

    // Best-effort: fetch headers for neighbour nodes. If any fail, skip them.
    const neighbourHeaders: NodeHeader[] = [];
    await Promise.all(
      items.map(async (row) => {
        try {
          const res = await this.getNode(row.neighbor);
          neighbourHeaders.push(res.header);
        } catch {
          // ignore individual neighbour fetch failures
        }
      })
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
}


