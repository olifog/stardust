export type JsonValue = string | number | boolean | null;

export interface OkResponse {
  ok: boolean;
}

export interface IdResponse {
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

export interface NodeWithNeighboursResult {
  node: NodeHeader;
  neighbourHeaders: NodeHeader[];
  edges: Edge[];
}


