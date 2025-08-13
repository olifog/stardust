@0x84b755f396eeae85;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("stardust::rpc");

# ----------- Common dictionaries (stored separately) -----------
#   - labelId -> string
#   - relTypeId -> string
#   - propKeyId -> string
#   - vecTagId -> string
# ---------------------------------------------------------------

struct Value {
  union {
    i64    @0 :Int64;
    f64    @1 :Float64;
    boolv  @2 :Bool;
    textId @3 :UInt32;   # interned string ID
    bytes  @4 :Data;     # small blobs only!!!!!
    nullv  @5 :Void;
  }
}

struct Property {
  keyId @0 :UInt32; # from propKey dict
  val   @1 :Value;
}

# sorted label IDs
struct LabelSet {
  labelIds @0 :List(UInt32);
}

# multiple vectors per node, each tagged. vector bytes stay separate from
# node record for cache locality (store here only in RPC; on disk keep in vec bucket).
struct VectorF32 {
  dim  @0 :UInt16;
  data @1 :Data;  # float32 bytes, len = dim*4
}
struct TaggedVector {
  tagId  @0 :UInt32;  # from vecTag dict
  vector @1 :VectorF32;
}

# -------------------- node / relationship -----------------------

struct NodeRef { id @0 :UInt64; }

# small node header: labels + (optionally) a few hot properties
# bulk/cold props in separate LMDB sub-bucket keyed by nodeId
struct NodeHeader {
  id       @0 :UInt64;
  labels   @1 :LabelSet;
  hotProps @2 :List(Property);
}

# relationship metadata: single type + KV props (small, bulk in separate bucket)
struct EdgeMeta {
  typeId @0 :UInt32;           # from relType dict
  props  @1 :List(Property);
}

struct EdgeRef {
  id  @0 :UInt64;
  src @1 :UInt64;
  dst @2 :UInt64;
}

# -------------------- RPC Request/Response ----------------------

struct CreateNodeParams {
  labels     @0 :LabelSet;            # optional; empty -> no labels
  hotProps   @1 :List(Property);      # optional small set
  coldProps  @2 :List(Property);      # stored out-of-line
  vectors    @3 :List(TaggedVector);  # stored out-of-line + HNSW enqueue
}

struct CreateNodeResult { node @0 :NodeRef; header @1 :NodeHeader; }

struct UpsertNodePropsParams {
  id        @0 :UInt64;
  setHot    @1 :List(Property);
  setCold   @2 :List(Property);
  unsetKeys @3 :List(UInt32);  # propKeyIds to remove (both hot/cold)
}

struct SetNodeLabelsParams {
  id          @0 :UInt64;
  addLabels   @1 :List(UInt32);
  removeLabels@2 :List(UInt32);
}

struct UpsertVectorParams {
  id     @0 :UInt64; # node id
  tagId  @1 :UInt32;
  vector @2 :VectorF32;
}

struct DeleteVectorParams {
  id    @0 :UInt64; # node id
  tagId @1 :UInt32;
}

struct AddEdgeParams {
  src  @0 :UInt64;
  dst  @1 :UInt64;
  meta @2 :EdgeMeta;
}

struct UpdateEdgePropsParams {
  edgeId    @0 :UInt64;
  setProps  @1 :List(Property);
  unsetKeys @2 :List(UInt32);
}

enum Direction { out @0; in @1; both @2; }

struct NeighborsParams {
  node        @0 :UInt64;
  direction   @1 :Direction;
  limit       @2 :UInt32;
  relTypeIn   @3 :List(UInt32);   # filter by relationship types (OR)
  neighborHas @4 :LabelSet;       # neighbor must have all of these labels
  # TODO: lots more filters
}

struct NeighborsResult {
  neighbors @0 :List(UInt64);
}

# KNN against a specific tagged vector index (HNSW per tagId)
struct KnnParams {
  tagId     @0 :UInt32;
  query     @1 :VectorF32;
  k         @2 :UInt32;
  # TODO: filters, pre and post?
}

struct KnnPair { id @0 :UInt64; score @1 :Float32; }

struct KnnResult { hits @0 :List(KnnPair); }

# batch for write coalescing
struct WriteOp {
  union {
    createNode      @0 :CreateNodeParams;
    upsertNodeProps @1 :UpsertNodePropsParams;
    setNodeLabels   @2 :SetNodeLabelsParams;
    upsertVector    @3 :UpsertVectorParams;
    deleteVector    @4 :DeleteVectorParams;
    addEdge         @5 :AddEdgeParams;
    updateEdgeProps @6 :UpdateEdgePropsParams;
  }
}
struct WriteBatch { ops @0 :List(WriteOp); }

## -------------------- Read / Delete APIs ------------------------

struct GetNodeParams { id @0 :UInt64; }
struct GetNodeResult { header @0 :NodeHeader; }

struct GetNodePropsParams {
  id     @0 :UInt64;
  # if empty, return all props (hot + cold)
  keyIds @1 :List(UInt32);
}
struct GetNodePropsResult { props @0 :List(Property); }

struct GetVectorsParams {
  id     @0 :UInt64;          # node id
  # if empty, return all vectors for the node
  tagIds @1 :List(UInt32);
}
struct GetVectorsResult { vectors @0 :List(TaggedVector); }

struct GetEdgeParams { edgeId @0 :UInt64; }

struct DeleteNodeParams { id @0 :UInt64; }
struct DeleteEdgeParams { edgeId @0 :UInt64; }

interface GraphDb {
  createNode      @0 (params :CreateNodeParams) -> (result :CreateNodeResult);
  upsertNodeProps @1 (params :UpsertNodePropsParams);
  setNodeLabels   @2 (params :SetNodeLabelsParams);

  upsertVector    @3 (params :UpsertVectorParams);
  deleteVector    @4 (params :DeleteVectorParams);

  addEdge         @5 (params :AddEdgeParams) -> (edge :EdgeRef);
  updateEdgeProps @6 (params :UpdateEdgePropsParams);

  neighbors       @7 (params :NeighborsParams) -> (result :NeighborsResult);
  knn             @8 (params :KnnParams) -> (result :KnnResult);

  writeBatch      @9 (batch :WriteBatch);

  # Reads
  getNode         @10 (params :GetNodeParams) -> (result :GetNodeResult);
  getNodeProps    @11 (params :GetNodePropsParams) -> (result :GetNodePropsResult);
  getVectors      @12 (params :GetVectorsParams) -> (result :GetVectorsResult);
  getEdge         @13 (params :GetEdgeParams) -> (edge :EdgeRef);

  # Deletes
  deleteNode      @14 (params :DeleteNodeParams);
  deleteEdge      @15 (params :DeleteEdgeParams);
}
