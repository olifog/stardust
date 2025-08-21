@0x84b755f396eeae85;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("stardust::rpc");

struct Value {
  union {
    i64   @0 :Int64;
    f64   @1 :Float64;
    boolv @2 :Bool;
    text  @3 :Text;    # wire uses Text, storage interns to id
    bytes @4 :Data;    # small blobs only!!!!! put big stuff in text
    nullv @5 :Void;
  }
}

struct Property {
  val @0 :Value;
  key @1 :Text; 
}

# sorted label names
struct LabelSet {
  names @0 :List(Text);
}

# multiple vectors per node, each tagged. vector bytes stay separate from
# node record for cache locality (store here only in RPC, on disk keep in vec bucket).
struct VectorF32 {
  dim  @0 :UInt16;
  data @1 :Data;  # float32 bytes, len = dim*4
}
struct TaggedVector {
  vector @0 :VectorF32;
  tag    @1 :Text;
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
  props @0 :List(Property);
  type  @1 :Text;
}

struct EdgeRef {
  id  @0 :UInt64;
  src @1 :UInt64;
  dst @2 :UInt64;
}

# -------------------- RPC Request/Response ----------------------

struct CreateNodeParams {
  labels     @0 :LabelSet;            # optional, empty -> no labels
  hotProps   @1 :List(Property);      # optional small set
  coldProps  @2 :List(Property);      # stored out-of-line
  vectors    @3 :List(TaggedVector);  # stored out-of-line + HNSW enqueue
}

struct CreateNodeResult { node @0 :NodeRef; header @1 :NodeHeader; }

struct UpsertNodePropsParams {
  id        @0 :UInt64;
  setHot    @1 :List(Property);
  setCold   @2 :List(Property);
  unsetKeys @3 :List(Text);  # propKeyIds to remove (both hot/cold)
}

struct SetNodeLabelsParams {
  id           @0 :UInt64;
  addLabels    @1 :List(Text);
  removeLabels @2 :List(Text);
}

struct UpsertVectorParams {
  id     @0 :UInt64; # node id
  vector @1 :VectorF32;
  tag    @2 :Text;
}

struct DeleteVectorParams {
  id  @0 :UInt64; # node id
  tag @1 :Text;
}

struct AddEdgeParams {
  src  @0 :UInt64;
  dst  @1 :UInt64;
  meta @2 :EdgeMeta;
}

struct UpdateEdgePropsParams {
  edgeId    @0 :UInt64;
  setProps  @1 :List(Property);
  unsetKeys @2 :List(Text);
}

enum Direction { out @0; in @1; both @2; }

struct ListAdjacencyParams {
  node      @0 :UInt64;
  direction @1 :Direction;
  limit     @2 :UInt32;
}

struct AdjacencyRow {
  neighbor  @0 :UInt64;
  edgeId    @1 :UInt64;
  type      @2 :Text;
  direction @3 :Direction;
}

struct ListAdjacencyResult { items @0 :List(AdjacencyRow); }

# KNN against a specific tagged vector index (HNSW per tagId)
struct KnnParams {
  tag   @0 :Text;
  query @1 :VectorF32;
  k     @2 :UInt32;
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
  id   @0 :UInt64;
  keys @1 :List(Text); # if empty, return all props (hot + cold)
}
struct GetNodePropsResult { props @0 :List(Property); }

struct GetVectorsParams {
  id   @0 :UInt64; # node id
  tags @1 :List(Text); # if empty, return all vectors for the node
}
struct GetVectorsResult { vectors @0 :List(TaggedVector); }

struct GetEdgeParams { edgeId @0 :UInt64; }

struct GetEdgePropsResult { props @0 :List(Property); }

struct ScanNodesByLabelResult { nodeIds @0 :List(UInt64); }

struct DegreeResult { count @0 :UInt64; }

struct DeleteNodeParams { id @0 :UInt64; }
struct DeleteEdgeParams { edgeId @0 :UInt64; }

interface Stardust {
  createNode      @0 (params :CreateNodeParams) -> (result :CreateNodeResult);
  upsertNodeProps @1 (params :UpsertNodePropsParams);
  setNodeLabels   @2 (params :SetNodeLabelsParams);

  upsertVector    @3 (params :UpsertVectorParams);
  deleteVector    @4 (params :DeleteVectorParams);

  addEdge         @5 (params :AddEdgeParams) -> (edge :EdgeRef);
  updateEdgeProps @6 (params :UpdateEdgePropsParams);

  listAdjacency   @7 (params :ListAdjacencyParams) -> (result :ListAdjacencyResult);
  knn             @8 (params :KnnParams) -> (result :KnnResult);

  writeBatch      @9 (batch :WriteBatch);

  # Reads
  getNode         @10 (params :GetNodeParams) -> (result :GetNodeResult);
  getNodeProps    @11 (params :GetNodePropsParams) -> (result :GetNodePropsResult);
  getVectors      @12 (params :GetVectorsParams) -> (result :GetVectorsResult);
  getEdge         @13 (params :GetEdgeParams) -> (edge :EdgeRef, meta :EdgeMeta);
  getEdgeProps    @14 (edgeId :UInt64, keys :List(Text)) -> (result :GetEdgePropsResult);
  scanNodesByLabel @15 (label :Text, limit :UInt32) -> (result :ScanNodesByLabelResult);
  degree          @16 (node :UInt64, direction :Direction) -> (result :DegreeResult);

  # Deletes
  deleteNode      @17 (params :DeleteNodeParams);
  deleteEdge      @18 (params :DeleteEdgeParams);
}
