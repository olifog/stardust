@0xbada5555bada5555;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("stardust::rpc");

struct VectorF32 {
  dim @0 :UInt16;
  data @1 :Data;  # raw float32 bytes, length = dim * 4
}

struct NodeRef {
  id @0 :UInt64;
}

struct EdgeMeta {
  typeId @0 :UInt32;
  weight @1 :Float32;
}

struct CreateNodeParams {
  labelId @0 :UInt32;
  vector @1 :VectorF32;  # optional by presence; empty -> no vector
}

struct CreateNodeResult { node @0 :NodeRef; }

struct AddEdgeParams {
  src @0 :UInt64;
  dst @1 :UInt64;
  meta @2 :EdgeMeta;
}

struct NeighborsParams {
  node @0 :UInt64;
  direction @1 :UInt8;    # 0=OUT,1=IN
  limit @2 :UInt32;
}

struct NeighborsResult {
  neighbors @0 :List(UInt64);
}

struct KnnParams {
  query @0 :VectorF32;
  k @1 :UInt32;
}

struct KnnPair {
  id @0 :UInt64;
  score @1 :Float32;  # similarity or -distance; document your choice
}

struct KnnResult {
  hits @0 :List(KnnPair);
}

interface GraphDb {
  createNode @0 (params :CreateNodeParams) -> (result :CreateNodeResult);
  addEdge    @1 (params :AddEdgeParams);
  neighbors  @2 (params :NeighborsParams) -> (result :NeighborsResult);
  knn        @3 (params :KnnParams) -> (result :KnnResult);
}
