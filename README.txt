
LMDB layout (buckets & keys)

Each bucket sorted by key to support range scans.

- nodes: <u64 nodeId> -> NodeHeader
- nodeColdProps: <u64 nodeId>|<u32 propKeyId> -> Value
- nodeVectors: <u64 nodeId>|<u32 tagId> -> VectorF32.bytes

- edgesBySrcType: <u64 src>|<u32 typeId>|<u64 dst>|<u64 edgeId> -> void
- edgesByDstType: <u64 dst>|<u32 typeId>|<u64 src>|<u64 edgeId> -> void
- edgesById: <u64 edgeId> -> EdgeRef
- edgeProps: <u64 edgeId>|<u32 propKeyId> -> Value

- labelIds:    <u32 id> -> string
- labelsByName: string -> <u32 id>
- relTypeIds / relTypesByName: same
- propKeyIds / propKeysByName: same
- vecTagIds / vecTagsByName:   same
- textIds:     <u32 id> -> string
- textsByName: string -> <u32 id>
- vecTagMeta: <u32 tagId> -> {dim, metric, hnswParams}

- meta: "nodeSeq" -> <u64>, "edgeSeq" -> <u64>, "schemaVersion" -> <u32>
- labelIndex: <u32 labelId>|<u64 nodeId> -> void

notes:
- fixed-width big-endian for all numeric fields

