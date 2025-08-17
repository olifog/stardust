#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace stardust
{

  inline void put_be64(std::string &s, uint64_t x)
  {
    for (int i = 7; i >= 0; --i)
      s.push_back(char((x >> (i * 8)) & 0xff));
  }
  inline void put_be32(std::string &s, uint32_t x)
  {
    for (int i = 3; i >= 0; --i)
      s.push_back(char((x >> (i * 8)) & 0xff));
  }

  inline uint64_t read_be64(const unsigned char *p)
  {
    uint64_t x = 0;
    for (int i = 0; i < 8; ++i)
      x = (x << 8) | p[i];
    return x;
  }
  inline uint32_t read_be32(const unsigned char *p)
  {
    uint32_t x = 0;
    for (int i = 0; i < 4; ++i)
      x = (x << 8) | p[i];
    return x;
  }

  // nodes: <u64 nodeId>
  inline std::string key_nodes_be(uint64_t nodeId)
  {
    std::string k;
    k.reserve(8);
    put_be64(k, nodeId);
    return k;
  }

  // nodeColdProps: <u64 nodeId>|<u32 propKeyId>
  inline std::string key_node_cold_prop_be(uint64_t nodeId, uint32_t propKeyId)
  {
    std::string k;
    k.reserve(8 + 4);
    put_be64(k, nodeId);
    put_be32(k, propKeyId);
    return k;
  }

  // nodeVectors: <u64 nodeId>|<u32 tagId>
  inline std::string key_node_vector_be(uint64_t nodeId, uint32_t tagId)
  {
    std::string k;
    k.reserve(8 + 4);
    put_be64(k, nodeId);
    put_be32(k, tagId);
    return k;
  }

  // edgesBySrcType: <u64 src>|<u32 typeId>|<u64 dst>|<u64 edgeId>
  inline std::string key_edge_by_src_type_be(uint64_t src, uint32_t typeId, uint64_t dst, uint64_t edgeId)
  {
    std::string k;
    k.reserve(8 + 4 + 8 + 8);
    put_be64(k, src);
    put_be32(k, typeId);
    put_be64(k, dst);
    put_be64(k, edgeId);
    return k;
  }

  // edgesByDstType: <u64 dst>|<u32 typeId>|<u64 src>|<u64 edgeId>
  inline std::string key_edge_by_dst_type_be(uint64_t dst, uint32_t typeId, uint64_t src, uint64_t edgeId)
  {
    std::string k;
    k.reserve(8 + 4 + 8 + 8);
    put_be64(k, dst);
    put_be32(k, typeId);
    put_be64(k, src);
    put_be64(k, edgeId);
    return k;
  }

  // edgesById: <u64 edgeId>
  inline std::string key_edge_by_id_be(uint64_t edgeId)
  {
    std::string k;
    k.reserve(8);
    put_be64(k, edgeId);
    return k;
  }

  // edgeProps: <u64 edgeId>|<u32 propKeyId>
  inline std::string key_edge_prop_be(uint64_t edgeId, uint32_t propKeyId)
  {
    std::string k;
    k.reserve(8 + 4);
    put_be64(k, edgeId);
    put_be32(k, propKeyId);
    return k;
  }

  // labelIds / relTypeIds / propKeyIds / vecTagIds / textIds: <u32 id>
  inline std::string key_u32_be(uint32_t id)
  {
    std::string k;
    k.reserve(4);
    put_be32(k, id);
    return k;
  }

  // *ByName buckets: raw string key
  inline std::string key_name(std::string_view name)
  {
    return std::string(name);
  }

  // vecTagMeta: <u32 tagId>
  inline std::string key_vec_tag_meta_be(uint32_t tagId) { return key_u32_be(tagId); }

  // labelIndex: <u32 labelId>|<u64 nodeId>
  inline std::string key_label_index_be(uint32_t labelId, uint64_t nodeId)
  {
    std::string k;
    k.reserve(4 + 8);
    put_be32(k, labelId);
    put_be64(k, nodeId);
    return k;
  }

  // meta bucket string keys
  inline std::string key_meta_node_seq() { return std::string("nodeSeq"); }
  inline std::string key_meta_edge_seq() { return std::string("edgeSeq"); }
  inline std::string key_meta_schema_version() { return std::string("schemaVersion"); }
  inline std::string key_meta_label_seq() { return std::string("labelSeq"); }
  inline std::string key_meta_reltype_seq() { return std::string("relTypeSeq"); }
  inline std::string key_meta_propkey_seq() { return std::string("propKeySeq"); }
  inline std::string key_meta_vectag_seq() { return std::string("vecTagSeq"); }
  inline std::string key_meta_text_seq() { return std::string("textSeq"); }

} // namespace stardust
