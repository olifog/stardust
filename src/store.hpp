#pragma once
#include "env.hpp"
#include "encode.hpp"
#include <cstdint>
#include <vector>
#include <variant>
#include <optional>
#include <string>
#include <string_view>

namespace stardust
{

  using Value = std::variant<int64_t, double, bool, uint32_t, std::string, std::monostate>;

  struct Property
  {
    uint32_t keyId{0};
    Value val{}; // int64, double, bool, textId(u32), bytes(std::string), null(monostate)
  };

  struct LabelSet
  {
    std::vector<uint32_t> labelIds; // sorted ids
  };

  struct VectorF32
  {
    uint16_t dim{0};
    std::string data; // raw float32 bytes, size = dim*4
  };

  struct TaggedVector
  {
    uint32_t tagId{0};
    VectorF32 vector{};
  };

  // -------------------- node / edge data ---------------------------

  struct NodeHeader
  {
    uint64_t id{0};
    LabelSet labels{};
    std::vector<Property> hotProps{}; // optional small set
  };

  struct EdgeMeta
  {
    uint32_t typeId{0};
    std::vector<Property> props{}; // small set, bulk in edgeProps bucket
  };

  struct EdgeRef
  {
    uint64_t id{0};
    uint64_t src{0};
    uint64_t dst{0};
  };

  enum class Direction : uint8_t
  {
    Out = 0,
    In = 1,
    Both = 2
  };

  // -------------------- params / results ---------------------------

  struct CreateNodeParams
  {
    LabelSet labels{};
    std::vector<Property> hotProps{};
    std::vector<Property> coldProps{};
    std::vector<TaggedVector> vectors{};
  };
  struct CreateNodeResult
  {
    uint64_t id{0};
    NodeHeader header{};
  };

  struct UpsertNodePropsParams
  {
    uint64_t id{0};
    std::vector<Property> setHot{};
    std::vector<Property> setCold{};
    std::vector<uint32_t> unsetKeys{};
  };

  struct SetNodeLabelsParams
  {
    uint64_t id{0};
    std::vector<uint32_t> addLabels{};
    std::vector<uint32_t> removeLabels{};
  };

  struct UpsertVectorParams
  {
    uint64_t id{0};
    uint32_t tagId{0};
    VectorF32 vector{};
  };

  struct DeleteVectorParams
  {
    uint64_t id{0};
    uint32_t tagId{0};
  };

  struct AddEdgeParams
  {
    uint64_t src{0};
    uint64_t dst{0};
    EdgeMeta meta{};
  };

  struct UpdateEdgePropsParams
  {
    uint64_t edgeId{0};
    std::vector<Property> setProps{};
    std::vector<uint32_t> unsetKeys{};
  };

  struct NeighborsParams
  {
    uint64_t node{0};
    Direction direction{Direction::Out};
    uint32_t limit{0};
    std::vector<uint32_t> relTypeIn{}; // OR filter by relationship types
    LabelSet neighborHas{};            // neighbor must have all labels
  };
  struct NeighborsResult
  {
    std::vector<uint64_t> neighbors;
  };

  struct KnnParams
  {
    uint32_t tagId{0};
    VectorF32 query{};
    uint32_t k{0};
  };
  struct KnnPair
  {
    uint64_t id{0};
    float score{0.0f};
  };
  struct KnnResult
  {
    std::vector<KnnPair> hits;
  };

  struct GetNodeParams
  {
    uint64_t id{0};
  };
  struct GetNodeResult
  {
    NodeHeader header{};
  };

  struct GetNodePropsParams
  {
    uint64_t id{0};
    std::vector<uint32_t> keyIds{};
  };
  struct GetNodePropsResult
  {
    std::vector<Property> props;
  };

  struct GetVectorsParams
  {
    uint64_t id{0};
    std::vector<uint32_t> tagIds{};
  };
  struct GetVectorsResult
  {
    std::vector<TaggedVector> vectors;
  };

  struct GetEdgeParams
  {
    uint64_t edgeId{0};
  };

  struct DeleteNodeParams
  {
    uint64_t id{0};
  };
  struct DeleteEdgeParams
  {
    uint64_t edgeId{0};
  };

  class Store
  {
  public:
    explicit Store(Env &e) : env_(e) {}

    // writes
    CreateNodeResult createNode(const CreateNodeParams &params);
    void upsertNodeProps(const UpsertNodePropsParams &params);
    void setNodeLabels(const SetNodeLabelsParams &params);
    void upsertVector(const UpsertVectorParams &params);
    void deleteVector(const DeleteVectorParams &params);
    EdgeRef addEdge(const AddEdgeParams &params);
    void updateEdgeProps(const UpdateEdgePropsParams &params);

    // reads / queries
    NeighborsResult neighbors(const NeighborsParams &params);
    KnnResult knn(const KnnParams &params);
    GetNodeResult getNode(const GetNodeParams &params);
    GetNodePropsResult getNodeProps(const GetNodePropsParams &params);
    GetVectorsResult getVectors(const GetVectorsParams &params);
    EdgeRef getEdge(const GetEdgeParams &params);

    // deletes
    void deleteNode(const DeleteNodeParams &params);
    void deleteEdge(const DeleteEdgeParams &params);

  private:
    Env &env_;
  };

} // namespace stardust
