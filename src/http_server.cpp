#include "http_server.hpp"

// Mongoose: single-file library
// We vendor it by FetchContent in CMake and include as <mongoose.h>
#include <mongoose.h>

#include <thread>
#include <atomic>
#include <charconv>
#include <cstring>
#include <vector>
#include <variant>
#include <string>
#include <cstdlib>
#include <optional>

namespace stardust::http
{

  namespace
  {
    struct ServerState
    {
      stardust::Store *store{nullptr};
    };

    // helpers -----------------------------------------------------------------
    static bool parseUint64(const mg_str &s, uint64_t &out)
    {
      out = 0;
      if (s.len == 0) return false;
      const char *b = s.buf;
      const char *e = s.buf + s.len;
      auto res = std::from_chars(b, e, out);
      return res.ec == std::errc{} && res.ptr == e;
    }

    static bool parseUint32(const mg_str &s, uint32_t &out)
    {
      out = 0;
      if (s.len == 0) return false;
      const char *b = s.buf;
      const char *e = s.buf + s.len;
      auto res = std::from_chars(b, e, out);
      return res.ec == std::errc{} && res.ptr == e;
    }

    static bool strEquals(const mg_str &s, const char *lit)
    {
      size_t n = strlen(lit);
      return s.len == n && memcmp(s.buf, lit, n) == 0;
    }

    static stardust::Direction parseDirection(const mg_str &s)
    {
      if (strEquals(s, "out")) return stardust::Direction::Out;
      if (strEquals(s, "in")) return stardust::Direction::In;
      return stardust::Direction::Both;
    }

    // Split CSV into vector of mg_str slices over the provided buffer (no alloc)
    static void splitCsv(const mg_str &s, std::vector<mg_str> &out)
    {
      size_t start = 0;
      for (size_t i = 0; i <= (size_t)s.len; ++i)
      {
        if (i == (size_t)s.len || s.buf[i] == ',')
        {
          mg_str part = mg_str_n(s.buf + start, (size_t)(i - start));
          if (part.len > 0) out.push_back(part);
          start = i + 1;
        }
      }
    }

    static bool parseBool(const mg_str &s, bool &out)
    {
      if (strEquals(s, "true") || strEquals(s, "1")) { out = true; return true; }
      if (strEquals(s, "false") || strEquals(s, "0")) { out = false; return true; }
      return false;
    }

    static bool parseNumber(const mg_str &s, stardust::Value &out)
    {
      int64_t i64{};
      const char *b = s.buf;
      const char *e = s.buf + s.len;
      auto ri = std::from_chars(b, e, i64);
      if (ri.ec == std::errc{} && ri.ptr == e) { out = i64; return true; }
      // try double
      char *endp = nullptr;
      std::string tmp(b, (size_t)(e - b));
      double d = std::strtod(tmp.c_str(), &endp);
      if (endp && *endp == '\0') { out = d; return true; }
      return false;
    }

    static stardust::Value parseValue(const mg_str &s)
    {
      bool bv{};
      if (parseBool(s, bv)) return stardust::Value{bv};
      stardust::Value v{};
      if (parseNumber(s, v)) return v;
      if (strEquals(s, "null")) return std::monostate{};
      // default: treat as bytes/string
      return std::string(s.buf, s.len);
    }

    // JSON helpers -----------------------------------------------------------------
    static int print_u64_array(mg_pfn_t out, void *arg, va_list *ap)
    {
      const uint64_t *data = va_arg(*ap, const uint64_t *);
      size_t count = va_arg(*ap, size_t);
      mg_xprintf(out, arg, "[");
      for (size_t i = 0; i < count; ++i)
        mg_xprintf(out, arg, "%s%llu", i ? "," : "", (unsigned long long)data[i]);
      mg_xprintf(out, arg, "]");
      return 0;
    }

    static void print_value_json(mg_pfn_t out, void *arg, const stardust::Value &v)
    {
      if (std::holds_alternative<int64_t>(v))
      {
        mg_xprintf(out, arg, "%lld", (long long)std::get<int64_t>(v));
        return;
      }
      if (std::holds_alternative<double>(v))
      {
        mg_xprintf(out, arg, "%g", std::get<double>(v));
        return;
      }
      if (std::holds_alternative<bool>(v))
      {
        mg_xprintf(out, arg, "%s", std::get<bool>(v) ? "true" : "false");
        return;
      }
      if (std::holds_alternative<uint32_t>(v))
      {
        mg_xprintf(out, arg, "%u", (unsigned)std::get<uint32_t>(v));
        return;
      }
      if (std::holds_alternative<std::string>(v))
      {
        const auto &s = std::get<std::string>(v);
        mg_xprintf(out, arg, "%m", MG_ESC(s.c_str()));
        return;
      }
      mg_xprintf(out, arg, "null");
    }

    static int print_props_object(mg_pfn_t out, void *arg, va_list *ap)
    {
      const stardust::Property *props = va_arg(*ap, const stardust::Property *);
      size_t count = va_arg(*ap, size_t);
      stardust::Store *store = va_arg(*ap, stardust::Store *);
      mg_xprintf(out, arg, "{");
      for (size_t i = 0; i < count; ++i)
      {
        std::string keyName = store->getPropKeyName(props[i].keyId);
        mg_xprintf(out, arg, "%s%m:", i ? "," : "", MG_ESC(keyName.c_str()));
        print_value_json(out, arg, props[i].val);
      }
      mg_xprintf(out, arg, "}");
      return 0;
    }

    static int print_label_names_array(mg_pfn_t out, void *arg, va_list *ap)
    {
      const uint32_t *labelIds = va_arg(*ap, const uint32_t *);
      size_t count = va_arg(*ap, size_t);
      stardust::Store *store = va_arg(*ap, stardust::Store *);
      mg_xprintf(out, arg, "[");
      for (size_t i = 0; i < count; ++i)
      {
        std::string nm = store->getLabelName(labelIds[i]);
        mg_xprintf(out, arg, "%s%m", i ? "," : "", MG_ESC(nm.c_str()));
      }
      mg_xprintf(out, arg, "]");
      return 0;
    }

    static int print_vectors_array(mg_pfn_t out, void *arg, va_list *ap)
    {
      const stardust::TaggedVector *vecs = va_arg(*ap, const stardust::TaggedVector *);
      size_t count = va_arg(*ap, size_t);
      stardust::Store *store = va_arg(*ap, stardust::Store *);
      mg_xprintf(out, arg, "[");
      for (size_t i = 0; i < count; ++i)
      {
        const auto &tv = vecs[i];
        std::string tagName = store->getVecTagName(tv.tagId);
        // base64 encode vector data
        const std::string &bin = tv.vector.data;
        size_t out_len = (bin.size() + 2) / 3 * 4 + 4;
        std::string b64;
        b64.resize(out_len);
        size_t n = mg_base64_encode((const unsigned char *)bin.data(), (size_t)bin.size(), (char *)b64.data(), (size_t)b64.size());
        b64.resize(n);
        mg_xprintf(out, arg, "%s{%m:%m,%m:%u,%m:%m}", i ? "," : "",
                   MG_ESC("tag"), MG_ESC(tagName.c_str()),
                   MG_ESC("dim"), (unsigned)tv.vector.dim,
                   MG_ESC("data"), MG_ESC(b64.c_str()));
      }
      mg_xprintf(out, arg, "]");
      return 0;
    }

    // HTTP handlers -----------------------------------------------------------------
    static void handle_health(struct mg_connection *c)
    {
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_create_node(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      stardust::CreateNodeParams in{};
      // labels=LabelA,LabelB
      char buf[4096];
      int n = mg_http_get_var(&hm->query, "labels", buf, sizeof(buf));
      if (n > 0)
      {
        mg_str labels = mg_str_n(buf, (size_t)n);
        std::vector<mg_str> parts; parts.reserve(8);
        splitCsv(labels, parts);
        for (auto part : parts)
        {
          std::string nm(part.buf, part.len);
          in.labels.labelIds.push_back(st->store->getOrCreateLabelId({nm, true}));
        }
      }

      auto res = st->store->createNode(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{%m:%llu}\n", MG_ESC("id"), (unsigned long long)res.id);
    }

    static void handle_add_edge(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char srcBuf[64], dstBuf[64], typeBuf[256];
      int ns = mg_http_get_var(&hm->query, "src", srcBuf, sizeof(srcBuf));
      int nd = mg_http_get_var(&hm->query, "dst", dstBuf, sizeof(dstBuf));
      int nt = mg_http_get_var(&hm->query, "type", typeBuf, sizeof(typeBuf));
      if (ns <= 0 || nd <= 0 || nt <= 0)
      {
        mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing src,dst,type\"}\n");
        return;
      }
      uint64_t src{}, dst{};
      mg_str srcStr = mg_str_n(srcBuf, (size_t)ns);
      mg_str dstStr = mg_str_n(dstBuf, (size_t)nd);
      mg_str typeStr = mg_str_n(typeBuf, (size_t)nt);
      if (!parseUint64(srcStr, src) || !parseUint64(dstStr, dst))
      {
        mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid src/dst\"}\n");
        return;
      }
      stardust::AddEdgeParams in{};
      in.src = src;
      in.dst = dst;
      in.meta.typeId = st->store->getOrCreateRelTypeId({std::string(typeStr.buf, typeStr.len), true});
      auto e = st->store->addEdge(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{%m:%llu}\n", MG_ESC("id"), (unsigned long long)e.id);
    }

    static void handle_neighbors(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char nodeBuf[64], dirBuf[16], limitBuf[32];
      int nn = mg_http_get_var(&hm->query, "node", nodeBuf, sizeof(nodeBuf));
      if (nn <= 0)
      {
        mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing node\"}\n");
        return;
      }
      uint64_t node{};
      mg_str nodeStr = mg_str_n(nodeBuf, (size_t)nn);
      if (!parseUint64(nodeStr, node))
      {
        mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid node\"}\n");
        return;
      }
      uint32_t limit = 50;
      int nl = mg_http_get_var(&hm->query, "limit", limitBuf, sizeof(limitBuf));
      if (nl > 0)
      {
        mg_str limitStr = mg_str_n(limitBuf, (size_t)nl);
        uint32_t parsed{}; if (parseUint32(limitStr, parsed)) limit = parsed;
      }
      stardust::NeighborsParams in{};
      in.node = node;
      int ndir = mg_http_get_var(&hm->query, "direction", dirBuf, sizeof(dirBuf));
      if (ndir > 0) { mg_str dirStr = mg_str_n(dirBuf, (size_t)ndir); in.direction = parseDirection(dirStr); }
      in.limit = limit;
      auto res = st->store->neighbors(in);

      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{%m:%M}\n",
                    MG_ESC("neighbors"), print_u64_array, res.neighbors.data(), res.neighbors.size());
    }

    static void handle_get_node(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      auto resv = st->store->getNode(stardust::GetNodeParams{.id = id});
      const auto &h = resv.header;
      mg_http_reply(c, 200, "Content-Type: application/json\r\n",
                    "{%m:{%m:%llu,%m:%M,%m:%M}}\n",
                    MG_ESC("header"),
                    MG_ESC("id"), (unsigned long long)h.id,
                    MG_ESC("labels"), print_label_names_array, h.labels.labelIds.data(), h.labels.labelIds.size(), st->store,
                    MG_ESC("hotProps"), print_props_object, h.hotProps.data(), h.hotProps.size(), st->store);
    }

    static void handle_get_node_props(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], keysBuf[4096];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      std::vector<uint32_t> keyIds;
      int nk = mg_http_get_var(&hm->query, "keys", keysBuf, sizeof(keysBuf));
      if (nk > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(keysBuf, (size_t)nk), parts);
        keyIds.reserve(parts.size());
        for (auto p : parts) keyIds.push_back(st->store->getOrCreatePropKeyId({std::string(p.buf, p.len), false}));
      }
      auto resv = st->store->getNodeProps(stardust::GetNodePropsParams{.id = id, .keyIds = keyIds});
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{%m:%M}\n",
                    MG_ESC("props"), print_props_object, resv.props.data(), resv.props.size(), st->store);
    }

    static void handle_get_vectors(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], tagsBuf[4096];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      std::vector<uint32_t> tagIds;
      int nt = mg_http_get_var(&hm->query, "tags", tagsBuf, sizeof(tagsBuf));
      if (nt > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(tagsBuf, (size_t)nt), parts);
        tagIds.reserve(parts.size());
        for (auto p : parts) tagIds.push_back(st->store->getOrCreateVecTagId({std::string(p.buf, p.len), false}));
      }
      auto resv = st->store->getVectors(stardust::GetVectorsParams{.id = id, .tagIds = tagIds});
      mg_http_reply(c, 200, "Content-Type: application/json\r\n",
                    "{%m:%M}\n", MG_ESC("vectors"), print_vectors_array, resv.vectors.data(), resv.vectors.size(), st->store);
    }

    static void handle_get_edge(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64];
      int ni = mg_http_get_var(&hm->query, "edgeId", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing edgeId\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid edgeId\"}\n"); return; }
      auto edge = st->store->getEdge(stardust::GetEdgeParams{.edgeId = id});
      mg_http_reply(c, 200, "Content-Type: application/json\r\n",
                    "{%m:%llu,%m:%llu,%m:%llu}\n",
                    MG_ESC("id"), (unsigned long long)edge.id,
                    MG_ESC("src"), (unsigned long long)edge.src,
                    MG_ESC("dst"), (unsigned long long)edge.dst);
    }

    static void handle_delete_node(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      st->store->deleteNode(stardust::DeleteNodeParams{.id = id});
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_delete_edge(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64];
      int ni = mg_http_get_var(&hm->query, "edgeId", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing edgeId\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid edgeId\"}\n"); return; }
      st->store->deleteEdge(stardust::DeleteEdgeParams{.edgeId = id});
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_set_node_labels(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], addBuf[4096], rmBuf[4096];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      stardust::SetNodeLabelsParams in{}; in.id = id;
      int na = mg_http_get_var(&hm->query, "add", addBuf, sizeof(addBuf));
      if (na > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(addBuf, (size_t)na), parts);
        for (auto p : parts) in.addLabels.push_back(st->store->getOrCreateLabelId({std::string(p.buf, p.len), true}));
      }
      int nr = mg_http_get_var(&hm->query, "remove", rmBuf, sizeof(rmBuf));
      if (nr > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(rmBuf, (size_t)nr), parts);
        for (auto p : parts) in.removeLabels.push_back(st->store->getOrCreateLabelId({std::string(p.buf, p.len), false}));
      }
      st->store->setNodeLabels(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void parse_kv_list(const mg_str &s, std::vector<std::pair<mg_str, mg_str>> &out)
    {
      size_t start = 0;
      for (size_t i = 0; i <= (size_t)s.len; ++i)
      {
        if (i == (size_t)s.len || s.buf[i] == ',')
        {
          mg_str part = mg_str_n(s.buf + start, (size_t)(i - start));
          if (part.len > 0)
          {
            // split by '=' first occurrence
            size_t eq = 0; bool found = false;
            for (; eq < (size_t)part.len; ++eq) if (part.buf[eq] == '=') { found = true; break; }
            if (found)
            {
              mg_str k = mg_str_n(part.buf, eq);
              mg_str v = mg_str_n(part.buf + eq + 1, (size_t)part.len - eq - 1);
              out.emplace_back(k, v);
            }
            else
            {
              mg_str k = part; mg_str v = mg_str_n("", 0);
              out.emplace_back(k, v);
            }
          }
          start = i + 1;
        }
      }
    }

    static void handle_upsert_node_props(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], setHotBuf[4096], setColdBuf[4096], unsetBuf[4096];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      stardust::UpsertNodePropsParams in{}; in.id = id;
      int nsh = mg_http_get_var(&hm->query, "setHot", setHotBuf, sizeof(setHotBuf));
      if (nsh > 0)
      {
        std::vector<std::pair<mg_str, mg_str>> kvs; parse_kv_list(mg_str_n(setHotBuf, (size_t)nsh), kvs);
        for (auto &kv : kvs)
        {
          std::string key(kv.first.buf, kv.first.len);
          stardust::Property p{}; p.keyId = st->store->getOrCreatePropKeyId({key, true}); p.val = parseValue(kv.second);
          in.setHot.push_back(std::move(p));
        }
      }
      int nsc = mg_http_get_var(&hm->query, "setCold", setColdBuf, sizeof(setColdBuf));
      if (nsc > 0)
      {
        std::vector<std::pair<mg_str, mg_str>> kvs; parse_kv_list(mg_str_n(setColdBuf, (size_t)nsc), kvs);
        for (auto &kv : kvs)
        {
          std::string key(kv.first.buf, kv.first.len);
          stardust::Property p{}; p.keyId = st->store->getOrCreatePropKeyId({key, true}); p.val = parseValue(kv.second);
          in.setCold.push_back(std::move(p));
        }
      }
      int nu = mg_http_get_var(&hm->query, "unset", unsetBuf, sizeof(unsetBuf));
      if (nu > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(unsetBuf, (size_t)nu), parts);
        for (auto p : parts) in.unsetKeys.push_back(st->store->getOrCreatePropKeyId({std::string(p.buf, p.len), false}));
      }
      st->store->upsertNodeProps(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_upsert_vector(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], tagBuf[256], dimBuf[32], dataBuf[32768], dataB64Buf[65536];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      int nt = mg_http_get_var(&hm->query, "tag", tagBuf, sizeof(tagBuf));
      if (ni <= 0 || nt <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id/tag\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      std::string tag(tagBuf, (size_t)nt);
      stardust::UpsertVectorParams in{}; in.id = id;
      stardust::VectorF32 vec{};
      int nd_csv = mg_http_get_var(&hm->query, "data", dataBuf, sizeof(dataBuf));
      int nd_b64 = mg_http_get_var(&hm->query, "data_b64", dataB64Buf, sizeof(dataB64Buf));
      if (nd_csv > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(dataBuf, (size_t)nd_csv), parts);
        vec.dim = (uint16_t)parts.size();
        vec.data.reserve(parts.size() * 4);
        for (auto p : parts)
        {
          std::string s(p.buf, p.len);
          float f = std::strtof(s.c_str(), nullptr);
          const char *fb = reinterpret_cast<const char *>(&f);
          vec.data.append(fb, sizeof(float));
        }
      }
      else if (nd_b64 > 0)
      {
        int nd = mg_http_get_var(&hm->query, "dim", dimBuf, sizeof(dimBuf));
        if (nd <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing dim for data_b64\"}\n"); return; }
        uint32_t dim{}; if (!parseUint32(mg_str_n(dimBuf, (size_t)nd), dim)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid dim\"}\n"); return; }
        vec.dim = (uint16_t)dim;
        std::string b64(dataB64Buf, (size_t)nd_b64);
        // Decoded size at most 3/4 of base64 length
        std::string bin; bin.resize((b64.size() * 3) / 4 + 4);
        size_t nout = mg_base64_decode(b64.c_str(), (size_t)b64.size(), (char *)bin.data(), (size_t)bin.size());
        bin.resize(nout);
        vec.data = std::move(bin);
      }
      else
      {
        mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing data or data_b64\"}\n");
        return;
      }
      std::optional<uint16_t> dimOpt; if (vec.dim != 0) dimOpt = vec.dim;
      in.tagId = st->store->getOrCreateVecTagId({tag, true, dimOpt});
      in.vector = std::move(vec);
      st->store->upsertVector(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_delete_vector(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], tagBuf[256];
      int ni = mg_http_get_var(&hm->query, "id", idBuf, sizeof(idBuf));
      int nt = mg_http_get_var(&hm->query, "tag", tagBuf, sizeof(tagBuf));
      if (ni <= 0 || nt <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing id/tag\"}\n"); return; }
      uint64_t id{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), id)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid id\"}\n"); return; }
      std::string tag(tagBuf, (size_t)nt);
      stardust::DeleteVectorParams in{}; in.id = id; in.tagId = st->store->getOrCreateVecTagId({tag, false});
      st->store->deleteVector(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_update_edge_props(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char idBuf[64], setBuf[4096], unsetBuf[4096];
      int ni = mg_http_get_var(&hm->query, "edgeId", idBuf, sizeof(idBuf));
      if (ni <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing edgeId\"}\n"); return; }
      uint64_t edgeId{}; if (!parseUint64(mg_str_n(idBuf, (size_t)ni), edgeId)) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"invalid edgeId\"}\n"); return; }
      stardust::UpdateEdgePropsParams in{}; in.edgeId = edgeId;
      int ns = mg_http_get_var(&hm->query, "set", setBuf, sizeof(setBuf));
      if (ns > 0)
      {
        std::vector<std::pair<mg_str, mg_str>> kvs; parse_kv_list(mg_str_n(setBuf, (size_t)ns), kvs);
        for (auto &kv : kvs)
        {
          std::string key(kv.first.buf, kv.first.len);
          stardust::Property p{}; p.keyId = st->store->getOrCreatePropKeyId({key, true}); p.val = parseValue(kv.second);
          in.setProps.push_back(std::move(p));
        }
      }
      int nu = mg_http_get_var(&hm->query, "unset", unsetBuf, sizeof(unsetBuf));
      if (nu > 0)
      {
        std::vector<mg_str> parts; splitCsv(mg_str_n(unsetBuf, (size_t)nu), parts);
        for (auto p : parts) in.unsetKeys.push_back(st->store->getOrCreatePropKeyId({std::string(p.buf, p.len), false}));
      }
      st->store->updateEdgeProps(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{\"ok\":true}\n");
    }

    static void handle_knn(struct mg_connection *c, ServerState *st, struct mg_http_message *hm)
    {
      char tagBuf[256], qBuf[65536], kBuf[32];
      int nt = mg_http_get_var(&hm->query, "tag", tagBuf, sizeof(tagBuf));
      int nq = mg_http_get_var(&hm->query, "q", qBuf, sizeof(qBuf));
      if (nt <= 0 || nq <= 0) { mg_http_reply(c, 400, "Content-Type: application/json\r\n", "{\"error\":\"missing tag/q\"}\n"); return; }
      uint32_t k = 10; int nk = mg_http_get_var(&hm->query, "k", kBuf, sizeof(kBuf)); if (nk > 0) { mg_str ks = mg_str_n(kBuf, (size_t)nk); uint32_t parsed{}; if (parseUint32(ks, parsed)) k = parsed; }
      std::string tag(tagBuf, (size_t)nt);
      std::vector<mg_str> parts; splitCsv(mg_str_n(qBuf, (size_t)nq), parts);
      stardust::VectorF32 qv{}; qv.dim = (uint16_t)parts.size(); qv.data.reserve(parts.size() * 4);
      for (auto p : parts)
      {
        std::string s(p.buf, p.len); float f = std::strtof(s.c_str(), nullptr);
        const char *fb = reinterpret_cast<const char *>(&f); qv.data.append(fb, sizeof(float));
      }
      stardust::KnnParams in{}; in.tagId = st->store->getOrCreateVecTagId({tag, false}); in.query = std::move(qv); in.k = k;
      auto resv = st->store->knn(in);
      mg_http_reply(c, 200, "Content-Type: application/json\r\n", "{%m:[%M]}\n",
                    MG_ESC("hits"), [](mg_pfn_t o, void *a, va_list *ap) -> int {
                      const stardust::KnnPair *hits = va_arg(*ap, const stardust::KnnPair *);
                      size_t count = va_arg(*ap, size_t);
                      mg_xprintf(o, a, "");
                      for (size_t i = 0; i < count; ++i)
                        mg_xprintf(o, a, "%s{%m:%llu,%m:%g}", i ? "," : "",
                                   MG_ESC("id"), (unsigned long long)hits[i].id,
                                   MG_ESC("score"), hits[i].score);
                      return 0;
                    }, resv.hits.data(), resv.hits.size());
    }

    static void ev_handler(struct mg_connection *c, int ev, void *ev_data)
    {
      if (ev == MG_EV_HTTP_MSG)
      {
        auto *hm = (struct mg_http_message *)ev_data;
        auto *st = (ServerState *)c->fn_data;

        if (mg_match(hm->uri, mg_str("/api/health"), NULL))
        {
          handle_health(c);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/node"), NULL))
        {
          handle_create_node(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/edge"), NULL))
        {
          handle_add_edge(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/neighbors"), NULL))
        {
          handle_neighbors(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/node"), NULL))
        {
          handle_get_node(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/nodeProps"), NULL))
        {
          handle_get_node_props(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/vectors"), NULL))
        {
          handle_get_vectors(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/edge"), NULL))
        {
          handle_get_edge(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "DELETE") && mg_match(hm->uri, mg_str("/api/node"), NULL))
        {
          handle_delete_node(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "DELETE") && mg_match(hm->uri, mg_str("/api/edge"), NULL))
        {
          handle_delete_edge(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/setNodeLabels"), NULL))
        {
          handle_set_node_labels(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/upsertNodeProps"), NULL))
        {
          handle_upsert_node_props(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/upsertVector"), NULL))
        {
          handle_upsert_vector(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/deleteVector"), NULL))
        {
          handle_delete_vector(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "POST") && mg_match(hm->uri, mg_str("/api/updateEdgeProps"), NULL))
        {
          handle_update_edge_props(c, st, hm);
          return;
        }
        if (strEquals(hm->method, "GET") && mg_match(hm->uri, mg_str("/api/knn"), NULL))
        {
          handle_knn(c, st, hm);
          return;
        }
        mg_http_reply(c, 404, "Content-Type: application/json\r\n", "{\"error\":\"not found\"}\n");
      }
    }

    void run_loop(struct mg_mgr *mgr)
    {
      for (;;)
      {
        mg_mgr_poll(mgr, 250);
      }
    }
  } // namespace

  void startHttpServer(stardust::Store &store, const std::string &bind)
  {
    std::thread([&store, bind]() {
      struct mg_mgr mgr{};
      mg_mgr_init(&mgr);

      ServerState st{.store = &store};
      struct mg_connection *c = mg_http_listen(&mgr, bind.c_str(), ev_handler, &st);
      if (c == nullptr)
      {
        // idk
        return;
      }
      run_loop(&mgr);
      mg_mgr_free(&mgr);
    }).detach();
  }

} // namespace stardust::http

 