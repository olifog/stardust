import { Ollama } from "ollama/browser";
import { useEffect, useRef, useState } from "react";
import type { NodeHeader } from "stardust";
import { StardustClient } from "stardust";

const vectorTag = import.meta.env.VITE_VECTOR_TAG ?? "text";
const ollamaUrl = import.meta.env.VITE_OLLAMA_URL ?? "http://localhost:11434";
const ollamaModel = import.meta.env.VITE_OLLAMA_MODEL ?? "nomic-embed-text:v1.5";
const ollamaClient = new Ollama({ host: ollamaUrl });
const stardustClient = new StardustClient({ baseUrl: "http://localhost:8080" });

export const NodeSearch = ({
  onSearch,
}: {
  onSearch: (id: number) => void;
}) => {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<
    Array<{ id: number; name: string; score: number }>
  >([]);
  const [isVisible, setIsVisible] = useState(false);
  const [isFading, setIsFading] = useState(false);
  const hideTimerRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (hideTimerRef.current != null) {
        window.clearTimeout(hideTimerRef.current);
        hideTimerRef.current = null;
      }
    };
  }, []);

  const embedText = async (text: string): Promise<number[]> => {
    const resp = await ollamaClient.embeddings({
      model: ollamaModel,
      prompt: text,
    });
    const vec = (resp as unknown as { embedding: number[] }).embedding;
    if (!Array.isArray(vec)) throw new Error("Invalid embedding response");
    return vec.map((x) => Number(x));
  };

  const deriveName = (header: NodeHeader): string => {
    const isMovie = header.labels?.some((l) => l.toLowerCase() === "movie");
    const maybeTitle = isMovie ? header.hotProps?.title : undefined;
    const maybeName = header.hotProps?.name;
    const displayNameCandidate =
      typeof maybeTitle === "string" && maybeTitle.trim().length > 0
        ? maybeTitle
        : typeof maybeName === "string" && maybeName.trim().length > 0
        ? maybeName
        : undefined;
    return String(displayNameCandidate ?? header.id);
  };

  const runTextSearch = async (text: string) => {
    try {
      const qvec = await embedText(text);
      const { hits } = await stardustClient.knn({ tag: vectorTag, q: qvec, k: 8 });
      const headers: Array<NodeHeader | null> = await Promise.all(
        hits.map(async (h) => {
          try {
            const { header } = await stardustClient.getNode(h.id);
            return header;
          } catch {
            return null;
          }
        })
      );
      const withNames = hits.map((h, i) => ({
        id: h.id,
        score: h.score,
        name: headers[i] ? deriveName(headers[i] as NodeHeader) : String(h.id),
      }));
      setResults(withNames);
      setIsVisible(true);
      setIsFading(false);
      // Auto-select top result
      if (withNames.length > 0) {
        onSearch(withNames[0].id);
      }
      if (hideTimerRef.current != null)
        window.clearTimeout(hideTimerRef.current);
      // Start fade after 4.5s, hide at 5s
      hideTimerRef.current = window.setTimeout(() => {
        setIsFading(true);
        hideTimerRef.current = window.setTimeout(() => {
          setIsVisible(false);
          setIsFading(false);
          hideTimerRef.current = null;
        }, 500);
      }, 4500);
    } catch (err) {
      console.error("Text search failed", err);
    }
  };

  return (
    <div className="absolute top-2 left-1/2 -translate-x-1/2 z-10 flex flex-col items-center">
      <input
        className="px-3 py-1 rounded bg-black/60 text-white placeholder-white/60 border border-white/30 focus:outline-none focus:ring-2 focus:ring-white/60"
        placeholder="Enter node id or text and press Enter"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            const q = query.trim();
            if (/^\d+$/.test(q)) {
              const id = Number(q);
              if (!Number.isNaN(id)) onSearch(id);
              return;
            }
            if (q.length > 0) {
              void runTextSearch(q);
            }
          }
        }}
      />

      {isVisible && results.length > 0 && (
        <div
          className={`mt-2 w-[28rem] px-3 py-2 rounded bg-black/70 text-white border border-white/20 text-xs transition-opacity duration-500 ${
            isFading ? "opacity-0" : "opacity-100"
          }`}
        >
          <div className="font-semibold mb-1 text-white/80">Top matches</div>
          <ul className="space-y-0.5">
            {results.map((r) => (
              <li key={r.id} className="flex justify-between gap-2">
                <span className="truncate">{r.name}</span>
                <span className="text-white/60">{r.score.toFixed(3)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};
