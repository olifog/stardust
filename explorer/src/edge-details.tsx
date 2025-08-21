import { useEffect, useState } from "react";
import { client } from "./graph-viewer";

interface EdgeDetailsProps {
  edgeId: number;
  initialSrcId?: number;
  initialDstId?: number;
  initialType?: string;
  onClose: () => void;
}

export const EdgeDetails = ({
  edgeId,
  initialSrcId,
  initialDstId,
  initialType,
  onClose,
}: EdgeDetailsProps) => {
  const [srcId, setSrcId] = useState<number | undefined>(initialSrcId);
  const [dstId, setDstId] = useState<number | undefined>(initialDstId);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [edgeType, setEdgeType] = useState<string | undefined>(initialType);
  const [props, setProps] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    let cancelled = false;
    const fetchEdge = async () => {
      setLoading(true);
      setError(null);
      try {
        const edge = await client.getEdge(edgeId) as unknown as { id: number; src: number; dst: number; type?: string; props?: Record<string, unknown> };
        if (!cancelled) {
          setSrcId(edge.src);
          setDstId(edge.dst);
          setEdgeType(edge.type);
          setProps(edge.props ?? {});
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load edge");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void fetchEdge();
    return () => {
      cancelled = true;
    };
  }, [edgeId]);

  return (
    <div className="text-white h-full flex flex-col">
      <div className="flex items-center justify-between px-3 py-2 border-b border-white/20 bg-black/40">
        <div className="font-semibold">Edge #{edgeId}</div>
        <button
          type="button"
          className="text-white/80 hover:text-white px-2 py-1 rounded hover:bg-white/10"
          onClick={onClose}
        >
          Close
        </button>
      </div>
      <div className="p-3 flex-1 overflow-auto space-y-3">
        {error && <div className="text-red-400 text-sm">{error}</div>}
        {loading && <div className="text-white/70 text-sm">Loading…</div>}
        {!loading && (
          <div className="space-y-2">
            <div>
              <div className="text-white/60 text-xs uppercase">Type</div>
              <div className="text-sm">{edgeType ?? "-"}</div>
            </div>
            <div>
              <div className="text-white/60 text-xs uppercase">Source</div>
              <div className="text-sm">{srcId ?? "-"}</div>
            </div>
            <div>
              <div className="text-white/60 text-xs uppercase">Target</div>
              <div className="text-sm">{dstId ?? "-"}</div>
            </div>
            <div>
              <div className="text-white/60 text-xs uppercase">Props</div>
              <pre className="text-xs whitespace-pre-wrap break-words bg-black/30 rounded p-2 border border-white/10">
                {props ? JSON.stringify(props, null, 2) : "-"}
              </pre>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};


