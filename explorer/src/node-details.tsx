import type { NodeObject } from "react-force-graph-3d";
import { useEffect, useState } from "react";
import { getNode, getNodeProps, getVectors, type NodeResponse, type NodePropsResponse, type GetVectorsResponse } from "./stardust";

export const NodeDetails = ({
  node,
  onLoadNeighbours,
  onClose,
}: {
  node: NodeObject;
  onLoadNeighbours: (id: number) => void;
  onClose: () => void;
}) => {
  const nodeId = Number(node.id);
  const [nodeData, setNodeData] = useState<NodeResponse | null>(null);
  const [propsData, setPropsData] = useState<NodePropsResponse | null>(null);
  const [vectorsData, setVectorsData] = useState<GetVectorsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAll = async () => {
      setLoading(true);
      setError(null);
      try {
        const [n, p, v] = await Promise.all([
          getNode(nodeId),
          getNodeProps(nodeId),
          getVectors(nodeId),
        ]);
        setNodeData(n);
        setPropsData(p);
        setVectorsData(v);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load node details");
      } finally {
        setLoading(false);
      }
    };
    void fetchAll();
  }, [nodeId]);

	return (
			<div className="text-white h-full flex flex-col">
        <div className="flex items-center justify-between px-3 py-2 border-b border-white/20 bg-black/40">
          <div className="font-semibold">Node #{nodeId}</div>
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
          {!loading && nodeData && (
            <div className="space-y-2">
              <div>
                <div className="text-white/60 text-xs uppercase">Labels</div>
                <div className="text-sm">{nodeData.header.labels.join(", ") || "-"}</div>
              </div>
              <div>
                <div className="text-white/60 text-xs uppercase">Hot Props</div>
                <pre className="text-xs whitespace-pre-wrap break-words bg-black/30 rounded p-2 border border-white/10">
                  {JSON.stringify(nodeData.header.hotProps, null, 2)}
                </pre>
              </div>
              {propsData && (
                <div>
                  <div className="text-white/60 text-xs uppercase">Props</div>
                  <pre className="text-xs whitespace-pre-wrap break-words bg-black/30 rounded p-2 border border-white/10">
                    {JSON.stringify(propsData.props, null, 2)}
                  </pre>
                </div>
              )}
              {vectorsData && (
                <div>
                  <div className="text-white/60 text-xs uppercase">Vectors</div>
                  <div className="space-y-1">
                    {vectorsData.vectors.length === 0 && (
                      <div className="text-sm">No vectors</div>
                    )}
                    {vectorsData.vectors.map((vec) => (
                      <div key={vec.tag} className="text-xs flex items-center justify-between bg-black/30 rounded p-2 border border-white/10">
                        <div>
                          <span className="text-white/80">{vec.tag}</span>
                          <span className="text-white/50"> · dim {vec.dim}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
        <div className="px-3 py-2 border-t border-white/20 bg-black/40 flex gap-2">
          <button
            type="button"
            className="px-3 py-1 rounded bg-white/20 hover:bg-white/30 text-white"
            onClick={() => onLoadNeighbours(nodeId)}
          >
            Load neighbours
          </button>
        </div>
      </div>
	);
};
