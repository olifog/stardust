import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph3D, {
  type ForceGraphMethods,
  type NodeObject,
} from "react-force-graph-3d";
import { NodeDetails } from "./node-details";
import { NodeSearch } from "./node-search";
import type { Edge, NodeHeader } from "./stardust";
import { getNodeWithNeighbours } from "./stardust";

// interface Node {
// 	id: string;
// 	labels: string[];
// 	hotProps: {
// 		[key: string]: string;
// 	};
// };

// interface Link {
// 	source: string;
// 	target: string;
// }

export const GraphViewer = () => {
  const fgRef = useRef<ForceGraphMethods | undefined>(undefined);
  const containerRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeObject | null>(null);

  const [nodes, setNodes] = useState<Record<string, NodeHeader>>({});
  const [edges, setEdges] = useState<Record<string, Edge>>({});

  const graphData = useMemo(() => {
    return {
      nodes: Object.values(nodes).map((node) => ({
        id: node.id.toString(),
        name: node.id.toString(),
        val: 1,
      })),
      links: Object.values(edges).map((edge) => ({
        source: edge.src.toString(),
        target: edge.dst.toString(),
      })),
    };
  }, [nodes, edges]);

  const [width, setWidth] = useState(0);
  const [height, setHeight] = useState(0);
  const [pendingFocusId, setPendingFocusId] = useState<number | null>(null);
  const nodeCoordsRef = useRef<
    Record<string, { x: number; y: number; z: number }>
  >({});

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;

    const applySize = () => {
      const nextWidth = element.clientWidth;
      const nextHeight = element.clientHeight;
      setWidth((prev) => (prev !== nextWidth ? nextWidth : prev));
      setHeight((prev) => (prev !== nextHeight ? nextHeight : prev));
    };
    applySize();

    if (typeof ResizeObserver !== "undefined") {
      const resizeObserver = new ResizeObserver(() => {
        applySize();
      });
      resizeObserver.observe(element);

      return () => resizeObserver.disconnect();
    }

    const onWindowResize = () => applySize();
    window.addEventListener("resize", onWindowResize);
    return () => window.removeEventListener("resize", onWindowResize);
  }, []);

  const addNodeAndNeighbours = useCallback(
    async (id: number): Promise<NodeHeader | undefined> => {
      try {
        const {
          node,
          neighbourHeaders,
          edges: neighbourEdges,
        } = await getNodeWithNeighbours(id, { direction: "both", limit: 64 });

        setNodes((prev) => {
          const next: Record<string, NodeHeader> = { ...prev };
          next[node.id.toString()] = node;
          for (const nh of neighbourHeaders) {
            next[nh.id.toString()] = nh;
          }
          return next;
        });

        setEdges((prev) => {
          const next: Record<string, Edge> = { ...prev };
          for (const e of neighbourEdges) {
            const key = `${e.src}-${e.dst}`;
            next[key] = e;
          }
          return next;
        });

        return node;
      } catch (err) {
        console.error("Failed to load node/neighbours", err);
      }
    },
    []
  );

  const handleNodeClick = useCallback(
    async (input: NodeObject | number) => {
      const id = typeof input === "number" ? input : Number(input.id);
      await addNodeAndNeighbours(id);
      setSelectedNode({ id: id.toString() } as NodeObject);
      if (
        typeof input !== "number" &&
        input.x != null &&
        input.y != null &&
        input.z != null
      ) {
        const x = input.x;
        const y = input.y;
        const z = input.z;
        const distance = 200;
        const distRatio = 1 + distance / Math.hypot(x, y, z);
        fgRef.current?.cameraPosition(
          { x: x * distRatio, y: y * distRatio, z: z * distRatio },
          { x, y, z },
          1000
        );
      } else {
        setPendingFocusId(id);
      }
    },
    [addNodeAndNeighbours]
  );

  useEffect(() => {
    if (pendingFocusId == null) return;
    let cancelled = false;
    let tries = 0;
    const focusId = pendingFocusId;
    const tryFocus = () => {
      if (cancelled) return;
      const coords = nodeCoordsRef.current[focusId.toString()];
      if (coords) {
        const { x, y, z } = coords;
        const distance = 200;
        const distRatio = 1 + distance / Math.hypot(x, y, z);
        fgRef.current?.cameraPosition(
          { x: x * distRatio, y: y * distRatio, z: z * distRatio },
          { x, y, z },
          1000
        );
        setPendingFocusId(null);
        return;
      }
      if (tries < 50) {
        tries += 1;
        setTimeout(tryFocus, 50);
      } else {
        setPendingFocusId(null);
      }
    };
    setTimeout(tryFocus, 100);
    return () => {
      cancelled = true;
    };
  }, [pendingFocusId]);

  return (
    <div ref={containerRef} className="relative w-full h-full">
      <NodeSearch onSearch={(id) => void handleNodeClick(id)} />
      <ForceGraph3D
        width={width}
        height={height}
        ref={fgRef}
        graphData={graphData}
        nodeLabel="id"
        nodeAutoColorBy="group"
        onNodeClick={(node) => void handleNodeClick(node)}
        nodePositionUpdate={(_, coords, node) => {
          if (node.id != null) {
            nodeCoordsRef.current[String(node.id)] = coords;
          }
        }}
      />

      {selectedNode && (
        <div className="absolute top-0 right-0 w-80 h-160 bg-black/50 border-b border-l border-white/20">
          <NodeDetails
            node={selectedNode}
            onLoadNeighbours={(id) => void addNodeAndNeighbours(id)}
            onClose={() => setSelectedNode(null)}
          />
        </div>
      )}
    </div>
  );
};
