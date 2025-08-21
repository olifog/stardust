import { useCallback, useEffect, useRef, useState } from "react";
import ForceGraph3D, {
  type ForceGraphMethods,
  type NodeObject,
} from "react-force-graph-3d";
import type { NodeHeader } from "stardust";
import { StardustClient } from "stardust";
import * as THREE from "three";
import SpriteText from "three-spritetext";
import { EdgeDetails } from "./edge-details";
import { NodeDetails } from "./node-details";
import { NodeSearch } from "./node-search";


export const client = new StardustClient({
  baseUrl: "http://localhost:8080",
});

export const GraphViewer = () => {
  const fgRef = useRef<ForceGraphMethods | undefined>(undefined);
  const containerRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeObject | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<{
    id: number;
    src?: number;
    dst?: number;
    type?: string;
  } | null>(null);

  const nodesRef = useRef<Record<string, NodeHeader>>({});
  interface VizEdge {
    id: number;
    src: number;
    dst: number;
    type?: string;
  }
  const edgesRef = useRef<Record<string, VizEdge>>({});

  // Auto-expand state
  const [isAutoExpanding, setIsAutoExpanding] = useState(false);
  const autoExpandTimerRef = useRef<number | null>(null);
  const nextAutoExpandIdRef = useRef<number>(1);

  const [graphData, setGraphData] = useState<{ nodes: Array<{ id: string; name: string; labels: string; val: number }>; links: Array<{ source: string; target: string; type?: string; id: number }> }>({ nodes: [], links: [] });
  const prevNodeCountRef = useRef<number>(0);
  const prevLinkCountRef = useRef<number>(0);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      const nodesArray = Object.values(nodesRef.current).map((node) => {
        const isMovie = node.labels?.some((l) => l.toLowerCase() === "movie");
        const maybeTitle = isMovie ? node.hotProps?.title : undefined;
        const maybeName = node.hotProps?.name;
        const displayNameCandidate =
          typeof maybeTitle === "string" && maybeTitle.trim().length > 0
            ? maybeTitle
            : typeof maybeName === "string" && maybeName.trim().length > 0
              ? maybeName
              : undefined;
        const name = displayNameCandidate ?? node.id.toString();
        return {
          id: node.id.toString(),
          name,
          labels: node.labels.join(", "),
          val: 1,
        };
      });
      const linksArray = Object.values(edgesRef.current).map((edge) => ({
        source: edge.src.toString(),
        target: edge.dst.toString(),
        type: edge.type,
        id: edge.id,
      }));

      const nextNodeCount = nodesArray.length;
      const nextLinkCount = linksArray.length;
      if (
        nextNodeCount !== prevNodeCountRef.current ||
        nextLinkCount !== prevLinkCountRef.current
      ) {
        prevNodeCountRef.current = nextNodeCount;
        prevLinkCountRef.current = nextLinkCount;
        setGraphData({ nodes: nodesArray, links: linksArray });
      }
    }, 1000);
    return () => window.clearInterval(intervalId);
  }, []);

  const [width, setWidth] = useState(0);
  const [height, setHeight] = useState(0);
  // Removed delayed focus logic; we will either focus immediately (no new data)
  // or not focus at all (new data fetched), requiring a second user action.
  const nodeCoordsRef = useRef<
    Record<string, { x: number; y: number; z: number; updatedAt: number }>
  >({});
  // Track node coordinates as they are computed by the graph

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
    async (
      id: number,
    ): Promise<{ header: NodeHeader; didAddData: boolean } | undefined> => {
      try {
        const [{ header }, { items }] = await Promise.all([
          client.getNode(id),
          client.adjacency({ node: id, direction: "both", limit: 64 }),
        ]);

        // Determine if this call will add any new nodes/edges to state

        const neighbourIds = Array.from(
          new Set(items.map((row) => row.neighbor)),
        );
        const neighbourHeaders: NodeHeader[] = [];
        await Promise.all(
          neighbourIds.map(async (nid) => {
            try {
              const res = await client.getNode(nid);
              neighbourHeaders.push(res.header);
            } catch {
              // ignore
            }
          }),
        );

        let didAddNodes = false;
        let didAddEdges = false;

        const headerKey = header.id.toString();
        if (!nodesRef.current[headerKey]) {
          nodesRef.current[headerKey] = header;
          didAddNodes = true;
        }

        for (const nh of neighbourHeaders) {
          const key = nh.id.toString();
          if (!nodesRef.current[key]) {
            nodesRef.current[key] = nh;
            didAddNodes = true;
          }
        }

        for (const row of items) {
          const src = row.direction === "in" ? row.neighbor : id;
          const dst = row.direction === "in" ? id : row.neighbor;
          const ve: VizEdge = { id: row.edgeId, src, dst, type: row.type };
          const key = String(ve.id);
          if (!edgesRef.current[key]) {
            edgesRef.current[key] = ve;
            didAddEdges = true;
          }
        }

        return { header, didAddData: didAddNodes || didAddEdges };
      } catch (err) {
        console.error("Failed to load node/neighbours", err);
      }
    },
    [],
  );

  const centerNodeWithoutRotation = useCallback(
    (coords: { x: number; y: number; z: number }, duration = 1000) => {
      const fg = fgRef.current;
      if (!fg) return;

      const camera = fg.camera();
      type MinimalOrbitControls = { target: THREE.Vector3 };
      const controls = fg.controls() as unknown as MinimalOrbitControls;
      if (!camera || !controls) return;

      const oldTarget = controls.target.clone();
      const delta = new THREE.Vector3(coords.x, coords.y, coords.z).sub(
        oldTarget,
      );
      const newPos = camera.position.clone().add(delta);

      fg.cameraPosition(
        { x: newPos.x, y: newPos.y, z: newPos.z },
        { x: coords.x, y: coords.y, z: coords.z },
        duration,
      );
    },
    [],
  );

  const handleNodeClick = useCallback(
    async (input: NodeObject | number) => {
      const id = typeof input === "number" ? input : Number(input.id);
      const res = await addNodeAndNeighbours(id);
      const didAddData = Boolean(res?.didAddData);
      setSelectedNode({ id: id.toString() } as NodeObject);
      // Case A (new data was added): do not focus; require user to click again
      if (didAddData) return;

      // Case B (no new data): focus immediately
      if (
        typeof input !== "number" &&
        input.x != null &&
        input.y != null &&
        input.z != null
      ) {
        const coords = { x: input.x, y: input.y, z: input.z } as {
          x: number;
          y: number;
          z: number;
        };
        centerNodeWithoutRotation(coords, 1000);
        return;
      }

      // If invoked via search (id only), try to use latest known coordinates
      const coords = nodeCoordsRef.current[id.toString()];
      if (coords) {
        centerNodeWithoutRotation(coords, 1000);
      }
    },
    [addNodeAndNeighbours, centerNodeWithoutRotation],
  );

  const focusNodeById = useCallback(
    (id: number) => {
      const coords = nodeCoordsRef.current[id.toString()];
      if (coords) {
        centerNodeWithoutRotation(coords, 1000);
      }
    },
    [centerNodeWithoutRotation],
  );

  const handleLinkClick = useCallback((linkObj: unknown) => {
    const link = linkObj as {
      id?: number;
      source?: { id?: string | number };
      target?: { id?: string | number };
      type?: string;
    };
    const edgeId = typeof link.id === "number" ? link.id : undefined;
    const src =
      link.source && link.source.id != null
        ? Number(link.source.id)
        : undefined;
    const dst =
      link.target && link.target.id != null
        ? Number(link.target.id)
        : undefined;
    setSelectedNode(null);
    if (edgeId != null) {
      setSelectedEdge({ id: edgeId, src, dst, type: link.type });
    }
  }, []);

  const toggleAutoExpand = useCallback(() => {
    if (autoExpandTimerRef.current != null) {
      window.clearInterval(autoExpandTimerRef.current);
      autoExpandTimerRef.current = null;
      setIsAutoExpanding(false);
      return;
    }
    setIsAutoExpanding(true);
    autoExpandTimerRef.current = window.setInterval(() => {
      const id = nextAutoExpandIdRef.current;
      nextAutoExpandIdRef.current = id + 1;
      void addNodeAndNeighbours(id);
    }, 10);
  }, [addNodeAndNeighbours]);

  useEffect(() => {
    return () => {
      if (autoExpandTimerRef.current != null) {
        window.clearInterval(autoExpandTimerRef.current);
      }
      autoExpandTimerRef.current = null;
    };
  }, []);

  // No delayed focusing effect anymore

  return (
    <div ref={containerRef} className="relative w-full h-full">
      <NodeSearch onSearch={(id) => void handleNodeClick(id)} />
      <div className="absolute top-2 left-2 z-10 flex items-center gap-2">
        <button
          type="button"
          className="px-3 py-1 rounded bg-black/60 text-white border border-white/30 hover:bg-black/70"
          onClick={toggleAutoExpand}
        >
          {isAutoExpanding ? "Stop autoexpand" : "Start autoexpand"}
        </button>
      </div>
      <ForceGraph3D
        width={width}
        height={height}
        ref={fgRef}
        graphData={graphData}
        nodeLabel="name"
        nodeAutoColorBy="labels"
        nodeThreeObjectExtend
        nodeThreeObject={(node: NodeObject) => {
          const anyNode = node as unknown as {
            id?: string | number;
            name?: string;
            color?: string;
            labels?: string[];
          };
          const text = anyNode.name ?? String(anyNode.id ?? "");
          const sprite = new SpriteText(text);
          if (anyNode.color) sprite.color = anyNode.color;
          sprite.textHeight = 2;
          sprite.center.y = -2.5;
          return sprite as unknown as THREE.Object3D;
        }}
        linkThreeObjectExtend
        linkThreeObject={(link: unknown) => {
          const anyLink = link as { type?: string };
          const label = anyLink.type ?? "";
          const sprite = new SpriteText(label);
          sprite.color = "lightgrey";
          sprite.textHeight = 1.5;
          return sprite as unknown as THREE.Object3D;
        }}
        linkPositionUpdate={(
          sprite: unknown,
          pos: {
            start: { x: number; y: number; z: number };
            end: { x: number; y: number; z: number };
          },
        ) => {
          const s = sprite as { position: { x: number; y: number; z: number } };
          const { start, end } = pos;
          s.position.x = start.x + (end.x - start.x) / 2;
          s.position.y = start.y + (end.y - start.y) / 2;
          s.position.z = start.z + (end.z - start.z) / 2;
        }}
        onNodeClick={(node) => {
          setSelectedEdge(null);
          void handleNodeClick(node);
        }}
        onLinkClick={(link) => handleLinkClick(link)}
        nodePositionUpdate={(_, coords, node) => {
          if (node.id != null) {
            nodeCoordsRef.current[String(node.id)] = {
              x: coords.x,
              y: coords.y,
              z: coords.z,
              updatedAt: Date.now(),
            };
          }
        }}
      />

      <div className="absolute bottom-2 right-2 z-10 px-3 py-1 rounded bg-black/60 text-white border border-white/30 text-sm">
        {graphData.nodes.length} nodes, {graphData.links.length} edges
      </div>

      {selectedNode && !selectedEdge && (
        <div className="absolute top-0 right-0 w-80 h-160 bg-black/50 border-b border-l border-white/20">
          <NodeDetails
            node={selectedNode}
            onRefocus={(id) => focusNodeById(id)}
            onClose={() => setSelectedNode(null)}
          />
        </div>
      )}
      {selectedEdge && !selectedNode && (
        <div className="absolute top-0 right-0 w-80 h-160 bg-black/50 border-b border-l border-white/20">
          <EdgeDetails
            edgeId={selectedEdge.id}
            initialSrcId={selectedEdge.src}
            initialDstId={selectedEdge.dst}
            initialType={selectedEdge.type}
            onClose={() => setSelectedEdge(null)}
          />
        </div>
      )}
    </div>
  );
};
