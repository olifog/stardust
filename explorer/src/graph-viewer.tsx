import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph3D, {
  type ForceGraphMethods,
  type NodeObject,
} from "react-force-graph-3d";
import type { NodeHeader } from "stardust";
import { StardustClient } from "stardust";
import * as THREE from "three";
import SpriteText from "three-spritetext";
import { NodeDetails } from "./node-details";
import { NodeSearch } from "./node-search";

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

export const client = new StardustClient({
  baseUrl: "http://localhost:8080",
});

export const GraphViewer = () => {
  const fgRef = useRef<ForceGraphMethods | undefined>(undefined);
  const containerRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeObject | null>(null);

  const [nodes, setNodes] = useState<Record<string, NodeHeader>>({});
  interface VizEdge {
    id: number;
    src: number;
    dst: number;
    type?: string;
  }
  const [edges, setEdges] = useState<Record<string, VizEdge>>({});

  const graphData = useMemo(() => {
    return {
      nodes: Object.values(nodes).map((node) => {
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
      }),
      links: Object.values(edges).map((edge) => ({
        source: edge.src.toString(),
        target: edge.dst.toString(),
        type: edge.type,
        id: edge.id,
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
        const [{ header }, { items }] = await Promise.all([
          client.getNode(id),
          client.adjacency({ node: id, direction: "both", limit: 64 }),
        ]);

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

        setNodes((prev) => {
          let next: Record<string, NodeHeader> = prev;
          let changed = false;

          const headerKey = header.id.toString();
          if (!prev[headerKey]) {
            if (!changed) next = { ...prev };
            next[headerKey] = header;
            changed = true;
          }

          for (const nh of neighbourHeaders) {
            const key = nh.id.toString();
            if (!prev[key]) {
              if (!changed) next = { ...prev };
              next[key] = nh;
              changed = true;
            }
          }

          return changed ? next : prev;
        });

        setEdges((prev) => {
          let next: Record<string, VizEdge> = prev;
          let changed = false;
          for (const row of items) {
            const src = row.direction === "in" ? row.neighbor : id;
            const dst = row.direction === "in" ? id : row.neighbor;
            const ve: VizEdge = { id: row.edgeId, src, dst, type: row.type };
            const key = String(ve.id);
            if (!prev[key]) {
              if (!changed) next = { ...prev };
              next[key] = ve;
              changed = true;
            }
          }
          return changed ? next : prev;
        });

        return header;
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
      await addNodeAndNeighbours(id);
      setSelectedNode({ id: id.toString() } as NodeObject);
      if (
        typeof input !== "number" &&
        input.x != null &&
        input.y != null &&
        input.z != null
      ) {
        centerNodeWithoutRotation({ x: input.x, y: input.y, z: input.z }, 3000);
      } else {
        setPendingFocusId(id);
      }
    },
    [addNodeAndNeighbours, centerNodeWithoutRotation],
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
        centerNodeWithoutRotation(coords, 1000);
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
  }, [pendingFocusId, centerNodeWithoutRotation]);

  return (
    <div ref={containerRef} className="relative w-full h-full">
      <NodeSearch onSearch={(id) => void handleNodeClick(id)} />
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
