import { useCallback, useRef } from "react";
import ForceGraph3D, { type ForceGraphMethods, type NodeObject, type LinkObject } from "react-force-graph-3d";

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
	const fgRef = useRef<ForceGraphMethods<NodeObject, LinkObject> | null>(null);

	const handleClick = useCallback((node: NodeObject) => {
		// Aim at node from outside it
		if (!node.x || !node.y || !node.z) {
			return;
		}
		const distance = 40;
		const distRatio = 1 + distance/Math.hypot(node.x, node.y, node.z);

		fgRef.current?.cameraPosition(
			{ x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio }, // new position
			{ x: node.x, y: node.y, z: node.z }, // lookAt ({ x, y, z })
			3000  // ms transition duration
		);
	}, [fgRef]);

	return <ForceGraph3D
		ref={fgRef}
		graphData={{
			nodes: [{
				id: "1",
				name: "Node 1",
				val: 1,
			}, {
				id: "2",
				name: "Node 2",
				val: 2,
			}],
			links: [{
				source: "1",
				target: "2",
			}],
		}}
		nodeLabel="id"
		nodeAutoColorBy="group"
		onNodeClick={handleClick}
	/>;
};
