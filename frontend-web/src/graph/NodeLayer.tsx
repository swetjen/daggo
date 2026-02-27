import type { PointerEvent as ReactPointerEvent, ReactNode } from "react";
import { worldToScreen } from "./GraphViewport";
import type { DagNodeModel, GraphViewport } from "./types";

type NodeLayerProps = {
  nodes: DagNodeModel[];
  viewport: GraphViewport;
  getNodeClassName: (node: DagNodeModel) => string;
  renderNodeContent: (node: DagNodeModel) => ReactNode;
  onNodeClick?: (node: DagNodeModel) => void;
  onNodePointerDown?: (event: ReactPointerEvent<HTMLButtonElement>, node: DagNodeModel) => void;
  onNodeMouseEnter?: (node: DagNodeModel) => void;
  onNodeMouseLeave?: (node: DagNodeModel) => void;
};

export function NodeLayer(props: NodeLayerProps) {
  const {
    nodes,
    viewport,
    getNodeClassName,
    renderNodeContent,
    onNodeClick,
    onNodePointerDown,
    onNodeMouseEnter,
    onNodeMouseLeave,
  } = props;

  return (
    <div className="dag-node-layer">
      {nodes.map((node) => {
        const screen = worldToScreen(viewport, { x: node.x, y: node.y });

        return (
          <div
            key={node.id}
            className="dag-screen-node"
            style={{
              left: `${screen.x}px`,
              top: `${screen.y}px`,
              width: `${node.width}px`,
              height: `${node.height}px`,
              transform: `scale(${viewport.scale})`,
              transformOrigin: "top left",
            }}
          >
            <button
              className={getNodeClassName(node)}
              style={{ left: 0, top: 0, width: "100%", height: "100%" }}
              onClick={() => onNodeClick?.(node)}
              onPointerDown={(event) => onNodePointerDown?.(event, node)}
              onMouseEnter={() => onNodeMouseEnter?.(node)}
              onMouseLeave={() => onNodeMouseLeave?.(node)}
            >
              {renderNodeContent(node)}
            </button>
          </div>
        );
      })}
    </div>
  );
}
