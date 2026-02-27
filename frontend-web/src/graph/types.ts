export type WorldPoint = {
  x: number;
  y: number;
};

export type WorldRect = {
  x: number;
  y: number;
  width: number;
  height: number;
};

export type GraphViewport = {
  tx: number;
  ty: number;
  scale: number;
};

export type DagNodeSizePreset = "compact" | "regular" | "wide";

export type DagNodeModel = {
  id: string;
  step_key: string;
  display_name: string;
  description: string;
  status: string;
  depends_on: string[];
  x: number;
  y: number;
  width: number;
  height: number;
  preset: DagNodeSizePreset;
};

export type DagEdgeModel = {
  from: string;
  to: string;
  backEdge: boolean;
};

export type DagLayoutResult = {
  width: number;
  height: number;
  nodes: DagNodeModel[];
  edges: DagEdgeModel[];
  dependenciesByKey: Record<string, string[]>;
  dependentsByKey: Record<string, string[]>;
};
