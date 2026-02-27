import type { DagEdgeModel, DagLayoutResult, DagNodeModel, DagNodeSizePreset, WorldPoint } from "./types";

export const DAG_NODE_HARD_CAP = 500;
export const WORLD_GRID_UNIT = 16;

const LAYOUT_COLUMN_GAP = 18 * WORLD_GRID_UNIT;
const LAYOUT_ROW_GAP = 7 * WORLD_GRID_UNIT;
const LAYOUT_PADDING_X = 3 * WORLD_GRID_UNIT;
const LAYOUT_PADDING_Y = 2 * WORLD_GRID_UNIT;
const DEFAULT_CANVAS_WIDTH = 900;
const DEFAULT_CANVAS_HEIGHT = 260;
const SINGLE_ROW_LANE_ANCHORS = [-2, -1, 1, 2];

type DagLayoutInputNode = {
  step_key: string;
  display_name: string;
  description: string;
  depends_on?: string[];
};

type DagLayoutInputJob = {
  nodes: DagLayoutInputNode[];
} | null;

const PRESET_DIMENSIONS: Record<DagNodeSizePreset, { width: number; height: number }> = {
  compact: { width: 192, height: 80 },
  regular: { width: 208, height: 80 },
  wide: { width: 240, height: 80 },
};

export function buildDeterministicDagLayout(
  job: DagLayoutInputJob,
  statuses: Record<string, string>,
  options: { normalizeStatus?: (status: string) => string } = {},
): DagLayoutResult {
  if (!job || job.nodes.length === 0) {
    return {
      width: DEFAULT_CANVAS_WIDTH,
      height: DEFAULT_CANVAS_HEIGHT,
      nodes: [],
      edges: [],
      dependenciesByKey: {},
      dependentsByKey: {},
    };
  }

  const normalizeStatus = options.normalizeStatus ?? ((status: string) => status);
  const nodeByKey = new Map(job.nodes.map((node) => [node.step_key, node]));

  const dependenciesByKey: Record<string, string[]> = {};
  const dependentsByKey: Record<string, string[]> = {};
  for (const node of job.nodes) {
    dependenciesByKey[node.step_key] = [];
    dependentsByKey[node.step_key] = [];
  }

  for (const node of job.nodes) {
    const dependencies = (node.depends_on ?? []).filter((dep) => nodeByKey.has(dep)).sort((left, right) => left.localeCompare(right));
    dependenciesByKey[node.step_key] = dependencies;
    for (const dep of dependencies) {
      const dependents = dependentsByKey[dep] ?? [];
      dependents.push(node.step_key);
      dependentsByKey[dep] = dependents;
    }
  }

  for (const stepKey of Object.keys(dependentsByKey)) {
    dependentsByKey[stepKey] = (dependentsByKey[stepKey] ?? []).sort((left, right) => left.localeCompare(right));
  }

  const depthByKey: Record<string, number> = {};
  const topoOrder = topologicalOrder(job.nodes.map((node) => node.step_key), dependenciesByKey, dependentsByKey, depthByKey);
  const maxDepth = topoOrder.reduce((max, key) => Math.max(max, depthByKey[key] ?? 0), 0);
  const initialLayers = buildLayersByDepth(topoOrder, depthByKey);
  const orderedLayers = reorderLayers(initialLayers, maxDepth, dependenciesByKey, dependentsByKey);

  const maxRowsInLayer = Math.max(...Array.from(orderedLayers.values(), (layer) => layer.length), 1);
  const maxNodeWidth = Math.max(...Object.values(PRESET_DIMENSIONS).map((preset) => preset.width));
  const maxNodeHeight = Math.max(...Object.values(PRESET_DIMENSIONS).map((preset) => preset.height));
  const width = LAYOUT_PADDING_X * 2 + maxDepth * LAYOUT_COLUMN_GAP + maxNodeWidth;
  const useSingleRowDecompression = maxRowsInLayer === 1 && hasLongSpanEdges(topoOrder, dependenciesByKey, depthByKey);
  const laneByKey = useSingleRowDecompression
    ? buildSingleRowLanes(topoOrder, dependenciesByKey, dependentsByKey, depthByKey)
    : null;

  let minLane = 0;
  let maxLane = maxRowsInLayer - 1;
  if (laneByKey) {
    minLane = Infinity;
    maxLane = -Infinity;
    for (const stepKey of topoOrder) {
      const lane = laneByKey[stepKey] ?? 0;
      minLane = Math.min(minLane, lane);
      maxLane = Math.max(maxLane, lane);
    }
    if (!Number.isFinite(minLane) || !Number.isFinite(maxLane)) {
      minLane = 0;
      maxLane = 0;
    }
  }
  const height = LAYOUT_PADDING_Y * 2 + (maxLane - minLane) * LAYOUT_ROW_GAP + maxNodeHeight;

  const positionedByKey: Record<string, DagNodeModel> = {};
  for (let depth = 0; depth <= maxDepth; depth += 1) {
    const layer = orderedLayers.get(depth) ?? [];
    const layerHeight = laneByKey ? maxNodeHeight : maxNodeHeight + Math.max(0, layer.length - 1) * LAYOUT_ROW_GAP;
    const yStart = laneByKey ? LAYOUT_PADDING_Y : LAYOUT_PADDING_Y + Math.max(0, (height - LAYOUT_PADDING_Y * 2 - layerHeight) / 2);

    for (let row = 0; row < layer.length; row += 1) {
      const stepKey = layer[row];
      if (!stepKey) {
        continue;
      }
      const node = nodeByKey.get(stepKey);
      if (!node) {
        continue;
      }
      const preset = chooseNodePreset(node);
      const dimensions = PRESET_DIMENSIONS[preset];
      positionedByKey[stepKey] = {
        id: stepKey,
        step_key: node.step_key,
        display_name: node.display_name,
        description: node.description,
        status: normalizeStatus(statuses[node.step_key] ?? "pending"),
        depends_on: dependenciesByKey[node.step_key] ?? [],
        x: LAYOUT_PADDING_X + depth * LAYOUT_COLUMN_GAP,
        y: laneByKey ? yStart + ((laneByKey[stepKey] ?? 0) - minLane) * LAYOUT_ROW_GAP : yStart + row * LAYOUT_ROW_GAP,
        width: dimensions.width,
        height: dimensions.height,
        preset,
      };
    }
  }

  const edges: DagEdgeModel[] = [];
  for (const to of topoOrder) {
    for (const from of dependenciesByKey[to] ?? []) {
      const source = positionedByKey[from];
      const target = positionedByKey[to];
      if (!source || !target) {
        continue;
      }
      edges.push({
        from,
        to,
        backEdge: source.x >= target.x,
      });
    }
  }

  const nodes = topoOrder
    .map((stepKey) => positionedByKey[stepKey])
    .filter((node): node is DagNodeModel => Boolean(node));

  return {
    width,
    height,
    nodes,
    edges,
    dependenciesByKey,
    dependentsByKey,
  };
}

function chooseNodePreset(node: DagLayoutInputNode): DagNodeSizePreset {
  const labelLength = Math.max(node.step_key.length, node.display_name?.length ?? 0);
  if (labelLength <= 16) {
    return "compact";
  }
  if (labelLength >= 30) {
    return "wide";
  }
  return "regular";
}

function topologicalOrder(
  stepKeys: string[],
  dependenciesByKey: Record<string, string[]>,
  dependentsByKey: Record<string, string[]>,
  depthByKey: Record<string, number>,
): string[] {
  const inDegree: Record<string, number> = {};
  const queue: string[] = [];
  const sortedStepKeys = [...stepKeys].sort((left, right) => left.localeCompare(right));

  for (const stepKey of sortedStepKeys) {
    inDegree[stepKey] = (dependenciesByKey[stepKey] ?? []).length;
    depthByKey[stepKey] = 0;
    if (inDegree[stepKey] === 0) {
      queue.push(stepKey);
    }
  }
  queue.sort((left, right) => left.localeCompare(right));

  const ordered: string[] = [];
  while (queue.length > 0) {
    const stepKey = queue.shift();
    if (!stepKey) {
      break;
    }
    ordered.push(stepKey);
    for (const dependent of dependentsByKey[stepKey] ?? []) {
      depthByKey[dependent] = Math.max(depthByKey[dependent] ?? 0, (depthByKey[stepKey] ?? 0) + 1);
      inDegree[dependent] = (inDegree[dependent] ?? 0) - 1;
      if (inDegree[dependent] === 0) {
        queue.push(dependent);
      }
    }
    queue.sort((left, right) => left.localeCompare(right));
  }

  if (ordered.length === sortedStepKeys.length) {
    return ordered;
  }

  const seen = new Set(ordered);
  const remainder = sortedStepKeys.filter((key) => !seen.has(key));
  for (const key of remainder) {
    ordered.push(key);
  }
  return ordered;
}

function buildLayersByDepth(topoOrder: string[], depthByKey: Record<string, number>): Map<number, string[]> {
  const layers = new Map<number, string[]>();
  for (const stepKey of topoOrder) {
    const depth = depthByKey[stepKey] ?? 0;
    const layer = layers.get(depth) ?? [];
    layer.push(stepKey);
    layers.set(depth, layer);
  }
  for (const [depth, layer] of layers.entries()) {
    layers.set(depth, [...layer].sort((left, right) => left.localeCompare(right)));
  }
  return layers;
}

function reorderLayers(
  layers: Map<number, string[]>,
  maxDepth: number,
  dependenciesByKey: Record<string, string[]>,
  dependentsByKey: Record<string, string[]>,
): Map<number, string[]> {
  const ordered = new Map<number, string[]>();
  for (let depth = 0; depth <= maxDepth; depth += 1) {
    ordered.set(depth, [...(layers.get(depth) ?? [])].sort((left, right) => left.localeCompare(right)));
  }

  for (let pass = 0; pass < 2; pass += 1) {
    for (let depth = 1; depth <= maxDepth; depth += 1) {
      const priorLayer = ordered.get(depth - 1) ?? [];
      const priorPositions = buildPositionMap(priorLayer);
      const nextLayer = ordered.get(depth) ?? [];
      ordered.set(depth, sortLayerByBarycenter(nextLayer, dependenciesByKey, priorPositions));
    }

    for (let depth = maxDepth - 1; depth >= 0; depth -= 1) {
      const nextLayer = ordered.get(depth + 1) ?? [];
      const nextPositions = buildPositionMap(nextLayer);
      const layer = ordered.get(depth) ?? [];
      ordered.set(depth, sortLayerByBarycenter(layer, dependentsByKey, nextPositions));
    }
  }

  return ordered;
}

function buildPositionMap(layer: string[]): Record<string, number> {
  const positions: Record<string, number> = {};
  for (let index = 0; index < layer.length; index += 1) {
    const stepKey = layer[index];
    if (!stepKey) {
      continue;
    }
    positions[stepKey] = index;
  }
  return positions;
}

function sortLayerByBarycenter(
  layer: string[],
  adjacencyByKey: Record<string, string[]>,
  adjacentPositions: Record<string, number>,
): string[] {
  return [...layer].sort((left, right) => {
    const leftScore = barycenter(adjacencyByKey[left] ?? [], adjacentPositions);
    const rightScore = barycenter(adjacencyByKey[right] ?? [], adjacentPositions);

    if (leftScore === null && rightScore === null) {
      return left.localeCompare(right);
    }
    if (leftScore === null) {
      return 1;
    }
    if (rightScore === null) {
      return -1;
    }
    if (Math.abs(leftScore - rightScore) > 0.0001) {
      return leftScore - rightScore;
    }
    return left.localeCompare(right);
  });
}

function barycenter(neighbors: string[], positions: Record<string, number>): number | null {
  if (neighbors.length === 0) {
    return null;
  }
  let total = 0;
  let count = 0;
  for (const neighbor of neighbors) {
    const position = positions[neighbor];
    if (position === undefined) {
      continue;
    }
    total += position;
    count += 1;
  }
  if (count === 0) {
    return null;
  }
  return total / count;
}

function hasLongSpanEdges(
  topoOrder: string[],
  dependenciesByKey: Record<string, string[]>,
  depthByKey: Record<string, number>,
): boolean {
  for (const stepKey of topoOrder) {
    const targetDepth = depthByKey[stepKey] ?? 0;
    for (const dependency of dependenciesByKey[stepKey] ?? []) {
      const sourceDepth = depthByKey[dependency] ?? 0;
      if (targetDepth - sourceDepth >= 2) {
        return true;
      }
    }
  }
  return false;
}

function buildSingleRowLanes(
  topoOrder: string[],
  dependenciesByKey: Record<string, string[]>,
  dependentsByKey: Record<string, string[]>,
  depthByKey: Record<string, number>,
): Record<string, number> {
  const laneByKey: Record<string, number> = {};
  for (const stepKey of topoOrder) {
    laneByKey[stepKey] = 0;
  }

  const anchoredLaneByKey: Record<string, number> = {};
  const longSpanSources = topoOrder.filter((stepKey) =>
    (dependentsByKey[stepKey] ?? []).some((dependent) => (depthByKey[dependent] ?? 0) - (depthByKey[stepKey] ?? 0) >= 2),
  );
  for (let index = 0; index < longSpanSources.length; index += 1) {
    const stepKey = longSpanSources[index];
    if (!stepKey) {
      continue;
    }
    anchoredLaneByKey[stepKey] = SINGLE_ROW_LANE_ANCHORS[index % SINGLE_ROW_LANE_ANCHORS.length] ?? 0;
    laneByKey[stepKey] = anchoredLaneByKey[stepKey] ?? 0;
  }

  for (const stepKey of topoOrder) {
    if (anchoredLaneByKey[stepKey] !== undefined) {
      continue;
    }
    const isRoot = (dependenciesByKey[stepKey] ?? []).length === 0;
    const isSink = (dependentsByKey[stepKey] ?? []).length === 0;
    if (isRoot || isSink) {
      anchoredLaneByKey[stepKey] = 0;
    }
  }

  const ordered = [...topoOrder];
  const reversed = [...topoOrder].reverse();
  for (let pass = 0; pass < 4; pass += 1) {
    for (const stepKey of ordered) {
      if (anchoredLaneByKey[stepKey] !== undefined) {
        laneByKey[stepKey] = anchoredLaneByKey[stepKey] ?? 0;
        continue;
      }
      const parentAverage = averageLane(dependenciesByKey[stepKey] ?? [], laneByKey);
      if (parentAverage === null) {
        continue;
      }
      laneByKey[stepKey] = clampLane(Math.round((laneByKey[stepKey] + parentAverage) / 2));
    }

    for (const stepKey of reversed) {
      if (anchoredLaneByKey[stepKey] !== undefined) {
        laneByKey[stepKey] = anchoredLaneByKey[stepKey] ?? 0;
        continue;
      }
      const childAverage = averageLane(dependentsByKey[stepKey] ?? [], laneByKey);
      if (childAverage === null) {
        continue;
      }
      laneByKey[stepKey] = clampLane(Math.round((laneByKey[stepKey] + childAverage) / 2));
    }
  }

  return laneByKey;
}

function averageLane(stepKeys: string[], laneByKey: Record<string, number>): number | null {
  if (stepKeys.length === 0) {
    return null;
  }
  let total = 0;
  let count = 0;
  for (const stepKey of stepKeys) {
    const lane = laneByKey[stepKey];
    if (lane === undefined) {
      continue;
    }
    total += lane;
    count += 1;
  }
  if (count === 0) {
    return null;
  }
  return total / count;
}

function clampLane(lane: number): number {
  if (lane < -3) {
    return -3;
  }
  if (lane > 3) {
    return 3;
  }
  return lane;
}

export function incomingAnchor(node: Pick<DagNodeModel, "x" | "y" | "height">): WorldPoint {
  return {
    x: node.x,
    y: node.y + node.height / 2,
  };
}

export function outgoingAnchor(node: Pick<DagNodeModel, "x" | "y" | "width" | "height">): WorldPoint {
  return {
    x: node.x + node.width,
    y: node.y + node.height / 2,
  };
}

export function centeredSlotOffset(slotIndex: number, slotCount: number, spacing: number): number {
  if (slotCount <= 1 || slotIndex < 0) {
    return 0;
  }
  const center = (slotCount - 1) / 2;
  return (slotIndex - center) * spacing;
}
