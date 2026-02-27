import { describe, expect, test } from "bun:test";
import { buildDeterministicDagLayout, incomingAnchor, outgoingAnchor } from "./dagLayout";

const statuses = {
  extract: "success",
  normalize: "running",
  enrich: "pending",
  report: "pending",
};

describe("buildDeterministicDagLayout", () => {
  test("returns deterministic positions regardless of input node order", () => {
    const left = buildDeterministicDagLayout(
      {
        nodes: [
          { step_key: "extract", display_name: "Extract", description: "", depends_on: [] },
          { step_key: "normalize", display_name: "Normalize", description: "", depends_on: ["extract"] },
          { step_key: "enrich", display_name: "Enrich", description: "", depends_on: ["normalize"] },
          { step_key: "report", display_name: "Report", description: "", depends_on: ["enrich"] },
        ],
      },
      statuses,
    );

    const right = buildDeterministicDagLayout(
      {
        nodes: [
          { step_key: "report", display_name: "Report", description: "", depends_on: ["enrich"] },
          { step_key: "enrich", display_name: "Enrich", description: "", depends_on: ["normalize"] },
          { step_key: "normalize", display_name: "Normalize", description: "", depends_on: ["extract"] },
          { step_key: "extract", display_name: "Extract", description: "", depends_on: [] },
        ],
      },
      statuses,
    );

    const leftByKey = Object.fromEntries(left.nodes.map((node) => [node.step_key, { x: node.x, y: node.y }]));
    const rightByKey = Object.fromEntries(right.nodes.map((node) => [node.step_key, { x: node.x, y: node.y }]));

    expect(rightByKey).toEqual(leftByKey);
    expect(left.edges.map((edge) => `${edge.from}->${edge.to}`)).toEqual(["extract->normalize", "normalize->enrich", "enrich->report"]);
  });

  test("builds deterministic horizontal anchors from the model", () => {
    const layout = buildDeterministicDagLayout(
      {
        nodes: [{ step_key: "extract", display_name: "Extract", description: "", depends_on: [] }],
      },
      statuses,
    );

    const node = layout.nodes[0];
    expect(node).toBeDefined();
    if (!node) {
      return;
    }

    const incoming = incomingAnchor(node);
    const outgoing = outgoingAnchor(node);

    expect(incoming.x).toBe(node.x);
    expect(incoming.y).toBe(node.y + node.height / 2);
    expect(outgoing.x).toBe(node.x + node.width);
    expect(outgoing.y).toBe(node.y + node.height / 2);
  });

  test("keeps pure linear chains in a single lane", () => {
    const layout = buildDeterministicDagLayout(
      {
        nodes: [
          { step_key: "a", display_name: "A", description: "", depends_on: [] },
          { step_key: "b", display_name: "B", description: "", depends_on: ["a"] },
          { step_key: "c", display_name: "C", description: "", depends_on: ["b"] },
          { step_key: "d", display_name: "D", description: "", depends_on: ["c"] },
        ],
      },
      {},
    );

    const uniqueY = new Set(layout.nodes.map((node) => node.y));
    expect(uniqueY.size).toBe(1);
  });

  test("decompresses single-lane graphs with long-span edges", () => {
    const layout = buildDeterministicDagLayout(
      {
        nodes: [
          { step_key: "a", display_name: "A", description: "", depends_on: [] },
          { step_key: "b", display_name: "B", description: "", depends_on: ["a"] },
          { step_key: "c", display_name: "C", description: "", depends_on: ["b"] },
          { step_key: "d", display_name: "D", description: "", depends_on: ["c"] },
          { step_key: "e", display_name: "E", description: "", depends_on: ["b", "c", "d"] },
        ],
      },
      {},
    );

    const yByKey = Object.fromEntries(layout.nodes.map((node) => [node.step_key, node.y]));
    const uniqueY = new Set(Object.values(yByKey));

    expect(uniqueY.size).toBeGreaterThan(1);
    expect(yByKey.b).not.toBe(yByKey.a);
  });
});
