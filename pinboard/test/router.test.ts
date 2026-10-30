import { test } from "vitest";
import assert from "node:assert/strict";
import { matchRoute, normalizeNamespace } from "../src/router.js";

test("normalizeNamespace adds leading slash and strips trailing", () => {
  assert.equal(normalizeNamespace("threads"), "/threads");
  assert.equal(normalizeNamespace("/a/threads/"), "/a/threads");
  assert.throws(() => normalizeNamespace("/"));
  assert.throws(() => normalizeNamespace("a//b"));
});

test("extracts id and remainder", () => {
  assert.deepEqual(matchRoute("/threads/t1/stream", ["/threads"]), {
    ns: "/threads",
    id: "t1",
    rest: "/stream",
  });
  assert.deepEqual(matchRoute("/threads/t1", ["/threads"]), {
    ns: "/threads",
    id: "t1",
    rest: "",
  });
});

test("picks the longest registered prefix", () => {
  const namespaces = ["/a", "/a/threads"];
  assert.deepEqual(matchRoute("/a/threads/t1/commands", namespaces), {
    ns: "/a/threads",
    id: "t1",
    rest: "/commands",
  });
  assert.deepEqual(matchRoute("/a/other/x", namespaces), {
    ns: "/a",
    id: "other",
    rest: "/x",
  });
});

test("matches only on segment boundaries", () => {
  assert.equal(matchRoute("/threadsx/t1", ["/threads"]), null);
  assert.equal(matchRoute("/other/t1", ["/threads"]), null);
});

test("requires an id segment", () => {
  assert.equal(matchRoute("/threads", ["/threads"]), null);
  assert.equal(matchRoute("/threads/", ["/threads"]), null);
});
