export interface RouteMatch {
  ns: string;
  id: string;
  rest: string;
}

export function normalizeNamespace(raw: string): string {
  let ns = raw.trim();
  if (!ns.startsWith("/")) ns = `/${ns}`;
  while (ns.length > 1 && ns.endsWith("/")) ns = ns.slice(0, -1);
  if (ns === "/" || ns.includes("//")) {
    throw new Error(`invalid namespace: ${JSON.stringify(raw)}`);
  }
  return ns;
}

export function matchRoute(
  pathname: string,
  namespaces: readonly string[],
): RouteMatch | null {
  let best: string | null = null;
  for (const ns of namespaces) {
    if (pathname !== ns && !pathname.startsWith(`${ns}/`)) continue;
    if (best === null || ns.length > best.length) best = ns;
  }
  if (best === null) return null;
  const remainder = pathname.slice(best.length);
  if (!remainder.startsWith("/")) return null;
  const idEnd = remainder.indexOf("/", 1);
  const id = idEnd === -1 ? remainder.slice(1) : remainder.slice(1, idEnd);
  if (id === "") return null;
  const rest = idEnd === -1 ? "" : remainder.slice(idEnd);
  return { ns: best, id, rest };
}
