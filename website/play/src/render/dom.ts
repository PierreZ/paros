// The whole "framework": two element builders and a class toggle.
//
// The app re-renders from the view on every change — nothing here keeps state
// of its own — so a builder that takes attributes and children is all the
// machinery a hand-drawn SVG stage and a side panel need.

/** Anything a builder accepts as a child. */
export type Child = Node | string | null | undefined | false;

type Attrs = Record<string, string | number | boolean | null | undefined>;

function apply(element: Element, attrs: Attrs, children: readonly Child[]): void {
  for (const [name, value] of Object.entries(attrs)) {
    if (value === null || value === undefined || value === false) continue;
    element.setAttribute(name, value === true ? '' : String(value));
  }
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    element.append(typeof child === 'string' ? document.createTextNode(child) : child);
  }
}

/** An HTML element. */
export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs: Attrs = {},
  ...children: Child[]
): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  apply(element, attrs, children);
  return element;
}

/** An SVG element. */
export function svg<K extends keyof SVGElementTagNameMap>(
  tag: K,
  attrs: Attrs = {},
  ...children: Child[]
): SVGElementTagNameMap[K] {
  const element = document.createElementNS('http://www.w3.org/2000/svg', tag);
  apply(element, attrs, children);
  return element;
}

/** Replace an element's children. */
export function replace(host: Element, ...children: Child[]): void {
  host.replaceChildren();
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    host.append(typeof child === 'string' ? document.createTextNode(child) : child);
  }
}
