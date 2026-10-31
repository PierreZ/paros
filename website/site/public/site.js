// Small helpers for every page (#810 (website redo 1/4)): a copy button on
// code blocks. The theme switch and the phone menu are in shared/theme.js.
(function () {
  document.querySelectorAll("pre.astro-code").forEach(function (pre) {
    var b = document.createElement("button");
    b.type = "button";
    b.className = "copy label";
    b.textContent = "Copy";
    b.addEventListener("click", function () {
      navigator.clipboard.writeText(pre.innerText).then(
        function () { b.textContent = "Copied"; setTimeout(function () { b.textContent = "Copy"; }, 1200); },
        function () { b.textContent = "Failed"; }
      );
    });
    var wrap = document.createElement("div");
    wrap.className = "codewrap";
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);
    wrap.appendChild(b);
  });
})();
