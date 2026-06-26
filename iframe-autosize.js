// Auto-size the embedded paros wasm demos to their content height. The demo
// (paros-wasm-demo/web/index.html), when embedded with ?embed=1, posts its
// document height on load / run / resize; here we match the message to the
// iframe that sent it (via contentWindow) and set that iframe's height. This
// replaces hand-tuned `height:NNNpx` on each <iframe>, so a demo that grows
// (more log slots, wrapped scenario chips) is never clipped.
window.addEventListener('message', function (e) {
  var d = e.data;
  if (!d || d.type !== 'paros-resize' || typeof d.height !== 'number') return;
  var frames = document.getElementsByTagName('iframe');
  for (var i = 0; i < frames.length; i++) {
    if (frames[i].contentWindow === e.source) {
      frames[i].style.height = Math.max(200, Math.min(2000, Math.ceil(d.height))) + 'px';
      break;
    }
  }
});
