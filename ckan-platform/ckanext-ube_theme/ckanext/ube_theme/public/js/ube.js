/**
 * UBE theme — hero entrance on homepage only (no scroll-triggered motion).
 */
(function () {
  "use strict";

  function initHero() {
    var hero = document.querySelector(".ube-animate-hero");
    if (!hero) return;
    requestAnimationFrame(function () {
      hero.classList.add("ube-animate-hero--ready");
    });
  }

  function boot() {
    initHero();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
