/**
 * UBE theme — scroll reveal on all pages + hero entrance on homepage.
 */
(function () {
  "use strict";

  var REVEAL_CLASS = "ube-reveal";
  var VISIBLE_CLASS = "ube-reveal--visible";

  var AUTO_SELECTORS = [
    ".ube-org-banner",
    ".ube-catalog-toolbar",
    ".ube-catalog-filter-hint",
    ".ube-catalog-domain",
    ".ube-catalog-layer",
    ".ube-catalog-card",
    ".ube-catalog-list > li",
    ".ube-dataset-hero",
    ".ube-dataset-badges",
    ".ube-resources-section",
    ".ube-search-page__head",
    ".ube-data-scope-notice",
    ".ube-quick-start",
    ".ube-stat-card",
    ".ube-dataset-card",
    ".ube-domain-hub",
    ".ube-pipeline__item",
    ".ube-cta-panel",
    ".ube-empty-state",
    ".primary .module",
    ".primary .context-info",
    ".secondary .module",
    ".resource-list .item",
    ".prose",
  ].join(", ");

  var prefersReduced =
    window.matchMedia &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function delayClass(index) {
    var slot = (index % 5) + 1;
    return "ube-reveal--delay-" + slot;
  }

  function shouldSkipAutoReveal(el) {
    if (el.classList.contains(REVEAL_CLASS)) return true;
    if (el.closest(".ube-animate-hero")) return true;
    if (el.closest(".ube-reveal")) return true;
    return false;
  }

  function applyAutoReveal() {
    var nodes = document.querySelectorAll(AUTO_SELECTORS);
    var index = 0;
    nodes.forEach(function (el) {
      if (shouldSkipAutoReveal(el)) return;
      el.classList.add(REVEAL_CLASS);
      var delay = delayClass(index);
      if (delay) el.classList.add(delay);
      index += 1;
    });
  }

  function showAllReveals() {
    document.querySelectorAll("." + REVEAL_CLASS).forEach(function (el) {
      el.classList.add(VISIBLE_CLASS);
    });
  }

  function initHero() {
    var hero = document.querySelector(".ube-animate-hero");
    if (!hero) return;
    requestAnimationFrame(function () {
      hero.classList.add("ube-animate-hero--ready");
    });
  }

  function animateCounter(el, target, duration) {
    var start = 0;
    var startTime = null;
    function frame(timestamp) {
      if (!startTime) startTime = timestamp;
      var progress = Math.min((timestamp - startTime) / duration, 1);
      var eased = 1 - Math.pow(1 - progress, 3);
      el.textContent = String(Math.round(start + (target - start) * eased));
      if (progress < 1) {
        window.requestAnimationFrame(frame);
      }
    }
    window.requestAnimationFrame(frame);
  }

  function initStatCounters() {
    if (prefersReduced) return;
    var counters = document.querySelectorAll("[data-ube-counter]");
    if (!counters.length || !("IntersectionObserver" in window)) return;

    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var el = entry.target;
          var target = parseInt(el.getAttribute("data-ube-counter") || "0", 10);
          if (!isNaN(target)) {
            animateCounter(el, target, 900);
          }
          observer.unobserve(el);
        });
      },
      { threshold: 0.4 }
    );

    counters.forEach(function (el) {
      observer.observe(el);
    });
  }

  function initScrollReveal() {
    applyAutoReveal();

    if (prefersReduced) {
      showAllReveals();
      return;
    }

    var reveals = document.querySelectorAll("." + REVEAL_CLASS);
    if (!reveals.length) return;

    if (!("IntersectionObserver" in window)) {
      showAllReveals();
      return;
    }

    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          entry.target.classList.add(VISIBLE_CLASS);
          observer.unobserve(entry.target);
        });
      },
      { root: null, rootMargin: "0px 0px -6% 0px", threshold: 0.1 }
    );

    reveals.forEach(function (el) {
      if (!el.classList.contains(VISIBLE_CLASS)) {
        observer.observe(el);
      }
    });
  }

  function boot() {
    document.body.classList.add("ube-motion");
    initHero();
    initScrollReveal();
    initStatCounters();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
