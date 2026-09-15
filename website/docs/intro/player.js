/* global document, window, URL */
(() => {
  const video = document.querySelector("#film");
  const status = document.querySelector("#media-status");
  let theme;
  let generation = 0;
  let pending;
  let cancelRestore = () => {};
  let loadPauses = 0;

  window.subscribeFilmTheme((nextTheme) => {
    if (nextTheme === theme) return;
    theme = nextTheme;
    const version = ++generation;
    const source = new URL(`../assets/streamskope-intro-${theme}.mp4`, window.location.href).href;
    // Keep the original position and play intent while successive sources are loading.
    const state = pending ?? {
      time: video.currentTime,
      playing: !video.paused && !video.ended,
      muted: video.muted,
      volume: video.volume,
      rate: video.playbackRate,
    };
    pending = state;
    cancelRestore();
    video.poster = `../assets/streamskope-intro-${theme}.png`;
    status.textContent = `Loading ${theme} video…`;
    const finish = () => {
      if (version !== generation || video.currentSrc !== source || video.seeking) return;
      cancelRestore();
      pending = undefined;
      status.textContent = "";
      if (state.playing) {
        video.play().catch(() => {
          if (version === generation) status.textContent = "Press play to continue.";
        });
      }
    };
    const restore = () => {
      if (version !== generation || video.currentSrc !== source || video.readyState < 1) return;
      video.removeEventListener("loadedmetadata", restore);
      video.muted = state.muted;
      video.volume = state.volume;
      video.playbackRate = state.rate;
      const target = Math.min(
        state.time,
        Number.isFinite(video.duration) ? video.duration : state.time,
      );
      video.addEventListener("seeked", finish);
      if (video.currentTime !== target) video.currentTime = target;
      // Keep the target position until the browser finishes seeking this source.
      if (!video.seeking) finish();
    };
    video.addEventListener("loadedmetadata", restore);
    cancelRestore = () => {
      video.removeEventListener("loadedmetadata", restore);
      video.removeEventListener("seeked", finish);
    };
    // Preserve the element itself so an active fullscreen session remains attached.
    video.defaultPlaybackRate = state.rate;
    if (!video.paused) loadPauses += 1;
    video.src = source;
    video.load();
  });

  video.addEventListener("volumechange", () => {
    if (pending) {
      pending.muted = video.muted;
      pending.volume = video.volume;
    }
  });
  video.addEventListener("ratechange", () => {
    if (pending) pending.rate = video.playbackRate;
  });
  video.addEventListener("play", () => {
    if (pending) pending.playing = true;
  });
  video.addEventListener("pause", () => {
    // load() queues a pause event when replacing a playing source.
    if (loadPauses > 0) loadPauses -= 1;
    else if (pending) pending.playing = false;
  });
  video.addEventListener("error", () => {
    if (video.error) status.textContent = "This video could not be played. Try reloading the page.";
  });
})();
