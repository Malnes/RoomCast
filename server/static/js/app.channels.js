function resetChannelUiState() {
  channelsCache = [];
  activeChannelId = null;
  persistActiveChannelPreference(null);
  if (typeof spotifySettingsSourceId !== 'undefined') spotifySettingsSourceId = null;
  radioPlaybackState = null;
  channelPendingEdits.clear();
  channelFormRefs.clear();
  if (channelsPanel) channelsPanel.innerHTML = '<div class="muted">No channels configured yet.</div>';
  if (spotifyChannelSelect) {
    spotifyChannelSelect.innerHTML = '';
    spotifyChannelSelect.disabled = true;
  }
  playerCarouselCards.clear();
  playerCarouselIndicatorRefs.clear();
  resetPlayerPanelSwipeEffects();
  if (playerCarouselTrack) playerCarouselTrack.innerHTML = '';
  if (playerCarouselIndicators) {
    playerCarouselIndicators.innerHTML = '';
    playerCarouselIndicators.hidden = true;
  }
  if (playerPlay) playerPlay.dataset.radioChannelId = '';
  renderPlayerCarousel();
  setPlayerIdleState('Create a channel to control playback', { forceClear: true });
  refreshNodeVolumeAccents();
  applyChannelTheme(null);
}

async function refreshChannels(options = {}) {
  if (channelFetchPromise && !options.force) {
    return channelFetchPromise;
  }
  channelFetchPromise = (async () => {
    try {
      const res = await fetch('/api/channels');
      await ensureOk(res);
      const data = await res.json();
      const previousActive = activeChannelId;
      const list = Array.isArray(data?.channels) ? data.channels : [];
      channelsCache = list.slice().sort((a, b) => (a.order || 0) - (b.order || 0));
      const playableChannels = getPlayerChannels();
      const hasCurrentActive = activeChannelId && playableChannels.some(ch => ch.id === activeChannelId);
      if (!hasCurrentActive) {
        const storedActiveId = readStoredActiveChannelId();
        const storedActiveValid = storedActiveId && playableChannels.some(ch => ch.id === storedActiveId);
        if (storedActiveValid) {
          activeChannelId = storedActiveId;
        } else if (playableChannels.length) {
          activeChannelId = playableChannels[0].id;
        } else {
          activeChannelId = channelsCache[0]?.id || null;
        }
      }
      persistActiveChannelPreference(activeChannelId);
      renderPlayerCarousel();
      populateSpotifyChannelSelect();
      renderChannelsPanel();
      applyChannelTheme(getActiveChannel());
      if (nodesCache.length) {
        renderNodes(nodesCache, { force: true });
      }
      refreshNodeVolumeAccents();
      if (!channelsCache.length) {
        setPlayerIdleState('Create a channel to control playback', { forceClear: true });
      }
      if (activeChannelId && previousActive !== activeChannelId) {
        onActiveChannelChanged(previousActive, activeChannelId);
      } else if (!previousActive && activeChannelId) {
        onActiveChannelChanged(null, activeChannelId);
      } else if (!activeChannelId) {
        setPlayerIdleState('Select a channel to control playback', { forceClear: true });
      }
      return channelsCache;
    } catch (err) {
      showError(`Failed to load channels: ${err.message}`);
      throw err;
    } finally {
      channelFetchPromise = null;
    }
  })();
  return channelFetchPromise;
}

function resetChannelScopedState() {
  playlistAbortController?.abort();
  playlistTracksAbortController?.abort();
  playlistSummaryAbortController?.abort();
  playlistAbortController = null;
  playlistTracksAbortController = null;
  playlistSummaryAbortController = null;
  playlistsCache = [];
  playlistsCacheFetchedAt = 0;
  playlistMetadataCache.clear();
  playlistTrackCache.clear();
  playlistSelected = null;
  playlistAutoSelectId = null;
  if (playlistGrid) playlistGrid.innerHTML = '';
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  setPlaylistErrorMessage('');
  resetPlaylistDetails();
  searchAbortController?.abort();
  searchAbortController = null;
  searchHasAttempted = false;
  lastSearchQuery = '';
  searchResultsState = defaultSearchBuckets();
  SEARCH_TABS.forEach(tab => renderSearchPane(tab));
  updateSearchTabCounts();
  queueAbortController?.abort();
  queueAbortController = null;
  resetQueueContent();
  setQueueErrorMessage('');
  if (playerTick) {
    clearInterval(playerTick);
    playerTick = null;
  }
  lastPlayerSnapshot = null;
}

function onActiveChannelChanged(previousId, nextId) {
  if (previousId === nextId) return;
  resetChannelScopedState();
  const nextChannel = getChannelById(nextId);
  updatePlayerDiscoveryButtons(nextChannel);
  if (!isRadioChannel(nextChannel) && isRadioOverlayOpen()) {
    closeRadioOverlay();
  }
  if (!nextId) {
    setPlayerIdleState('Select a channel to control playback', { forceClear: true });
    return;
  }
  fetchPlayerStatus();
  if (playlistOverlay && playlistOverlay.classList.contains('is-open')) {
    openPlaylistOverlay();
  }
  if (queueOverlay && queueOverlay.classList.contains('is-open')) {
    fetchQueue();
  }
  refreshNodeVolumeAccents();
}

function updatePlayerDiscoveryButtons(channel) {
  const radio = isRadioChannel(channel);
  const abs = isAudiobookshelfChannel(channel);
  if (playerPlaylistsBtn) {
    playerPlaylistsBtn.setAttribute('aria-label', radio ? 'Discover radio stations' : abs ? 'Browse podcasts' : 'Browse playlists');
  }
  if (playerSearchBtn) {
    playerSearchBtn.setAttribute('aria-label', radio ? 'Search radio stations' : 'Search Spotify');
  }
}

function handlePlayerPlaylistsClick() {
  setVolumeSliderOpen(false);
  const channel = getActiveChannel();
  if (isRadioChannel(channel)) {
    openRadioOverlay({ channel });
  } else {
    openPlaylistOverlay();
  }
}

function handlePlayerSearchClick() {
  setVolumeSliderOpen(false);
  const channel = getActiveChannel();
  if (isRadioChannel(channel)) {
    openRadioOverlay({ channel, tab: 'search', focusSearch: true });
  } else {
    openSearchOverlay();
  }
}

function reportSpotifyError(detail) {
  const reason = getErrorMessage(detail);
  const normalized = (reason || '').toLowerCase();
  // Fresh installs have no Spotify provider; don't show scary connection banners in that case.
  if (normalized.includes('provider is not installed')) {
    return;
  }
  const message = reason
    ? `Spotify connection issue: ${reason}. ${SPOTIFY_ALERT_HELP}`
    : `Spotify connection issue. ${SPOTIFY_ALERT_HELP}`;
  if (persistentAlertState &&
      persistentAlertState.key === SPOTIFY_ALERT_KEY &&
      persistentAlertState.message === message) {
    return;
  }
  showPersistentAlert(message, { key: SPOTIFY_ALERT_KEY });
}

function handlePersistentAlertDismiss() {
  const key = persistentAlertEl?.dataset?.alertKey;
  if (key && key !== PWA_UPDATE_ALERT_KEY) suppressPersistentAlert(key);
  hidePersistentAlert();
}

function handlePersistentAlertAction() {
  if (typeof persistentAlertActionHandler === 'function') {
    persistentAlertActionHandler();
  }
}

function markSpotifyHealthy() {
  clearPersistentAlertSuppression(SPOTIFY_ALERT_KEY);
  if (persistentAlertState?.key === SPOTIFY_ALERT_KEY) {
    hidePersistentAlert();
  }
}

function promptPwaUpdate(worker) {
  if (!worker) return;
  showPersistentAlert('A new RoomCast update is ready. Reload to get the latest UI.', {
    key: PWA_UPDATE_ALERT_KEY,
    actionLabel: PWA_UPDATE_ACTION_LABEL,
    dismissLabel: 'Later',
    dismissAriaLabel: 'Dismiss update notification',
    onAction: () => {
      worker.postMessage({ type: 'SKIP_WAITING' });
      hidePersistentAlert();
    },
  });
}

function registerRoomcastServiceWorker() {
  if (!('serviceWorker' in navigator)) return;
  let refreshing = false;
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (refreshing) return;
    refreshing = true;
    window.location.reload();
  });
  navigator.serviceWorker.register('/sw.js')
    .then(registration => {
      if (registration.waiting) {
        promptPwaUpdate(registration.waiting);
      }
      const listenForInstalled = worker => {
        if (!worker) return;
        worker.addEventListener('statechange', () => {
          if (worker.state === 'installed') {
            if (navigator.serviceWorker.controller) {
              promptPwaUpdate(worker);
            } else {
              hidePersistentAlert();
            }
          }
        });
      };
      listenForInstalled(registration.installing);
      registration.addEventListener('updatefound', () => {
        listenForInstalled(registration.installing);
      });
    })
    .catch(err => {
      console.warn('Service worker registration failed:', err);
    });
}

let takeoverBannerPositionFrame = null;

function positionTakeoverBanner() {
  if (!takeoverBanner || takeoverBanner.hidden || !playerPanel) return;
  const actionRect = headerActions?.getBoundingClientRect() || appHeader?.getBoundingClientRect();
  const playerRect = playerPanel.getBoundingClientRect();
  const topAnchor = actionRect?.bottom ?? 80;
  const playerTop = Number.isFinite(playerRect?.top) ? playerRect.top : (window.innerHeight - 220);
  const gap = Math.max(0, playerTop - topAnchor);
  const midpoint = topAnchor + gap / 2;
  const minTop = topAnchor + 24;
  const maxTop = playerTop - 24;
  let targetTop = midpoint;
  if (Number.isFinite(minTop) && Number.isFinite(maxTop)) {
    if (minTop > maxTop) {
      targetTop = playerTop - 32;
    } else {
      targetTop = Math.min(Math.max(targetTop, minTop), maxTop);
    }
  }
  const clamped = Number.isFinite(targetTop) ? targetTop : (playerTop - 120);
  takeoverBanner.style.top = `${Math.max(80, clamped)}px`;
}

function scheduleTakeoverBannerPosition() {
  if (!takeoverBanner || takeoverBanner.hidden) return;
  if (takeoverBannerPositionFrame) {
    cancelAnimationFrame(takeoverBannerPositionFrame);
  }
  takeoverBannerPositionFrame = requestAnimationFrame(() => {
    takeoverBannerPositionFrame = null;
    positionTakeoverBanner();
  });
}

window.addEventListener('resize', scheduleTakeoverBannerPosition, { passive: true });
window.addEventListener('scroll', scheduleTakeoverBannerPosition, { passive: true });

function setTakeoverBannerVisible(visible, message) {
  if (!takeoverBanner) return;
  const next = !!visible;
  takeoverBanner.classList.toggle('is-visible', next);
  takeoverBanner.hidden = !next;
  takeoverBanner.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (next) {
    if (takeoverMessage && message) {
      takeoverMessage.textContent = message;
    }
    scheduleTakeoverBannerPosition();
  } else {
    takeoverBanner.style.top = '';
  }
}

function updateTakeoverBanner(status) {
  if (!takeoverBanner) return;
  const active = !!status?.active;
  const playing = !!status?.is_playing;
  const isRoomcastDevice = !!status?.device_is_roomcast;
  const shouldShow = active && playing && !isRoomcastDevice;
  if (!shouldShow) {
    setTakeoverBannerVisible(false);
    return;
  }
  const deviceName = (status?.device?.name || '').trim();
  const message = deviceName
    ? `Spotify is playing on “${deviceName}”.`
    : 'Spotify is playing on another device.';
  setTakeoverBannerVisible(true, message);
}

async function handleTakeoverClick() {
  if (!takeoverButton) return;
  takeoverButton.disabled = true;
  try {
    await activateRoomcastDevice(true);
    showSuccess('Taking over playback on RoomCast…');
    markSpotifyHealthy();
  } catch (err) {
    showError(`Failed to take over session: ${err.message}`);
    reportSpotifyError(err);
  } finally {
    takeoverButton.disabled = false;
  }
}

if (persistentAlertDismiss) {
  persistentAlertDismiss.addEventListener('click', handlePersistentAlertDismiss);
}
if (persistentAlertAction) {
  persistentAlertAction.addEventListener('click', handlePersistentAlertAction);
}

function normalizeNodeUrl(value) {
  let url = (value || '').trim();
  if (!url) return '';
  if (url.startsWith('browser:')) return url.replace(/\/+$/, '');
  if (!url.includes('://')) url = `http://${url}`;
  return url.replace(/\/+$/, '');
}

function formatTimestamp(seconds) {
  if (typeof seconds !== 'number' || Number.isNaN(seconds) || seconds <= 0) return '';
  const date = new Date(seconds * 1000);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString();
}

function describeNodeHost(url) {
  if (!url) return '';
  if (url.startsWith('browser:')) return 'Browser session';
  try {
    const parsed = new URL(url);
    return parsed.hostname || url;
  } catch (_) {
    return url;
  }
}

function findNodeByFingerprint(fingerprint) {
  if (!fingerprint) return null;
  return nodesCache.find(n => n.fingerprint && n.fingerprint === fingerprint) || null;
}

async function ensureOk(res, fallback) {
  if (res.ok) return res;
  let detail = fallback || `HTTP ${res.status}`;
  try {
    const text = await res.text();
    if (text) {
      try {
        const parsed = JSON.parse(text);
        if (parsed?.detail) detail = parsed.detail;
        else if (parsed?.message) detail = parsed.message;
        else detail = text;
      } catch (_) {
        detail = text;
      }
    }
  } catch (_) {
    /* ignore body parse errors */
  }
  throw new Error(detail);
}

async function readResponseDetail(res, fallback) {
  let detail = fallback || `HTTP ${res.status}`;
  try {
    const text = await res.text();
    if (text) {
      try {
        const parsed = JSON.parse(text);
        if (typeof parsed === 'string') detail = parsed;
        else if (parsed?.detail) detail = parsed.detail;
        else if (parsed?.message) detail = parsed.message;
        else if (parsed?.error?.message) detail = parsed.error.message;
        else detail = text;
      } catch (_) {
        detail = text;
      }
    }
  } catch (_) {
    /* ignore body parse errors */
  }
  return detail;
}

function normalizePercent(value, fallback = 0) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const rounded = Math.round(num);
  return Math.max(0, Math.min(100, rounded));
}

function computeEffectiveVolume(requested, maxPercent) {
  const safeRequested = normalizePercent(requested, 0);
  const safeLimit = normalizePercent(maxPercent, 100);
  return Math.round((safeRequested * safeLimit) / 100);
}

function getActiveDeviceId() {
  if (playerStatus?.device?.id) return playerStatus.device.id;
  if (activeDeviceId) return activeDeviceId;
  return null;
}

function hasAgentUpdate(node) {
  if (!node) return false;
  if (node.update_available === true) return true;
  if (node.update_available === false) return false;
  const latest = node.latest_agent_version;
  const current = node.agent_version;
  if (latest && current) return latest !== current;
  return !!latest && !current;
}

function setPlayButtonIcon(playing, options = {}) {
  if (!playerPlay) return;
  const variant = options.variant === 'radio' ? 'radio' : 'default';
  if (variant === 'radio') {
    const label = options.label || (playing ? 'Stop radio' : 'Play radio');
    playerPlay.innerHTML = playing ? ICON_STOP : ICON_PLAY;
    playerPlay.setAttribute('aria-label', label);
    playerPlay.title = label;
    playerPlay.dataset.playerVariant = 'radio';
    return;
  }
  const label = options.label || (playing ? 'Pause' : 'Play');
  playerPlay.innerHTML = playing ? ICON_PAUSE : ICON_PLAY;
  playerPlay.setAttribute('aria-label', label);
  playerPlay.title = label;
  playerPlay.dataset.playerVariant = 'default';
}

function setShuffleActive(active) {
  if (!playerShuffleBtn) return;
  const next = !!active;
  playerShuffleBtn.classList.toggle('is-active', next);
  playerShuffleBtn.setAttribute('aria-pressed', next ? 'true' : 'false');
  playerShuffleBtn.setAttribute('aria-label', next ? 'Disable shuffle' : 'Enable shuffle');
}

function setRepeatMode(mode) {
  if (!playerRepeatBtn) return;
  const normalized = mode === 'context' || mode === 'track' ? mode : 'off';
  playerRepeatBtn.dataset.mode = normalized;
  playerRepeatBtn.classList.toggle('is-active', normalized !== 'off');
  playerRepeatBtn.classList.toggle('repeat-track', normalized === 'track');
  playerRepeatBtn.setAttribute('aria-pressed', normalized !== 'off' ? 'true' : 'false');
  const label = normalized === 'track'
    ? 'Repeat current track'
    : normalized === 'context'
      ? 'Repeat queue'
      : 'Enable repeat';
  playerRepeatBtn.setAttribute('aria-label', label);
}

function setPlayerArtInteractivity(enabled) {
  if (!playerArt) return;
  const next = !!enabled;
  playerArt.dataset.queueEnabled = next ? 'true' : 'false';
  playerArt.setAttribute('tabindex', next ? '0' : '-1');
  playerArt.setAttribute('aria-disabled', next ? 'false' : 'true');
  playerArt.setAttribute('aria-hidden', next ? 'false' : 'true');
}

function isPlayerArtInteractive() {
  return playerArt?.dataset?.queueEnabled === 'true';
}

function setVolumeSliderOpen(open) {
  if (!playerVolumeInline || !playerVolumeToggle) return;
  const next = !!open;
  playerVolumeInline.classList.toggle('is-open', next);
  playerVolumeToggle.setAttribute('aria-expanded', next ? 'true' : 'false');
  const slider = playerVolumeInline.querySelector('.player-volume-slider');
  if (slider) slider.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (next && masterVolume) {
    setTimeout(() => masterVolume.focus({ preventScroll: true }), 0);
  }
}

function setPlaylistOverlayOpen(open) {
  if (!playlistOverlay) return;
  const next = !!open;
  playlistOverlay.classList.toggle('is-open', next);
  playlistOverlay.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (playerPlaylistsBtn) playerPlaylistsBtn.setAttribute('aria-expanded', next ? 'true' : 'false');
  if (next) document.addEventListener('keydown', handlePlaylistOverlayKey, true);
  else document.removeEventListener('keydown', handlePlaylistOverlayKey, true);
}

function setPlaylistView(mode) {
  const isTracks = mode === 'tracks';
  if (playlistGridView) playlistGridView.hidden = isTracks;
  if (playlistTracksView) playlistTracksView.hidden = !isTracks;
  if (playlistBackBtn) playlistBackBtn.hidden = !isTracks;
  if (playlistToolbar) {
    playlistToolbar.hidden = isTracks;
    playlistToolbar.setAttribute('aria-hidden', isTracks ? 'true' : 'false');
    playlistToolbar.style.display = isTracks ? 'none' : '';
  }
  if (!isTracks) {
    if (playlistSelectedName) playlistSelectedName.textContent = '';
    if (playlistSelectedOwner) {
      playlistSelectedOwner.textContent = '';
      playlistSelectedOwner.hidden = true;
    }
    resetPlaylistDetails();
  }
}

function isCacheFresh(timestamp, ttl) {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return false;
  return Date.now() - timestamp < ttl;
}

function rememberPlaylistMetadata(item, timestamp = Date.now()) {
  if (!item?.id) return;
  playlistMetadataCache.set(item.id, { ...item, _cachedAt: timestamp });
}

function getCachedPlaylistMetadata(playlistId) {
  if (!playlistId) return null;
  const entry = playlistMetadataCache.get(playlistId);
  if (!entry) return null;
  if (!isCacheFresh(entry._cachedAt, PLAYLIST_CACHE_TTL_MS)) {
    playlistMetadataCache.delete(playlistId);
    return null;
  }
  return entry;
}

function ensurePlaylistsLoaded(options = {}) {
  const forceRefresh = !!options.forceRefresh;
  const hasFreshCache = !forceRefresh
    && playlistsCache.length
    && isCacheFresh(playlistsCacheFetchedAt, PLAYLIST_CACHE_TTL_MS);
  if (hasFreshCache) {
    setPlaylistLoadingState(false);
    renderPlaylistGrid(playlistsCache);
    maybeAutoSelectPlaylist();
    return;
  }
  fetchPlaylists();
}

