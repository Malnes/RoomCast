const nodesEl = document.getElementById('nodes');
const clientsSettingsEl = document.getElementById('clients-settings');
const errorEl = document.getElementById('error');
const successEl = document.getElementById('success');
const persistentAlertEl = document.getElementById('persistent-alert');
const persistentAlertMessage = document.getElementById('persistent-alert-message');
const persistentAlertDismiss = document.getElementById('persistent-alert-dismiss');
const addNodeContainer = document.querySelector('[data-add-node-container]');
const addNodeToggle = document.getElementById('add-node-button');
const addNodeMenu = document.getElementById('add-node-menu');
const addHardwareNodeBtn = document.getElementById('add-hardware-node');
const createWebNodeBtn = document.getElementById('create-web-node');
const saveSpotifyBtn = document.getElementById('save-spotify');
const spClientId = document.getElementById('sp-client-id');
const spClientSecret = document.getElementById('sp-client-secret');
const spRedirect = document.getElementById('sp-redirect');
const spName = document.getElementById('sp-name');
const spBitrate = document.getElementById('sp-bitrate');
const spInitVol = document.getElementById('sp-initvol');
setRangeProgress(spInitVol, spInitVol?.value || 0, spInitVol?.max || 100);
const spNormalise = document.getElementById('sp-normalise');
const nodeName = document.getElementById('node-name');
const nodeUrl = document.getElementById('node-url');
const librespotStatus = document.getElementById('librespot-status');
const settingsOverlay = document.getElementById('settings-overlay');
const openSettingsBtn = document.getElementById('open-settings');
const closeSettingsBtn = document.getElementById('close-settings');
const spotifySpinner = document.getElementById('spotify-spinner');
const discoverOverlay = document.getElementById('discover-overlay');
const closeDiscoverBtn = document.getElementById('close-discover');
const discoverSpinner = document.getElementById('discover-spinner');
const discoverStatus = document.getElementById('discover-status');
const discoverList = document.getElementById('discover-list');
const startDiscoverBtn = document.getElementById('start-discover');
const masterVolume = document.getElementById('master-volume');
setRangeProgress(masterVolume, masterVolume?.value || 0, masterVolume?.max || 100);
const playerVolumeInline = document.getElementById('player-volume-inline');
const playerVolumeToggle = document.getElementById('player-volume-toggle');
const playerPanel = document.getElementById('player-panel');
const playerShuffleBtn = document.getElementById('player-shuffle');
const playerPrev = document.getElementById('player-prev');
const playerPlay = document.getElementById('player-play');
const playerNext = document.getElementById('player-next');
const playerRepeatBtn = document.getElementById('player-repeat');
const playerSeek = document.getElementById('player-seek');
setRangeProgress(playerSeek, playerSeek?.value || 0, playerSeek?.max || 1);
const playerTimeCurrent = document.getElementById('player-time-current');
const playerTimeTotal = document.getElementById('player-time-total');
const playerArt = document.getElementById('player-art');
const playerTitle = document.getElementById('player-title');
const playerArtist = document.getElementById('player-artist');
const playerPlaylistsBtn = document.getElementById('player-playlists');
const playerSearchBtn = document.getElementById('player-search');
const takeoverBanner = document.getElementById('takeover-banner');
const takeoverMessage = document.getElementById('takeover-message');
const takeoverButton = document.getElementById('takeover-button');
const playlistOverlay = document.getElementById('playlist-overlay');
const playlistCloseBtn = document.getElementById('playlist-close');
const playlistGrid = document.getElementById('playlist-grid');
const playlistTracklist = document.getElementById('playlist-tracklist');
const playlistToolbar = document.getElementById('playlist-toolbar');
const playlistLoading = document.getElementById('playlist-loading');
const playlistError = document.getElementById('playlist-error');
const playlistEmpty = document.getElementById('playlist-empty');
const playlistBackBtn = document.getElementById('playlist-back');
const playlistSelectedName = document.getElementById('playlist-selected-name');
const playlistSelectedOwner = document.getElementById('playlist-selected-owner');
const playlistSummaryEl = document.getElementById('playlist-summary');
const playlistTrackProgress = document.getElementById('playlist-track-progress');
const playlistLoadMoreBtn = document.getElementById('playlist-load-more');
const playlistTrackFilterInput = document.getElementById('playlist-track-filter');
const playlistGridView = document.querySelector('.playlist-grid-view');
const playlistTracksView = document.querySelector('.playlist-tracks-view');
const playlistSubtitle = document.getElementById('playlist-modal-subtitle');
const playlistSearchInput = document.getElementById('playlist-search');
const playlistSortSelect = document.getElementById('playlist-sort');
if (playlistLoadMoreBtn) {
  playlistLoadMoreBtn.addEventListener('click', () => {
    if (!playlistSelected) return;
    const state = ensurePlaylistTrackState(playlistSelected.id);
    if (!state || !state.hasMore || state.loadingMore) return;
    fetchPlaylistTracksPage(playlistSelected, { offset: state.nextOffset, append: true });
  });
}
if (playlistTrackFilterInput) {
  playlistTrackFilterInput.addEventListener('input', () => {
    playlistTrackSearchTerm = playlistTrackFilterInput.value.trim().toLowerCase();
    if (!playlistSelected) {
      if (!playlistTrackSearchTerm) return;
      return;
    }
    const state = ensurePlaylistTrackState(playlistSelected.id);
    renderPlaylistTracks(state);
  });
}
const searchOverlay = document.getElementById('search-overlay');
const searchCloseBtn = document.getElementById('search-close');
const searchForm = document.getElementById('spotify-search-form');
const searchInput = document.getElementById('spotify-search-input');
const searchTabs = Array.from(document.querySelectorAll('.search-tab'));
const searchPanes = Array.from(document.querySelectorAll('[data-search-pane]'));
const searchPaneMap = searchPanes.reduce((acc, pane) => {
  const key = pane?.dataset?.searchPane;
  if (key) acc[key] = pane;
  return acc;
}, {});
const searchLoading = document.getElementById('search-loading');
const searchError = document.getElementById('search-error');
const searchSubtitle = document.getElementById('search-modal-subtitle');
const queueOverlay = document.getElementById('queue-overlay');
const queueCloseBtn = document.getElementById('queue-close');
const queueLoading = document.getElementById('queue-loading');
const queueError = document.getElementById('queue-error');
const queueEmpty = document.getElementById('queue-empty');
const queueCurrent = document.getElementById('queue-current');
const queueList = document.getElementById('queue-list');
const coverArtBackdrop = document.getElementById('cover-art-backdrop');
const coverArtBackgroundToggle = document.getElementById('cover-art-background');
const collapsiblePanels = Array.from(document.querySelectorAll('[data-collapsible]'));
let playerStatus = null;
let activeDeviceId = null;
let playerTick = null;
let discoverAbortController = null;
let discoverResultsCount = 0;
let nodeSettingsModal = null;
let nodeSettingsContent = null;
let nodeSettingsTitle = null;
let nodeSettingsNodeId = null;
let useCoverArtBackground = true;
let lastCoverArtUrl = null;
const PLAYLIST_PAGE_LIMIT = 50;
let playlistsCache = [];
let playlistSelected = null;
const playlistTrackCache = new Map();
let playlistAbortController = null;
let playlistTracksAbortController = null;
let playlistSummaryAbortController = null;
let playlistSearchTerm = '';
let playlistSortOrder = 'recent';
let playlistTrackSearchTerm = '';
let activeTrackId = null;
let activePlaylistContextId = null;
let playlistAutoSelectId = null;
const SEARCH_TABS = ['tracks', 'albums', 'artists', 'playlists'];
let searchActiveTab = 'tracks';
let lastSearchQuery = '';
let searchHasAttempted = false;
let searchResultsState = defaultSearchBuckets();
let searchAbortController = null;
let queueAbortController = null;
const playlistNameCollator = typeof Intl !== 'undefined' && typeof Intl.Collator === 'function'
  ? new Intl.Collator(undefined, { sensitivity: 'base' })
  : { compare: (a, b) => {
      if (a === b) return 0;
      return a > b ? 1 : -1;
    } };



function initializePlayerButtons() {
  if (playerVolumeToggle) playerVolumeToggle.innerHTML = ICON_VOLUME_ON;
  if (playerShuffleBtn) {
    playerShuffleBtn.innerHTML = ICON_SHUFFLE;
    setShuffleActive(false);
  }
  if (playerPrev) playerPrev.innerHTML = ICON_PREV;
  if (playerNext) playerNext.innerHTML = ICON_NEXT;
  if (playerRepeatBtn) {
    playerRepeatBtn.innerHTML = ICON_REPEAT;
    setRepeatMode('off');
  }
  setPlayButtonIcon(false);
}

function clearMessages() {
  errorEl.style.display = 'none';
  successEl.style.display = 'none';
}

const toastQueue = [];
let toastActive = false;
let persistentAlertState = null;
const persistentAlertSuppression = new Set();
const SPOTIFY_ALERT_KEY = 'spotify';
const SPOTIFY_ALERT_HELP = 'Open Settings > Spotify setup and tap "Save Spotify config" to reconnect.';

function showToast(el, msg, timeout = 3500) {
  el.innerText = msg;
  el.style.display = 'block';
  el.style.opacity = '1';
  el.style.transform = 'translateY(0)';
  setTimeout(() => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(-8px)';
    setTimeout(() => {
      el.style.display = 'none';
    }, 200);
  }, timeout);
}

function enqueueToast(el, msg, timeout) {
  toastQueue.push({ el, msg, timeout });
  if (!toastActive) runToastQueue();
}

function runToastQueue() {
  if (!toastQueue.length) {
    toastActive = false;
    return;
  }
  toastActive = true;
  const { el, msg, timeout } = toastQueue.shift();
  showToast(el, msg, timeout);
  setTimeout(runToastQueue, (timeout || 3500) + 250);
}

function showError(msg) {
  enqueueToast(errorEl, msg, 4500);
}

function showSuccess(msg) {
  enqueueToast(successEl, msg, 3000);
}

function getErrorMessage(err) {
  if (!err) return '';
  if (typeof err === 'string') return err;
  if (err instanceof Error && err.message) return err.message;
  if (typeof err.message === 'string') return err.message;
  try {
    return String(err);
  } catch (_) {
    return 'Unknown error';
  }
}

function showPersistentAlert(message, options = {}) {
  if (!persistentAlertEl || !persistentAlertMessage) return;
  const key = options.key || '';
  if (key && persistentAlertSuppression.has(key)) return;
  persistentAlertMessage.textContent = message;
  persistentAlertEl.dataset.alertKey = key;
  persistentAlertEl.classList.add('is-visible');
  persistentAlertEl.hidden = false;
  persistentAlertEl.setAttribute('aria-hidden', 'false');
  persistentAlertState = { key, message };
}

function hidePersistentAlert() {
  if (!persistentAlertEl) return;
  persistentAlertEl.classList.remove('is-visible');
  persistentAlertEl.setAttribute('aria-hidden', 'true');
  persistentAlertEl.hidden = true;
  persistentAlertEl.dataset.alertKey = '';
  persistentAlertState = null;
}

function suppressPersistentAlert(key) {
  if (!key) return;
  persistentAlertSuppression.add(key);
}

function clearPersistentAlertSuppression(key) {
  if (!key) return;
  persistentAlertSuppression.delete(key);
}

function reportSpotifyError(detail) {
  const reason = getErrorMessage(detail);
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
  if (key) suppressPersistentAlert(key);
  hidePersistentAlert();
}

function markSpotifyHealthy() {
  clearPersistentAlertSuppression(SPOTIFY_ALERT_KEY);
}

function setTakeoverBannerVisible(visible, message) {
  if (!takeoverBanner) return;
  const next = !!visible;
  takeoverBanner.classList.toggle('is-visible', next);
  takeoverBanner.hidden = !next;
  takeoverBanner.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (next && takeoverMessage && message) {
    takeoverMessage.textContent = message;
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

function describePan(value) {
  const num = typeof value === 'number' ? value : Number(value);
  if (Number.isNaN(num) || Math.abs(num) < 0.01) return 'Center';
  const pct = Math.round(Math.abs(num) * 100);
  return num > 0 ? `Right ${pct}%` : `Left ${pct}%`;
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

function setRangeProgress(el, value, maxOverride) {
  if (!el) return;
  const min = Number(el.min ?? 0);
  const rawMax = maxOverride !== undefined ? Number(maxOverride) : Number(el.max);
  const safeMin = Number.isFinite(min) ? min : 0;
  const safeMax = Number.isFinite(rawMax) ? rawMax : safeMin + 1;
  const span = Math.max(0.0001, safeMax - safeMin);
  const rawValue = value !== undefined ? Number(value) : Number(el.value);
  const safeValue = Number.isFinite(rawValue) ? rawValue : safeMin;
  const ratio = (safeValue - safeMin) / span;
  const percent = Math.max(0, Math.min(100, ratio * 100));
  el.style.background = `linear-gradient(90deg, var(--accent) 0%, var(--accent) ${percent}%, rgba(255,255,255,0.12) ${percent}%, rgba(255,255,255,0.12) 100%)`;
}

function setPlayButtonIcon(playing) {
  if (!playerPlay) return;
  playerPlay.innerHTML = playing ? ICON_PAUSE : ICON_PLAY;
  playerPlay.setAttribute('aria-label', playing ? 'Pause' : 'Play');
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

function setPlaylistLoadingState(isLoading, message) {
  if (!playlistLoading) return;
  const next = !!isLoading;
  playlistLoading.hidden = !next;
  playlistLoading.setAttribute('aria-hidden', next ? 'false' : 'true');
  const textEl = playlistLoading.querySelector('span:last-child');
  if (textEl && message) textEl.textContent = message;
}

function setPlaylistErrorMessage(message) {
  if (!playlistError) return;
  const text = (message || '').trim();
  playlistError.textContent = text;
  playlistError.hidden = !text;
  playlistError.setAttribute('aria-hidden', text ? 'false' : 'true');
}

function handlePlaylistOverlayKey(evt) {
  if (evt.key === 'Escape' && playlistOverlay?.classList.contains('is-open')) {
    evt.stopPropagation();
    closePlaylistOverlay();
    playerPlaylistsBtn?.focus({ preventScroll: true });
  }
}

function renderPlaylistGrid(items) {
  if (!playlistGrid) return;
  playlistGrid.innerHTML = '';
  const list = Array.isArray(items) ? items : [];
  const filtered = list.filter(item => {
    if (!playlistSearchTerm) return true;
    const haystack = `${item?.name || ''} ${item?.owner || ''} ${item?.description || ''}`.toLowerCase();
    return haystack.includes(playlistSearchTerm);
  });
  const sorted = sortPlaylists(filtered);
  sorted.forEach(item => {
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'playlist-card';
    card.setAttribute('role', 'listitem');
    const playlistId = item?.id || extractSpotifyPlaylistId(item?.uri);
    if (playlistId) card.dataset.playlistId = playlistId;
    const cover = document.createElement('img');
    cover.className = 'playlist-cover';
    cover.alt = item?.name || 'Playlist cover';
    const fallbackCover = PLAYLIST_FALLBACK_COVER;
    cover.src = item?.image?.url || fallbackCover;
    cover.loading = 'lazy';
    const title = document.createElement('div');
    title.className = 'playlist-card-title';
    title.textContent = item?.name || 'Untitled playlist';
    const ownerText = item?.owner ? `by ${item.owner}` : '';
    const tracksText = typeof item?.tracks_total === 'number' ? `${item.tracks_total} tracks` : '';
    card.appendChild(cover);
    card.appendChild(title);
    if (ownerText) {
      const owner = document.createElement('div');
      owner.className = 'playlist-card-owner';
      owner.textContent = ownerText;
      card.appendChild(owner);
    }
    if (tracksText) {
      const counts = document.createElement('div');
      counts.className = 'playlist-card-owner';
      counts.textContent = tracksText;
      card.appendChild(counts);
    }

    card.addEventListener('click', () => selectPlaylist(item));
    syncPlaylistHighlightForElement(card);
    playlistGrid.appendChild(card);
  });
  updatePlaylistCardHighlights();
  if (playlistEmpty) {
    if (!list.length) {
      playlistEmpty.hidden = false;
      playlistEmpty.textContent = 'No playlists available.';
    } else if (!sorted.length) {
      playlistEmpty.hidden = false;
      playlistEmpty.textContent = 'No playlists match your search.';
    } else {
      playlistEmpty.hidden = true;
      playlistEmpty.textContent = '';
    }
  }
}

function sortPlaylists(items) {
  if (!Array.isArray(items)) return [];
  const sorted = [...items];
  if (playlistSortOrder === 'name') {
    sorted.sort((a, b) => {
      const result = playlistNameCollator.compare(a?.name || '', b?.name || '');
      if (result !== 0) return result;
      return (a?._order ?? 0) - (b?._order ?? 0);
    });
    return sorted;
  }
  sorted.sort((a, b) => (a?._order ?? 0) - (b?._order ?? 0));
  return sorted;
}
function extractSpotifyPlaylistId(value) {
  if (!value || typeof value !== 'string') return null;
  const uriMatch = value.match(/^spotify:playlist:([A-Za-z0-9]+)$/);
  if (uriMatch) return uriMatch[1];
  const urlMatch = value.match(/playlist\/([A-Za-z0-9]+)(?:[/?]|$)/);
  if (urlMatch) return urlMatch[1];
  return null;
}

function updateActivePlaylistContext(status) {
  const ctx = status?.context;
  if (ctx?.type === 'playlist') {
    activePlaylistContextId = extractSpotifyPlaylistId(ctx?.uri);
  } else {
    activePlaylistContextId = null;
  }
}

function maybeAutoSelectPlaylist() {
  if (!playlistAutoSelectId || !Array.isArray(playlistsCache) || !playlistsCache.length) return;
  const targetId = playlistAutoSelectId;
  const match = playlistsCache.find(item => item?.id && item.id === targetId);
  playlistAutoSelectId = null;
  if (match) {
    selectPlaylist(match);
  }
}

function extractSpotifyTrackId(value) {
  if (!value || typeof value !== 'string') return null;
  const uriMatch = value.match(/^spotify:track:([A-Za-z0-9]+)$/);
  if (uriMatch) return uriMatch[1];
  const urlMatch = value.match(/track\/([A-Za-z0-9]+)(?:[/?]|$)/);
  if (urlMatch) return urlMatch[1];
  return null;
}

function resolveTrackId(track) {
  if (!track) return null;
  return track.id || extractSpotifyTrackId(track.uri) || null;
}

function syncTrackHighlightForElement(el) {
  if (!el) return;
  const trackId = el.dataset.trackId;
  const isActive = !!trackId && !!activeTrackId && trackId === activeTrackId;
  el.classList.toggle('is-active-track', isActive);
}

function updateTrackHighlights() {
  document.querySelectorAll('[data-track-id]').forEach(syncTrackHighlightForElement);
}

function syncPlaylistHighlightForElement(el) {
  if (!el) return;
  const playlistId = el.dataset.playlistId;
  const isActive = !!playlistId && !!activePlaylistContextId && playlistId === activePlaylistContextId;
  el.classList.toggle('is-active-playlist', isActive);
}

function updatePlaylistCardHighlights() {
  document.querySelectorAll('[data-playlist-id]').forEach(syncPlaylistHighlightForElement);
}

function applyPlaybackHighlights() {
  updateTrackHighlights();
  updatePlaylistCardHighlights();
}

function ensurePlaylistTrackState(playlistId) {
  if (!playlistId) return null;
  let state = playlistTrackCache.get(playlistId);
  if (!state) {
    state = {
      id: playlistId,
      items: [],
      total: null,
      nextOffset: 0,
      hasMore: false,
      loadingMore: false,
      loadedDurationMs: 0,
      summary: { status: 'idle' },
    };
    playlistTrackCache.set(playlistId, state);
  }
  return state;
}

function renderPlaylistTracks(stateOrList) {
  if (!playlistTracklist) return;
  const items = Array.isArray(stateOrList?.items)
    ? stateOrList.items
    : Array.isArray(stateOrList)
      ? stateOrList
      : [];
  const filtered = filterPlaylistTracks(items);
  playlistTracklist.innerHTML = '';
  if (!items.length) {
    const empty = document.createElement('div');
    empty.className = 'playlist-empty muted';
    empty.textContent = 'No tracks in this playlist yet.';
    playlistTracklist.appendChild(empty);
    return;
  }
  if (!filtered.length) {
    const empty = document.createElement('div');
    empty.className = 'playlist-empty muted';
    empty.textContent = 'No songs match your filter.';
    playlistTracklist.appendChild(empty);
    return;
  }
  filtered.forEach(track => {
    const row = document.createElement('button');
    row.type = 'button';
    row.className = 'playlist-track-row';
    const trackId = resolveTrackId(track);
    if (trackId) row.dataset.trackId = trackId;
    row.setAttribute('role', 'listitem');
    row.addEventListener('click', () => playPlaylistTrack(track));

    const coverWrap = document.createElement('div');
    coverWrap.className = 'search-track-cover-wrap';
    const cover = document.createElement('img');
    cover.className = 'search-track-cover';
    cover.alt = '';
    cover.src = track?.image?.url || PLAYLIST_FALLBACK_COVER;
    const playOverlay = document.createElement('div');
    playOverlay.className = 'search-track-play';
    playOverlay.innerHTML = TRACK_PLAY_ICON;
    coverWrap.appendChild(cover);
    coverWrap.appendChild(playOverlay);

    if (typeof track?.position === 'number') {
      const indexBadge = document.createElement('div');
      indexBadge.className = 'playlist-track-index';
      indexBadge.textContent = String(track.position + 1).padStart(2, '0');
      coverWrap.appendChild(indexBadge);
    }

    const meta = document.createElement('div');
    meta.className = 'search-track-meta';
    const title = document.createElement('div');
    title.className = 'search-track-title';
    title.textContent = track?.name || 'Untitled track';
    meta.appendChild(title);

    const subtitle = document.createElement('div');
    subtitle.className = 'search-track-subtitle';
    subtitle.textContent = track?.artists || track?.album || '';
    meta.appendChild(subtitle);

    const extra = document.createElement('div');
    extra.className = 'playlist-track-extra';
    let hasExtra = false;
    if (track?.album) {
      const album = document.createElement('div');
      album.className = 'playlist-track-album';
      album.textContent = track.album;
      extra.appendChild(album);
      hasExtra = true;
    }
    if (track?.duration_ms) {
      const duration = document.createElement('div');
      duration.className = 'playlist-track-duration';
      duration.textContent = msToTime(track.duration_ms);
      extra.appendChild(duration);
      hasExtra = true;
    }
    if (hasExtra) meta.appendChild(extra);

    row.appendChild(coverWrap);
    row.appendChild(meta);
    syncTrackHighlightForElement(row);
    playlistTracklist.appendChild(row);
  });
  updateTrackHighlights();
}

function filterPlaylistTracks(list) {
  if (!playlistTrackSearchTerm) return list;
  const term = playlistTrackSearchTerm;
  return list.filter(track => {
    if (!track) return false;
    const haystack = `${track.name || ''} ${track.artists || ''} ${track.album || ''}`.toLowerCase();
    return haystack.includes(term);
  });
}

function updatePlaylistProgress(state) {
  if (!playlistTrackProgress || !playlistSelected || !state || playlistSelected.id !== state.id) {
    if (playlistTrackProgress) playlistTrackProgress.textContent = '';
    return;
  }
  const loaded = state.items.length;
  const total = typeof state.total === 'number' ? state.total : playlistSelected?.tracks_total;
  if (!loaded && !total) {
    playlistTrackProgress.textContent = '';
    return;
  }
  const durationLoaded = state.loadedDurationMs > 0 ? ` • ${formatDurationHuman(state.loadedDurationMs)} loaded` : '';
  if (total && loaded < total) {
    playlistTrackProgress.textContent = `Showing ${loaded} of ${total} songs${durationLoaded}`;
  } else if (total) {
    playlistTrackProgress.textContent = `Showing all ${total} songs${durationLoaded}`;
  } else {
    playlistTrackProgress.textContent = `Showing ${loaded} songs${durationLoaded}`;
  }
}

function updatePlaylistLoadMore(state) {
  if (!playlistLoadMoreBtn) return;
  if (!playlistSelected || !state || playlistSelected.id !== state.id) {
    playlistLoadMoreBtn.hidden = true;
    playlistLoadMoreBtn.disabled = false;
    playlistLoadMoreBtn.textContent = 'Load more songs';
    return;
  }
  const total = typeof state.total === 'number' ? state.total : null;
  const hasMore = state.hasMore && (!total || state.items.length < total);
  playlistLoadMoreBtn.hidden = !hasMore;
  playlistLoadMoreBtn.disabled = !!state.loadingMore;
  playlistLoadMoreBtn.textContent = state.loadingMore ? 'Loading…' : 'Load more songs';
}

function updatePlaylistSummary(state) {
  if (!playlistSummaryEl) return;
  if (!playlistSelected || !state || playlistSelected.id !== state.id) {
    playlistSummaryEl.textContent = 'Select a playlist to see details.';
    return;
  }
  const summary = state.summary || { status: 'idle' };
  if (summary.status === 'idle') {
    playlistSummaryEl.textContent = 'Loading playlist details…';
    return;
  }
  if (summary.status === 'loading') {
    playlistSummaryEl.textContent = 'Calculating playlist length…';
    return;
  }
  if (summary.status === 'error') {
    playlistSummaryEl.textContent = `Unable to load playlist length: ${summary.error}`;
    return;
  }
  if (summary.status === 'resolved') {
    const totalTracks = typeof summary.tracksTotal === 'number'
      ? summary.tracksTotal
      : typeof state.total === 'number'
        ? state.total
        : playlistSelected?.tracks_total;
    const durationText = typeof summary.durationMs === 'number'
      ? formatDurationHuman(summary.durationMs)
      : '—';
    if (totalTracks) {
      playlistSummaryEl.textContent = `${totalTracks} song${totalTracks === 1 ? '' : 's'} • ${durationText}`;
    } else {
      playlistSummaryEl.textContent = `Playlist length • ${durationText}`;
    }
    return;
  }
  playlistSummaryEl.textContent = 'Loading playlist details…';
}

function resetPlaylistDetails() {
  if (playlistSummaryEl) playlistSummaryEl.textContent = 'Select a playlist to see details.';
  if (playlistTrackProgress) playlistTrackProgress.textContent = '';
  if (playlistLoadMoreBtn) {
    playlistLoadMoreBtn.hidden = true;
    playlistLoadMoreBtn.disabled = false;
    playlistLoadMoreBtn.textContent = 'Load more songs';
  }
  resetPlaylistTrackFilter();
}

function resetPlaylistFilters() {
  playlistSearchTerm = '';
  playlistSortOrder = 'recent';
  if (playlistSearchInput) playlistSearchInput.value = '';
  if (playlistSortSelect) playlistSortSelect.value = 'recent';
}

function resetPlaylistTrackFilter() {
  playlistTrackSearchTerm = '';
  if (playlistTrackFilterInput) playlistTrackFilterInput.value = '';
}

async function fetchPlaylists() {
  if (!playlistOverlay) return;
  playlistAbortController?.abort();
  playlistAbortController = new AbortController();
  setPlaylistErrorMessage('');
  setPlaylistLoadingState(true, 'Loading playlists…');
  if (playlistEmpty) playlistEmpty.hidden = true;
  try {
    const collected = [];
    let offset = 0;
    let hasNext = true;
    let reportedTotal = null;
    while (hasNext) {
      const params = new URLSearchParams({ limit: String(PLAYLIST_PAGE_LIMIT), offset: String(offset) });
      const res = await fetch(`/api/spotify/playlists?${params.toString()}`, { signal: playlistAbortController.signal });
      await ensureOk(res);
      const data = await res.json();
      const items = Array.isArray(data?.items) ? data.items : [];
      collected.push(...items);
      if (typeof data?.total === 'number') reportedTotal = data.total;
      const fetchedCount = collected.length;
      if (playlistLoading && (reportedTotal || fetchedCount)) {
        if (typeof reportedTotal === 'number' && reportedTotal > 0 && fetchedCount < reportedTotal) {
          setPlaylistLoadingState(true, `Loading playlists (${Math.min(fetchedCount, reportedTotal)} of ${reportedTotal})…`);
        } else if (!reportedTotal) {
          setPlaylistLoadingState(true, `Loading playlists (${fetchedCount})…`);
        }
      }
      const limitValue = typeof data?.limit === 'number' ? data.limit : PLAYLIST_PAGE_LIMIT;
      const offsetValue = typeof data?.offset === 'number' ? data.offset : offset;
      offset = offsetValue + limitValue;
      const serverHasNext = !!data?.next;
      hasNext = serverHasNext && (typeof reportedTotal === 'number' ? fetchedCount < reportedTotal : true);
      if (!hasNext) break;
    }
    playlistsCache = collected.map((item, idx) => ({ ...item, _order: idx }));
    renderPlaylistGrid(playlistsCache);
    maybeAutoSelectPlaylist();
  } catch (err) {
    if (err.name === 'AbortError') return;
    playlistsCache = [];
    if (playlistGrid) playlistGrid.innerHTML = '';
    if (playlistEmpty) {
      playlistEmpty.hidden = false;
      playlistEmpty.textContent = 'Unable to load playlists right now.';
    }
    setPlaylistErrorMessage(`Failed to load playlists: ${err.message}`);
    reportSpotifyError(err);
  } finally {
    setPlaylistLoadingState(false);
  }
}

async function fetchPlaylistTracksPage(playlist, options = {}) {
  if (!playlist?.id) return;
  const state = ensurePlaylistTrackState(playlist.id);
  if (!state) return;
  const offsetOverride = typeof options.offset === 'number' ? options.offset : null;
  const targetOffset = offsetOverride !== null ? offsetOverride : (options.append ? state.nextOffset : 0);
  const isInitialPage = targetOffset === 0;
  playlistTracksAbortController?.abort();
  playlistTracksAbortController = new AbortController();
  if (isInitialPage) {
    setPlaylistErrorMessage('');
    setPlaylistLoadingState(true, 'Loading tracks…');
  } else {
    state.loadingMore = true;
    updatePlaylistLoadMore(state);
  }
  try {
    const params = new URLSearchParams({ limit: '100', offset: String(targetOffset) });
    const res = await fetch(`/api/spotify/playlists/${encodeURIComponent(playlist.id)}/tracks?${params.toString()}`, { signal: playlistTracksAbortController.signal });
    await ensureOk(res);
    const data = await res.json();
    const tracks = Array.isArray(data?.items) ? data.items : [];
    if (isInitialPage) state.items = tracks;
    else state.items = state.items.concat(tracks);
    state.loadedDurationMs = state.items.reduce((sum, item) => sum + (Number(item?.duration_ms) || 0), 0);
    state.total = typeof data?.total === 'number' ? data.total : state.total;
    const limitValue = typeof data?.limit === 'number' ? data.limit : tracks.length;
    const offsetValue = typeof data?.offset === 'number' ? data.offset : targetOffset;
    state.nextOffset = offsetValue + limitValue;
    const serverHasNext = Boolean(data?.next);
    const totalKnown = typeof state.total === 'number';
    state.hasMore = serverHasNext || (totalKnown ? state.nextOffset < state.total : tracks.length === limitValue && limitValue > 0);
    renderPlaylistTracks(state);
    updatePlaylistProgress(state);
    updatePlaylistLoadMore(state);
  } catch (err) {
    if (err.name === 'AbortError') return;
    setPlaylistErrorMessage(`Failed to load tracks: ${err.message}`);
    reportSpotifyError(err);
  } finally {
    if (isInitialPage) setPlaylistLoadingState(false);
    state.loadingMore = false;
    updatePlaylistLoadMore(state);
    playlistTracksAbortController = null;
  }
}

async function fetchPlaylistSummary(playlist) {
  if (!playlist?.id) return;
  const state = ensurePlaylistTrackState(playlist.id);
  if (!state) return;
  const currentStatus = state.summary?.status;
  if (currentStatus === 'loading' || currentStatus === 'resolved') {
    updatePlaylistSummary(state);
    return;
  }
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = new AbortController();
  state.summary = { status: 'loading' };
  updatePlaylistSummary(state);
  try {
    const res = await fetch(`/api/spotify/playlists/${encodeURIComponent(playlist.id)}/summary`, { signal: playlistSummaryAbortController.signal });
    await ensureOk(res);
    const data = await res.json();
    const tracksTotal = typeof data?.tracks_total === 'number'
      ? data.tracks_total
      : playlist?.tracks_total ?? state.total;
    const durationMs = typeof data?.duration_ms_total === 'number' ? data.duration_ms_total : null;
    state.summary = { status: 'resolved', tracksTotal, durationMs };
    if (typeof tracksTotal === 'number') state.total = tracksTotal;
  } catch (err) {
    if (err.name === 'AbortError') {
      state.summary = { status: 'idle' };
    } else {
      state.summary = { status: 'error', error: err.message };
      reportSpotifyError(err);
    }
  } finally {
    updatePlaylistSummary(state);
    playlistSummaryAbortController = null;
  }
}

function openPlaylistOverlay() {
  if (!playlistOverlay) return;
  resetPlaylistFilters();
  resetPlaylistTrackFilter();
  playlistSelected = null;
  playlistsCache = [];
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  if (playlistGrid) playlistGrid.innerHTML = '';
  if (playlistEmpty) {
    playlistEmpty.hidden = true;
    playlistEmpty.textContent = '';
  }
  setPlaylistView('playlists');
  setPlaylistOverlayOpen(true);
  playlistAutoSelectId = activePlaylistContextId || null;
  if (playlistSubtitle) playlistSubtitle.textContent = '';
  setPlaylistErrorMessage('');
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  setPlaylistLoadingState(true, 'Loading playlists…');
  fetchPlaylists();
}

function closePlaylistOverlay() {
  setPlaylistOverlayOpen(false);
  playlistAbortController?.abort();
  playlistTracksAbortController?.abort();
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  playlistSelected = null;
  playlistAutoSelectId = null;
  resetPlaylistFilters();
  resetPlaylistDetails();
  if (playlistTracklist) playlistTracklist.innerHTML = '';
}

function handlePlaylistBack() {
  playlistSelected = null;
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  if (playlistSubtitle) playlistSubtitle.textContent = '';
  setPlaylistErrorMessage('');
  setPlaylistView('playlists');
  resetPlaylistDetails();
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  if (playlistsCache.length) renderPlaylistGrid(playlistsCache);
  else fetchPlaylists();
}

function selectPlaylist(playlist) {
  if (!playlist?.id) {
    showError('Unable to open this playlist.');
    return;
  }
  setPlaylistErrorMessage('');
  resetPlaylistTrackFilter();
  playlistSelected = playlist;
  if (playlistSelectedName) playlistSelectedName.textContent = playlist?.name || 'Playlist';
  if (playlistSelectedOwner) {
    playlistSelectedOwner.textContent = playlist?.owner ? `by ${playlist.owner}` : '';
    playlistSelectedOwner.hidden = !playlist?.owner;
  }
  if (playlistSubtitle) playlistSubtitle.textContent = '';
  setPlaylistView('tracks');
  if (playlistTracklist) playlistTracklist.scrollTop = 0;
  const state = ensurePlaylistTrackState(playlist.id);
  updatePlaylistSummary(state);
  updatePlaylistProgress(state);
  updatePlaylistLoadMore(state);
  if (state?.items?.length) renderPlaylistTracks(state);
  else if (playlistTracklist) playlistTracklist.innerHTML = '';
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  if (!state.summary || state.summary.status === 'idle' || state.summary.status === 'error') {
    fetchPlaylistSummary(playlist);
  } else {
    updatePlaylistSummary(state);
  }
  if (!state.items.length) {
    fetchPlaylistTracksPage(playlist, { offset: 0 });
  }
}

function playPlaylistTrack(track) {
  if (!playlistSelected || !track?.uri || !playlistSelected?.uri) {
    showError('Unable to start playback for this track.');
    return;
  }
  const body = { context_uri: playlistSelected.uri };
  if (typeof track.position === 'number') body.offset = { position: track.position };
  else if (track.uri) body.offset = { uri: track.uri };
  playerAction('/api/spotify/player/play', body);
  closePlaylistOverlay();
}

function defaultSearchBuckets() {
  return {
    tracks: { items: [] },
    albums: { items: [] },
    artists: { items: [] },
    playlists: { items: [] },
  };
}

function setSearchOverlayOpen(open) {
  if (!searchOverlay) return;
  const next = !!open;
  searchOverlay.classList.toggle('is-open', next);
  searchOverlay.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (playerSearchBtn) playerSearchBtn.setAttribute('aria-expanded', next ? 'true' : 'false');
  if (next) {
    document.addEventListener('keydown', handleSearchOverlayKey, true);
    setTimeout(() => {
      if (searchInput) searchInput.focus({ preventScroll: true });
    }, 50);
  } else {
    document.removeEventListener('keydown', handleSearchOverlayKey, true);
    searchAbortController?.abort();
    searchAbortController = null;
    searchHasAttempted = false;
    lastSearchQuery = '';
    searchResultsState = defaultSearchBuckets();
    setSearchLoading(false);
    setSearchError('');
    if (searchSubtitle) searchSubtitle.textContent = '';
    if (searchInput) searchInput.value = '';
    SEARCH_TABS.forEach(tab => renderSearchPane(tab));
  }
}

function handleSearchOverlayKey(event) {
  if (event.key === 'Escape') {
    event.preventDefault();
    closeSearchOverlay();
  }
}

function openSearchOverlay() {
  setSearchOverlayOpen(true);
  setSearchActiveTab('tracks');
  if (!searchHasAttempted) renderSearchPane(searchActiveTab);
}

function closeSearchOverlay() {
  setSearchOverlayOpen(false);
  if (playerSearchBtn) playerSearchBtn.focus({ preventScroll: true });
}

function setSearchLoading(isLoading, message) {
  if (!searchLoading) return;
  const next = !!isLoading;
  searchLoading.hidden = !next;
  searchLoading.setAttribute('aria-hidden', next ? 'false' : 'true');
  const label = searchLoading.querySelector('span:last-child');
  if (label && message) label.textContent = message;
  if (searchForm) {
    const submitBtn = searchForm.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.disabled = next;
  }
}

function setSearchError(message) {
  if (!searchError) return;
  const text = (message || '').trim();
  searchError.textContent = text;
  const hasText = !!text;
  searchError.hidden = !hasText;
  searchError.setAttribute('aria-hidden', hasText ? 'false' : 'true');
}

function setSearchActiveTab(tab) {
  if (!SEARCH_TABS.includes(tab)) return;
  searchActiveTab = tab;
  searchTabs.forEach(btn => {
    if (!btn || !btn.dataset.searchTab) return;
    const isActive = btn.dataset.searchTab === tab;
    btn.classList.toggle('is-active', isActive);
    btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
  });
  searchPanes.forEach(pane => {
    const key = pane?.dataset?.searchPane;
    if (!key) return;
    pane.hidden = key !== tab;
  });
  renderSearchPane(tab);
}

function renderSearchPane(tab) {
  switch (tab) {
    case 'albums':
      renderSearchAlbums(searchResultsState.albums);
      break;
    case 'artists':
      renderSearchArtists(searchResultsState.artists);
      break;
    case 'playlists':
      renderSearchPlaylists(searchResultsState.playlists);
      break;
    case 'tracks':
    default:
      renderSearchTracks(searchResultsState.tracks);
      break;
  }
}

function setQueueOverlayOpen(open) {
  if (!queueOverlay) return;
  const next = !!open;
  queueOverlay.classList.toggle('is-open', next);
  queueOverlay.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (next) {
    document.addEventListener('keydown', handleQueueOverlayKey, true);
  } else {
    document.removeEventListener('keydown', handleQueueOverlayKey, true);
  }
}

function handleQueueOverlayKey(evt) {
  if (evt.key === 'Escape' && queueOverlay?.classList.contains('is-open')) {
    evt.stopPropagation();
    closeQueueOverlay();
  }
}

function setQueueLoadingState(isLoading) {
  if (!queueLoading) return;
  const next = !!isLoading;
  queueLoading.hidden = !next;
  queueLoading.setAttribute('aria-hidden', next ? 'false' : 'true');
}

function setQueueErrorMessage(message) {
  if (!queueError) return;
  const text = (message || '').trim();
  queueError.textContent = text;
  queueError.hidden = !text;
  queueError.setAttribute('aria-hidden', text ? 'false' : 'true');
}

function resetQueueContent() {
  if (queueCurrent) {
    queueCurrent.innerHTML = '';
    queueCurrent.hidden = true;
  }
  if (queueList) {
    queueList.innerHTML = '';
    queueList.hidden = true;
  }
  if (queueEmpty) {
    queueEmpty.hidden = true;
  }
}

function renderQueueOverlay(data) {
  const current = data?.current || null;
  const items = Array.isArray(data?.queue) ? data.queue.filter(Boolean) : [];
  renderQueueCurrent(current);
  renderQueueItems(items);
}

function renderQueueCurrent(track) {
  if (!queueCurrent) return;
  queueCurrent.innerHTML = '';
  if (!track) {
    queueCurrent.hidden = true;
    return;
  }
  queueCurrent.hidden = false;
  const heading = document.createElement('div');
  heading.className = 'queue-section-title';
  heading.textContent = 'Now playing';
  queueCurrent.appendChild(heading);
  queueCurrent.appendChild(createQueueTrack(track, { isCurrent: true }));
}

function renderQueueItems(items) {
  if (!queueList) return;
  queueList.innerHTML = '';
  if (!items.length) {
    queueList.hidden = true;
    if (queueEmpty) {
      queueEmpty.hidden = false;
      queueEmpty.textContent = 'No upcoming tracks.';
    }
    return;
  }
  queueList.hidden = false;
  if (queueEmpty) queueEmpty.hidden = true;
  const heading = document.createElement('div');
  heading.className = 'queue-section-title';
  heading.textContent = 'Next up';
  queueList.appendChild(heading);
  const list = document.createElement('div');
  list.className = 'queue-tracklist';
  items.forEach((track, index) => {
    list.appendChild(createQueueTrack(track, { index: index + 1 }));
  });
  queueList.appendChild(list);
}

function createQueueTrack(track, options = {}) {
  const row = document.createElement('div');
  row.className = 'queue-track';
  if (options.isCurrent) row.classList.add('is-current');
  const coverWrap = document.createElement('div');
  coverWrap.className = 'queue-track-cover-wrap';
  const img = document.createElement('img');
  img.className = 'queue-track-cover';
  img.alt = track?.name ? `${track.name} cover art` : 'Track cover art';
  img.src = track?.image?.url || PLAYLIST_FALLBACK_COVER;
  coverWrap.appendChild(img);
  if (typeof options.index === 'number') {
    const badge = document.createElement('div');
    badge.className = 'queue-track-index';
    badge.textContent = String(options.index);
    coverWrap.appendChild(badge);
  }
  const meta = document.createElement('div');
  meta.className = 'queue-track-meta';
  const title = document.createElement('div');
  title.className = 'queue-track-title';
  title.textContent = track?.name || 'Unknown track';
  meta.appendChild(title);
  const subtitle = document.createElement('div');
  subtitle.className = 'queue-track-subtitle';
  subtitle.textContent = track?.artists || '—';
  meta.appendChild(subtitle);
  if (track?.album) {
    const album = document.createElement('div');
    album.className = 'queue-track-album';
    album.textContent = track.album;
    meta.appendChild(album);
  }
  row.appendChild(coverWrap);
  row.appendChild(meta);
  const duration = document.createElement('div');
  duration.className = 'queue-track-duration';
  duration.textContent = typeof track?.duration_ms === 'number'
    ? msToTime(track.duration_ms)
    : '—';
  row.appendChild(duration);
  return row;
}

async function fetchQueue() {
  if (!queueOverlay) return;
  queueAbortController?.abort();
  const controller = new AbortController();
  queueAbortController = controller;
  resetQueueContent();
  setQueueErrorMessage('');
  setQueueLoadingState(true);
  try {
    const res = await fetch('/api/spotify/player/queue', { signal: controller.signal });
    await ensureOk(res);
    const data = await res.json();
    renderQueueOverlay(data);
    markSpotifyHealthy();
  } catch (err) {
    if (err.name === 'AbortError') return;
    setQueueErrorMessage(`Unable to load queue: ${err.message}`);
    reportSpotifyError(err);
  } finally {
    setQueueLoadingState(false);
    if (queueAbortController === controller) queueAbortController = null;
  }
}

function openQueueOverlay() {
  if (!queueOverlay || !isPlayerArtInteractive()) return;
  setVolumeSliderOpen(false);
  setQueueOverlayOpen(true);
  fetchQueue();
}

function closeQueueOverlay() {
  setQueueOverlayOpen(false);
  queueAbortController?.abort();
  queueAbortController = null;
  setQueueLoadingState(false);
  setQueueErrorMessage('');
  resetQueueContent();
  if (playerArt && isPlayerArtInteractive()) {
    playerArt.focus({ preventScroll: true });
  }
}

function renderSearchTracks(bucket = {}) {
  const pane = searchPaneMap.tracks;
  if (!pane) return;
  pane.innerHTML = '';
  if (!searchHasAttempted) {
    pane.appendChild(createSearchHint('Enter a search term to get started.'));
    return;
  }
  const items = Array.isArray(bucket?.items) ? bucket.items : [];
  if (!items.length) {
    pane.appendChild(createSearchEmpty('No songs found.'));
    return;
  }
  items.forEach(track => {
    const row = document.createElement('button');
    row.type = 'button';
    row.className = 'search-track';
    const trackId = resolveTrackId(track);
    if (trackId) row.dataset.trackId = trackId;
    row.addEventListener('click', () => playSearchTrack(track));
    const coverWrap = document.createElement('div');
    coverWrap.className = 'search-track-cover-wrap';
    const cover = document.createElement('img');
    cover.className = 'search-track-cover';
    cover.alt = '';
    cover.src = track?.image?.url || PLAYLIST_FALLBACK_COVER;
    const playOverlay = document.createElement('div');
    playOverlay.className = 'search-track-play';
    playOverlay.innerHTML = TRACK_PLAY_ICON;
    coverWrap.appendChild(cover);
    coverWrap.appendChild(playOverlay);
    const meta = document.createElement('div');
    meta.className = 'search-track-meta';
    const title = document.createElement('div');
    title.className = 'search-track-title';
    title.textContent = track?.name || 'Track';
    const subtitle = document.createElement('div');
    subtitle.className = 'search-track-subtitle';
    const infoParts = [];
    if (track?.artists) infoParts.push(track.artists);
    if (track?.album) infoParts.push(track.album);
    subtitle.textContent = infoParts.join(' • ');
    meta.appendChild(title);
    meta.appendChild(subtitle);
    if (track?.duration_ms) {
      const duration = document.createElement('div');
      duration.className = 'search-track-subtitle';
      duration.textContent = msToTime(track.duration_ms);
      meta.appendChild(duration);
    }
    row.appendChild(coverWrap);
    row.appendChild(meta);
    syncTrackHighlightForElement(row);
    pane.appendChild(row);
  });
  updateTrackHighlights();
}

function renderSearchAlbums(bucket = {}) {
  const pane = searchPaneMap.albums;
  if (!pane) return;
  pane.innerHTML = '';
  if (!searchHasAttempted) {
    pane.appendChild(createSearchHint('Search for any album title.'));
    return;
  }
  const items = Array.isArray(bucket?.items) ? bucket.items : [];
  if (!items.length) {
    pane.appendChild(createSearchEmpty('No albums found.'));
    return;
  }
  const grid = document.createElement('div');
  grid.className = 'search-grid';
  items.forEach(album => {
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'search-card search-album-card';
    card.addEventListener('click', () => playSearchAlbum(album));
    const img = document.createElement('img');
    img.alt = '';
    img.src = album?.image?.url || PLAYLIST_FALLBACK_COVER;
    const title = document.createElement('div');
    title.className = 'search-card-title';
    title.textContent = album?.name || 'Album';
    const subtitle = document.createElement('div');
    subtitle.className = 'search-card-subtitle';
    subtitle.textContent = album?.artists || '';
    card.appendChild(img);
    card.appendChild(title);
    if (subtitle.textContent) card.appendChild(subtitle);
    if (album?.release_date) {
      const release = document.createElement('div');
      release.className = 'search-card-subtitle';
      release.textContent = `Released ${album.release_date}`;
      card.appendChild(release);
    }
    grid.appendChild(card);
  });
  pane.appendChild(grid);
}

function renderSearchArtists(bucket = {}) {
  const pane = searchPaneMap.artists;
  if (!pane) return;
  pane.innerHTML = '';
  if (!searchHasAttempted) {
    pane.appendChild(createSearchHint('Search for any artist name.'));
    return;
  }
  const items = Array.isArray(bucket?.items) ? bucket.items : [];
  if (!items.length) {
    pane.appendChild(createSearchEmpty('No artists found.'));
    return;
  }
  const grid = document.createElement('div');
  grid.className = 'search-grid';
  items.forEach(artist => {
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'search-card search-artist-card';
    card.addEventListener('click', () => playSearchArtist(artist));
    const img = document.createElement('img');
    img.alt = '';
    img.src = artist?.image?.url || PLAYLIST_FALLBACK_COVER;
    const title = document.createElement('div');
    title.className = 'search-card-title';
    title.textContent = artist?.name || 'Artist';
    const subtitle = document.createElement('div');
    subtitle.className = 'search-card-subtitle';
    subtitle.textContent = artist?.genres || '';
    const followers = document.createElement('div');
    followers.className = 'search-artist-followers';
    followers.textContent = formatFollowerCount(artist?.followers);
    card.appendChild(img);
    card.appendChild(title);
    if (subtitle.textContent) card.appendChild(subtitle);
    if (followers.textContent) card.appendChild(followers);
    grid.appendChild(card);
  });
  pane.appendChild(grid);
}

function renderSearchPlaylists(bucket = {}) {
  const pane = searchPaneMap.playlists;
  if (!pane) return;
  pane.innerHTML = '';
  if (!searchHasAttempted) {
    pane.appendChild(createSearchHint('Search for collaborative or saved playlists.'));
    return;
  }
  const items = Array.isArray(bucket?.items) ? bucket.items : [];
  if (!items.length) {
    pane.appendChild(createSearchEmpty('No playlists found.'));
    return;
  }
  const grid = document.createElement('div');
  grid.className = 'search-grid';
  items.forEach(list => {
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'search-card playlist-card';
    const playlistId = list?.id || extractSpotifyPlaylistId(list?.uri);
    if (playlistId) card.dataset.playlistId = playlistId;
    card.addEventListener('click', () => playSearchPlaylist(list));
    const img = document.createElement('img');
    img.alt = '';
    img.src = list?.image?.url || PLAYLIST_FALLBACK_COVER;
    const title = document.createElement('div');
    title.className = 'search-card-title';
    title.textContent = list?.name || 'Playlist';
    const subtitle = document.createElement('div');
    subtitle.className = 'search-card-subtitle';
    const owner = list?.owner ? `by ${list.owner}` : '';
    const tracks = typeof list?.tracks_total === 'number' ? `${list.tracks_total} tracks` : '';
    subtitle.textContent = [owner, tracks].filter(Boolean).join(' • ');
    card.appendChild(img);
    card.appendChild(title);
    if (subtitle.textContent) card.appendChild(subtitle);
    syncPlaylistHighlightForElement(card);
    grid.appendChild(card);
  });
  pane.appendChild(grid);
  updatePlaylistCardHighlights();
}

function updateSearchTabCounts() {
  searchTabs.forEach(btn => {
    if (!btn || !btn.dataset.searchTab) return;
    const base = btn.dataset.baseLabel || btn.textContent.trim();
    btn.dataset.baseLabel = base;
    const bucket = searchResultsState[btn.dataset.searchTab] || {};
    const count = Array.isArray(bucket.items) ? bucket.items.length : 0;
    btn.textContent = searchHasAttempted && count ? `${base} (${count})` : base;
  });
}

function createSearchHint(message) {
  const div = document.createElement('div');
  div.className = 'search-hint';
  div.textContent = message;
  return div;
}

function createSearchEmpty(message) {
  const div = document.createElement('div');
  div.className = 'search-empty';
  div.textContent = message;
  return div;
}

function formatFollowerCount(value) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return '';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1).replace(/\.0$/, '')}M followers`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1).replace(/\.0$/, '')}K followers`;
  return `${value} followers`;
}

function playSearchTrack(track) {
  if (!track?.uri) {
    showError('Unable to play that track right now.');
    return;
  }
  playerAction('/api/spotify/player/play', { uris: [track.uri] });
  closeSearchOverlay();
}

function playSearchAlbum(album) {
  if (!album?.uri) {
    showError('Unable to play that album right now.');
    return;
  }
  playerAction('/api/spotify/player/play', { context_uri: album.uri });
  closeSearchOverlay();
}

function playSearchPlaylist(list) {
  if (!list?.uri) {
    showError('Unable to play that playlist right now.');
    return;
  }
  playerAction('/api/spotify/player/play', { context_uri: list.uri });
  closeSearchOverlay();
}

function playSearchArtist(artist) {
  if (!artist?.uri) {
    showError('Unable to play that artist right now.');
    return;
  }
  playerAction('/api/spotify/player/play', { context_uri: artist.uri });
  closeSearchOverlay();
}

async function runSpotifySearch(query) {
  const term = (query || '').trim();
  if (!term) {
    if (searchInput) searchInput.focus();
    return;
  }
  searchHasAttempted = true;
  setSearchError('');
  setSearchLoading(true, `Searching Spotify for "${term}"…`);
  searchAbortController?.abort();
  searchAbortController = new AbortController();
  try {
    const params = new URLSearchParams({ q: term, limit: '10' });
    const res = await fetch(`/api/spotify/search?${params.toString()}`, { signal: searchAbortController.signal });
    await ensureOk(res);
    const data = await res.json();
    searchResultsState = {
      tracks: data?.tracks || { items: [] },
      albums: data?.albums || { items: [] },
      artists: data?.artists || { items: [] },
      playlists: data?.playlists || { items: [] },
    };
    lastSearchQuery = data?.query || term;
    if (searchSubtitle && lastSearchQuery) searchSubtitle.textContent = `Results for "${lastSearchQuery}"`;
    updateSearchTabCounts();
    renderSearchPane(searchActiveTab);
  } catch (err) {
    if (err.name === 'AbortError') return;
    setSearchError(`Failed to search Spotify: ${err.message}`);
    reportSpotifyError(err);
  } finally {
    setSearchLoading(false);
  }
}

function setAddNodeMenuOpen(open) {
  if (!addNodeContainer || !addNodeMenu || !addNodeToggle) return;
  const next = !!open;
  addNodeContainer.classList.toggle('is-open', next);
  addNodeMenu.setAttribute('aria-hidden', next ? 'false' : 'true');
  addNodeToggle.setAttribute('aria-expanded', next ? 'true' : 'false');
}

function setCollapsiblePanelState(panel, open) {
  if (!panel) return;
  const trigger = panel.querySelector('.collapsible-header');
  const content = panel.querySelector('.collapsible-content');
  const next = !!open;
  panel.dataset.open = next ? 'true' : 'false';
  if (trigger) trigger.setAttribute('aria-expanded', next ? 'true' : 'false');
  if (content) content.setAttribute('aria-hidden', next ? 'false' : 'true');
}

function initCollapsiblePanels() {
  collapsiblePanels.forEach(panel => {
    const trigger = panel.querySelector('.collapsible-header');
    if (!trigger) return;
    trigger.addEventListener('click', () => {
      const isOpen = panel.dataset.open === 'true';
      setCollapsiblePanelState(panel, !isOpen);
    });
    setCollapsiblePanelState(panel, false);
  });
}

function collapseAllPanels() {
  collapsiblePanels.forEach(panel => setCollapsiblePanelState(panel, false));
}

function applyCoverArtBackground() {
  if (!coverArtBackdrop) return;
  if (useCoverArtBackground && lastCoverArtUrl) {
    const safeUrl = lastCoverArtUrl.replace(/"/g, '\"');
    coverArtBackdrop.style.backgroundImage = `${COVER_ART_BACKDROP_OVERLAY}, url("${safeUrl}")`;
  } else {
    coverArtBackdrop.style.backgroundImage = '';
  }
  document.body.classList.toggle('use-cover-art-background', useCoverArtBackground && !!lastCoverArtUrl);
}

function setCoverArtBackgroundEnabled(enabled) {
  const next = !!enabled;
  if (useCoverArtBackground === next) {
    applyCoverArtBackground();
    return;
  }
  useCoverArtBackground = next;
  applyCoverArtBackground();
}

function syncGeneralSettingsUI() {
  if (coverArtBackgroundToggle) {
    coverArtBackgroundToggle.checked = useCoverArtBackground;
  }
}

function applyMuteButtonState(btn, muted) {
  const active = !!muted;
  btn.dataset.muted = active ? 'true' : 'false';
  btn.innerHTML = active ? ICON_VOLUME_OFF : ICON_VOLUME_ON;
  btn.title = active ? 'Unmute' : 'Mute';
  btn.setAttribute('aria-label', active ? 'Unmute' : 'Mute');
  if (active) {
    btn.style.borderColor = '#f59e0b';
    btn.style.color = '#fbbf24';
    btn.style.background = 'rgba(251,191,36,0.18)';
  } else {
    btn.style.borderColor = 'rgba(255,255,255,0.14)';
    btn.style.color = '#e2e8f0';
    btn.style.background = 'rgba(15,23,42,0.8)';
  }
}

let nodesCache = [];
let pendingNodesRender = null;
let pendingNodesForce = false;
const eqState = {};
const eqDirtyNodes = {};
const eqUpdateTimers = {};
const camillaPendingNodes = {};
const EQ_FREQUENCIES = {
  peq15: [25, 40, 63, 100, 160, 250, 400, 630, 1000, 1600, 2500, 4000, 6300, 10000, 16000],
  peq31: [20, 25, 31.5, 40, 50, 63, 80, 100, 125, 160, 200, 250, 315, 400, 500, 630, 800, 1000, 1250, 1600, 2000, 2500, 3150, 4000, 5000, 6300, 8000, 10000, 12500, 16000, 20000],
};
const EQ_GAIN_RANGE = { min: -12, max: 12 };
const EQ_Q_RANGE = { min: 0.2, max: 10 };
const EQ_PUSH_DEBOUNCE_MS = 120;
const LOG_FREQ = { min: Math.log10(20), max: Math.log10(20000) };
const EQ_SKIN_STORAGE_KEY = 'eq-skin';
function loadEqSkinPreference() {
  try {
    const stored = localStorage.getItem(EQ_SKIN_STORAGE_KEY);
    if (stored === 'faders' || stored === 'classic') return stored;
  } catch (_) {
    /* ignore storage errors */
  }
  return 'classic';
}
function persistEqSkinPreference(value) {
  try {
    localStorage.setItem(EQ_SKIN_STORAGE_KEY, value);
  } catch (_) {
    /* ignore storage errors */
  }
}
let eqSkin = loadEqSkinPreference();
const EQ_ICON_SVG = `
  <svg viewBox="0 0 24 24" aria-hidden="true" role="img" focusable="false" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
    <line x1="7" y1="3" x2="7" y2="9"></line>
    <line x1="7" y1="13" x2="7" y2="21"></line>
    <circle cx="7" cy="11" r="1.8" fill="currentColor" stroke="none"></circle>
    <line x1="12" y1="3" x2="12" y2="7"></line>
    <line x1="12" y1="13" x2="12" y2="21"></line>
    <circle cx="12" cy="9" r="1.8" fill="currentColor" stroke="none"></circle>
    <line x1="17" y1="3" x2="17" y2="11"></line>
    <line x1="17" y1="17" x2="17" y2="21"></line>
    <circle cx="17" cy="15" r="1.8" fill="currentColor" stroke="none"></circle>
  </svg>`;
const ICON_VOLUME_ON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M11 5L6 9H3v6h3l5 4z"/><path d="M15 9a3 3 0 010 6"/></svg>`;
const ICON_VOLUME_OFF = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M11 5L6 9H3v6h3l5 4z"/><path d="M19 9l-6 6"/><path d="M13 9l6 6"/></svg>`;
const ICON_SHUFFLE = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3h4v4"/><path d="M4 20l16-16"/><path d="M4 4l5 5"/><path d="M15 15l5 5v-4"/></svg>`;
const ICON_PREV = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="15 18 9 12 15 6 15 18"/><line x1="6" y1="6" x2="6" y2="18"/></svg>`;
const ICON_NEXT = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="9 18 15 12 9 6 9 18"/><line x1="18" y1="6" x2="18" y2="18"/></svg>`;
const ICON_PLAY = `<svg viewBox="0 0 24 24" fill="currentColor"><polygon points="8,5 20,12 8,19"/></svg>`;
const ICON_PAUSE = `<svg viewBox="0 0 24 24" fill="currentColor"><rect x="7" y="5" width="4" height="14" rx="1"/><rect x="13" y="5" width="4" height="14" rx="1"/></svg>`;
const TRACK_PLAY_ICON = '<svg viewBox="0 0 24 24" fill="currentColor" role="img" aria-hidden="true"><polygon points="9,6 19,12 9,18"/></svg>';
const ICON_REPEAT = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="17 1 21 5 17 9"/><path d="M3 11V9a4 4 0 014-4h14"/><polyline points="7 23 3 19 7 15"/><path d="M21 13v2a4 4 0 01-4 4H3"/></svg>`;
const PLAYLIST_FALLBACK_COVER = 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 300" preserveAspectRatio="xMidYMid meet"><defs><linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="%2322c55e" stop-opacity="0.35"/><stop offset="100%" stop-color="%230f172a"/></linearGradient></defs><rect width="300" height="300" fill="url(%23g)"/><text x="150" y="160" text-anchor="middle" font-size="90" fill="%23f8fafc">&#9835;</text></svg>';
const COVER_ART_BACKDROP_OVERLAY = 'linear-gradient(135deg, rgba(2,6,23,0.92), rgba(2,6,23,0.65))';
initializePlayerButtons();
initCollapsiblePanels();
let eqModal = null;
const spotifyAuthBtn = document.getElementById('spotify-auth');
const spotifyDashboardBtn = document.getElementById('spotify-dashboard');
const spotifyLinkStatus = document.getElementById('spotify-link-status');

function isInteractiveNodeElement(el) {
  if (!el) return false;
  const tag = el.tagName;
  if (tag === 'SELECT' || tag === 'TEXTAREA') return true;
  if (tag === 'INPUT') {
    const type = (el.type || '').toLowerCase();
    return type !== 'button' && type !== 'submit' && type !== 'reset';
  }
  return false;
}

function shouldDeferNodeRender() {
  const active = document.activeElement;
  if (!active) return false;
  if (!nodesEl.contains(active)) return false;
  return isInteractiveNodeElement(active);
}

function flushPendingNodesRender() {
  if (!pendingNodesRender) return;
  if (!pendingNodesForce && shouldDeferNodeRender()) return;
  const next = pendingNodesRender;
  pendingNodesRender = null;
  pendingNodesForce = false;
  commitRenderNodes(next);
}

nodesEl.addEventListener('focusout', () => {
  setTimeout(() => {
    if (!shouldDeferNodeRender()) {
      flushPendingNodesRender();
    }
  }, 0);
}, true);

document.addEventListener('visibilitychange', () => {
  if (!shouldDeferNodeRender()) {
    flushPendingNodesRender();
  }
});

function renderNodes(nodes, options = {}) {
  nodesCache = nodes;
  pendingNodesRender = nodes;
  if (options.force) {
    pendingNodesForce = true;
  }
  flushPendingNodesRender();
}

function commitRenderNodes(nodes) {
  nodesEl.innerHTML = '';
  if (!nodes.length) {
    nodesEl.innerHTML = '<div class="muted">No nodes registered yet.</div>';
    Object.keys(camillaPendingNodes).forEach(id => delete camillaPendingNodes[id]);
    refreshNodeSettingsModal();
    return;
  }
  nodes.forEach(n => {
    hydrateEqFromNode(n);
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    const header = document.createElement('div');
    header.className = 'node-header';
    const title = document.createElement('div');
    title.style.display = 'flex';
    title.style.alignItems = 'center';
    title.style.gap = '8px';
    const titleStrong = document.createElement('strong');
    titleStrong.textContent = n.name;
    title.appendChild(titleStrong);
    header.appendChild(title);

    const gearWrap = document.createElement('div');
    gearWrap.className = 'node-gear';
    const gearBtn = document.createElement('button');
    gearBtn.className = 'node-icon-btn';
    gearBtn.textContent = '⚙︎';
    gearBtn.setAttribute('aria-label', `Settings for ${n.name}`);
    gearBtn.addEventListener('click', (event) => {
      event.stopPropagation();
      openNodeSettingsModal(n.id);
    });
    gearWrap.appendChild(gearBtn);
    header.appendChild(gearWrap);
    wrapper.appendChild(header);

    const statusRow = document.createElement('div');
    statusRow.className = 'node-status';
    const isBrowser = n.type === 'browser';
    const paired = !!n.paired;
    const configured = isBrowser ? true : !!n.configured;
    const online = isBrowser ? true : n.online !== false;
    const restarting = !!n.restarting;
    const updateAvailable = hasAgentUpdate(n);
    const updating = !!n.updating;
    const disableAgentControls = !isBrowser && (!paired || !configured || restarting || !online);
    const eqBtn = document.createElement('button');
    eqBtn.className = 'node-icon-btn';
    eqBtn.innerHTML = EQ_ICON_SVG;
    eqBtn.setAttribute('aria-label', 'Equalizer');
    eqBtn.title = 'Equalizer';
    eqBtn.disabled = n.type !== 'browser' && (!paired || !configured || restarting || !online);
    eqBtn.addEventListener('click', (event) => {
      event.stopPropagation();
      openEqModal(n.id, n.name);
    });
    gearWrap.insertBefore(eqBtn, gearBtn);
    if (!isBrowser) {
      const onlinePill = document.createElement('span');
      onlinePill.className = `status-pill ${online ? 'ok' : 'warn'}`;
      onlinePill.textContent = online ? 'Online' : 'Offline';
      title.appendChild(onlinePill);
    }
    if (isBrowser) {
      const browserPill = document.createElement('span');
      browserPill.className = 'status-pill ok';
      browserPill.textContent = 'Browser node';
      statusRow.appendChild(browserPill);
    } else if (!paired) {
      const pairPill = document.createElement('span');
      pairPill.className = 'status-pill warn';
      pairPill.textContent = 'Pairing required';
      statusRow.appendChild(pairPill);
    }
    if (!isBrowser) {
      if (!configured) {
        const cfgPill = document.createElement('span');
        cfgPill.className = 'status-pill warn';
        cfgPill.textContent = 'Needs config';
        statusRow.appendChild(cfgPill);
      }
    }
    if (!isBrowser && (updateAvailable || updating)) {
      const versionMeta = document.createElement('div');
      versionMeta.className = 'label';
      let text = n.agent_version ? `Agent ${n.agent_version}` : 'Agent version unknown';
      if (updateAvailable) {
        if (n.latest_agent_version) {
          text = n.agent_version
            ? `Agent ${n.agent_version} → ${n.latest_agent_version}`
            : `Update ${n.latest_agent_version} available`;
        } else {
          text += ' – update available';
        }
      }
      versionMeta.textContent = text;
      statusRow.appendChild(versionMeta);
    }
    if (restarting) {
      const restartPill = document.createElement('span');
      restartPill.className = 'status-pill warn';
      restartPill.textContent = 'Restarting';
      statusRow.appendChild(restartPill);
    }
    if (updating) {
      const updatingPill = document.createElement('span');
      updatingPill.className = 'status-pill warn';
      updatingPill.textContent = 'Updating';
      statusRow.appendChild(updatingPill);
    }
    wrapper.appendChild(statusRow);

    const volRow = document.createElement('div');
    volRow.style.display = 'grid';
    volRow.style.gridTemplateColumns = 'auto minmax(0, 1fr) auto';
    volRow.style.alignItems = 'center';
    volRow.style.gap = '8px';
    const muteBtn = document.createElement('button');
    muteBtn.className = 'node-mute-btn';
    muteBtn.disabled = disableAgentControls;
    muteBtn.addEventListener('click', () => toggleMute(n.id, muteBtn));
    applyMuteButtonState(muteBtn, n.muted === true);
    volRow.appendChild(muteBtn);
    const volInput = document.createElement('input');
    volInput.type = 'range';
    volInput.min = 0;
    volInput.max = 100;
    const parsedVolume = Number(n.volume_percent);
    const maxVolumePercent = normalizePercent(n.max_volume_percent, 100);
    volInput.value = Number.isFinite(parsedVolume) ? parsedVolume : 75;
    volInput.disabled = disableAgentControls;
    volInput.style.width = '100%';
    setRangeProgress(volInput, volInput.value, volInput.max || 100);
    let volumeMeta = null;
    const shouldShowLimit = maxVolumePercent < 100;
    const updateVolumeMeta = () => {
      if (!shouldShowLimit || !volumeMeta) return;
      const requested = normalizePercent(volInput.value, 0);
      const applied = computeEffectiveVolume(requested, maxVolumePercent);
      volumeMeta.textContent = `Output ${applied}% · Max ${maxVolumePercent}%`;
    };
    if (shouldShowLimit) {
      volumeMeta = document.createElement('div');
      volumeMeta.className = 'label';
      updateVolumeMeta();
    }
    volInput.addEventListener('input', () => {
      setRangeProgress(volInput, volInput.value, volInput.max || 100);
      updateVolumeMeta();
    });
    volInput.addEventListener('change', () => setNodeVolume(n.id, volInput.value));
    volRow.appendChild(volInput);
    if (volumeMeta) {
      volRow.appendChild(volumeMeta);
    }
    wrapper.appendChild(volRow);

    if (n.type === 'browser') {
      const panLabel = document.createElement('div');
      panLabel.className = 'label';
      let currentPan = typeof n.pan === 'number' ? n.pan : 0;
      panLabel.innerText = `Pan – ${describePan(currentPan)}`;
      wrapper.appendChild(panLabel);

      const panInput = document.createElement('input');
      panInput.type = 'range';
      panInput.min = -100;
      panInput.max = 100;
      panInput.step = 2;
      panInput.value = Math.round(currentPan * 100);
      setRangeProgress(panInput, panInput.value, panInput.max || 100);
      panInput.addEventListener('input', () => {
        const nextPan = Number(panInput.value) / 100;
        panLabel.innerText = `Pan – ${describePan(nextPan)}`;
        setRangeProgress(panInput, panInput.value, panInput.max || 100);
      });
      panInput.addEventListener('change', async () => {
        const nextPan = Math.max(-1, Math.min(1, Number(panInput.value) / 100));
        try {
          await setNodePan(n.id, nextPan);
          currentPan = nextPan;
        } catch (err) {
          panInput.value = Math.round(currentPan * 100);
          panLabel.innerText = `Pan – ${describePan(currentPan)}`;
        }
        setRangeProgress(panInput, panInput.value, panInput.max || 100);
      });
      wrapper.appendChild(panInput);
    }

    const actions = document.createElement('div');
    actions.className = 'node-actions';
    if (n.type !== 'browser' && updateAvailable) {
      const updateBtn = document.createElement('button');
      updateBtn.className = 'small-btn';
      updateBtn.textContent = updating ? 'Updating…' : 'Update node';
      updateBtn.disabled = !paired || restarting || updating;
      if (!updating) {
        updateBtn.addEventListener('click', () => updateNode(n.id, updateBtn));
      }
      actions.appendChild(updateBtn);
    }
    wrapper.appendChild(actions);

    nodesEl.appendChild(wrapper);
  });

  const nodeIds = new Set(nodes.map(n => n.id));
  Object.keys(camillaPendingNodes).forEach(id => {
    if (!nodeIds.has(id)) delete camillaPendingNodes[id];
  });
  refreshNodeSettingsModal();
}

function renderClients(groups, target) {
  target.innerHTML = '';
  groups.forEach(group => {
    group.clients.forEach(c => {
      const panel = document.createElement('div');
      panel.style.marginBottom = '10px';
      const row = document.createElement('div');
      row.style.display = 'grid';
      row.style.gridTemplateColumns = '1fr 120px';
      row.style.gap = '12px';
      row.style.alignItems = 'center';
      const label = document.createElement('div');
      label.innerHTML = `<div>${c.friendlyName || c.id}</div><div class="label">${group.stream?.name || 'Spotify'}</div>`;
      const input = document.createElement('input');
      input.type = 'range';
      input.min = 0;
      input.max = 100;
      input.value = c.config?.volume?.percent ?? 50;
      setRangeProgress(input, input.value, input.max || 100);
      input.addEventListener('input', () => setRangeProgress(input, input.value, input.max || 100));
      input.addEventListener('change', () => setVolume(c.id, input.value));
      row.appendChild(label);
      row.appendChild(input);
      panel.appendChild(row);
      target.appendChild(panel);
    });
  });
  if (!groups.length) {
    target.innerHTML = '<div class="muted">No clients connected yet.</div>';
  }
}

function appendDiscovered(items) {
  const list = Array.isArray(items) ? items : [items];
  list.forEach(item => {
    if (!item || !item.url) return;
    const row = document.createElement('div');
    row.className = 'panel discover-row';
    row.style.marginBottom = '8px';
    const title = document.createElement('div');
    const versionLabel = item.version ? `Agent ${item.version}` : 'Version unknown';
    title.innerHTML = `<strong>${item.host}</strong> <span class="muted">${item.url}</span><div class="label">${versionLabel}</div>`;
    const nameInput = document.createElement('input');
    const existing = findNodeByFingerprint(item.fingerprint);
    if (existing) {
      nameInput.value = existing.name || existing.id || `Node ${item.host}`;
      nameInput.disabled = true;
    } else {
      nameInput.value = `Node ${item.host}`;
    }
    nameInput.style.marginTop = '6px';
    const btn = document.createElement('button');
    btn.className = 'small-btn';
    btn.textContent = existing ? 'Relink node' : 'Register';
    btn.style.marginTop = '6px';
    btn.addEventListener('click', () => registerNodeWithName(
      existing ? existing.name : nameInput.value,
      item.url,
      btn,
      existing ? existing.id : undefined,
      item.fingerprint,
    ));
    row.appendChild(title);
    row.appendChild(nameInput);
    if (existing) {
      const hint = document.createElement('div');
      hint.className = 'label';
      hint.textContent = `Matches registered node “${existing.name || existing.id}”`;
      row.appendChild(hint);
    }
    row.appendChild(btn);
    discoverList.appendChild(row);
  });
}

async function fetchStatus() {
  try {
    clearMessages();
    const res = await fetch('/api/snapcast/status');
    await ensureOk(res);
    const data = await res.json();
    renderClients(data.server?.groups || [], clientsSettingsEl);
  } catch (err) {
    showError(`Failed to fetch snapcast status: ${err.message}`);
  }
}

async function fetchNodes(options = {}) {
  try {
    const res = await fetch('/api/nodes');
    await ensureOk(res);
    const data = await res.json();
    renderNodes(data.nodes || [], options);
  } catch (err) {
    showError(`Failed to load nodes: ${err.message}`);
  }
}

async function fetchSpotifyConfig() {
  try {
    const res = await fetch('/api/config/spotify');
    await ensureOk(res);
    const cfg = await res.json();
    spName.value = cfg.device_name || 'RoomCast';
    spBitrate.value = cfg.bitrate || 320;
    spInitVol.value = cfg.initial_volume ?? 75;
    setRangeProgress(spInitVol, spInitVol.value, spInitVol.max || 100);
    spNormalise.checked = cfg.normalisation ?? true;
    spClientId.value = cfg.client_id || '';
    spRedirect.value = cfg.redirect_uri || '';
    if (cfg.has_client_secret) spClientSecret.placeholder = 'stored';
    else spClientSecret.placeholder = 'client secret';
    if (spotifyLinkStatus) {
      if (cfg.has_oauth_token) {
        spotifyLinkStatus.textContent = 'Spotify account linked via OAuth';
        spotifyLinkStatus.className = 'status-pill ok';
      } else {
        spotifyLinkStatus.textContent = 'Spotify account not linked';
        spotifyLinkStatus.className = 'status-pill warn';
      }
    }
  } catch (err) {
    showError(`Failed to load Spotify config: ${err.message}`);
  }
}

async function fetchLibrespotStatus() {
  try {
    const res = await fetch('/api/librespot/status');
    await ensureOk(res);
    const data = await res.json();
    librespotStatus.innerText = `Status: ${data.state || 'unknown'}${data.message ? ' – ' + data.message : ''}`;
  } catch (err) {
    librespotStatus.innerText = `Failed to load status: ${err.message}`;
  }
}

async function discoverNodes() {
  if (discoverAbortController) {
    discoverAbortController.abort();
    discoverAbortController = null;
  }
  discoverResultsCount = 0;
  discoverList.innerHTML = '';
  discoverStatus.textContent = 'Preparing scan…';
  discoverSpinner.style.display = 'block';
  startDiscoverBtn.disabled = true;
  startDiscoverBtn.textContent = 'Scanning…';
  discoverAbortController = new AbortController();
  const decoder = new TextDecoder();
  let buffer = '';

  const processLine = (line) => {
    if (!line) return;
    let payload;
    try {
      payload = JSON.parse(line);
    } catch (err) {
      return;
    }
    if (payload.type === 'start') {
      const nets = Array.isArray(payload.networks) && payload.networks.length ? payload.networks.join(', ') : 'detected networks';
      const limited = payload.limited ? ' (limited – narrow networks for deeper scan)' : '';
      discoverStatus.textContent = `Scanning ${nets} (${payload.host_count ?? '?'} hosts)${limited}.`;
      return;
    }
    if (payload.type === 'discovered') {
      if (discoverResultsCount === 0) {
        discoverList.innerHTML = '';
      }
      discoverResultsCount += 1;
      appendDiscovered(payload.data);
      discoverStatus.textContent = `Found ${discoverResultsCount} node${discoverResultsCount === 1 ? '' : 's'} so far…`;
      return;
    }
    if (payload.type === 'complete') {
      discoverStatus.textContent = payload.found ? `Scan complete – ${payload.found} node${payload.found === 1 ? '' : 's'} found.` : 'Scan complete – no nodes responded.';
      return;
    }
    if (payload.type === 'cancelled') {
      discoverStatus.textContent = `Scan cancelled after ${payload.found || discoverResultsCount} result${(payload.found || discoverResultsCount) === 1 ? '' : 's'}.`;
      return;
    }
    if (payload.type === 'error') {
      discoverStatus.textContent = `Discovery error: ${payload.message || 'unknown error'}`;
    }
  };

  try {
    const res = await fetch('/api/nodes/discover', { signal: discoverAbortController.signal });
    await ensureOk(res);
    if (!res.body) throw new Error('Streaming not supported in this browser');
    const reader = res.body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx = buffer.indexOf('\n');
      while (idx >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        processLine(line);
        idx = buffer.indexOf('\n');
      }
    }
    const tail = buffer.trim();
    if (tail) processLine(tail);
  } catch (err) {
    if (err.name === 'AbortError') {
      discoverStatus.textContent = 'Scan cancelled.';
    } else {
      discoverStatus.textContent = `Failed to discover: ${err.message}`;
    }
  } finally {
    discoverSpinner.style.display = 'none';
    startDiscoverBtn.disabled = false;
    startDiscoverBtn.textContent = 'Scan';
    discoverAbortController = null;
    if (!discoverResultsCount && discoverList.children.length === 0) {
      discoverList.innerHTML = '<div class="muted">No agents found yet. Ensure nodes run on the same subnet and respond on port 9700.</div>';
    }
  }
}

async function setVolume(clientId, percent) {
  try {
    const res = await fetch(`/api/snapcast/clients/${clientId}/volume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent: Number(percent) }),
    });
    await ensureOk(res);
  } catch (err) {
    showError(`Failed to set volume: ${err.message}`);
  }
}

async function setNodeVolume(nodeId, percent) {
  try {
    const res = await fetch(`/api/nodes/${nodeId}/volume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent: Number(percent) }),
    });
    await ensureOk(res);
    showSuccess('Node volume updated');
  } catch (err) {
    showError(`Failed to set node volume: ${err.message}`);
  }
}

async function setNodeMaxVolume(nodeId, percent, sliderEl) {
  const value = normalizePercent(percent, 100);
  if (sliderEl) sliderEl.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/max-volume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent: value }),
    });
    await ensureOk(res);
    showSuccess('Max volume updated');
    await fetchNodes({ force: true });
  } catch (err) {
    showError(`Failed to set max volume: ${err.message}`);
  } finally {
    if (sliderEl) sliderEl.disabled = false;
  }
}

async function setNodePan(nodeId, pan) {
  const clamped = Math.max(-1, Math.min(1, Number(pan)));
  try {
    const res = await fetch(`/api/nodes/${nodeId}/pan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pan: clamped }),
    });
    await ensureOk(res);
    showSuccess('Web node pan updated');
  } catch (err) {
    showError(`Failed to set pan: ${err.message}`);
    throw err;
  }
}

async function toggleMute(nodeId, btn) {
  const currentlyMuted = btn.dataset.muted === 'true';
  btn.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/mute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ muted: !currentlyMuted }),
    });
    await ensureOk(res);
    const data = await res.json();
    const muted = data?.result?.muted ?? !currentlyMuted;
    applyMuteButtonState(btn, muted);
  } catch (err) {
    showError(`Failed to toggle mute: ${err.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function pairNode(nodeId, btn) {
  if (btn) btn.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/pair`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force: true }),
    });
    await ensureOk(res);
    showSuccess('Node paired');
    await fetchNodes();
  } catch (err) {
    showError(`Failed to pair node: ${err.message}`);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function renameNode(nodeId, currentName) {
  const nextName = prompt('Rename node', currentName || 'Node');
  if (nextName === null) return;
  const trimmed = nextName.trim();
  if (!trimmed) {
    showError('Node name cannot be empty');
    return;
  }
  try {
    const res = await fetch(`/api/nodes/${nodeId}/rename`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: trimmed }),
    });
    await ensureOk(res);
    showSuccess('Node renamed');
    await fetchNodes();
  } catch (err) {
    showError(`Failed to rename node: ${err.message}`);
  }
}

async function configureNode(nodeId, btn) {
  if (btn) btn.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/configure`, { method: 'POST' });
    await ensureOk(res);
    showSuccess('Node audio configured');
    await fetchNodes();
  } catch (err) {
    showError(`Failed to configure node: ${err.message}`);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function setNodeOutput(nodeId, deviceId, selectEl) {
  if (!deviceId) return;
  const originalValue = selectEl ? selectEl.value : null;
  if (selectEl) selectEl.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/outputs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ device: deviceId }),
    });
    await ensureOk(res);
    showSuccess('Audio output updated');
    await fetchNodes({ force: true });
  } catch (err) {
    showError(`Failed to set output: ${err.message}`);
    if (selectEl && originalValue !== null) {
      selectEl.value = originalValue;
    }
  } finally {
    if (selectEl) selectEl.disabled = false;
  }
}

async function checkNodeUpdates(nodeId, btn) {
  const originalLabel = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Checking…';
  }
  try {
    const res = await fetch(`/api/nodes/${nodeId}/check-updates`, { method: 'POST' });
    await ensureOk(res);
    const data = await res.json().catch(() => ({}));
    if (data?.update_available) {
      showSuccess('Update available for this node.');
    } else {
      showSuccess('Node is up to date.');
    }
    await fetchNodes();
  } catch (err) {
    showError(`Failed to check updates: ${err.message}`);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalLabel || 'Check for updates';
    }
  }
}

async function updateNode(nodeId, btn) {
  const originalLabel = btn ? btn.textContent : '';
  let failed = false;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Updating…';
  }
  try {
    const res = await fetch(`/api/nodes/${nodeId}/update`, { method: 'POST' });
    await ensureOk(res);
    showSuccess('Node update triggered. Agent will restart shortly.');
    fetchNodes();
    setTimeout(fetchNodes, 15000);
  } catch (err) {
    showError(`Failed to update node: ${err.message}`);
    failed = true;
  } finally {
    if (btn && failed) {
      btn.disabled = false;
      btn.textContent = originalLabel || 'Update node';
    }
  }
}

async function restartNode(nodeId, btn) {
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Restarting…';
  }
  try {
    const res = await fetch(`/api/nodes/${nodeId}/restart`, { method: 'POST' });
    await ensureOk(res);
    showSuccess('Node restart requested');
    await fetchNodes();
  } catch (err) {
    showError(`Failed to restart node: ${err.message}`);
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Restart node';
    }
  }
}

async function unregisterNode(nodeId) {
  const node = nodesCache.find(n => n.id === nodeId);
  const name = node?.name ? `"${node.name}"` : 'this node';
  const confirmed = window.confirm(`Are you sure you want to unregister ${name}?`);
  if (!confirmed) return;
  try {
    const res = await fetch(`/api/nodes/${nodeId}`, { method: 'DELETE' });
    await ensureOk(res);
    showSuccess('Node unregistered');
    await fetchNodes();
  } catch (err) {
    showError(`Failed to unregister node: ${err.message}`);
  }
}

function closeNodeSettingsModal() {
  if (nodeSettingsModal) {
    nodeSettingsModal.remove();
    nodeSettingsModal = null;
    nodeSettingsContent = null;
    nodeSettingsTitle = null;
  }
  nodeSettingsNodeId = null;
}

function openNodeSettingsModal(nodeId) {
  nodeSettingsNodeId = nodeId;
  if (!nodeSettingsModal) {
    nodeSettingsModal = document.createElement('div');
    nodeSettingsModal.className = 'settings-overlay';
    const card = document.createElement('div');
    card.className = 'settings-card';
    const header = document.createElement('div');
    header.className = 'settings-header';
    nodeSettingsTitle = document.createElement('div');
    nodeSettingsTitle.className = 'section-title';
    nodeSettingsTitle.style.margin = '0';
    header.appendChild(nodeSettingsTitle);
    const closeBtn = document.createElement('button');
    closeBtn.className = 'icon-btn';
    closeBtn.textContent = '✕';
    closeBtn.addEventListener('click', closeNodeSettingsModal);
    header.appendChild(closeBtn);
    card.appendChild(header);
    nodeSettingsContent = document.createElement('div');
    nodeSettingsContent.className = 'node-settings-content';
    card.appendChild(nodeSettingsContent);
    nodeSettingsModal.appendChild(card);
    nodeSettingsModal.addEventListener('click', (evt) => {
      if (evt.target === nodeSettingsModal) closeNodeSettingsModal();
    });
    document.body.appendChild(nodeSettingsModal);
  }
  nodeSettingsModal.style.display = 'flex';
  renderNodeSettingsContent();
}

function refreshNodeSettingsModal() {
  if (nodeSettingsModal && nodeSettingsNodeId) {
    renderNodeSettingsContent();
  }
}

function renderNodeSettingsContent() {
  if (!nodeSettingsModal || !nodeSettingsContent || !nodeSettingsNodeId) return;
  const node = nodesCache.find(n => n.id === nodeSettingsNodeId);
  if (!node) {
    closeNodeSettingsModal();
    return;
  }
  nodeSettingsTitle.textContent = `Node settings – ${node.name || 'Node'}`;
  nodeSettingsContent.innerHTML = '';

  const isBrowser = node.type === 'browser';
  const paired = !!node.paired;
  const configured = isBrowser ? true : !!node.configured;
  const online = isBrowser ? true : node.online !== false;
  const restarting = !!node.restarting;
  const updating = !!node.updating;
  const updateAvailable = hasAgentUpdate(node);
  const outputs = node.outputs || {};
  const outputOptions = Array.isArray(outputs.options) ? outputs.options : [];
  const selectedOutput = outputs.selected || node.playback_device || '';
  const disableOutputs = !isBrowser && (!paired || !configured || restarting || !online || updating);

  const detailsPanel = document.createElement('div');
  detailsPanel.className = 'panel';
  const detailsTitle = document.createElement('div');
  detailsTitle.className = 'section-title';
  detailsTitle.textContent = 'Details';
  detailsPanel.appendChild(detailsTitle);
  detailsPanel.appendChild(createMetaRow('Node type', isBrowser ? 'Browser node' : 'Hardware node'));
  if (node.url) {
    detailsPanel.appendChild(createMetaRow('Endpoint', node.url));
  }
  const host = describeNodeHost(node.url);
  if (host && host !== node.url) {
    detailsPanel.appendChild(createMetaRow('Host/IP', host));
  }
  if (node.agent_version) {
    let versionText = `Agent ${node.agent_version}`;
    if (updating) versionText += ' (updating…)';
    else if (updateAvailable && node.latest_agent_version) versionText += ` → ${node.latest_agent_version}`;
    detailsPanel.appendChild(createMetaRow('Version', versionText));
  }
  if (node.fingerprint) {
    detailsPanel.appendChild(createMetaRow('Fingerprint', node.fingerprint));
  }
  nodeSettingsContent.appendChild(detailsPanel);

  if (!isBrowser) {
    const audioPanel = document.createElement('div');
    audioPanel.className = 'panel';
    const audioTitle = document.createElement('div');
    audioTitle.className = 'section-title';
    audioTitle.textContent = 'Audio output';
    audioPanel.appendChild(audioTitle);
    if (!outputOptions.length) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'No outputs detected yet.';
      audioPanel.appendChild(empty);
      if (selectedOutput) {
        audioPanel.appendChild(createMetaRow('Current device', selectedOutput));
      }
    } else {
      const select = document.createElement('select');
      select.style.width = '100%';
      const mappedOptions = [...outputOptions];
      if (selectedOutput && !mappedOptions.some(opt => opt.id === selectedOutput)) {
        mappedOptions.unshift({ id: selectedOutput, label: `${selectedOutput} (current)` });
      }
      mappedOptions.forEach(opt => {
        const optionEl = document.createElement('option');
        optionEl.value = opt.id;
        optionEl.textContent = opt.label || opt.id;
        select.appendChild(optionEl);
      });
      if (selectedOutput) {
        select.value = selectedOutput;
      }
      select.disabled = disableOutputs;
      select.addEventListener('change', () => {
        if (!select.value) return;
        setNodeOutput(node.id, select.value, select);
      });
      audioPanel.appendChild(select);
    }
    nodeSettingsContent.appendChild(audioPanel);
  }

  const limitsPanel = document.createElement('div');
  limitsPanel.className = 'panel';
  const limitsTitle = document.createElement('div');
  limitsTitle.className = 'section-title';
  limitsTitle.textContent = 'Playback limit';
  limitsPanel.appendChild(limitsTitle);
  const currentMaxVolume = normalizePercent(node.max_volume_percent, 100);
  const requestedVolume = normalizePercent(node.volume_percent, 75);
  const appliedVolume = computeEffectiveVolume(requestedVolume, currentMaxVolume);
  const maxLabel = document.createElement('div');
  maxLabel.className = 'label';
  const updateMaxLabel = (value, applied) => {
    maxLabel.textContent = `Max volume – ${value}% (current output ${applied}%)`;
  };
  updateMaxLabel(currentMaxVolume, appliedVolume);
  limitsPanel.appendChild(maxLabel);
  const maxSlider = document.createElement('input');
  maxSlider.type = 'range';
  maxSlider.min = 0;
  maxSlider.max = 100;
  maxSlider.value = currentMaxVolume;
  setRangeProgress(maxSlider, maxSlider.value, maxSlider.max || 100);
  const disableMaxVolume = !isBrowser && (!paired || !online || restarting || updating);
  maxSlider.disabled = disableMaxVolume;
  maxSlider.addEventListener('input', () => {
    setRangeProgress(maxSlider, maxSlider.value, maxSlider.max || 100);
    const nextValue = normalizePercent(maxSlider.value, currentMaxVolume);
    const applied = computeEffectiveVolume(requestedVolume, nextValue);
    updateMaxLabel(nextValue, applied);
  });
  maxSlider.addEventListener('change', async () => {
    const nextValue = normalizePercent(maxSlider.value, currentMaxVolume);
    if (nextValue === currentMaxVolume) return;
    try {
      await setNodeMaxVolume(node.id, nextValue, maxSlider);
    } finally {
      setRangeProgress(maxSlider, maxSlider.value, maxSlider.max || 100);
    }
  });
  limitsPanel.appendChild(maxSlider);
  const maxHelp = document.createElement('div');
  maxHelp.className = 'muted';
  maxHelp.style.fontSize = '12px';
  maxHelp.style.marginTop = '8px';
  maxHelp.textContent = 'Caps the usable range of the regular volume fader for this node.';
  limitsPanel.appendChild(maxHelp);
  nodeSettingsContent.appendChild(limitsPanel);

  const actionsPanel = document.createElement('div');
  actionsPanel.className = 'panel';
  const actionsTitle = document.createElement('div');
  actionsTitle.className = 'section-title';
  actionsTitle.textContent = 'Actions';
  actionsPanel.appendChild(actionsTitle);
  const actionStack = document.createElement('div');
  actionStack.className = 'modal-actions';

  actionStack.appendChild(createNodeSettingsAction('Rename node', () => renameNode(node.id, node.name)));
  if (!isBrowser) {
    const pairBtn = createNodeSettingsAction(paired ? 'Rotate key' : 'Pair node', (btn) => pairNode(node.id, btn), { disabled: restarting });
    if (!paired) {
      pairBtn.style.background = 'linear-gradient(135deg, #f97316, #ea580c)';
    }
    actionStack.appendChild(pairBtn);

    const configBtn = createNodeSettingsAction(configured ? 'Reconfigure audio' : 'Configure audio', (btn) => configureNode(node.id, btn), { disabled: !paired || restarting });
    if (!configured) {
      configBtn.style.background = 'linear-gradient(135deg, #22c55e, #16a34a)';
    }
    actionStack.appendChild(configBtn);

    const restartBtn = createNodeSettingsAction(restarting ? 'Restarting…' : 'Restart node', (btn) => restartNode(node.id, btn), { disabled: restarting });
    actionStack.appendChild(restartBtn);

    const checkBtn = createNodeSettingsAction('Check for updates', (btn) => checkNodeUpdates(node.id, btn), { disabled: restarting || updating });
    actionStack.appendChild(checkBtn);

    if (updateAvailable) {
      const updateBtn = createNodeSettingsAction(updating ? 'Updating…' : 'Update node', (btn) => updateNode(node.id, btn), { disabled: restarting || updating });
      actionStack.appendChild(updateBtn);
    }
  }
  const unregisterBtn = createNodeSettingsAction('Unregister node', () => {
    closeNodeSettingsModal();
    unregisterNode(node.id);
  }, { danger: true });
  actionStack.appendChild(unregisterBtn);
  actionsPanel.appendChild(actionStack);
  nodeSettingsContent.appendChild(actionsPanel);
}

function createNodeSettingsAction(label, handler, options = {}) {
  const btn = document.createElement('button');
  btn.textContent = label;
  if (options.danger) {
    btn.style.background = 'linear-gradient(135deg, #ef4444, #dc2626)';
    btn.style.color = '#fff';
  } else if (options.secondary) {
    btn.style.background = 'rgba(255,255,255,0.08)';
    btn.style.color = '#e2e8f0';
  }
  btn.disabled = !!options.disabled;
  if (typeof handler === 'function') {
    btn.addEventListener('click', () => handler(btn));
  }
  return btn;
}

function createMetaRow(labelText, valueText) {
  const row = document.createElement('div');
  row.className = 'node-meta-row';
  const label = document.createElement('div');
  label.className = 'label';
  label.textContent = labelText;
  label.style.marginTop = '0';
  const value = document.createElement('div');
  value.className = 'node-meta-value';
  value.textContent = valueText || '—';
  row.appendChild(label);
  row.appendChild(value);
  return row;
}

function clampValue(val, min, max) {
  return Math.min(max, Math.max(min, val));
}

function isEqDirty(nodeId) {
  return !!eqDirtyNodes[nodeId];
}

function markEqDirty(nodeId) {
  eqDirtyNodes[nodeId] = true;
}

function markEqClean(nodeId) {
  eqDirtyNodes[nodeId] = false;
}

function eqBandsMatch(a = [], b = []) {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    const bandA = a[i] || {};
    const bandB = b[i] || {};
    if (Math.abs(Number(bandA.freq || 0) - Number(bandB.freq || 0)) > 0.01) return false;
    if (Math.abs(Number(bandA.gain || 0) - Number(bandB.gain || 0)) > 0.01) return false;
    if (Math.abs(Number(bandA.q || 0) - Number(bandB.q || 0)) > 0.01) return false;
  }
  return true;
}

function eqStatesMatch(a, b) {
  if (!a || !b) return false;
  if (a.mode !== b.mode) return false;
  return eqBandsMatch(a.bands, b.bands);
}

function defaultEqBands(mode) {
  const freqs = EQ_FREQUENCIES[mode] || EQ_FREQUENCIES.peq15;
  return freqs.map(freq => ({ freq, gain: 0, q: 1 }));
}

function getEqSkin() {
  return eqSkin === 'faders' ? 'faders' : 'classic';
}

function setEqSkin(nextSkin) {
  const normalized = nextSkin === 'faders' ? 'faders' : 'classic';
  if (eqSkin === normalized) return;
  eqSkin = normalized;
  persistEqSkinPreference(eqSkin);
}

function normalizeEqPayload(payload) {
  if (!payload) return { mode: 'peq15', bands: defaultEqBands('peq15') };
  const sourceBands = Array.isArray(payload.bands) ? payload.bands : [];
  const mode = (payload.preset === 'peq31' || Number(payload.band_count) >= 31) ? 'peq31' : 'peq15';
  const template = defaultEqBands(mode);
  template.forEach((band, idx) => {
    const src = sourceBands[idx];
    if (!src) return;
    band.freq = clampValue(Number(src.freq) || band.freq, 20, 20000);
    band.gain = clampValue(Number(src.gain) || 0, EQ_GAIN_RANGE.min, EQ_GAIN_RANGE.max);
    band.q = clampValue(Number(src.q) || 1, EQ_Q_RANGE.min, EQ_Q_RANGE.max);
  });
  return { mode, bands: template };
}

function hydrateEqFromNode(node) {
  if (!node || !node.id) return;
  const normalized = normalizeEqPayload(node.eq);
  const current = eqState[node.id];
  if (!current) {
    eqState[node.id] = normalized;
    markEqClean(node.id);
    return;
  }
  if (isEqDirty(node.id)) return;
  if (!eqStatesMatch(current, normalized)) {
    eqState[node.id] = normalized;
  }
  markEqClean(node.id);
}

function getEqState(nodeId) {
  if (!eqState[nodeId]) {
    eqState[nodeId] = { mode: 'peq15', bands: defaultEqBands('peq15') };
  }
  return eqState[nodeId];
}

function setEqMode(nodeId, mode) {
  const target = EQ_FREQUENCIES[mode] ? mode : 'peq15';
  const nextBands = defaultEqBands(target);
  const current = getEqState(nodeId).bands;
  nextBands.forEach((band, idx) => {
    if (current[idx]) {
      band.freq = clampValue(current[idx].freq, 20, 20000);
      band.gain = clampValue(current[idx].gain, EQ_GAIN_RANGE.min, EQ_GAIN_RANGE.max);
      band.q = clampValue(current[idx].q, EQ_Q_RANGE.min, EQ_Q_RANGE.max);
    }
  });
  eqState[nodeId] = { mode: target, bands: nextBands };
  markEqDirty(nodeId);
}

function freqToSlider(freq) {
  const logF = Math.log10(clampValue(freq, 20, 20000));
  const ratio = (logF - LOG_FREQ.min) / (LOG_FREQ.max - LOG_FREQ.min);
  return Math.round(ratio * 1000);
}

function sliderToFreq(value) {
  const ratio = clampValue(value, 0, 1000) / 1000;
  const logF = LOG_FREQ.min + ratio * (LOG_FREQ.max - LOG_FREQ.min);
  return Math.pow(10, logF);
}

function scheduleEqPush(nodeId) {
  clearTimeout(eqUpdateTimers[nodeId]);
  eqUpdateTimers[nodeId] = setTimeout(() => pushEq(nodeId), EQ_PUSH_DEBOUNCE_MS);
}

function handleCamillaPending(nodeId, pending) {
  const prev = camillaPendingNodes[nodeId] || false;
  camillaPendingNodes[nodeId] = pending;
  if (prev === pending) return;
  const node = nodesCache.find(n => n.id === nodeId);
  const label = node?.name || 'Node';
  if (pending) {
    showError(`${label}: EQ queued until DSP is available`);
  } else {
    showSuccess(`${label}: EQ applied after DSP recovery`);
  }
}

async function pushEq(nodeId) {
  const state = getEqState(nodeId);
  const payload = {
    preset: state.mode,
    band_count: state.bands.length,
    bands: state.bands.map(b => ({
      freq: Number(b.freq),
      gain: Number(b.gain),
      q: Number(b.q),
    })),
  };
  try {
    const res = await fetch(`/api/nodes/${nodeId}/eq`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    await ensureOk(res);
    let data = null;
    try {
      data = await res.json();
    } catch (_) {
      data = null;
    }
    const node = nodesCache.find(n => n.id === nodeId);
    if (node) {
      node.eq = {
        preset: payload.preset,
        band_count: payload.band_count,
        bands: payload.bands.map(b => ({ ...b })),
      };
    }
    markEqClean(nodeId);
    const pending = !!data?.result?.camilla_pending;
    handleCamillaPending(nodeId, pending);
  } catch (err) {
    showError(`Failed to update EQ: ${err.message}`);
  }
}

function renderEqBandsClassic(nodeId, container) {
  const state = getEqState(nodeId);
  container.innerHTML = '';
  state.bands.forEach((band, idx) => {
    const row = document.createElement('div');
    row.className = 'eq-band-row';

    const label = document.createElement('div');
    label.className = 'eq-band-label';
    label.textContent = `#${idx + 1}`;
    row.appendChild(label);

    const freqCell = document.createElement('div');
    freqCell.className = 'eq-band-cell';
    const freqLabel = document.createElement('label');
    freqLabel.textContent = 'Freq (Hz)';
    const freqInput = document.createElement('input');
    freqInput.type = 'number';
    freqInput.min = 20;
    freqInput.max = 20000;
    freqInput.value = Math.round(band.freq);
    freqInput.addEventListener('change', () => {
      band.freq = clampValue(Number(freqInput.value) || band.freq, 20, 20000);
      freqSlider.value = freqToSlider(band.freq);
      setRangeProgress(freqSlider, freqSlider.value, freqSlider.max || 1000);
      markEqDirty(nodeId);
      scheduleEqPush(nodeId);
    });
    const freqSlider = document.createElement('input');
    freqSlider.type = 'range';
    freqSlider.min = 0;
    freqSlider.max = 1000;
    freqSlider.value = freqToSlider(band.freq);
    setRangeProgress(freqSlider, freqSlider.value, freqSlider.max || 1000);
    freqSlider.addEventListener('input', () => {
      band.freq = clampValue(sliderToFreq(Number(freqSlider.value)), 20, 20000);
      freqInput.value = Math.round(band.freq);
      setRangeProgress(freqSlider, freqSlider.value, freqSlider.max || 1000);
      markEqDirty(nodeId);
      scheduleEqPush(nodeId);
    });
    freqCell.appendChild(freqLabel);
    freqCell.appendChild(freqInput);
    freqCell.appendChild(freqSlider);
    row.appendChild(freqCell);

    const gainCell = document.createElement('div');
    gainCell.className = 'eq-band-cell';
    const gainLabel = document.createElement('label');
    gainLabel.textContent = 'Gain (dB)';
    const gainSlider = document.createElement('input');
    gainSlider.type = 'range';
    gainSlider.min = EQ_GAIN_RANGE.min;
    gainSlider.max = EQ_GAIN_RANGE.max;
    gainSlider.step = 0.5;
    gainSlider.value = band.gain;
    setRangeProgress(gainSlider, gainSlider.value, gainSlider.max || 1);
    const gainValue = document.createElement('div');
    gainValue.className = 'muted';
    gainValue.textContent = `${band.gain.toFixed(1)} dB`;
    gainSlider.addEventListener('input', () => {
      band.gain = clampValue(Number(gainSlider.value), EQ_GAIN_RANGE.min, EQ_GAIN_RANGE.max);
      gainValue.textContent = `${band.gain.toFixed(1)} dB`;
      setRangeProgress(gainSlider, gainSlider.value, gainSlider.max || 1);
      markEqDirty(nodeId);
      scheduleEqPush(nodeId);
    });
    gainCell.appendChild(gainLabel);
    gainCell.appendChild(gainSlider);
    gainCell.appendChild(gainValue);
    row.appendChild(gainCell);

    const qCell = document.createElement('div');
    qCell.className = 'eq-band-cell';
    const qLabel = document.createElement('label');
    qLabel.textContent = 'Q';
    const qInput = document.createElement('input');
    qInput.type = 'number';
    qInput.step = 0.1;
    qInput.min = EQ_Q_RANGE.min;
    qInput.max = EQ_Q_RANGE.max;
    qInput.value = band.q.toFixed(2);
    qInput.addEventListener('change', () => {
      band.q = clampValue(Number(qInput.value) || band.q, EQ_Q_RANGE.min, EQ_Q_RANGE.max);
      markEqDirty(nodeId);
      scheduleEqPush(nodeId);
    });
    qCell.appendChild(qLabel);
    qCell.appendChild(qInput);
    row.appendChild(qCell);

    container.appendChild(row);
  });
}

function renderEqBandsFaders(nodeId, container) {
  const state = getEqState(nodeId);
  container.innerHTML = '';
  const view = document.createElement('div');
  view.className = 'eq-fader-view';
  state.bands.forEach((band, idx) => {
    const fader = document.createElement('div');
    fader.className = 'eq-fader';

    const meta = document.createElement('div');
    meta.className = 'eq-fader-meta';
    const bandIndex = document.createElement('span');
    bandIndex.textContent = `#${idx + 1}`;
    const freqLabel = document.createElement('span');
    freqLabel.textContent = `${Math.round(band.freq)} Hz`;
    meta.appendChild(bandIndex);
    meta.appendChild(freqLabel);
    fader.appendChild(meta);

    const sliderWrap = document.createElement('div');
    sliderWrap.className = 'eq-fader-slider-wrap';
    const track = document.createElement('div');
    track.className = 'eq-fader-track';
    sliderWrap.appendChild(track);
    const gainSlider = document.createElement('input');
    gainSlider.type = 'range';
    gainSlider.className = 'eq-fader-slider';
    gainSlider.setAttribute('orient', 'vertical');
    gainSlider.setAttribute('aria-label', `Gain ${Math.round(band.freq)} Hz`);
    gainSlider.min = EQ_GAIN_RANGE.min;
    gainSlider.max = EQ_GAIN_RANGE.max;
    gainSlider.step = 0.5;
    gainSlider.value = band.gain.toFixed(1);
    const gainValue = document.createElement('div');
    gainValue.className = 'eq-fader-gain';
    gainValue.textContent = `${band.gain.toFixed(1)} dB`;
    const knob = document.createElement('div');
    knob.className = 'eq-fader-thumb';
    gainSlider.addEventListener('input', () => {
      band.gain = clampValue(Number(gainSlider.value), EQ_GAIN_RANGE.min, EQ_GAIN_RANGE.max);
      gainValue.textContent = `${band.gain.toFixed(1)} dB`;
      updateFaderThumbPosition(sliderWrap, gainSlider, knob);
      markEqDirty(nodeId);
      scheduleEqPush(nodeId);
    });
    sliderWrap.appendChild(gainSlider);
    sliderWrap.appendChild(knob);
    attachFaderPointerHandlers(sliderWrap, gainSlider);
    fader.appendChild(sliderWrap);
    fader.appendChild(gainValue);
    view.appendChild(fader);
  });
  container.appendChild(view);
  refreshFaderThumbs(view);
}

function refreshFaderThumbs(root = document) {
  const scope = root instanceof Element ? root : document;
  requestAnimationFrame(() => {
    scope.querySelectorAll('.eq-fader-slider').forEach(slider => {
      const wrap = slider.closest('.eq-fader-slider-wrap');
      const knob = wrap?.querySelector('.eq-fader-thumb');
      if (wrap && knob) {
        updateFaderThumbPosition(wrap, slider, knob);
      }
    });
  });
}

function updateFaderThumbPosition(wrap, slider, knob) {
  if (!wrap || !slider || !knob) return;
  const track = wrap.querySelector('.eq-fader-track');
  if (!track) return;
  const min = Number(slider.min);
  const max = Number(slider.max);
  const range = Math.max(1, max - min);
  const valueRatio = (Number(slider.value) - min) / range;
  const clampedRatio = clampValue(valueRatio, 0, 1);
  const trackTop = track.offsetTop;
  const trackHeight = track.offsetHeight;
  const y = trackTop + trackHeight - (trackHeight * clampedRatio);
  knob.style.setProperty('--fader-thumb-top', `${y}px`);
}

function attachFaderPointerHandlers(wrap, slider) {
  if (!wrap || !slider) return;
  const handleMove = (event) => {
    updateSliderValueFromPointer(event, wrap, slider);
  };
  const release = (event) => {
    wrap.classList.remove('is-dragging');
    wrap.removeEventListener('pointermove', handleMove);
    wrap.removeEventListener('pointerup', release);
    wrap.removeEventListener('pointercancel', release);
    wrap.releasePointerCapture?.(event.pointerId);
  };
  wrap.addEventListener('pointerdown', (event) => {
    if (event.button !== 0 && event.pointerType === 'mouse') return;
    event.preventDefault();
    slider.focus();
    if (event.ctrlKey || event.metaKey) {
      wrap.classList.remove('is-dragging');
      const resetValue = formatSliderValue(0, slider);
      if (slider.value !== resetValue) {
        slider.value = resetValue;
        slider.dispatchEvent(new Event('input', { bubbles: true }));
      }
      return;
    }
    wrap.classList.add('is-dragging');
    wrap.setPointerCapture?.(event.pointerId);
    updateSliderValueFromPointer(event, wrap, slider);
    wrap.addEventListener('pointermove', handleMove);
    wrap.addEventListener('pointerup', release);
    wrap.addEventListener('pointercancel', release);
  });
}

function updateSliderValueFromPointer(event, wrap, slider) {
  const track = wrap.querySelector('.eq-fader-track');
  const rect = (track || wrap).getBoundingClientRect();
  const height = rect.height || 1;
  const ratio = clampValue((rect.bottom - event.clientY) / height, 0, 1);
  const min = Number(slider.min);
  const max = Number(slider.max);
  const range = max - min;
  const step = slider.step && slider.step !== 'any' ? Number(slider.step) : null;
  let nextValue = min + ratio * range;
  if (step) {
    const steps = Math.round((nextValue - min) / step);
    nextValue = min + steps * step;
  }
  nextValue = clampValue(nextValue, min, max);
  const previous = Number(slider.value);
  if (Math.abs(previous - nextValue) < 1e-6) return;
  slider.value = formatSliderValue(nextValue, slider);
  slider.dispatchEvent(new Event('input', { bubbles: true }));
}

function formatSliderValue(value, slider) {
  const step = slider.step && slider.step !== 'any' ? Number(slider.step) : null;
  if (!step) return String(Math.round(value));
  const decimals = (step.toString().split('.')[1] || '').length;
  return value.toFixed(decimals);
}

window.addEventListener('resize', () => refreshFaderThumbs());


function renderEqBands(nodeId, container) {
  const isFaderSkin = getEqSkin() === 'faders';
  container.classList.toggle('is-fader-view', isFaderSkin);
  if (isFaderSkin) {
    renderEqBandsFaders(nodeId, container);
  } else {
    renderEqBandsClassic(nodeId, container);
  }
}

function openEqModal(nodeId, nodeName) {
  const node = nodesCache.find(n => n.id === nodeId);
  if (node?.eq && !isEqDirty(nodeId)) {
    eqState[nodeId] = normalizeEqPayload(node.eq);
    markEqClean(nodeId);
  } else if (!eqState[nodeId]) {
    getEqState(nodeId);
    markEqClean(nodeId);
  }
  if (eqModal) eqModal.remove();
  eqModal = document.createElement('div');
  eqModal.className = 'settings-overlay';
  eqModal.style.display = 'flex';
  const card = document.createElement('div');
  card.className = 'settings-card';
  const header = document.createElement('div');
  header.className = 'settings-header';
  header.innerHTML = `<div class="section-title" style="margin:0;">EQ – ${nodeName}</div>`;
  const closeBtn = document.createElement('button');
  closeBtn.className = 'icon-btn';
  closeBtn.textContent = '✕';
  closeBtn.addEventListener('click', () => eqModal.remove());
  header.appendChild(closeBtn);
  card.appendChild(header);

  const controls = document.createElement('div');
  controls.className = 'eq-toolbar';
  const skinSelect = document.createElement('select');
  skinSelect.className = 'eq-skin-select';
  skinSelect.innerHTML = '<option value="classic">Detailed controls</option><option value="faders">Fader view</option>';
  skinSelect.value = getEqSkin();
  skinSelect.addEventListener('change', () => {
    setEqSkin(skinSelect.value);
    renderEqBands(nodeId, bandList);
  });
  const modeSelect = document.createElement('select');
  modeSelect.innerHTML = '<option value="peq15">15-band parametric</option><option value="peq31">31-band parametric</option>';
  modeSelect.value = getEqState(nodeId).mode;
  modeSelect.addEventListener('change', () => {
    setEqMode(nodeId, modeSelect.value);
    renderEqBands(nodeId, bandList);
    scheduleEqPush(nodeId);
  });
  const resetBtn = document.createElement('button');
  resetBtn.className = 'small-btn';
  resetBtn.textContent = 'Reset';
  resetBtn.addEventListener('click', () => {
    eqState[nodeId] = { mode: modeSelect.value, bands: defaultEqBands(modeSelect.value) };
    markEqDirty(nodeId);
    renderEqBands(nodeId, bandList);
    scheduleEqPush(nodeId);
  });
  const savePresetBtn = document.createElement('button');
  savePresetBtn.className = 'small-btn';
  savePresetBtn.textContent = 'Save preset';
  savePresetBtn.addEventListener('click', () => savePreset(nodeId, presetList, modeSelect, bandList));
  controls.appendChild(skinSelect);
  controls.appendChild(modeSelect);
  controls.appendChild(resetBtn);
  controls.appendChild(savePresetBtn);
  card.appendChild(controls);

  const bandList = document.createElement('div');
  bandList.className = 'eq-band-list';
  renderEqBands(nodeId, bandList);
  card.appendChild(bandList);

  const presetWrap = document.createElement('div');
  presetWrap.className = 'panel';
  presetWrap.style.marginTop = '12px';
  const presetTitle = document.createElement('div');
  presetTitle.className = 'section-title';
  presetTitle.innerText = 'Presets';
  presetWrap.appendChild(presetTitle);
  const presetList = document.createElement('div');
  presetList.id = 'preset-list';
  presetList.className = 'muted';
  presetWrap.appendChild(presetList);
  card.appendChild(presetWrap);

  eqModal.appendChild(card);
  document.body.appendChild(eqModal);
  renderPresets(nodeId, presetList, modeSelect, bandList);
}

function savePreset(nodeId, presetList, modeSelect, bandList) {
  const name = prompt('Preset name?');
  if (!name) return;
  const presets = JSON.parse(localStorage.getItem('eq-presets') || '{}');
  const state = getEqState(nodeId);
  presets[name] = { mode: state.mode, bands: state.bands.map(b => ({ ...b })) };
  localStorage.setItem('eq-presets', JSON.stringify(presets));
  renderPresets(nodeId, presetList, modeSelect, bandList);
  showSuccess('Preset saved');
}

function renderPresets(nodeId, container, modeSelect, bandList) {
  const presets = JSON.parse(localStorage.getItem('eq-presets') || '{}');
  const entries = Object.entries(presets);
  if (!entries.length) {
    container.innerHTML = 'No presets saved yet.';
    return;
  }
  container.innerHTML = '';
  entries.forEach(([name, data]) => {
    const row = document.createElement('div');
    row.className = 'node-actions';
    const label = document.createElement('div');
    label.textContent = name;
    const apply = document.createElement('button');
    apply.className = 'small-btn';
    apply.textContent = 'Apply';
    apply.addEventListener('click', () => {
      eqState[nodeId] = normalizeEqPayload(data);
      modeSelect.value = getEqState(nodeId).mode;
      markEqDirty(nodeId);
      renderEqBands(nodeId, bandList);
      scheduleEqPush(nodeId);
    });
    row.appendChild(label);
    row.appendChild(apply);
    container.appendChild(row);
  });
}

async function setMasterVolume(percent) {
  try {
    const res = await fetch('/api/snapcast/master-volume', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent: Number(percent) }),
    });
    await ensureOk(res);
  } catch (err) {
    showError(`Failed to set master volume: ${err.message}`);
  }
}

function msToTime(ms) {
  const total = Math.floor(ms / 1000);
  const m = Math.floor(total / 60);
  const s = String(total % 60).padStart(2, '0');
  return `${m}:${s}`;
}

function formatDurationHuman(ms) {
  const totalSeconds = Math.floor(ms / 1000);
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) return '0 min';
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (hours > 0) parts.push(`${hours} hr${hours === 1 ? '' : 's'}`);
  if (minutes > 0 || !parts.length) {
    parts.push(`${minutes} min`);
  } else if (seconds > 0) {
    parts.push(`${seconds} sec`);
  }
  if (parts.length === 1 && minutes === 0 && seconds > 0) {
    parts[0] = `${seconds} sec`;
  }
  return parts.join(' ');
}

async function fetchPlayerStatus() {
  try {
    const res = await fetch('/api/spotify/player/status');
    await ensureOk(res);
    playerStatus = await res.json();
    renderPlayer(playerStatus);
    markSpotifyHealthy();
  } catch (err) {
    if (playerTick) {
      clearInterval(playerTick);
      playerTick = null;
    }
    playerStatus = null;
    playerPanel.style.display = 'flex';
    playerTitle.textContent = 'Player unavailable';
    playerArtist.textContent = '';
    playerSeek.disabled = true;
    playerTimeCurrent.textContent = '0:00';
    playerTimeTotal.textContent = '0:00';
    playerArt.style.display = 'none';
    playerArt.alt = '';
    setPlayerArtInteractivity(false);
    setRangeProgress(playerSeek, 0, playerSeek.max || 1);
    setPlayButtonIcon(false);
    playerPrev.disabled = true;
    playerPlay.disabled = true;
    playerNext.disabled = true;
    if (playerShuffleBtn) playerShuffleBtn.disabled = true;
    if (playerRepeatBtn) playerRepeatBtn.disabled = true;
    setShuffleActive(false);
    setRepeatMode('off');
    setTakeoverBannerVisible(false);
    reportSpotifyError(err);
  }
}

function renderPlayer(status) {
  const item = status?.item || {};
  const active = !!status?.active;
  updateActivePlaylistContext(status);
  updateTakeoverBanner(status);
  activeDeviceId = active && status?.device?.id ? status.device.id : null;
  playerPanel.style.display = 'flex';
  playerTitle.textContent = active ? (item.name || '—') : 'No active playback';
  const artists = (item.artists || []).map(a => a.name).join(', ');
  playerArtist.textContent = active ? (artists || '—') : '';
  const art = active ? (item.album?.images?.[1]?.url || item.album?.images?.[0]?.url) : null;
  if (art) {
    playerArt.src = art;
    playerArt.alt = item?.name ? `${item.name} cover art` : 'Album art';
    playerArt.style.display = 'block';
    setPlayerArtInteractivity(true);
  } else {
    playerArt.style.display = 'none';
    playerArt.alt = '';
    setPlayerArtInteractivity(false);
  }
  lastCoverArtUrl = art || null;
  applyCoverArtBackground();
  const duration = item.duration_ms || 0;
  const progress = status?.progress_ms || 0;
  playerSeek.max = duration || 1;
  playerSeek.value = progress;
  setRangeProgress(playerSeek, progress, duration || 1);
  playerTimeCurrent.textContent = msToTime(progress);
  playerTimeTotal.textContent = msToTime(duration);
  const playing = !!status?.is_playing && active;
  setPlayButtonIcon(playing);
  playerPrev.disabled = !active;
  playerPlay.disabled = !active;
  playerNext.disabled = !active;
  playerSeek.disabled = !active;
  if (playerShuffleBtn) {
    playerShuffleBtn.disabled = !active;
    setShuffleActive(active && !!status?.shuffle_state);
  }
  if (playerRepeatBtn) {
    playerRepeatBtn.disabled = !active;
    setRepeatMode(active ? status?.repeat_state : 'off');
  }

  if (playerTick) {
    clearInterval(playerTick);
    playerTick = null;
  }
  if (active) {
    playerTick = setInterval(() => {
      if (!playerStatus || !playerStatus.is_playing) return;
      playerStatus.progress_ms = (playerStatus.progress_ms || 0) + 1000;
      if (playerStatus.progress_ms > (playerStatus.item?.duration_ms || 0)) {
        playerStatus.progress_ms = playerStatus.item?.duration_ms || 0;
      }
      const prog = playerStatus.progress_ms || 0;
      const dur = playerStatus.item?.duration_ms || 0;
      playerSeek.value = prog;
      setRangeProgress(playerSeek, prog, dur || 1);
      playerTimeCurrent.textContent = msToTime(prog);
      playerTimeTotal.textContent = msToTime(dur);
    }, 1000);
  }
}

function buildPlayerActionPath(path, body) {
  const hasDevice = body && typeof body === 'object' && Object.prototype.hasOwnProperty.call(body, 'device_id');
  if (hasDevice) return path;
  const deviceId = getActiveDeviceId();
  if (!deviceId) return path;
  const separator = path.includes('?') ? '&' : '?';
  return `${path}${separator}device_id=${encodeURIComponent(deviceId)}`;
}

async function activateRoomcastDevice(play = false) {
  const payload = { play: !!play };
  const res = await fetch('/api/spotify/player/activate-roomcast', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  await ensureOk(res);
  const data = await res.json();
  if (data?.device_id) activeDeviceId = data.device_id;
  setTimeout(fetchPlayerStatus, 400);
  return data;
}

async function playerAction(path, body, options = {}) {
  const allowRoomcastFallback = options.roomcastFallback !== false;
  let attemptedRoomcastActivation = false;
  while (true) {
    try {
      const targetPath = buildPlayerActionPath(path, body);
      const res = await fetch(targetPath, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
      });
      if (res.ok) {
        setTimeout(fetchPlayerStatus, 500);
        return;
      }
      const detail = await readResponseDetail(res);
      const normalizedDetail = (detail || '').toLowerCase();
      const canActivateRoomcast = allowRoomcastFallback
        && !attemptedRoomcastActivation
        && res.status === 404
        && normalizedDetail.includes('no active device');
      if (canActivateRoomcast) {
        await activateRoomcastDevice(false);
        attemptedRoomcastActivation = true;
        continue;
      }
      throw new Error(detail);
    } catch (err) {
      showError(`Player action failed: ${err.message}`);
      reportSpotifyError(err);
      return;
    }
  }
}

async function startSpotifyAuth() {
  try {
    const res = await fetch('/api/spotify/auth-url');
    await ensureOk(res);
    const data = await res.json();
    if (data.url) window.open(data.url, '_blank');
  } catch (err) {
    showError(`Failed to start Spotify auth: ${err.message}`);
    reportSpotifyError(err);
  }
}


async function registerNode() {
  try {
    await registerNodeWithName(nodeName.value || 'Node', nodeUrl.value);
    nodeName.value = '';
    nodeUrl.value = '';
  } catch (err) {
    showError(`Failed to register node: ${err.message}`);
  }
}

async function registerNodeWithName(name, url, btn, nodeId, fingerprint) {
  const normalizedUrl = normalizeNodeUrl(url);
  if (!normalizedUrl) {
    showError('Agent URL is required');
    return;
  }
  if (!nodeId && nodesCache.some(n => n.url === normalizedUrl)) {
    showSuccess('Node already registered');
    return;
  }
  if (btn) btn.disabled = true;
  const payload = { name, url: normalizedUrl };
  if (nodeId) payload.id = nodeId;
  if (fingerprint) payload.fingerprint = fingerprint;
  try {
    const res = await fetch('/api/nodes/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    await ensureOk(res);
    showSuccess(nodeId ? 'Node relinked' : 'Node registered');
    await fetchNodes();
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function saveSpotify() {
  try {
    spotifySpinner.style.display = 'block';
    saveSpotifyBtn.disabled = true;
    const payload = {
      device_name: spName.value || 'RoomCast',
      bitrate: Number(spBitrate.value) || 320,
      initial_volume: Number(spInitVol.value) || 75,
      normalisation: spNormalise.checked,
      client_id: spClientId.value,
      client_secret: spClientSecret.value,
      redirect_uri: spRedirect.value || 'http://localhost:8000/api/spotify/callback',
    };
    if (!payload.client_secret) delete payload.client_secret;
    const res = await fetch('/api/config/spotify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    await ensureOk(res);
    spClientSecret.value = '';
    showSuccess('Spotify config saved. Librespot will reload and connect.');
    await pollLibrespotStatus();
  } catch (err) {
    showError(`Failed to save Spotify config: ${err.message}`);
  } finally {
    saveSpotifyBtn.disabled = false;
    spotifySpinner.style.display = 'none';
  }
}

async function pollLibrespotStatus() {
  for (let i = 0; i < 8; i++) {
    await new Promise(r => setTimeout(r, 1200));
    await fetchLibrespotStatus();
    const text = librespotStatus.innerText || '';
    if (!text.includes('starting') && !text.includes('waiting') && !text.includes('unknown')) break;
  }
}

function openSettings() {
  settingsOverlay.style.display = 'flex';
  collapseAllPanels();
  syncGeneralSettingsUI();
  fetchSpotifyConfig();
  fetchLibrespotStatus();
  fetchStatus();
}
function closeSettings() { settingsOverlay.style.display = 'none'; }

function openDiscover() {
  discoverOverlay.style.display = 'flex';
  discoverResultsCount = 0;
  discoverStatus.textContent = 'Ready to scan.';
  discoverList.innerHTML = '';
  discoverSpinner.style.display = 'none';
  startDiscoverBtn.disabled = false;
  startDiscoverBtn.textContent = 'Scan';
}
function closeDiscover() {
  if (discoverAbortController) {
    discoverAbortController.abort();
    discoverAbortController = null;
  }
  discoverSpinner.style.display = 'none';
  startDiscoverBtn.disabled = false;
  startDiscoverBtn.textContent = 'Scan';
  discoverOverlay.style.display = 'none';
}

startDiscoverBtn.addEventListener('click', discoverNodes);
saveSpotifyBtn.addEventListener('click', saveSpotify);
if (spInitVol) {
  spInitVol.addEventListener('input', () => setRangeProgress(spInitVol, spInitVol.value, spInitVol.max || 100));
}
openSettingsBtn.addEventListener('click', openSettings);
closeSettingsBtn.addEventListener('click', closeSettings);
closeDiscoverBtn.addEventListener('click', closeDiscover);
if (coverArtBackgroundToggle) {
  coverArtBackgroundToggle.addEventListener('change', () => {
    setCoverArtBackgroundEnabled(coverArtBackgroundToggle.checked);
  });
}
const handleAddNodeKey = evt => {
  if (evt.key === 'Escape') {
    setAddNodeMenuOpen(false);
    addNodeToggle?.focus({ preventScroll: true });
  }
};
if (addNodeToggle) {
  addNodeToggle.addEventListener('click', evt => {
    evt.stopPropagation();
    const next = !addNodeContainer?.classList.contains('is-open');
    setAddNodeMenuOpen(next);
  });
  addNodeToggle.addEventListener('keydown', handleAddNodeKey);
}
if (addNodeMenu) {
  addNodeMenu.addEventListener('click', evt => evt.stopPropagation());
  addNodeMenu.addEventListener('keydown', handleAddNodeKey);
}
if (addHardwareNodeBtn) {
  addHardwareNodeBtn.addEventListener('click', () => {
    setAddNodeMenuOpen(false);
    openDiscover();
  });
}
if (createWebNodeBtn) {
  createWebNodeBtn.addEventListener('click', () => {
    setAddNodeMenuOpen(false);
    window.open('/web-node', '_blank');
  });
}
if (playerVolumeInline) {
  playerVolumeInline.addEventListener('click', evt => evt.stopPropagation());
}
if (playerVolumeToggle) {
  playerVolumeToggle.addEventListener('click', evt => {
    evt.stopPropagation();
    const next = !playerVolumeInline || !playerVolumeInline.classList.contains('is-open');
    setVolumeSliderOpen(next);
  });
}
const handleVolumeKey = evt => {
  if (evt.key === 'Escape') {
    setVolumeSliderOpen(false);
    playerVolumeToggle?.focus({ preventScroll: true });
  }
};
if (playerVolumeToggle) playerVolumeToggle.addEventListener('keydown', handleVolumeKey);
if (masterVolume) {
  masterVolume.addEventListener('input', () => setRangeProgress(masterVolume, masterVolume.value, masterVolume.max || 100));
  masterVolume.addEventListener('change', () => setMasterVolume(masterVolume.value));
  masterVolume.addEventListener('keydown', handleVolumeKey);
}
document.addEventListener('click', evt => {
  if (playerVolumeInline && !playerVolumeInline.contains(evt.target)) {
    setVolumeSliderOpen(false);
  }
  if (addNodeContainer && !addNodeContainer.contains(evt.target)) {
    setAddNodeMenuOpen(false);
  }
});
if (playlistOverlay) {
  playlistOverlay.addEventListener('click', evt => {
    if (evt.target === playlistOverlay) closePlaylistOverlay();
  });
}
if (playerPlaylistsBtn) playerPlaylistsBtn.setAttribute('aria-expanded', 'false');
if (playerPlaylistsBtn) playerPlaylistsBtn.addEventListener('click', openPlaylistOverlay);
if (playlistCloseBtn) playlistCloseBtn.addEventListener('click', closePlaylistOverlay);
if (playlistBackBtn) playlistBackBtn.addEventListener('click', handlePlaylistBack);
if (playlistSearchInput) {
  playlistSearchInput.addEventListener('input', evt => {
    playlistSearchTerm = (evt.target.value || '').trim().toLowerCase();
    renderPlaylistGrid(playlistsCache);
  });
}
if (playlistSortSelect) {
  playlistSortSelect.addEventListener('change', evt => {
    const value = (evt.target.value || '').toLowerCase();
    playlistSortOrder = value === 'name' ? 'name' : 'recent';
    renderPlaylistGrid(playlistsCache);
  });
}

if (playerSearchBtn) playerSearchBtn.setAttribute('aria-expanded', 'false');
if (playerSearchBtn) {
  playerSearchBtn.addEventListener('click', () => {
    setVolumeSliderOpen(false);
    openSearchOverlay();
  });
}
if (searchCloseBtn) searchCloseBtn.addEventListener('click', closeSearchOverlay);
if (searchOverlay) {
  searchOverlay.addEventListener('click', evt => {
    if (evt.target === searchOverlay) closeSearchOverlay();
  });
}
if (searchForm) {
  searchForm.addEventListener('submit', evt => {
    evt.preventDefault();
    runSpotifySearch(searchInput?.value || '');
  });
}
searchTabs.forEach(btn => {
  if (!btn || !btn.dataset.searchTab) return;
  if (!btn.dataset.baseLabel) btn.dataset.baseLabel = btn.textContent.trim();
  btn.addEventListener('click', () => setSearchActiveTab(btn.dataset.searchTab));
});
SEARCH_TABS.forEach(tab => renderSearchPane(tab));
updateSearchTabCounts();

if (queueOverlay) {
  queueOverlay.addEventListener('click', evt => {
    if (evt.target === queueOverlay) closeQueueOverlay();
  });
}
if (queueCloseBtn) queueCloseBtn.addEventListener('click', closeQueueOverlay);
if (playerArt) {
  playerArt.addEventListener('click', () => {
    if (!isPlayerArtInteractive()) return;
    openQueueOverlay();
  });
  playerArt.addEventListener('keydown', evt => {
    if (evt.key === 'Enter' || evt.key === ' ') {
      if (!isPlayerArtInteractive()) return;
      evt.preventDefault();
      openQueueOverlay();
    }
  });
}

setPlayerArtInteractivity(false);

playerPrev.addEventListener('click', () => {
  if (playerPrev.disabled) return;
  playerAction('/api/spotify/player/previous');
});
playerPlay.addEventListener('click', () => {
  if (playerPlay.disabled) return;
  if (playerStatus?.is_playing) playerAction('/api/spotify/player/pause');
  else playerAction('/api/spotify/player/play');
});
playerNext.addEventListener('click', () => {
  if (playerNext.disabled) return;
  playerAction('/api/spotify/player/next');
});
playerSeek.addEventListener('input', () => setRangeProgress(playerSeek, playerSeek.value, playerSeek.max || 1));
playerSeek.addEventListener('change', () => playerAction('/api/spotify/player/seek', { position_ms: Number(playerSeek.value) }));
playerShuffleBtn.addEventListener('click', () => {
  if (playerShuffleBtn.disabled) return;
  const current = typeof playerStatus?.shuffle_state === 'boolean'
    ? playerStatus.shuffle_state
    : playerShuffleBtn.classList.contains('is-active');
  const payload = { state: !current };
  const deviceId = getActiveDeviceId();
  if (deviceId) payload.device_id = deviceId;
  playerAction('/api/spotify/player/shuffle', payload);
});
playerRepeatBtn.addEventListener('click', () => {
  if (playerRepeatBtn.disabled) return;
  const current = playerStatus?.repeat_state || playerRepeatBtn.dataset.mode || 'off';
  const next = current === 'off' ? 'context' : current === 'context' ? 'track' : 'off';
  const payload = { mode: next };
  const deviceId = getActiveDeviceId();
  if (deviceId) payload.device_id = deviceId;
  playerAction('/api/spotify/player/repeat', payload);
});
spotifyAuthBtn.addEventListener('click', startSpotifyAuth);
spotifyDashboardBtn.addEventListener('click', () => window.open('https://developer.spotify.com/dashboard', '_blank'));
if (takeoverButton) takeoverButton.addEventListener('click', handleTakeoverClick);

const NODE_REFRESH_MS = 4000;
fetchNodes();
fetchStatus();
fetchPlayerStatus();
setInterval(fetchNodes, NODE_REFRESH_MS);
setInterval(fetchPlayerStatus, 4000);
