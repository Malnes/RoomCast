const nodesEl = document.getElementById('nodes');
const clientsSettingsEl = document.getElementById('clients-settings');
const errorEl = document.getElementById('error');
const successEl = document.getElementById('success');
const persistentAlertEl = document.getElementById('persistent-alert');
const persistentAlertMessage = document.getElementById('persistent-alert-message');
const persistentAlertAction = document.getElementById('persistent-alert-action');
const persistentAlertDismiss = document.getElementById('persistent-alert-dismiss');
const addNodeContainer = document.querySelector('[data-add-node-container]');
const addNodeToggle = document.getElementById('add-node-button');
const addNodeMenu = document.getElementById('add-node-menu');
const addHardwareNodeBtn = document.getElementById('add-hardware-node');
const addControllerNodeBtn = document.getElementById('add-controller-node');
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
const playerCardContents = document.querySelector('.player-card-contents');
const playerCarouselTrack = document.getElementById('player-carousel-track');
const playerCarouselIndicators = document.getElementById('player-carousel-indicators');
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
const headerActions = document.querySelector('.header-actions');
const appHeader = document.querySelector('header');
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
const channelsPanel = document.getElementById('channels-panel');
const spotifyChannelSelect = document.getElementById('spotify-channel-select');
const radioOverlay = document.getElementById('radio-overlay');
const radioCloseBtn = document.getElementById('radio-close');
const radioTabs = Array.from(document.querySelectorAll('.radio-tab'));
const radioPanes = Array.from(document.querySelectorAll('[data-radio-pane]'));
const radioPaneMap = radioPanes.reduce((acc, pane) => {
  const key = pane?.dataset?.radioPane;
  if (key) acc[key] = pane;
  return acc;
}, {});
const radioGenreList = document.getElementById('radio-genre-list');
const radioTopButtons = Array.from(document.querySelectorAll('.radio-top-btn'));
const radioCountrySelect = document.getElementById('radio-country-select');
const radioSearchForm = document.getElementById('radio-search-form');
const radioSearchInput = document.getElementById('radio-search-input');
const radioResults = document.getElementById('radio-results');
const radioResultsStatus = document.getElementById('radio-results-status');
const radioResultsList = document.getElementById('radio-results-list');
const radioModalChannelName = document.getElementById('radio-modal-channel-name');
const radioModalSubtitle = document.getElementById('radio-modal-subtitle');
const confirmOverlay = document.getElementById('confirm-overlay');
const confirmModalTitle = document.getElementById('confirm-modal-title');
const confirmModalMessage = document.getElementById('confirm-modal-message');
const confirmModalCancel = document.getElementById('confirm-modal-cancel');
const confirmModalAccept = document.getElementById('confirm-modal-accept');
const DEFAULT_CHANNEL_COLOR = '#22c55e';
const WIFI_SIGNAL_THRESHOLDS = [
  { min: 75, bars: 4, label: 'Excellent signal' },
  { min: 55, bars: 3, label: 'Good signal' },
  { min: 35, bars: 2, label: 'Fair signal' },
  { min: 15, bars: 1, label: 'Weak signal' },
];
const ACTIVE_CHANNEL_STORAGE_KEY = 'roomcast-active-channel';

function readStoredActiveChannelId() {
  try {
    return localStorage.getItem(ACTIVE_CHANNEL_STORAGE_KEY) || null;
  } catch (_) {
    return null;
  }
}

function persistActiveChannelPreference(channelId) {
  try {
    if (channelId) {
      localStorage.setItem(ACTIVE_CHANNEL_STORAGE_KEY, channelId);
    } else {
      localStorage.removeItem(ACTIVE_CHANNEL_STORAGE_KEY);
    }
  } catch (_) {
    /* storage unavailable */
  }
}
if (playerPanel) {
  playerPanel.addEventListener('pointerdown', handleCarouselPointerDown);
  playerPanel.addEventListener('pointermove', handleCarouselPointerMove);
  playerPanel.addEventListener('pointerup', handleCarouselPointerUp);
  playerPanel.addEventListener('pointercancel', handleCarouselPointerCancel);
  playerPanel.addEventListener('keydown', handleCarouselKeydown);
}
window.addEventListener('resize', () => {
  syncPlayerCarouselToActive({ animate: false });
});
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
const collapsiblePanels = Array.from(document.querySelectorAll('[data-collapsible]'));
let playerStatus = null;
let radioPlaybackState = null;
let activeDeviceId = null;
let playerTick = null;
let discoverAbortController = null;
let discoverResultsCount = 0;
let channelsCache = [];
let activeChannelId = readStoredActiveChannelId();
let channelFetchPromise = null;
let spotifySettingsChannelId = null;
const channelPendingEdits = new Map();
const channelFormRefs = new Map();
let nodeSettingsModal = null;
let nodeSettingsContent = null;
let nodeSettingsTitle = null;
let nodeSettingsNodeId = null;
let lastCoverArtUrl = null;
let confirmDialogResolver = null;
let confirmDialogPreviousFocus = null;
const PLAYLIST_PAGE_LIMIT = 50;
const PLAYLIST_CACHE_TTL_MS = 60 * 60 * 1000;
const PLAYLIST_TRACK_CACHE_TTL_MS = 60 * 60 * 1000;
const PLAYER_SNAPSHOT_TTL_MS = 24 * 60 * 60 * 1000;
let playlistsCache = [];
let playlistsCacheFetchedAt = 0;
const playlistMetadataCache = new Map();
let playlistSelected = null;
const playlistTrackCache = new Map();
let lastPlayerSnapshot = null;
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
function defaultSearchBuckets() {
  return {
    tracks: { items: [] },
    albums: { items: [] },
    artists: { items: [] },
    playlists: { items: [] },
  };
}
let searchActiveTab = 'tracks';
let lastSearchQuery = '';
let searchHasAttempted = false;
let searchResultsState = defaultSearchBuckets();
let searchAbortController = null;
const RADIO_TABS = ['genres', 'top', 'countries', 'search'];
let radioActiveTab = 'genres';
let radioActiveChannelId = null;
let radioTopMetric = 'votes';
const radioDataCache = {
  genres: null,
  countries: null,
  top: { votes: null, clicks: null },
};
let radioResultsAbortController = null;
let radioCurrentResults = [];
let radioResultsContextLabel = '';
const playerCarouselCards = new Map();
const playerCarouselIndicatorRefs = new Map();
// Runtime data for the swipeable player carousel.
let playerCarouselState = {
  isDragging: false,
  pointerId: null,
  startX: 0,
  deltaX: 0,
  width: 0,
  captured: false,
};
const PLAYER_SWIPE_DIRECTIONS = { FORWARD: 'forward', BACKWARD: 'backward' };
let playerPanelSwipeState = { cleanup: null, timerId: null, unhideTimer: null };
let pendingPlayerEntryDirection = null;
let queueAbortController = null;
const playlistNameCollator = typeof Intl !== 'undefined' && typeof Intl.Collator === 'function'
  ? new Intl.Collator(undefined, { sensitivity: 'base' })
  : { compare: (a, b) => {
      if (a === b) return 0;
      return a > b ? 1 : -1;
    } };
const appShell = document.getElementById('app-shell');
const authShell = document.getElementById('auth-shell');
const authLoading = document.getElementById('auth-loading');
const loginView = document.getElementById('login-view');
const onboardingView = document.getElementById('onboarding-view');
const loginForm = document.getElementById('login-form');
const loginUsername = document.getElementById('login-username');
const loginPassword = document.getElementById('login-password');
const loginError = document.getElementById('login-error');
const onboardingForm = document.getElementById('onboarding-form');
const onboardingServerName = document.getElementById('onboarding-server-name');
const onboardingUsername = document.getElementById('onboarding-username');
const onboardingPassword = document.getElementById('onboarding-password');
const onboardingError = document.getElementById('onboarding-error');
const serverNameDisplay = document.getElementById('server-name-display');
const queueServerName = document.getElementById('queue-server-name');
const serverNameInput = document.getElementById('server-name-input');
const saveServerNameBtn = document.getElementById('save-server-name');
const userStatusEl = document.getElementById('user-status');
const currentUserNameEl = document.getElementById('current-user-name');
const currentUserRoleEl = document.getElementById('current-user-role');
const logoutButton = document.getElementById('logout-button');
const usersListEl = document.getElementById('users-list');
const addUserForm = document.getElementById('add-user-form');
const newUserUsername = document.getElementById('new-user-username');
const newUserPassword = document.getElementById('new-user-password');
const newUserRole = document.getElementById('new-user-role');
const usersPanelNote = document.getElementById('users-panel-note');
let authState = { initialized: false, authenticated: false, server_name: 'RoomCast', user: null };
let appBootstrapped = false;
let nodePollTimer = null;
let playerPollTimer = null;
let usersCache = [];
let usersLoaded = false;
const NODE_POLL_INTERVAL_MS = 4000;
let nodesSocket = null;
let nodesSocketConnected = false;
let nodesSocketRetryTimer = null;
let nodesSocketRetryAttempt = 0;
let nodesSocketShouldConnect = false;

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
  const { accent } = resolveRangeAccent(el);
  const inactive = 'rgba(255,255,255,0.12)';
  el.style.background = `linear-gradient(90deg, ${accent} 0%, ${accent} ${percent}%, ${inactive} ${percent}%, ${inactive} 100%)`;
  el.style.backgroundSize = '100% 4px';
  el.style.backgroundPosition = 'center';
  el.style.backgroundRepeat = 'no-repeat';
}

function isAuthenticated() {
  return !!authState?.authenticated;
}

function isAdminUser() {
  return !!authState?.user && authState.user.role === 'admin';
}

function setInlineMessage(target, message) {
  if (!target) return;
  target.textContent = message || '';
}

function setServerBranding(name) {
  const normalized = (name || 'RoomCast').trim() || 'RoomCast';
  if (serverNameDisplay) serverNameDisplay.textContent = normalized;
  if (queueServerName) queueServerName.textContent = normalized;
  if (serverNameInput && document.activeElement !== serverNameInput) {
    serverNameInput.value = normalized;
  }
  document.title = `${normalized} – RoomCast`;
}

function updateUserStatusUI() {
  if (!userStatusEl) return;
  if (!authState?.user) {
    userStatusEl.hidden = true;
    return;
  }
  userStatusEl.hidden = false;
  if (currentUserNameEl) currentUserNameEl.textContent = authState.user.username || 'User';
  if (currentUserRoleEl) currentUserRoleEl.textContent = (authState.user.role || 'member').replace(/^./, ch => ch.toUpperCase());
}

function showAuthLoading() {
  if (authShell) authShell.hidden = false;
  if (appShell) appShell.hidden = true;
  if (authLoading) authLoading.hidden = false;
  if (loginView) loginView.hidden = true;
  if (onboardingView) onboardingView.hidden = true;
}

function showLoginScreen() {
  if (authShell) authShell.hidden = false;
  if (appShell) appShell.hidden = true;
  if (authLoading) authLoading.hidden = true;
  if (loginView) {
    loginView.hidden = false;
    setTimeout(() => loginUsername?.focus(), 50);
  }
  if (onboardingView) onboardingView.hidden = true;
  setInlineMessage(loginError, '');
  resetUsersState();
}

function showOnboardingScreen() {
  if (authShell) authShell.hidden = false;
  if (appShell) appShell.hidden = true;
  if (authLoading) authLoading.hidden = true;
  if (onboardingView) {
    onboardingView.hidden = false;
    onboardingServerName.value = authState.server_name || 'RoomCast';
    setTimeout(() => onboardingServerName?.focus(), 50);
  }
  if (loginView) loginView.hidden = true;
  setInlineMessage(onboardingError, '');
  resetUsersState();
}

function stopDataPolling() {
  if (nodePollTimer) {
    clearInterval(nodePollTimer);
    nodePollTimer = null;
  }
  if (playerPollTimer) {
    clearInterval(playerPollTimer);
    playerPollTimer = null;
  }
  stopNodeSocket();
  resetChannelUiState();
}

function startDataPolling() {
  if (!isAuthenticated()) return;
  startNodeSocket();
  const runNodePoll = () => {
    if (nodesSocketConnected) return;
    fetchNodes();
  };
  if (!nodePollTimer) {
    runNodePoll();
    nodePollTimer = setInterval(runNodePoll, NODE_POLL_INTERVAL_MS);
  }
  if (!playerPollTimer) {
    fetchPlayerStatus();
    playerPollTimer = setInterval(fetchPlayerStatus, 4000);
  }
  fetchStatus();
}

function enterAppShell() {
  if (authShell) authShell.hidden = true;
  if (appShell) appShell.hidden = false;
  setServerBranding(authState.server_name);
  updateUserStatusUI();
  syncGeneralSettingsUI();
  renderUsersList();
  refreshChannels().catch(() => {
    /* errors surfaced via toast */
  });
  startDataPolling();
}

async function requestAuthStatus() {
  const res = await fetch('/api/auth/status');
  await ensureOk(res);
  return res.json();
}

async function refreshAuthState() {
  try {
    const status = await requestAuthStatus();
    authState = status;
    setServerBranding(authState.server_name);
    if (!authState.initialized) {
      stopDataPolling();
      showOnboardingScreen();
      return;
    }
    if (!authState.authenticated) {
      stopDataPolling();
      showLoginScreen();
      return;
    }
    enterAppShell();
  } catch (err) {
    if (authLoading) {
      authLoading.hidden = false;
      const loadingMessage = authLoading.querySelector('p');
      if (loadingMessage) {
        loadingMessage.setAttribute('aria-live', 'polite');
        loadingMessage.textContent = `Failed to check session: ${err.message}`;
      }
    }
  }
}

function handleAuthSuccess(payload) {
  if (payload?.server_name) authState.server_name = payload.server_name;
  if (payload?.user) authState.user = payload.user;
  authState.initialized = true;
  authState.authenticated = true;
  enterAppShell();
}

async function bootstrapAuth() {
  showAuthLoading();
  await refreshAuthState();
}



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
const PWA_UPDATE_ALERT_KEY = 'pwa-update';
const PWA_UPDATE_ACTION_LABEL = 'Update now';
let persistentAlertActionHandler = null;
const DEFAULT_ALERT_DISMISS_LABEL = persistentAlertDismiss?.textContent || '✕';
const DEFAULT_ALERT_DISMISS_ARIA = persistentAlertDismiss?.getAttribute('aria-label') || 'Dismiss alert';

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
  if (msg) {
    console.debug('Success:', msg);
  }
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
  const actionHandler = typeof options.onAction === 'function' ? options.onAction : null;
  persistentAlertActionHandler = actionHandler;
  if (persistentAlertAction) {
    if (actionHandler) {
      persistentAlertAction.hidden = false;
      persistentAlertAction.textContent = options.actionLabel || 'View details';
      persistentAlertAction.disabled = false;
    } else {
      persistentAlertAction.hidden = true;
      persistentAlertAction.textContent = '';
    }
  }
  if (persistentAlertDismiss) {
    const dismissLabel = options.dismissLabel || DEFAULT_ALERT_DISMISS_LABEL;
    const dismissAria = options.dismissAriaLabel || DEFAULT_ALERT_DISMISS_ARIA;
    persistentAlertDismiss.textContent = dismissLabel;
    persistentAlertDismiss.setAttribute('aria-label', dismissAria);
  }
  persistentAlertEl.classList.add('is-visible');
  persistentAlertEl.hidden = false;
  persistentAlertEl.setAttribute('aria-hidden', 'false');
  persistentAlertState = { key, message };
}

function hideConfirmDialogOverlay() {
  if (!confirmOverlay) return;
  confirmOverlay.classList.remove('is-visible');
  confirmOverlay.style.display = 'none';
  confirmOverlay.hidden = true;
  confirmOverlay.setAttribute('aria-hidden', 'true');
  document.removeEventListener('keydown', handleConfirmDialogKeydown, true);
  if (confirmDialogPreviousFocus?.focus) {
    confirmDialogPreviousFocus.focus();
  }
  confirmDialogPreviousFocus = null;
}

function settleConfirmDialog(result) {
  if (typeof confirmDialogResolver === 'function') {
    confirmDialogResolver(result);
    confirmDialogResolver = null;
  }
}

function closeConfirmDialog(result = false) {
  hideConfirmDialogOverlay();
  settleConfirmDialog(result);
}

function handleConfirmDialogKeydown(event) {
  if (event.key === 'Escape') {
    event.preventDefault();
    closeConfirmDialog(false);
  }
}

function openConfirmDialog(options = {}) {
  if (!confirmOverlay || !confirmModalTitle || !confirmModalMessage || !confirmModalAccept || !confirmModalCancel) {
    const fallbackMessage = options.message || options.title || 'Are you sure?';
    const confirmed = window.confirm(fallbackMessage || 'Are you sure?');
    return Promise.resolve(confirmed);
  }
  if (confirmDialogResolver) {
    closeConfirmDialog(false);
  }
  const {
    title = 'Are you sure?',
    message = '',
    confirmLabel = 'Confirm',
    cancelLabel = 'Cancel',
    tone = 'default',
  } = options;
  confirmModalTitle.textContent = title;
  confirmModalMessage.textContent = message;
  confirmModalAccept.textContent = confirmLabel;
  confirmModalCancel.textContent = cancelLabel;
  confirmModalAccept.classList.toggle('danger-btn', tone === 'danger');
  confirmOverlay.hidden = false;
  confirmOverlay.style.display = 'flex';
  confirmOverlay.setAttribute('aria-hidden', 'false');
  requestAnimationFrame(() => confirmOverlay.classList.add('is-visible'));
  confirmDialogPreviousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  document.addEventListener('keydown', handleConfirmDialogKeydown, true);
  return new Promise(resolve => {
    confirmDialogResolver = resolve;
    setTimeout(() => {
      confirmModalAccept.focus();
    }, 30);
  });
}

if (confirmOverlay) {
  confirmOverlay.addEventListener('click', event => {
    if (event.target === confirmOverlay) {
      closeConfirmDialog(false);
    }
  });
}
if (confirmModalCancel) {
  confirmModalCancel.addEventListener('click', () => closeConfirmDialog(false));
}
if (confirmModalAccept) {
  confirmModalAccept.addEventListener('click', () => closeConfirmDialog(true));
}

function hidePersistentAlert() {
  if (!persistentAlertEl) return;
  persistentAlertEl.classList.remove('is-visible');
  persistentAlertEl.setAttribute('aria-hidden', 'true');
  persistentAlertEl.hidden = true;
  persistentAlertEl.dataset.alertKey = '';
  persistentAlertActionHandler = null;
  if (persistentAlertAction) {
    persistentAlertAction.hidden = true;
    persistentAlertAction.textContent = '';
  }
  if (persistentAlertDismiss) {
    persistentAlertDismiss.textContent = DEFAULT_ALERT_DISMISS_LABEL;
    persistentAlertDismiss.setAttribute('aria-label', DEFAULT_ALERT_DISMISS_ARIA);
  }
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

function normalizeChannelColorInput(value) {
  if (!value) return null;
  let raw = String(value).trim().toLowerCase();
  if (!raw) return null;
  if (!raw.startsWith('#')) raw = `#${raw}`;
  if (raw.length === 4) {
    const [, r, g, b] = raw;
    raw = `#${r}${r}${g}${g}${b}${b}`;
  }
  if (raw.length !== 7) return null;
  if (!/^#[0-9a-f]{6}$/i.test(raw)) return null;
  return raw;
}

function hexToRgbParts(hexValue) {
  const normalized = normalizeChannelColorInput(hexValue);
  if (!normalized) return null;
  const r = parseInt(normalized.slice(1, 3), 16);
  const g = parseInt(normalized.slice(3, 5), 16);
  const b = parseInt(normalized.slice(5, 7), 16);
  if ([r, g, b].some(num => Number.isNaN(num))) return null;
  return [r, g, b];
}

function rgbaFromHex(hexValue, alpha = 1) {
  const rgb = hexToRgbParts(hexValue);
  if (!rgb) return null;
  const parsed = Number(alpha);
  const normalizedAlpha = Math.max(0, Math.min(1, Number.isFinite(parsed) ? parsed : 1));
  return `rgba(${rgb.join(',')}, ${normalizedAlpha})`;
}

function darkenHexColor(hexValue, amount = 0.2) {
  const rgb = hexToRgbParts(hexValue);
  if (!rgb) return null;
  const factor = Math.max(0, Math.min(1, 1 - amount));
  const shaded = rgb.map(part => Math.round(part * factor));
  return `#${shaded.map(value => value.toString(16).padStart(2, '0')).join('')}`;
}

function applyChannelTheme(channel) {
  const fallbackColor = DEFAULT_CHANNEL_COLOR;
  const fallbackDark = '#16a34a';
  const fallbackRgb = '34,197,94';
  const color = normalizeChannelColorInput(channel?.color) || fallbackColor;
  const rgbParts = hexToRgbParts(color) || hexToRgbParts(fallbackColor);
  const rgbString = rgbParts ? rgbParts.join(',') : fallbackRgb;
  const darker = darkenHexColor(color, 0.18) || fallbackDark;
  document.documentElement?.style?.setProperty('--accent', color);
  document.documentElement?.style?.setProperty('--accent-dark', darker);
  document.documentElement?.style?.setProperty('--accent-rgb', rgbString);
}

function withChannel(path, channelId = getActiveChannelId()) {
  if (!channelId) return path;
  const separator = path.includes('?') ? '&' : '?';
  return `${path}${separator}channel_id=${encodeURIComponent(channelId)}`;
}

function getChannelById(channelId) {
  if (!channelId) return null;
  return channelsCache.find(ch => ch.id === channelId) || null;
}

function isChannelEnabled(channel) {
  return channel?.enabled !== false;
}

function isRadioChannel(channel) {
  if (!channel) return false;
  return (channel.source || '').toLowerCase() === 'radio';
}

function getRadioState(channel) {
  if (!isRadioChannel(channel)) return null;
  return channel.radio_state || null;
}

function updateChannelRadioState(channelId, radioState) {
  if (!channelId) return;
  const idx = channelsCache.findIndex(ch => ch.id === channelId);
  if (idx === -1) return;
  channelsCache[idx] = { ...channelsCache[idx], radio_state: radioState || null };
}

function resolveRadioPlaybackSnapshot(channel) {
  if (!channel || !channel.id) {
    return { channelId: null, playbackEnabled: false, hasStation: false, enabled: false };
  }
  if (radioPlaybackState && radioPlaybackState.channelId === channel.id) {
    return { ...radioPlaybackState };
  }
  const state = getRadioState(channel) || {};
  return {
    channelId: channel.id,
    playbackEnabled: state.playback_enabled !== false,
    hasStation: !!state.stream_url,
    enabled: channel.enabled !== false,
  };
}

function getPlayerChannels() {
  return channelsCache.filter(isChannelEnabled);
}

function getChannelAccentColor(channelId) {
  const channel = getChannelById(channelId);
  return normalizeChannelColorInput(channel?.color);
}

function getNodeChannelAccent(node) {
  if (!node) return DEFAULT_CHANNEL_COLOR;
  const resolvedId = resolveNodeChannelId(node);
  return getChannelAccentColor(resolvedId) || DEFAULT_CHANNEL_COLOR;
}

function applyRangeAccent(el, colorValue) {
  if (!el) return;
  const normalized = normalizeChannelColorInput(colorValue);
  if (normalized) {
    el.style.setProperty('--range-accent', normalized);
    el.dataset.rangeAccent = normalized;
    const rgbParts = hexToRgbParts(normalized);
    if (rgbParts) {
      const rgbValue = rgbParts.join(',');
      el.style.setProperty('--range-accent-rgb', rgbValue);
      el.dataset.rangeAccentRgb = rgbValue;
    } else {
      el.style.removeProperty('--range-accent-rgb');
      delete el.dataset.rangeAccentRgb;
    }
  } else {
    el.style.removeProperty('--range-accent');
    el.style.removeProperty('--range-accent-rgb');
    delete el.dataset.rangeAccent;
    delete el.dataset.rangeAccentRgb;
  }
}

function resolveRangeAccent(el) {
  const fallback = 'var(--accent)';
  const fallbackRgb = 'var(--accent-rgb)';
  if (!el) return { accent: fallback, accentRgb: fallbackRgb };
  const accent = (el.dataset?.rangeAccent || el.style?.getPropertyValue('--range-accent') || '').trim() || fallback;
  const accentRgb = (el.dataset?.rangeAccentRgb || el.style?.getPropertyValue('--range-accent-rgb') || '').trim() || fallbackRgb;
  return { accent, accentRgb };
}

function resolveNodeChannelId(node) {
  if (node?.channel_id && channelsCache.some(ch => ch.id === node.channel_id)) {
    return node.channel_id;
  }
  const playable = getPlayerChannels();
  if (playable.length) return playable[0].id;
  return channelsCache[0]?.id || null;
}

function updateChannelDotColor(target, channelId) {
  if (!target) return;
  const color = getChannelAccentColor(channelId) || '#94a3b8';
  target.style.background = color;
}

function getActiveChannelId() {
  const playable = getPlayerChannels();
  if (!playable.length) {
    activeChannelId = null;
    return null;
  }
  if (activeChannelId && playable.some(ch => ch.id === activeChannelId)) {
    return activeChannelId;
  }
  activeChannelId = playable[0].id;
  return activeChannelId;
}

function getActiveChannel() {
  const cid = getActiveChannelId();
  return cid ? getChannelById(cid) : null;
}

function resolveChannelSwipeDirection(previousId, nextId, hintedDirection) {
  if (hintedDirection === PLAYER_SWIPE_DIRECTIONS.FORWARD || hintedDirection === PLAYER_SWIPE_DIRECTIONS.BACKWARD) {
    return hintedDirection;
  }
  if (!previousId || !nextId || previousId === nextId) return null;
  const playable = getPlayerChannels();
  const prevIndex = playable.findIndex(ch => ch.id === previousId);
  const nextIndex = playable.findIndex(ch => ch.id === nextId);
  if (prevIndex === -1 || nextIndex === -1) return null;
  return nextIndex > prevIndex ? PLAYER_SWIPE_DIRECTIONS.FORWARD : PLAYER_SWIPE_DIRECTIONS.BACKWARD;
}

function sanitizePlayerCardClone(clone) {
  if (!clone) return;
  clone.setAttribute('aria-hidden', 'true');
  clone.querySelectorAll('button, input, select, textarea, a, [tabindex]').forEach(el => {
    el.setAttribute('tabindex', '-1');
    el.setAttribute('aria-hidden', 'true');
  });
}

function triggerPlayerPanelSwipe(direction) {
  if (!playerPanel || !playerCardContents) return;
  if (!direction || channelsCache.length <= 1) {
    pendingPlayerEntryDirection = null;
    return;
  }
  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    pendingPlayerEntryDirection = null;
    return;
  }
  pendingPlayerEntryDirection = direction;
  playerCardContents.classList.add('is-swipe-hidden');
  if (playerPanelSwipeState.unhideTimer) {
    clearTimeout(playerPanelSwipeState.unhideTimer);
    playerPanelSwipeState.unhideTimer = null;
  }
  playerPanelSwipeState.unhideTimer = setTimeout(() => {
    playerCardContents?.classList.remove('is-swipe-hidden');
    pendingPlayerEntryDirection = null;
    playerPanelSwipeState.unhideTimer = null;
  }, 900);
  const contentRect = playerCardContents.getBoundingClientRect();
  const panelRect = playerPanel.getBoundingClientRect();
  if (!contentRect.width || !contentRect.height) return;
  if (playerPanelSwipeState.cleanup) {
    playerPanelSwipeState.cleanup();
  }
  const clone = playerCardContents.cloneNode(true);
  clone.classList.add('player-card-contents-clone');
  sanitizePlayerCardClone(clone);
  clone.style.top = `${contentRect.top - panelRect.top}px`;
  clone.style.left = `${contentRect.left - panelRect.left}px`;
  clone.style.width = `${contentRect.width}px`;
  clone.style.height = `${contentRect.height}px`;
  playerPanel.appendChild(clone);

  const outgoingClass = direction === PLAYER_SWIPE_DIRECTIONS.FORWARD ? 'slide-out-left' : 'slide-out-right';

  void clone.offsetWidth;
  clone.classList.add(outgoingClass);

  const cleanup = () => {
    if (playerPanelSwipeState.cleanup !== cleanup) return;
    if (clone.isConnected) clone.remove();
    if (playerPanelSwipeState.timerId) {
      clearTimeout(playerPanelSwipeState.timerId);
      playerPanelSwipeState.timerId = null;
    }
    playerPanelSwipeState.cleanup = null;
  };

  playerPanelSwipeState.cleanup = cleanup;

  clone.addEventListener('animationend', cleanup, { once: true });

  playerPanelSwipeState.timerId = setTimeout(() => {
    cleanup();
  }, 600);
}

function playPendingPlayerPanelEntryAnimation() {
  if (!playerCardContents || !pendingPlayerEntryDirection) return;
  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    pendingPlayerEntryDirection = null;
    playerCardContents.classList.remove('is-swiping', 'slide-in-from-right', 'slide-in-from-left');
    return;
  }
  const direction = pendingPlayerEntryDirection;
  pendingPlayerEntryDirection = null;
  const incomingClass = direction === PLAYER_SWIPE_DIRECTIONS.FORWARD ? 'slide-in-from-right' : 'slide-in-from-left';
  if (playerPanelSwipeState.unhideTimer) {
    clearTimeout(playerPanelSwipeState.unhideTimer);
    playerPanelSwipeState.unhideTimer = null;
  }
  playerCardContents.classList.remove('is-swipe-hidden');
  playerCardContents.classList.remove('is-swiping', 'slide-in-from-right', 'slide-in-from-left');
  playerCardContents.classList.add('is-swiping', incomingClass);
  const handleAnimationEnd = event => {
    if (event.target !== playerCardContents) return;
    playerCardContents.classList.remove('is-swiping', 'slide-in-from-right', 'slide-in-from-left');
  };
  playerCardContents.addEventListener('animationend', handleAnimationEnd, { once: true });
}

function resetPlayerPanelSwipeEffects() {
  pendingPlayerEntryDirection = null;
  if (playerPanelSwipeState.cleanup) {
    playerPanelSwipeState.cleanup();
  }
  if (playerPanelSwipeState.timerId) {
    clearTimeout(playerPanelSwipeState.timerId);
    playerPanelSwipeState.timerId = null;
  }
  if (playerPanelSwipeState.unhideTimer) {
    clearTimeout(playerPanelSwipeState.unhideTimer);
    playerPanelSwipeState.unhideTimer = null;
  }
  if (playerCardContents) {
    playerCardContents.classList.remove('is-swipe-hidden', 'is-swiping', 'slide-in-from-right', 'slide-in-from-left');
  }
}

function setActiveChannel(channelId, options = {}) {
  if (!channelId || channelId === activeChannelId) return;
  const targetChannel = getChannelById(channelId);
  if (!targetChannel || !isChannelEnabled(targetChannel)) return;
  const previous = activeChannelId;
  activeChannelId = channelId;
  persistActiveChannelPreference(activeChannelId);
  if (previous && options.animate !== false) {
    const direction = resolveChannelSwipeDirection(previous, channelId, options.direction);
    if (direction) {
      triggerPlayerPanelSwipe(direction);
    }
  } else {
    pendingPlayerEntryDirection = null;
  }
  const animateCarousel = options.carouselAnimate !== false;
  syncPlayerCarouselToActive({ animate: animateCarousel });
  applyChannelTheme(getActiveChannel());
  onActiveChannelChanged(previous, channelId);
}



function updatePlayerCarouselCards(channel) {
  if (!channel || !channel.id) return null;
  let card = playerCarouselCards.get(channel.id);
  if (!card) {
    card = document.createElement('div');
    card.className = 'player-carousel-card';
    card.dataset.channelId = channel.id;
    playerCarouselCards.set(channel.id, card);
  }
  return card;
}

function updatePlayerCarouselIndicatorState() {
  const activeId = getActiveChannelId();
  playerCarouselIndicatorRefs.forEach((btn, channelId) => {
    const isActive = channelId === activeId;
    btn.classList.toggle('is-active', isActive);
    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
  });
}

function updatePlayerCarouselIndicators() {
  if (!playerCarouselIndicators) return;
  playerCarouselIndicators.innerHTML = '';
  playerCarouselIndicatorRefs.clear();
  const playableChannels = getPlayerChannels();
  if (playableChannels.length <= 1) {
    playerCarouselIndicators.hidden = true;
    return;
  }
  playerCarouselIndicators.hidden = false;
  playableChannels.forEach(channel => {
    if (!channel?.id) return;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'player-carousel-indicator';
    btn.dataset.channelId = channel.id;
    btn.setAttribute('aria-label', `Switch to ${channel.name || channel.id}`);
    const color = getChannelAccentColor(channel.id) || DEFAULT_CHANNEL_COLOR;
    btn.style.setProperty('--indicator-color', color);
    btn.addEventListener('click', () => setActiveChannel(channel.id));
    playerCarouselIndicators.appendChild(btn);
    playerCarouselIndicatorRefs.set(channel.id, btn);
  });
  updatePlayerCarouselIndicatorState();
}

function getActiveChannelIndex() {
  const activeId = getActiveChannelId();
  const playable = getPlayerChannels();
  if (!activeId || !playable.length) return 0;
  const idx = playable.findIndex(ch => ch.id === activeId);
  return idx === -1 ? 0 : idx;
}

function updatePlayerCarouselPosition(index, animate = true) {
  if (!playerCarouselTrack || !playerPanel) return;
  const width = playerPanel.clientWidth || window.innerWidth || 1;
  const dragOffset = playerCarouselState.isDragging ? playerCarouselState.deltaX : 0;
  const offset = -index * width + dragOffset;
  if (!animate || playerCarouselState.isDragging) {
    playerCarouselTrack.style.transition = 'none';
  } else {
    playerCarouselTrack.style.transition = '';
  }
  playerCarouselTrack.style.transform = `translate3d(${offset}px, 0, 0)`;
}

function syncPlayerCarouselToActive(options = {}) {
  const animate = options.animate !== false;
  const activeId = getActiveChannelId();
  playerCarouselCards.forEach(card => {
    card.classList.toggle('is-active', card.dataset.channelId === activeId);
  });
  updatePlayerCarouselIndicatorState();
  updatePlayerCarouselPosition(getActiveChannelIndex(), animate);
  if (playerPanel) {
    playerPanel.classList.toggle('is-carousel-enabled', getPlayerChannels().length > 1);
  }
}

function renderPlayerCarousel() {
  if (!playerCarouselTrack) return;
  const playableChannels = getPlayerChannels();
  if (!playableChannels.length) {
    if (playerCarouselTrack) {
      playerCarouselTrack.innerHTML = '<div class="player-carousel-empty">Enable a channel in Settings to begin playback.</div>';
    }
    playerCarouselCards.clear();
    updatePlayerCarouselIndicators();
    syncPlayerCarouselToActive({ animate: false });
    if (playerPanel) playerPanel.classList.remove('is-carousel-enabled');
    return;
  }
  const fragment = document.createDocumentFragment();
  const seen = new Set();
  playableChannels.forEach(channel => {
    if (!channel?.id) return;
    const card = updatePlayerCarouselCards(channel);
    if (card) fragment.appendChild(card);
    seen.add(channel.id);
  });
  playerCarouselTrack.innerHTML = '';
  playerCarouselTrack.appendChild(fragment);
  Array.from(playerCarouselCards.keys()).forEach(channelId => {
    if (!seen.has(channelId)) {
      playerCarouselCards.get(channelId)?.remove();
      playerCarouselCards.delete(channelId);
    }
  });
  updatePlayerCarouselIndicators();
  syncPlayerCarouselToActive({ animate: false });
}

function selectAdjacentChannel(step) {
  const playable = getPlayerChannels();
  if (!playable.length) return false;
  const activeId = getActiveChannelId();
  const currentIdx = playable.findIndex(ch => ch.id === activeId);
  if (currentIdx === -1) return false;
  const nextIdx = step < 0 ? Math.max(0, currentIdx - 1) : Math.min(playable.length - 1, currentIdx + 1);
  if (nextIdx === currentIdx) return false;
  const nextChannel = playable[nextIdx];
  if (nextChannel?.id) {
    const direction = step < 0 ? PLAYER_SWIPE_DIRECTIONS.BACKWARD : PLAYER_SWIPE_DIRECTIONS.FORWARD;
    setActiveChannel(nextChannel.id, { direction });
    return true;
  }
  return false;
}

function isInteractiveControl(target) {
  if (!target || target === document || target === window) return false;
  return !!target.closest('button, input, select, textarea, a, [role="button"], [role="slider"], [contenteditable="true"]');
}

function shouldStartCarouselGesture(event) {
  if (!playerPanel || channelsCache.length <= 1) return false;
  if (event.pointerType === 'mouse' && event.button !== 0) return false;
  if (isInteractiveControl(event.target)) return false;
  return true;
}

function handleCarouselPointerDown(event) {
  if (!shouldStartCarouselGesture(event) || playerCarouselState.isDragging) return;
  playerCarouselState.isDragging = true;
  playerCarouselState.pointerId = event.pointerId;
  playerCarouselState.startX = event.clientX;
  playerCarouselState.deltaX = 0;
  playerCarouselState.width = playerPanel?.clientWidth || window.innerWidth || 1;
  playerCarouselState.captured = false;
  playerPanel.classList.add('is-dragging');
  if (playerPanel?.setPointerCapture) {
    try {
      playerPanel.setPointerCapture(event.pointerId);
      playerCarouselState.captured = true;
    } catch (err) {
      console.debug('Pointer capture failed', err);
    }
  }
}

function handleCarouselPointerMove(event) {
  if (!playerCarouselState.isDragging || event.pointerId !== playerCarouselState.pointerId) return;
  const delta = event.clientX - playerCarouselState.startX;
  const max = playerCarouselState.width ? playerCarouselState.width * 1.1 : 400;
  playerCarouselState.deltaX = Math.max(-max, Math.min(max, delta));
  updatePlayerCarouselPosition(getActiveChannelIndex(), false);
  if (Math.abs(delta) > 6) event.preventDefault();
}

function releaseCarouselPointer(pointerId) {
  if (playerCarouselState.captured && playerPanel?.releasePointerCapture) {
    try {
      playerPanel.releasePointerCapture(pointerId);
    } catch (err) {
      console.debug('Pointer release failed', err);
    }
  }
  playerCarouselState.captured = false;
  playerPanel?.classList.remove('is-dragging');
}

function finishCarouselGesture(event, cancelled = false) {
  if (!playerCarouselState.isDragging || event.pointerId !== playerCarouselState.pointerId) return;
  const delta = playerCarouselState.deltaX;
  const width = playerCarouselState.width || 1;
  const threshold = Math.min(140, width * 0.25);
  releaseCarouselPointer(event.pointerId);
  const moved = !cancelled && Math.abs(delta) >= threshold;
  let changed = false;
  if (moved) {
    changed = delta < 0 ? selectAdjacentChannel(1) : selectAdjacentChannel(-1);
  }
  playerCarouselState = {
    isDragging: false,
    pointerId: null,
    startX: 0,
    deltaX: 0,
    width: 0,
    captured: false,
  };
  if (!changed) {
    syncPlayerCarouselToActive({ animate: true });
  }
}

function handleCarouselPointerUp(event) {
  finishCarouselGesture(event, false);
}

function handleCarouselPointerCancel(event) {
  finishCarouselGesture(event, true);
}

function handleCarouselKeydown(event) {
  if (!playerPanel || !channelsCache.length) return;
  if (event.altKey || event.ctrlKey || event.metaKey) return;
  if (event.key === 'ArrowLeft') {
    if (selectAdjacentChannel(-1)) event.preventDefault();
  } else if (event.key === 'ArrowRight') {
    if (selectAdjacentChannel(1)) event.preventDefault();
  }
}

function populateSpotifyChannelSelect() {
  if (!spotifyChannelSelect) return;
  spotifyChannelSelect.innerHTML = '';
  if (!channelsCache.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No channels available';
    spotifyChannelSelect.appendChild(option);
    spotifyChannelSelect.disabled = true;
    return;
  }
  spotifyChannelSelect.disabled = false;
  channelsCache.forEach(channel => {
    const option = document.createElement('option');
    option.value = channel.id;
    option.textContent = channel.name || channel.id;
    spotifyChannelSelect.appendChild(option);
  });
  const resolved = getSettingsChannelId();
  if (resolved) spotifyChannelSelect.value = resolved;
}

function getSettingsChannelId() {
  if (spotifySettingsChannelId && channelsCache.some(ch => ch.id === spotifySettingsChannelId)) {
    return spotifySettingsChannelId;
  }
  spotifySettingsChannelId = getActiveChannelId();
  if (spotifyChannelSelect && spotifySettingsChannelId) {
    spotifyChannelSelect.value = spotifySettingsChannelId;
  }
  return spotifySettingsChannelId;
}

function renderChannelsPanel() {
  if (!channelsPanel) return;
  channelsPanel.innerHTML = '';
  channelFormRefs.clear();
  if (!channelsCache.length) {
    channelsPanel.innerHTML = '<div class="muted">No channels configured yet.</div>';
    return;
  }
  channelsCache.forEach(channel => {
    const pending = channelPendingEdits.get(channel.id) || {};
    const nameValue = Object.prototype.hasOwnProperty.call(pending, 'name')
      ? pending.name
      : channel.name || '';
    const hasPendingColor = Object.prototype.hasOwnProperty.call(pending, 'color');
    const colorValue = hasPendingColor ? pending.color : channel.color;
    const enabled = isChannelEnabled(channel);

    const card = document.createElement('div');
    card.className = 'channel-card';
    card.dataset.channelId = channel.id;
    card.dataset.channelEnabled = enabled ? 'true' : 'false';

    const header = document.createElement('div');
    header.className = 'channel-card-header';

    const nameGroup = document.createElement('div');
    const nameLabel = document.createElement('label');
    nameLabel.textContent = 'Channel name';
    const nameInput = document.createElement('input');
    nameInput.value = nameValue;
    nameGroup.appendChild(nameLabel);
    nameGroup.appendChild(nameInput);
    header.appendChild(nameGroup);

    const colorGroup = document.createElement('div');
    const colorLabel = document.createElement('label');
    colorLabel.textContent = 'Accent colour';
    const colorInputs = document.createElement('div');
    colorInputs.className = 'channel-color-inputs';
    const colorPicker = document.createElement('input');
    colorPicker.type = 'color';
    colorPicker.value = normalizeChannelColorInput(colorValue) || '#22c55e';
    const colorText = document.createElement('input');
    colorText.type = 'text';
    colorText.placeholder = '#22c55e';
    colorText.value = colorValue || '';
    colorInputs.appendChild(colorPicker);
    colorInputs.appendChild(colorText);
    colorGroup.appendChild(colorLabel);
    colorGroup.appendChild(colorInputs);
    header.appendChild(colorGroup);

    card.appendChild(header);

  const availability = document.createElement('div');
  availability.className = 'channel-card-availability';

  const availabilityMeta = document.createElement('div');
  availabilityMeta.className = 'channel-availability-meta';
  const availabilityLabel = document.createElement('div');
  availabilityLabel.className = 'label';
  availabilityLabel.textContent = 'Channel availability';
  const availabilityHelp = document.createElement('div');
  availabilityHelp.className = 'channel-availability-help';
  availabilityHelp.textContent = 'Hidden from the player carousel when disabled.';
  availabilityMeta.appendChild(availabilityLabel);
  availabilityMeta.appendChild(availabilityHelp);

  const availabilityControl = document.createElement('div');
  availabilityControl.className = 'channel-availability-control';
  const availabilityToggle = document.createElement('input');
  availabilityToggle.type = 'checkbox';
  availabilityToggle.checked = enabled;
  availabilityToggle.setAttribute('aria-label', `Toggle ${nameValue || channel.id} availability`);
  const availabilityState = document.createElement('span');
  availabilityState.className = 'channel-availability-state';
  availabilityState.textContent = enabled ? 'Enabled' : 'Disabled';
  availabilityControl.appendChild(availabilityToggle);
  availabilityControl.appendChild(availabilityState);

  availability.appendChild(availabilityMeta);
  availability.appendChild(availabilityControl);
  card.appendChild(availability);

    const status = document.createElement('div');
    status.className = 'channel-card-status';
    status.textContent = 'Synced';
    card.appendChild(status);

    const actions = document.createElement('div');
    actions.className = 'channel-card-actions';
    const saveBtn = document.createElement('button');
    saveBtn.type = 'button';
    saveBtn.className = 'small-btn';
    saveBtn.textContent = 'Save changes';
    saveBtn.disabled = true;
    actions.appendChild(saveBtn);
    card.appendChild(actions);

    channelsPanel.appendChild(card);

    const refs = {
      card,
      nameInput,
      colorPicker,
      colorTextInput: colorText,
      saveButton: saveBtn,
      statusEl: status,
      availabilityToggle,
      availabilityState,
    };
    channelFormRefs.set(channel.id, refs);

    nameInput.addEventListener('input', () => updateChannelCardState(channel.id));
    colorPicker.addEventListener('input', () => {
      colorText.value = colorPicker.value;
      updateChannelCardState(channel.id);
    });
    colorText.addEventListener('input', () => {
      const normalized = normalizeChannelColorInput(colorText.value);
      if (normalized) {
        colorPicker.value = normalized;
      }
      updateChannelCardState(channel.id);
    });
    availabilityToggle.addEventListener('change', () => handleChannelAvailabilityChange(channel.id, availabilityToggle));
    saveBtn.addEventListener('click', () => saveChannelChanges(channel.id));

    updateChannelCardState(channel.id);
  });
}

function updateChannelCardState(channelId) {
  const refs = channelFormRefs.get(channelId);
  if (!refs) return;
  const base = getChannelById(channelId) || {};
  const channelEnabled = isChannelEnabled(base);
  const nameValue = (refs.nameInput.value || '').trim();
  const colorRaw = (refs.colorTextInput.value || '').trim();
  const normalizedColor = colorRaw ? normalizeChannelColorInput(colorRaw) : null;
  if (colorRaw && !normalizedColor) {
    refs.statusEl.textContent = 'Use #RGB or #RRGGBB';
    refs.card.dataset.dirty = 'false';
    refs.saveButton.disabled = true;
    channelPendingEdits.delete(channelId);
    return;
  }
  if (!nameValue) {
    refs.statusEl.textContent = 'Channel name required';
    refs.card.dataset.dirty = 'false';
    refs.saveButton.disabled = true;
    channelPendingEdits.delete(channelId);
    return;
  }
  const pending = {};
  let hasDiff = false;
  if (nameValue !== base.name) {
    pending.name = nameValue;
    hasDiff = true;
  }
  const baseColor = base.color || null;
  const nextColor = normalizedColor || null;
  if (nextColor !== baseColor) {
    pending.color = nextColor;
    hasDiff = true;
  }
  if (hasDiff) {
    channelPendingEdits.set(channelId, pending);
    refs.card.dataset.dirty = 'true';
    refs.statusEl.textContent = 'Unsaved changes';
    refs.saveButton.disabled = false;
  } else {
    channelPendingEdits.delete(channelId);
    refs.card.dataset.dirty = 'false';
    refs.statusEl.textContent = channelEnabled ? 'Channel enabled' : 'Channel disabled';
    refs.saveButton.disabled = true;
  }
}

async function saveChannelChanges(channelId) {
  const pending = channelPendingEdits.get(channelId);
  const refs = channelFormRefs.get(channelId);
  if (!pending || !refs) return;
  try {
    refs.saveButton.disabled = true;
    refs.saveButton.textContent = 'Saving…';
    refs.statusEl.textContent = 'Saving…';
    const res = await fetch(`/api/channels/${encodeURIComponent(channelId)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pending),
    });
    await ensureOk(res);
    const data = await res.json();
    channelPendingEdits.delete(channelId);
    if (data?.channel) {
      const idx = channelsCache.findIndex(ch => ch.id === channelId);
      if (idx !== -1) channelsCache[idx] = data.channel;
    }
    showSuccess('Channel updated.');
    renderPlayerCarousel();
    populateSpotifyChannelSelect();
    renderChannelsPanel();
    if (channelId === getActiveChannelId()) {
      applyChannelTheme(getActiveChannel());
    }
    refreshNodeVolumeAccents();
  } catch (err) {
    refs.statusEl.textContent = `Save failed: ${err.message}`;
    showError(`Failed to update channel: ${err.message}`);
  } finally {
    if (refs.saveButton) {
      refs.saveButton.textContent = 'Save changes';
      refs.saveButton.disabled = !channelPendingEdits.has(channelId);
    }
  }
}

async function handleChannelAvailabilityChange(channelId, checkboxEl) {
  const refs = channelFormRefs.get(channelId);
  if (!checkboxEl) return;
  const nextEnabled = checkboxEl.checked;
  const previousActiveId = getActiveChannelId();
  const canUpdateStatus = !channelPendingEdits.has(channelId);
  checkboxEl.disabled = true;
  if (refs?.availabilityState) {
    refs.availabilityState.textContent = nextEnabled ? 'Enabling…' : 'Disabling…';
  }
  if (canUpdateStatus && refs?.statusEl) {
    refs.statusEl.textContent = nextEnabled ? 'Enabling…' : 'Disabling…';
  }
  try {
    const res = await fetch(`/api/channels/${encodeURIComponent(channelId)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: nextEnabled }),
    });
    await ensureOk(res);
    const data = await res.json();
    let updatedChannel = data?.channel || null;
    const idx = channelsCache.findIndex(ch => ch.id === channelId);
    if (idx !== -1) {
      updatedChannel = updatedChannel || { ...channelsCache[idx], enabled: nextEnabled };
      channelsCache[idx] = updatedChannel;
    }
    if (refs) {
      refs.card.dataset.channelEnabled = nextEnabled ? 'true' : 'false';
      if (refs.availabilityState) {
        refs.availabilityState.textContent = nextEnabled ? 'Enabled' : 'Disabled';
      }
      if (canUpdateStatus && refs.statusEl) {
        refs.statusEl.textContent = nextEnabled ? 'Channel enabled' : 'Channel disabled';
      }
    }
    const nextActiveId = getActiveChannelId();
    renderPlayerCarousel();
    populateSpotifyChannelSelect();
    refreshNodeVolumeAccents();
    applyChannelTheme(getActiveChannel());
    if (!getPlayerChannels().length) {
      setPlayerIdleState('Enable a channel to control playback', { forceClear: true });
    }
    if (previousActiveId !== nextActiveId) {
      onActiveChannelChanged(previousActiveId, nextActiveId);
    }
    showSuccess(nextEnabled ? 'Channel enabled' : 'Channel disabled');
  } catch (err) {
    checkboxEl.checked = !nextEnabled;
    if (refs?.availabilityState) {
      refs.availabilityState.textContent = isChannelEnabled(getChannelById(channelId)) ? 'Enabled' : 'Disabled';
    }
    if (refs?.statusEl && canUpdateStatus) {
      refs.statusEl.textContent = `Toggle failed: ${err.message}`;
    }
    showError(`Failed to ${nextEnabled ? 'enable' : 'disable'} channel: ${err.message}`);
  } finally {
    checkboxEl.disabled = false;
  }
}

