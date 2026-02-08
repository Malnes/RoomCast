const nodesEl = document.getElementById('nodes');
const clientsSettingsEl = document.getElementById('clients-settings');
const errorEl = document.getElementById('error');
const successEl = document.getElementById('success');
const persistentAlertEl = document.getElementById('persistent-alert');
const persistentAlertMessage = document.getElementById('persistent-alert-message');
const persistentAlertAction = document.getElementById('persistent-alert-action');
const persistentAlertDismiss = document.getElementById('persistent-alert-dismiss');
const addNodeToggle = document.getElementById('add-node-button');
const reviewWebNodeRequestsBtn = document.getElementById('review-web-node-requests');
const addMenu = document.getElementById('add-menu');
const addMenuNodeBtn = document.getElementById('add-menu-node');
const addMenuSectionBtn = document.getElementById('add-menu-section');
const addNodeOverlay = document.getElementById('add-node-overlay');
const addNodeTitleEl = document.getElementById('add-node-title');
const addNodeCloseBtn = document.getElementById('add-node-close');
const addNodeOptionsPane = document.getElementById('add-node-options');
const addNodeSectionPane = document.getElementById('add-node-section');
const addSectionNameInput = document.getElementById('add-section-name');
const addSectionCreateBtn = document.getElementById('add-section-create');
const addNodeSonosPane = document.getElementById('add-node-sonos');
const addNodeBackBtn = document.getElementById('add-node-back');
const addNodeOptionHardware = document.getElementById('add-node-option-hardware');
const addNodeOptionController = document.getElementById('add-node-option-controller');
const addNodeOptionWeb = document.getElementById('add-node-option-web');
const addNodeOptionSonos = document.getElementById('add-node-option-sonos');
const addNodeSonosScanBtn = document.getElementById('add-node-sonos-scan');
const addNodeSonosSpinner = document.getElementById('add-node-sonos-spinner');
const addNodeSonosStatus = document.getElementById('add-node-sonos-status');
const addNodeSonosList = document.getElementById('add-node-sonos-list');
const saveSpotifyBtn = document.getElementById('save-spotify');
const spName = document.getElementById('sp-name');
const spBitrate = document.getElementById('sp-bitrate');
const spInitVol = document.getElementById('sp-initvol');
setRangeProgress(spInitVol, spInitVol?.value || 0, spInitVol?.max || 100);
const spNormalise = document.getElementById('sp-normalise');
const spShowOutputVolume = document.getElementById('sp-show-output-volume');
const spotifyLinkWizardOpenBtn = document.getElementById('spotify-link-wizard-open');
const spotifyLinkWizard = document.getElementById('spotify-link-wizard');
const spotifyLinkCloseBtn = document.getElementById('spotify-link-close');
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
const webNodeApprovalOverlay = document.getElementById('web-node-approval-overlay');
const webNodeApprovalSubtitle = document.getElementById('web-node-approval-subtitle');
const webNodeApprovalName = document.getElementById('web-node-approval-name');
const webNodeApprovalInfo = document.getElementById('web-node-approval-info');
const webNodeApprovalQueue = document.getElementById('web-node-approval-queue');
const webNodeApprovalCloseBtn = document.getElementById('web-node-approval-close');
const webNodeApprovalApproveBtn = document.getElementById('web-node-approval-approve');
const webNodeApprovalDenyBtn = document.getElementById('web-node-approval-deny');
const masterVolume = document.getElementById('master-volume');
setRangeProgress(masterVolume, masterVolume?.value || 0, masterVolume?.max || 100);
const masterVolumeValue = document.getElementById('master-volume-value');
const spotifySourceVolume = document.getElementById('spotify-source-volume');
setRangeProgress(spotifySourceVolume, spotifySourceVolume?.value || 0, spotifySourceVolume?.max || 100);
const spotifySourceVolumeValue = document.getElementById('spotify-source-volume-value');
const playerVolumeInline = document.getElementById('player-volume-inline');
const playerVolumeToggle = document.getElementById('player-volume-toggle');
const playerMasterVolumeControl = document.getElementById('player-master-volume-control');
const playerSpotifyVolumeControl = document.getElementById('player-spotify-volume-control');
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
const playerText = document.getElementById('player-text');
const streamInfoOverlay = document.getElementById('stream-info-overlay');
const streamInfoCloseBtn = document.getElementById('stream-info-close');
const streamInfoChannelLabel = document.getElementById('stream-info-channel');
const streamInfoLoading = document.getElementById('stream-info-loading');
const streamInfoError = document.getElementById('stream-info-error');
const streamInfoContent = document.getElementById('stream-info-content');
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
const playlistModalTitle = document.getElementById('playlist-modal-title');
const playlistSubtitle = document.getElementById('playlist-modal-subtitle');
const playlistSearchInput = document.getElementById('playlist-search');
const playlistSortSelect = document.getElementById('playlist-sort');
const channelsPanel = document.getElementById('channels-panel');
const spotifyChannelSelect = document.getElementById('spotify-channel-select');
const spotifyAccountInfo = document.getElementById('spotify-account-info');
let spotifyAuthLinked = false;
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
const channelsCountSelect = document.getElementById('channels-count');
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
if (playerText) {
  playerText.addEventListener('click', handleStreamInfoRequest);
  playerText.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      handleStreamInfoRequest();
    }
  });
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
    if (typeof playlistOverlayMode !== 'undefined' && playlistOverlayMode === 'audiobookshelf') {
      renderAudiobookshelfEpisodes(typeof absEpisodesCache !== 'undefined' ? absEpisodesCache : []);
      return;
    }
    if (!playlistSelected) {
      if (!playlistTrackSearchTerm) return;
      return;
    }
    const state = ensurePlaylistTrackState(playlistSelected.id);
    renderPlaylistTracks(state);
  });
}
if (streamInfoCloseBtn) {
  streamInfoCloseBtn.addEventListener('click', closeStreamInfoOverlay);
}
if (streamInfoOverlay) {
  streamInfoOverlay.addEventListener('click', event => {
    if (event.target === streamInfoOverlay) {
      closeStreamInfoOverlay();
    }
  });
}
document.addEventListener('keydown', handleStreamInfoKeydown, true);
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
let spotifySettingsSourceId = null;
let spotifySourcesCache = [];
let spotifySourcesFetchPromise = null;
let spotifySourcesLoaded = false;
const spotifyPlayerUiConfigBySource = new Map();
const spotifyPlayerUiConfigFetchBySource = new Map();
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
const RADIO_TABS = ['favorites', 'genres', 'top', 'countries', 'search'];
let radioActiveTab = 'genres';
let radioActiveChannelId = null;
let radioTopMetric = 'votes';
const radioDataCache = {
  genres: null,
  countries: null,
  top: { votes: null, clicks: null },
  favorites: null,
};
let radioResultsAbortController = null;
let radioCurrentResults = [];
let radioResultsContextLabel = '';

let playlistPickerMode = false;
let playlistPickerOnSelect = null;
let playlistPickerTitle = '';
let playlistPickerSubtitle = '';

const playerContextMenu = document.getElementById('player-context-menu');
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
let streamInfoAbortController = null;
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
const browserNodeToggle = document.getElementById('browser-node-toggle');
const userStatusEl = document.getElementById('user-status');
const currentUserNameEl = document.getElementById('current-user-name');
const currentUserRoleEl = document.getElementById('current-user-role');
const logoutButton = document.getElementById('logout-button');
const usersToolbarEl = document.getElementById('users-toolbar');
const usersAddBtn = document.getElementById('users-add-btn');
const usersPanelLockEl = document.getElementById('users-panel-lock');
const usersListEl = document.getElementById('users-list');
const userModalOverlay = document.getElementById('user-modal-overlay');
const userModalTitle = document.getElementById('user-modal-title');
const userModalCloseBtn = document.getElementById('user-modal-close');
const userModalForm = document.getElementById('user-modal-form');
const userModalUsernameInput = document.getElementById('user-modal-username');
const userModalRoleInput = document.getElementById('user-modal-role');
const userModalPasswordLabel = document.getElementById('user-modal-password-label');
const userModalPasswordInput = document.getElementById('user-modal-password');
const userModalPasswordHelp = document.getElementById('user-modal-password-help');
const userModalDeleteNote = document.getElementById('user-modal-delete-note');
const userModalDeleteBtn = document.getElementById('user-modal-delete');
const userModalCancelBtn = document.getElementById('user-modal-cancel');
const userModalSaveBtn = document.getElementById('user-modal-save');
let authState = { initialized: false, authenticated: false, server_name: 'RoomCast', user: null };
let appBootstrapped = false;
let nodePollTimer = null;
let playerPollTimer = null;
let usersCache = [];
let usersLoaded = false;
const privateBrowserNodeState = {
  pc: null,
  ws: null,
  nodeId: null,
  audio: null,
  starting: null,
};
const NODE_POLL_INTERVAL_MS = 4000;
let nodesSocket = null;
let nodesSocketConnected = false;
let nodesSocketRetryTimer = null;
let nodesSocketRetryAttempt = 0;
let nodesSocketShouldConnect = false;
let webNodeRequestQueue = [];
let webNodeActiveRequestId = null;
let webNodeApprovalBusy = false;
let webNodeApprovalManuallyHidden = false;

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
  const isVertical = el.classList?.contains('range-vertical');
  if (isVertical) {
    el.style.background = `linear-gradient(0deg, ${accent} 0%, ${accent} ${percent}%, ${inactive} ${percent}%, ${inactive} 100%)`;
    el.style.backgroundSize = '4px 100%';
    el.style.backgroundPosition = 'center';
    el.style.backgroundRepeat = 'no-repeat';
  } else {
    el.style.background = `linear-gradient(90deg, ${accent} 0%, ${accent} ${percent}%, ${inactive} ${percent}%, ${inactive} 100%)`;
    el.style.backgroundSize = '100% 4px';
    el.style.backgroundPosition = 'center';
    el.style.backgroundRepeat = 'no-repeat';
  }
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

if (browserNodeToggle) {
  browserNodeToggle.addEventListener('change', handleBrowserNodeToggle);
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
  setWebNodeRequests([]);
  resetChannelUiState();
  stopPrivateBrowserNodeSession({ unregister: false, silent: true });
}

function ensurePrivateBrowserAudio() {
  if (privateBrowserNodeState.audio) return privateBrowserNodeState.audio;
  const audio = document.createElement('audio');
  audio.autoplay = true;
  audio.muted = true;
  audio.controls = false;
  audio.playsInline = true;
  audio.style.display = 'none';
  audio.id = 'private-browser-node-audio';
  document.body.appendChild(audio);
  privateBrowserNodeState.audio = audio;
  return audio;
}

function requestPrivateBrowserAudioUnlock() {
  const audio = privateBrowserNodeState.audio;
  if (!audio) return;
  if (audio.dataset.unlocked === 'true') return;
  const unlock = () => {
    audio.muted = false;
    audio.play().catch(() => {});
    audio.dataset.unlocked = 'true';
  };
  const onceOpts = { once: true, capture: true };
  document.addEventListener('pointerdown', unlock, onceOpts);
  document.addEventListener('keydown', unlock, onceOpts);
}

async function waitForIce(connection, timeoutMs = 2500) {
  if (connection.iceGatheringState === 'complete') return;
  await new Promise(resolve => {
    const timer = setTimeout(() => {
      connection.removeEventListener('icegatheringstatechange', check);
      resolve();
    }, timeoutMs);
    function check() {
      if (connection.iceGatheringState === 'complete') {
        clearTimeout(timer);
        connection.removeEventListener('icegatheringstatechange', check);
        resolve();
      }
    }
    connection.addEventListener('icegatheringstatechange', check);
  });
}

function privateBrowserWsUrl(nodeId) {
  const proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
  return `${proto}${window.location.host}/ws/web-node?node_id=${encodeURIComponent(nodeId)}`;
}

function handlePrivateBrowserControlMessage(msg) {
  const audio = privateBrowserNodeState.audio;
  if (!audio || !msg) return;
  if (msg.type === 'volume') {
    const percent = Math.max(0, Math.min(100, Number(msg.percent ?? 0)));
    audio.volume = percent / 100;
  } else if (msg.type === 'mute') {
    audio.muted = !!msg.muted;
  } else if (msg.type === 'session' && msg.state === 'ended') {
    stopPrivateBrowserNodeSession({ unregister: false, silent: true });
  }
}

function connectPrivateBrowserSocket(nodeId) {
  if (!nodeId) return;
  const ws = new WebSocket(privateBrowserWsUrl(nodeId));
  ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      handlePrivateBrowserControlMessage(msg);
    } catch (err) {
      console.warn('Invalid browser node control message', err);
    }
  };
  ws.onclose = () => {
    if (privateBrowserNodeState.nodeId) {
      stopPrivateBrowserNodeSession({ unregister: false, silent: true });
    }
  };
  privateBrowserNodeState.ws = ws;
}

async function startPrivateBrowserNodeSession({ silent = false } = {}) {
  if (!authState?.user) return;
  if (privateBrowserNodeState.pc || privateBrowserNodeState.starting) return privateBrowserNodeState.starting;
  privateBrowserNodeState.starting = (async () => {
    try {
      const audio = ensurePrivateBrowserAudio();
      const name = `${authState.user.username || 'User'} browser`;
      const pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
      privateBrowserNodeState.pc = pc;
      pc.ontrack = (event) => {
        try {
          if (event.receiver && typeof event.receiver.playoutDelayHint === 'number') {
            event.receiver.playoutDelayHint = 0.2;
          }
        } catch (_) {
          /* no-op */
        }
        const [stream] = event.streams;
        if (stream) {
          audio.srcObject = stream;
          audio.play().catch(() => {});
          requestPrivateBrowserAudioUnlock();
        }
      };
      pc.onconnectionstatechange = () => {
        if (!privateBrowserNodeState.pc) return;
        if (['disconnected', 'failed'].includes(pc.connectionState)) {
          stopPrivateBrowserNodeSession({ unregister: false, silent: true });
        }
      };

      const rawOffer = await pc.createOffer({ offerToReceiveAudio: true });
      await pc.setLocalDescription(rawOffer);
      await waitForIce(pc);

      const res = await fetch('/api/web-nodes/private-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          sdp: pc.localDescription?.sdp,
          type: pc.localDescription?.type,
        }),
      });
      await ensureOk(res);
      const data = await res.json();
      privateBrowserNodeState.nodeId = data?.node?.id || null;
      await pc.setRemoteDescription({ type: data.answer_type, sdp: data.answer });
      if (privateBrowserNodeState.nodeId) {
        connectPrivateBrowserSocket(privateBrowserNodeState.nodeId);
      }
      requestPrivateBrowserAudioUnlock();
      if (!silent) showSuccess('Browser node connected.');
    } catch (err) {
      stopPrivateBrowserNodeSession({ unregister: false, silent: true });
      if (!silent) showError(`Failed to start browser node: ${err.message}`);
      throw err;
    } finally {
      privateBrowserNodeState.starting = null;
    }
  })();
  return privateBrowserNodeState.starting;
}

async function stopPrivateBrowserNodeSession({ unregister = true, silent = false } = {}) {
  const nodeId = privateBrowserNodeState.nodeId;
  if (privateBrowserNodeState.ws) {
    privateBrowserNodeState.ws.onclose = null;
    try { privateBrowserNodeState.ws.close(); } catch (_) {}
    privateBrowserNodeState.ws = null;
  }
  if (privateBrowserNodeState.pc) {
    try { privateBrowserNodeState.pc.close(); } catch (_) {}
    privateBrowserNodeState.pc = null;
  }
  privateBrowserNodeState.nodeId = null;
  if (unregister && nodeId) {
    try {
      const res = await fetch('/api/web-nodes/private-session', { method: 'DELETE' });
      await ensureOk(res);
    } catch (err) {
      if (!silent) console.warn('Failed to unregister browser node', err);
    }
  }
}

async function updateUserSettings(updates) {
  const res = await fetch('/api/users/me/settings', {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates || {}),
  });
  await ensureOk(res);
  const data = await res.json();
  if (authState?.user) {
    authState.user.settings = data?.settings || {};
  }
  return data;
}

async function syncPrivateBrowserNodePreference() {
  if (!authState?.user) return;
  const enabled = !!authState.user.settings?.browser_node_enabled;
  if (enabled) {
    await startPrivateBrowserNodeSession({ silent: true });
  } else {
    await stopPrivateBrowserNodeSession({ unregister: true, silent: true });
  }
}

async function handleBrowserNodeToggle(evt) {
  if (!browserNodeToggle) return;
  const desired = browserNodeToggle.checked;
  browserNodeToggle.disabled = true;
  try {
    await updateUserSettings({ browser_node_enabled: desired });
    if (desired) {
      await startPrivateBrowserNodeSession();
    } else {
      await stopPrivateBrowserNodeSession({ unregister: true });
    }
  } catch (err) {
    browserNodeToggle.checked = !desired;
    try {
      await updateUserSettings({ browser_node_enabled: !desired });
    } catch (_) {
      /* ignore rollback */
    }
    showError(err.message);
  } finally {
    browserNodeToggle.disabled = false;
    syncGeneralSettingsUI();
  }
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
  if (isAdminUser() && typeof fetchWebNodeRequests === 'function') {
    fetchWebNodeRequests({ forceOpen: true, silent: true });
  }
}

  if (playerText) {
    playerText.addEventListener('click', handleStreamInfoRequest);
    playerText.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        handleStreamInfoRequest();
      }
    });
  }
function enterAppShell() {
  if (authShell) authShell.hidden = true;
  if (appShell) appShell.hidden = false;
  setServerBranding(authState.server_name);
  updateUserStatusUI();
  syncGeneralSettingsUI();
  syncPrivateBrowserNodePreference();
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
const SPOTIFY_ALERT_HELP = 'Open Settings > Music providers > Spotify settings and tap "Save Spotify config" to reconnect.';
const PWA_UPDATE_ALERT_KEY = 'pwa-update';
const PWA_UPDATE_ACTION_LABEL = 'Update now';
const WEB_NODE_ALERT_KEY = 'web-node-request';
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

function handleStreamInfoRequest() {
  if (!activeChannelId) {
    showError('No active channel selected.');
    return;
  }
  openStreamInfoOverlay(activeChannelId);
}

async function openStreamInfoOverlay(channelId) {
  if (!streamInfoOverlay || !streamInfoLoading || !streamInfoContent) return;
  if (streamInfoAbortController) {
    streamInfoAbortController.abort();
  }
  streamInfoAbortController = new AbortController();
  streamInfoOverlay.hidden = false;
  streamInfoOverlay.classList.add('is-open');
  streamInfoOverlay.setAttribute('aria-hidden', 'false');
  streamInfoLoading.hidden = false;
  if (streamInfoError) {
    streamInfoError.hidden = true;
    streamInfoError.textContent = '';
  }
  streamInfoContent.hidden = true;
  streamInfoContent.innerHTML = '';
  if (streamInfoChannelLabel) {
    streamInfoChannelLabel.textContent = resolveChannelName(channelId);
  }
  try {
    const payload = await fetchStreamDiagnostics(channelId, streamInfoAbortController.signal);
    renderStreamInfo(payload, channelId);
  } catch (err) {
    if (err?.name === 'AbortError') return;
    const message = getErrorMessage(err) || 'Failed to load stream diagnostics.';
    if (streamInfoError) {
      streamInfoError.textContent = message;
      streamInfoError.hidden = false;
    } else {
      showError(message);
    }
  } finally {
    if (streamInfoLoading) streamInfoLoading.hidden = true;
    streamInfoAbortController = null;
  }
}

function closeStreamInfoOverlay() {
  if (streamInfoAbortController) {
    streamInfoAbortController.abort();
    streamInfoAbortController = null;
  }
  if (!streamInfoOverlay) return;
  streamInfoOverlay.classList.remove('is-open');
  streamInfoOverlay.setAttribute('aria-hidden', 'true');
  streamInfoOverlay.hidden = true;
}

function handleStreamInfoKeydown(event) {
  if (event.key !== 'Escape') return;
  if (!streamInfoOverlay || streamInfoOverlay.hidden || !streamInfoOverlay.classList.contains('is-open')) return;
  event.preventDefault();
  closeStreamInfoOverlay();
}

async function fetchStreamDiagnostics(channelId, signal) {
  const query = channelId ? `?channel_id=${encodeURIComponent(channelId)}` : '';
  const res = await fetch(`/api/streams/diagnostics${query}`, { signal });
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = body.detail;
    } catch (_) {
      /* ignore */
    }
    throw new Error(detail);
  }
  return res.json();
}

function renderStreamInfo(payload, channelId) {
  if (!streamInfoContent) return;
  const channels = Array.isArray(payload?.channels) ? payload.channels : [];
  const channel = channels.find(entry => entry?.id === channelId) || channels[0] || null;
  streamInfoContent.innerHTML = '';
  if (!channel) {
    const empty = document.createElement('div');
    empty.className = 'stream-info-empty';
    empty.textContent = 'Channel diagnostics are unavailable right now.';
    streamInfoContent.appendChild(empty);
    streamInfoContent.hidden = false;
    return;
  }
  if (streamInfoChannelLabel) {
    streamInfoChannelLabel.textContent = resolveChannelName(channel.id);
  }
  const fragment = document.createDocumentFragment();
  const sourceSection = buildStreamSourceSection(channel);
  if (sourceSection) fragment.appendChild(sourceSection);
  const snapSection = buildSnapserverSection(channel, payload?.snapserver);
  if (snapSection) fragment.appendChild(snapSection);
  const rtcSection = buildWebrtcSection(channel);
  if (rtcSection) fragment.appendChild(rtcSection);
  if (!fragment.childNodes.length) {
    const empty = document.createElement('div');
    empty.className = 'stream-info-empty';
    empty.textContent = 'No diagnostics metrics available for this channel.';
    fragment.appendChild(empty);
  }
  streamInfoContent.appendChild(fragment);
  streamInfoContent.hidden = false;
}

function resolveChannelName(channelId) {
  if (!channelId) return 'Channel';
  const match = channelsCache.find(ch => ch.id === channelId);
  return (match?.name || channelId).trim();
}

function buildStreamSourceSection(channel) {
  const section = createInfoSection('Source', 'Input path and snapstream mapping');
  const metrics = createMetricsGrid();
  metrics.appendChild(createMetric('Origin', describeChannelOrigin(channel)));
  if (channel?.spotify?.bitrate_kbps) {
    metrics.appendChild(createMetric('Spotify bitrate', `${channel.spotify.bitrate_kbps} kbps`));
  }
  if (channel?.spotify?.normalisation !== undefined) {
    metrics.appendChild(createMetric('Normalisation', formatBoolean(channel.spotify.normalisation)));
  }
  if (channel?.spotify?.status) {
    const status = channel.spotify.status_message ? `${channel.spotify.status} · ${channel.spotify.status_message}` : channel.spotify.status;
    metrics.appendChild(createMetric('Librespot status', status));
  }
  metrics.appendChild(createMetric('Snap stream', channel.snap_stream || '—'));
  if (channel?.fifo_path) {
    metrics.appendChild(createMetric('FIFO path', channel.fifo_path));
  }
  section.appendChild(metrics);
  return section;
}

function buildSnapserverSection(channel, snapserverMeta) {
  const stream = channel?.snapserver_stream;
  const hasClients = Array.isArray(channel?.hardware_clients) && channel.hardware_clients.length > 0;
  if (!stream && !hasClients && !snapserverMeta?.error) {
    return null;
  }
  const section = createInfoSection('Snapserver stream', 'PCM feed served to hardware nodes');
  const metrics = createMetricsGrid();
  if (stream?.format) {
    metrics.appendChild(createMetric('Sample format', formatSampleFormat(stream.format)));
  }
  if (stream?.codec) {
    metrics.appendChild(createMetric('Codec', stream.codec.toUpperCase()));
  }
  if (stream?.uri) {
    metrics.appendChild(createMetric('URI', stream.uri));
  }
  if (snapserverMeta) {
    const defaultHost = window.location.hostname || 'snapserver';
    const host = `${snapserverMeta.host || defaultHost}:${snapserverMeta.port || 1780}`;
    metrics.appendChild(createMetric('Server', host));
    if (snapserverMeta.error) {
      metrics.appendChild(createMetric('Server state', `Error: ${snapserverMeta.error}`));
    }
  }
  section.appendChild(metrics);
  const heading = document.createElement('div');
  heading.className = 'stream-info-list-title';
  const hardwareTotal = channel?.listeners?.hardware_connected ?? channel?.listeners?.hardware ?? 0;
  heading.textContent = `Hardware listeners (${hardwareTotal})`;
  section.appendChild(heading);
  if (hasClients) {
    const list = document.createElement('ul');
    list.className = 'stream-info-list';
    channel.hardware_clients.forEach(client => {
      list.appendChild(buildHardwareClientItem(client));
    });
    section.appendChild(list);
  } else {
    const empty = document.createElement('div');
    empty.className = 'stream-info-empty';
    empty.textContent = 'No Snapcast clients are currently attached to this stream.';
    section.appendChild(empty);
  }
  return section;
}

function buildWebrtcSection(channel) {
  const relay = channel?.webrtc;
  if (!relay) return null;
  const section = createInfoSection('WebRTC relay', 'Browser node encoder diagnostics');
  const metrics = createMetricsGrid();
  if (relay.sample_rate) {
    metrics.appendChild(createMetric('Relay sample rate', formatSampleRate(relay.sample_rate)));
  }
  if (relay.frame_duration_ms) {
    metrics.appendChild(createMetric('Frame size', `${relay.frame_duration_ms} ms`));
  }
  if (relay.pump) {
    metrics.appendChild(createMetric('Pump bitrate', formatBitrate(relay.pump.avg_bitrate_bps)));
    metrics.appendChild(createMetric('Pump restarts', relay.pump.restarts ?? 0));
    metrics.appendChild(createMetric('Last restart', formatRelativeTime(relay.pump.last_restart)));
    if (relay.pump.last_error) {
      metrics.appendChild(createMetric('Last pump error', relay.pump.last_error));
    }
  }
  if (relay.broadcaster) {
    metrics.appendChild(createMetric('Broadcaster bitrate', formatBitrate(relay.broadcaster.avg_bitrate_bps)));
    metrics.appendChild(createMetric('Queue overflows', relay.broadcaster.queue_overflows ?? 0));
    metrics.appendChild(createMetric('Peak level', formatDbfs(relay.broadcaster.last_peak_dbfs)));
    metrics.appendChild(createMetric('RMS level', formatDbfs(relay.broadcaster.last_rms_dbfs)));
    if (relay.broadcaster.avg_channel_difference !== undefined) {
      metrics.appendChild(createMetric('Avg L/R delta', formatChannelDifference(relay.broadcaster.avg_channel_difference)));
    }
  }
  section.appendChild(metrics);
  const heading = document.createElement('div');
  heading.className = 'stream-info-list-title';
  const sessionCount = Array.isArray(relay.sessions) ? relay.sessions.length : 0;
  heading.textContent = `Web sessions (${sessionCount})`;
  section.appendChild(heading);
  if (sessionCount) {
    const list = document.createElement('ul');
    list.className = 'stream-info-list';
    relay.sessions.forEach(session => list.appendChild(buildWebSessionItem(session, relay)));
    section.appendChild(list);
  } else {
    const empty = document.createElement('div');
    empty.className = 'stream-info-empty';
    empty.textContent = 'No browser listeners are currently connected.';
    section.appendChild(empty);
  }
  return section;
}

function createInfoSection(title, subtitle) {
  const section = document.createElement('section');
  section.className = 'stream-info-section';
  if (title) {
    const heading = document.createElement('div');
    heading.className = 'section-title';
    heading.textContent = title;
    section.appendChild(heading);
  }
  if (subtitle) {
    const sub = document.createElement('div');
    sub.className = 'muted';
    sub.textContent = subtitle;
    section.appendChild(sub);
  }
  return section;
}

function createMetricsGrid() {
  const grid = document.createElement('div');
  grid.className = 'stream-info-metrics';
  return grid;
}

function createMetric(label, value) {
  const metric = document.createElement('div');
  metric.className = 'stream-info-metric';
  const labelEl = document.createElement('div');
  labelEl.className = 'stream-info-metric-label';
  labelEl.textContent = label;
  const valueEl = document.createElement('div');
  valueEl.className = 'stream-info-metric-value';
  valueEl.textContent = value ?? '—';
  metric.appendChild(labelEl);
  metric.appendChild(valueEl);
  return metric;
}

function buildHardwareClientItem(client) {
  const item = document.createElement('li');
  item.className = 'stream-info-list-item';
  const name = client?.node_name || client?.configured_name || client?.host?.name || client?.id || 'Unnamed client';
  const title = document.createElement('div');
  title.className = 'stream-info-metric-value';
  title.textContent = name;
  item.appendChild(title);
  const statusBits = [];
  statusBits.push(client?.connected ? 'Connected' : 'Offline');
  if (client?.version) statusBits.push(`v${client.version}`);
  if (typeof client?.latency_ms === 'number') statusBits.push(`${client.latency_ms} ms latency`);
  const status = document.createElement('div');
  status.className = 'stream-info-list-sub';
  status.textContent = statusBits.join(' · ');
  item.appendChild(status);
  const detailBits = [];
  if (typeof client?.volume_percent === 'number') {
    const volume = client.muted ? `${client.volume_percent}% (muted)` : `${client.volume_percent}%`;
    detailBits.push(`Volume ${volume}`);
  }
  if (client?.node_name && client?.node_id) {
    detailBits.push(`Node ${client.node_name}`);
  }
  if (detailBits.length) {
    const detail = document.createElement('div');
    detail.className = 'stream-info-list-sub';
    detail.textContent = detailBits.join(' · ');
    item.appendChild(detail);
  }
  return item;
}

function buildWebSessionItem(session, relay) {
  const item = document.createElement('li');
  item.className = 'stream-info-list-item';
  const title = document.createElement('div');
  title.className = 'stream-info-metric-value';
  title.textContent = session?.node_name || session?.node_id || 'Browser node';
  item.appendChild(title);
  const stateBits = [];
  if (session?.connection_state) stateBits.push(`Peer ${session.connection_state}`);
  if (session?.ice_state) stateBits.push(`ICE ${session.ice_state}`);
  if (session?.signaling_state) stateBits.push(`SDP ${session.signaling_state}`);
  const state = document.createElement('div');
  state.className = 'stream-info-list-sub';
  state.textContent = stateBits.join(' · ') || 'State unknown';
  item.appendChild(state);
  const queueFrames = Number(session?.pending_frames) || 0;
  const frameDuration = relay?.frame_duration_ms || 20;
  const bufferMs = queueFrames * frameDuration;
  const detailBits = [];
  detailBits.push(`Queue ${queueFrames} frames${bufferMs ? ` (${bufferMs} ms)` : ''}`);
  detailBits.push(`Pan ${formatPan(session?.pan)}`);
  const detail = document.createElement('div');
  detail.className = 'stream-info-list-sub';
  detail.textContent = detailBits.join(' · ');
  item.appendChild(detail);
  return item;
}

function describeChannelOrigin(channel) {
  const source = (channel?.source || 'spotify').toLowerCase();
  if (source === 'radio') {
    const station = channel?.radio_state?.station_name || channel?.radio_state?.stream_url;
    return station ? `Radio · ${station}` : 'Radio';
  }
  if (channel?.spotify?.device_name) {
    return `Spotify · ${channel.spotify.device_name}`;
  }
  return 'Spotify';
}

function formatSampleFormat(format) {
  if (!format) return '—';
  const rate = formatSampleRate(format.sample_rate);
  const depth = format?.bit_depth ? `${format.bit_depth}-bit` : '—';
  const channels = format?.channels ? format.channels.toString().toUpperCase() : 'stereo';
  return `${rate} / ${depth} / ${channels}`;
}

function formatSampleRate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return value ? String(value) : '—';
  if (numeric >= 1000) {
    const khz = numeric / 1000;
    return `${khz % 1 === 0 ? khz.toFixed(0) : khz.toFixed(1)} kHz`;
  }
  return `${numeric} Hz`;
}

function formatBitrate(bps) {
  const numeric = Number(bps);
  if (!Number.isFinite(numeric) || numeric <= 0) return '—';
  const kbps = numeric / 1000;
  return `${kbps >= 100 ? kbps.toFixed(0) : kbps.toFixed(1)} kbps`;
}

function formatDbfs(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '—';
  return `${numeric.toFixed(1)} dBFS`;
}

function formatChannelDifference(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '—';
  if (numeric < 1) return '<1';
  if (numeric >= 1000) return `${numeric.toFixed(0)}`;
  return numeric.toFixed(1);
}

function formatRelativeTime(timestamp) {
  const numeric = Number(timestamp);
  if (!Number.isFinite(numeric) || numeric <= 0) return '—';
  const diff = Math.max(0, Math.round(Date.now() / 1000 - numeric));
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return `${Math.round(diff / 86400)}d ago`;
}

function formatBoolean(value) {
  if (value === undefined || value === null) return '—';
  return value ? 'On' : 'Off';
}

function formatPan(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '0';
  if (Math.abs(numeric) < 0.01) return 'Center';
  return numeric.toFixed(2);
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

function normalizeWebNodeRequest(entry) {
  if (!entry || typeof entry !== 'object' || !entry.id) return null;
  const requestedAt = typeof entry.requested_at === 'number'
    ? entry.requested_at
    : typeof entry.created_at === 'number'
      ? entry.created_at
      : null;
  return {
    id: entry.id,
    name: (entry.name || 'Web node').trim() || 'Web node',
    client_host: entry.client_host || null,
    requested_at: requestedAt,
  };
}

function sortWebNodeRequests(list) {
  return list.slice().sort((a, b) => (a.requested_at || 0) - (b.requested_at || 0));
}

function getActiveWebNodeRequest() {
  if (!webNodeRequestQueue.length) return null;
  const current = webNodeRequestQueue.find(req => req.id === webNodeActiveRequestId);
  return current || webNodeRequestQueue[0] || null;
}

function setWebNodeRequests(requests, options = {}) {
  const normalized = sortWebNodeRequests((Array.isArray(requests) ? requests : [])
    .map(normalizeWebNodeRequest)
    .filter(Boolean));
  webNodeRequestQueue = normalized;
  if (!webNodeRequestQueue.some(req => req.id === webNodeActiveRequestId)) {
    webNodeActiveRequestId = webNodeRequestQueue[0]?.id || null;
  }
  if (!webNodeRequestQueue.length) {
    webNodeApprovalManuallyHidden = false;
  }
  syncWebNodeApprovalState({ forceOpen: options.forceOpen === true });
}

function upsertWebNodeRequest(entry) {
  const normalized = normalizeWebNodeRequest(entry);
  if (!normalized) return;
  const idx = webNodeRequestQueue.findIndex(req => req.id === normalized.id);
  if (idx === -1) {
    webNodeRequestQueue = sortWebNodeRequests([...webNodeRequestQueue, normalized]);
    if (!webNodeActiveRequestId) {
      webNodeActiveRequestId = normalized.id;
    }
    webNodeApprovalManuallyHidden = false;
    clearPersistentAlertSuppression(WEB_NODE_ALERT_KEY);
    syncWebNodeApprovalState({ forceOpen: true });
    return;
  }
  webNodeRequestQueue[idx] = normalized;
  webNodeRequestQueue = sortWebNodeRequests(webNodeRequestQueue);
  syncWebNodeApprovalState({});
}

function removeWebNodeRequest(requestId) {
  if (!requestId) return;
  const next = webNodeRequestQueue.filter(req => req.id !== requestId);
  if (next.length === webNodeRequestQueue.length) return;
  webNodeRequestQueue = sortWebNodeRequests(next);
  if (!webNodeRequestQueue.some(req => req.id === webNodeActiveRequestId)) {
    webNodeActiveRequestId = webNodeRequestQueue[0]?.id || null;
  }
  if (!webNodeRequestQueue.length) {
    webNodeApprovalManuallyHidden = false;
  }
  syncWebNodeApprovalState({});
}

function updateReviewWebNodeRequestsButton() {
  if (!reviewWebNodeRequestsBtn) return;
  const count = isAdminUser() ? webNodeRequestQueue.length : 0;
  if (!count) {
    reviewWebNodeRequestsBtn.hidden = true;
    reviewWebNodeRequestsBtn.setAttribute('aria-hidden', 'true');
    reviewWebNodeRequestsBtn.dataset.count = '0';
    return;
  }
  reviewWebNodeRequestsBtn.hidden = false;
  reviewWebNodeRequestsBtn.setAttribute('aria-hidden', 'false');
  reviewWebNodeRequestsBtn.dataset.count = String(count);
  reviewWebNodeRequestsBtn.textContent = count === 1
    ? 'Review web node request'
    : `Review ${count} web node requests`;
}

function describeWebNodeRequest(request) {
  if (!request) return 'Waiting for approval';
  const parts = [];
  if (typeof request.requested_at === 'number') {
    parts.push(`Requested ${formatRelativeTime(request.requested_at)}`);
  }
  if (request.client_host) {
    parts.push(`From ${request.client_host}`);
  }
  return parts.length ? parts.join(' · ') : 'Waiting for approval';
}

function renderWebNodeApprovalContent() {
  if (!webNodeApprovalOverlay) return;
  const request = getActiveWebNodeRequest();
  const hasRequest = !!request;
  if (webNodeApprovalName) {
    webNodeApprovalName.textContent = request?.name || 'Web node';
  }
  if (webNodeApprovalInfo) {
    webNodeApprovalInfo.textContent = hasRequest ? describeWebNodeRequest(request) : 'No pending web nodes.';
  }
  if (webNodeApprovalSubtitle) {
    webNodeApprovalSubtitle.textContent = isAdminUser()
      ? 'Approve to let this browser listen.'
      : 'Sign in as an admin to manage web nodes.';
  }
  if (webNodeApprovalQueue) {
    const others = hasRequest ? webNodeRequestQueue.filter(entry => entry.id !== request.id) : webNodeRequestQueue;
    if (!others.length) {
      webNodeApprovalQueue.textContent = 'No other requests waiting.';
    } else {
      const names = others.slice(0, 3).map(entry => entry.name || 'Web node');
      const summary = others.length === 1
        ? '1 more request waiting'
        : `${others.length} more requests waiting`;
      const suffix = others.length > names.length ? `${names.join(', ')}…` : names.join(', ');
      webNodeApprovalQueue.textContent = `${summary}: ${suffix}`;
    }
  }
  const disableActions = !hasRequest || !isAdminUser() || webNodeApprovalBusy;
  if (webNodeApprovalApproveBtn) {
    webNodeApprovalApproveBtn.disabled = disableActions;
  }
  if (webNodeApprovalDenyBtn) {
    webNodeApprovalDenyBtn.disabled = disableActions;
  }
}

function isWebNodeApprovalOpen() {
  return !!(webNodeApprovalOverlay && webNodeApprovalOverlay.classList.contains('is-open'));
}

function openWebNodeApprovalOverlay() {
  if (!webNodeApprovalOverlay || !webNodeRequestQueue.length) return;
  webNodeApprovalOverlay.hidden = false;
  webNodeApprovalOverlay.style.display = 'flex';
  webNodeApprovalOverlay.classList.add('is-open');
  webNodeApprovalOverlay.setAttribute('aria-hidden', 'false');
  webNodeApprovalManuallyHidden = false;
  renderWebNodeApprovalContent();
  if (webNodeApprovalApproveBtn) {
    setTimeout(() => webNodeApprovalApproveBtn.focus({ preventScroll: true }), 50);
  }
}

function closeWebNodeApprovalOverlay(options = {}) {
  if (!webNodeApprovalOverlay) return;
  webNodeApprovalOverlay.classList.remove('is-open');
  webNodeApprovalOverlay.style.display = 'none';
  webNodeApprovalOverlay.hidden = true;
  webNodeApprovalOverlay.setAttribute('aria-hidden', 'true');
  if (options.manual && webNodeRequestQueue.length) {
    webNodeApprovalManuallyHidden = true;
  } else if (!webNodeRequestQueue.length) {
    webNodeApprovalManuallyHidden = false;
  }
}

function updateWebNodeRequestAlert() {
  const count = webNodeRequestQueue.length;
  if (!count || !isAdminUser()) {
    if (persistentAlertState?.key === WEB_NODE_ALERT_KEY) {
      hidePersistentAlert();
    }
    return;
  }
  if (persistentAlertState && persistentAlertState.key && persistentAlertState.key !== WEB_NODE_ALERT_KEY) {
    return;
  }
  const activeRequest = getActiveWebNodeRequest();
  const name = activeRequest?.name || 'Web node';
  const message = count === 1
    ? `${name} is waiting for approval.`
    : `${count} web nodes are waiting for approval.`;
  showPersistentAlert(message, {
    key: WEB_NODE_ALERT_KEY,
    actionLabel: 'Review request',
    dismissLabel: 'Later',
    dismissAriaLabel: 'Hide approval reminder',
    onAction: () => openWebNodeApprovalOverlay(),
  });
}

function syncWebNodeApprovalState(options = {}) {
  updateReviewWebNodeRequestsButton();
  renderWebNodeApprovalContent();
  const hasRequests = webNodeRequestQueue.length > 0;
  if (!hasRequests) {
    closeWebNodeApprovalOverlay({ manual: false });
    if (persistentAlertState?.key === WEB_NODE_ALERT_KEY) {
      hidePersistentAlert();
    }
    return;
  }
  updateWebNodeRequestAlert();
  if (!isAdminUser()) {
    closeWebNodeApprovalOverlay({ manual: false });
    return;
  }
  if (!webNodeApprovalManuallyHidden || options.forceOpen) {
    openWebNodeApprovalOverlay();
  }
}

async function sendWebNodeRequestDecision(action, options = {}) {
  const request = getActiveWebNodeRequest();
  if (!request) return;
  if (!isAdminUser()) {
    showError('Only admins can manage web node approvals.');
    return;
  }
  if (webNodeApprovalBusy) return;
  webNodeApprovalBusy = true;
  renderWebNodeApprovalContent();
  try {
    const res = await fetch(`/api/web-nodes/requests/${encodeURIComponent(request.id)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action,
        reason: options.reason || undefined,
      }),
    });
    await ensureOk(res);
    const verb = action === 'approve' ? 'Approved' : 'Denied';
    showSuccess(`${verb} ${request.name}.`);
  } catch (err) {
    showError(`Failed to ${action} request: ${err.message}`);
  } finally {
    webNodeApprovalBusy = false;
    renderWebNodeApprovalContent();
  }
}

async function handleWebNodeApprovalApprove(event) {
  event?.preventDefault();
  await sendWebNodeRequestDecision('approve');
}

async function handleWebNodeApprovalDeny(event) {
  event?.preventDefault();
  const request = getActiveWebNodeRequest();
  if (!request) return;
  const confirmed = await openConfirmDialog({
    title: 'Deny web node?',
    message: `Block ${request.name} from connecting?`,
    confirmLabel: 'Deny',
    cancelLabel: 'Keep waiting',
    tone: 'danger',
  });
  if (!confirmed) return;
  await sendWebNodeRequestDecision('deny');
}

function handleWebNodeApprovalOverlayClick(event) {
  if (!webNodeApprovalOverlay || event.target !== webNodeApprovalOverlay) return;
  closeWebNodeApprovalOverlay({ manual: true });
}

function handleWebNodeApprovalKeydown(event) {
  if (event.key !== 'Escape') return;
  if (!isWebNodeApprovalOpen()) return;
  event.preventDefault();
  closeWebNodeApprovalOverlay({ manual: true });
}

if (webNodeApprovalOverlay) {
  webNodeApprovalOverlay.addEventListener('click', handleWebNodeApprovalOverlayClick);
}
if (webNodeApprovalCloseBtn) {
  webNodeApprovalCloseBtn.addEventListener('click', () => closeWebNodeApprovalOverlay({ manual: true }));
}
if (webNodeApprovalApproveBtn) {
  webNodeApprovalApproveBtn.addEventListener('click', handleWebNodeApprovalApprove);
}
if (webNodeApprovalDenyBtn) {
  webNodeApprovalDenyBtn.addEventListener('click', handleWebNodeApprovalDeny);
}
document.addEventListener('keydown', handleWebNodeApprovalKeydown, true);
if (reviewWebNodeRequestsBtn) {
  reviewWebNodeRequestsBtn.addEventListener('click', () => {
    if (!webNodeRequestQueue.length) {
      showError('No pending web node requests.');
      return;
    }
    openWebNodeApprovalOverlay();
  });
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
  const normalized = String(channelId).trim().toLowerCase();
  if (!normalized) return null;
  return channelsCache.find(ch => String(ch?.id || '').trim().toLowerCase() === normalized) || null;
}

function isChannelEnabled(channel) {
  const source = (channel?.source || '').trim().toLowerCase();
  return !!source && source !== 'none';
}

function isRadioChannel(channel) {
  if (!channel) return false;
  return (channel.source || '').toLowerCase() === 'radio';
}

function isAudiobookshelfChannel(channel) {
  if (!channel) return false;
  return (channel.source || '').toLowerCase() === 'audiobookshelf';
}

function isSpotifyChannel(channel) {
  if (!channel) return false;
  return (channel.source || '').toLowerCase() === 'spotify';
}

function getRadioState(channel) {
  if (!isRadioChannel(channel)) return null;
  return channel.radio_state || null;
}

function getAudiobookshelfState(channel) {
  if (!isAudiobookshelfChannel(channel)) return null;
  return channel.abs_state || null;
}

function updateChannelRadioState(channelId, radioState) {
  if (!channelId) return;
  const idx = channelsCache.findIndex(ch => ch.id === channelId);
  if (idx === -1) return;
  channelsCache[idx] = { ...channelsCache[idx], radio_state: radioState || null };
}

function updateChannelAudiobookshelfState(channelId, absState) {
  if (!channelId) return;
  const idx = channelsCache.findIndex(ch => ch.id === channelId);
  if (idx === -1) return;
  channelsCache[idx] = { ...channelsCache[idx], abs_state: absState || null };
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
    enabled: isChannelEnabled(channel),
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
  if (!node) return null;
  const resolvedId = resolveNodeChannelId(node);
  if (!resolvedId) return null;
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
  const channelId = (node?.channel_id || '').trim().toLowerCase();
  if (!channelId) return null;
  return channelsCache.some(ch => ch.id === channelId) ? channelId : null;
}

function updateChannelDotColor(target, channelId) {
  if (!target) return;
  const color = getChannelAccentColor(channelId) || '#94a3b8';
  target.style.setProperty('--node-channel-accent', color);
  target.style.background = color;
}

function setChannelDotConnecting(target, channelId, connecting) {
  if (!target) return;
  updateChannelDotColor(target, channelId);
  if (connecting) {
    target.classList.add('is-connecting');
  } else {
    target.classList.remove('is-connecting');
  }
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
      playerCarouselTrack.innerHTML = '<div class="player-carousel-empty">Select a source for a channel in Settings to begin playback.</div>';
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

async function refreshSpotifySources() {
  if (spotifySourcesFetchPromise) return spotifySourcesFetchPromise;
  spotifySourcesFetchPromise = (async () => {
    try {
      const res = await fetch('/api/sources');
      await ensureOk(res);
      const data = await res.json();
      spotifySourcesCache = Array.isArray(data?.sources) ? data.sources : [];
      spotifySourcesLoaded = true;
      return spotifySourcesCache;
    } catch (_) {
      spotifySourcesCache = [];
      spotifySourcesLoaded = false;
      return spotifySourcesCache;
    } finally {
      spotifySourcesFetchPromise = null;
    }
  })();
  return spotifySourcesFetchPromise;
}

function getSettingsChannelId() {
  // Historical name; returns selected Spotify source id.
  const sources = Array.isArray(spotifySourcesCache) ? spotifySourcesCache : [];
  if (spotifySettingsSourceId && sources.some(src => src.id === spotifySettingsSourceId)) {
    return spotifySettingsSourceId;
  }
  spotifySettingsSourceId = sources[0]?.id || 'spotify:a';
  return spotifySettingsSourceId;
}

function populateSpotifyChannelSelect() {
  // No UI selector when providers represent instances.
}

function renderChannelsPanel() {
  if (!channelsPanel) return;
  channelsPanel.innerHTML = '';
  channelFormRefs.clear();
  if (!channelsCache.length) {
    channelsPanel.innerHTML = '<div class="muted">No channels configured yet.</div>';
    return;
  }

  syncChannelsCountSelect();

  const installedProviders = (typeof providersInstalledCache === 'undefined' || !Array.isArray(providersInstalledCache))
    ? []
    : providersInstalledCache;
  const spotifyProviderEnabled = installedProviders.some(p => (p?.id || '').toLowerCase() === 'spotify' && p?.enabled);
  const radioProviderEnabled = installedProviders.some(p => (p?.id || '').toLowerCase() === 'radio' && p?.enabled);
  const audiobookshelfProviderEnabled = installedProviders.some(p => (p?.id || '').toLowerCase() === 'audiobookshelf' && p?.enabled);

  channelsCache.forEach(channel => {
    const pending = channelPendingEdits.get(channel.id) || {};
    const nameValue = Object.prototype.hasOwnProperty.call(pending, 'name')
      ? pending.name
      : channel.name || '';
    const hasPendingColor = Object.prototype.hasOwnProperty.call(pending, 'color');
    const colorValue = hasPendingColor ? pending.color : channel.color;
    const hasPendingSourceRef = Object.prototype.hasOwnProperty.call(pending, 'source_ref');
    const sourceRefValue = hasPendingSourceRef ? pending.source_ref : channel.source_ref;
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

    const sourceGroup = document.createElement('div');
    const sourceLabel = document.createElement('label');
    sourceLabel.textContent = 'Source';
    const sourceSelect = document.createElement('select');
    sourceSelect.setAttribute('aria-label', `Select source for ${nameValue || channel.id}`);

    const optionNone = document.createElement('option');
    optionNone.value = '';
    optionNone.textContent = 'No source';
    sourceSelect.appendChild(optionNone);

    if (radioProviderEnabled) {
      const optionRadio = document.createElement('option');
      optionRadio.value = 'radio';
      optionRadio.textContent = 'Radio';
      sourceSelect.appendChild(optionRadio);
    }

    if (audiobookshelfProviderEnabled) {
      const optionAbs = document.createElement('option');
      optionAbs.value = 'audiobookshelf';
      optionAbs.textContent = 'Audiobookshelf';
      sourceSelect.appendChild(optionAbs);
    }

    if (spotifyProviderEnabled) {
      const sources = Array.isArray(spotifySourcesCache) ? spotifySourcesCache : [];
      if (!sources.length) {
        const optionLoading = document.createElement('option');
        optionLoading.value = '__loading';
        optionLoading.textContent = spotifySourcesLoaded ? 'No Spotify sources installed' : 'Loading Spotify sources…';
        sourceSelect.appendChild(optionLoading);
        optionLoading.disabled = true;
        if (!spotifySourcesLoaded) {
          refreshSpotifySources().then(() => renderChannelsPanel()).catch(() => {});
        }
      } else {
        sources.forEach(source => {
          if (!source?.id) return;
          const option = document.createElement('option');
          option.value = source.id;
          option.textContent = source.name || source.id;
          sourceSelect.appendChild(option);
        });
      }
    }

    const baseSource = (channel.source || '').trim().toLowerCase();
    const baseRef = (sourceRefValue || '').trim().toLowerCase();
    let resolvedSelection = '';
    if (baseSource === 'radio' || baseRef.startsWith('radio')) {
      resolvedSelection = radioProviderEnabled ? 'radio' : '';
    } else if (baseSource === 'audiobookshelf' || baseRef.startsWith('audiobookshelf')) {
      resolvedSelection = audiobookshelfProviderEnabled ? 'audiobookshelf' : '';
    } else if (baseSource === 'spotify' || baseRef.startsWith('spotify:')) {
      resolvedSelection = spotifyProviderEnabled ? (baseRef || 'spotify:a') : '';
    }
    const availableValues = Array.from(sourceSelect.options).map(opt => opt.value);
    sourceSelect.value = availableValues.includes(resolvedSelection) ? resolvedSelection : '';

    sourceGroup.appendChild(sourceLabel);
    sourceGroup.appendChild(sourceSelect);
    header.appendChild(sourceGroup);

    card.appendChild(header);

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
      sourceSelect,
      saveButton: saveBtn,
      statusEl: status,
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
    sourceSelect.addEventListener('change', () => updateChannelCardState(channel.id));
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

  if (refs.sourceSelect && !refs.sourceSelect.disabled) {
    const selected = (refs.sourceSelect.value || '').trim().toLowerCase();
    const normalizedSelected = selected === '__loading' ? '' : selected;
    const baseSource = (base.source || '').trim().toLowerCase();
    const baseRef = (base.source_ref || '').trim().toLowerCase();
    let normalizedBase = '';
    if (baseSource === 'radio' || baseRef.startsWith('radio')) {
      normalizedBase = 'radio';
      } else if (baseSource === 'audiobookshelf' || baseRef.startsWith('audiobookshelf')) {
        normalizedBase = 'audiobookshelf';
    } else if (baseSource === 'spotify' || baseRef.startsWith('spotify:')) {
      normalizedBase = baseRef || 'spotify:a';
    }
    if (normalizedSelected !== normalizedBase) {
      pending.source_ref = normalizedSelected;
      hasDiff = true;
    }
  }
  if (hasDiff) {
    channelPendingEdits.set(channelId, pending);
    refs.card.dataset.dirty = 'true';
    refs.statusEl.textContent = 'Unsaved changes';
    refs.saveButton.disabled = false;
  } else {
    channelPendingEdits.delete(channelId);
    refs.card.dataset.dirty = 'false';
    refs.statusEl.textContent = channelEnabled ? 'Source configured' : 'No source selected';
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

function syncChannelsCountSelect() {
  if (!channelsCountSelect) return;
  if (!channelsCountSelect.dataset.initialized) {
    channelsCountSelect.innerHTML = '';
    for (let i = 1; i <= 10; i += 1) {
      const option = document.createElement('option');
      option.value = String(i);
      option.textContent = String(i);
      channelsCountSelect.appendChild(option);
    }
    channelsCountSelect.addEventListener('change', () => handleChannelsCountChange());
    channelsCountSelect.dataset.initialized = 'true';
  }
  channelsCountSelect.value = String(Math.max(1, Math.min(10, channelsCache.length || 1)));
  channelsCountSelect.disabled = !isAdminUser();
}

async function handleChannelsCountChange() {
  if (!channelsCountSelect) return;
  if (!isAdminUser()) {
    showError('Only admins can change the channel count.');
    syncChannelsCountSelect();
    return;
  }
  const nextCount = Number(channelsCountSelect.value);
  if (!Number.isFinite(nextCount) || nextCount < 1 || nextCount > 10) {
    showError('Channel count must be between 1 and 10.');
    syncChannelsCountSelect();
    return;
  }
  try {
    channelsCountSelect.disabled = true;
    const res = await fetch('/api/channels/count', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count: nextCount }),
    });
    await ensureOk(res);
    await refreshChannels({ force: true });
    showSuccess('Channel count updated.');
  } catch (err) {
    showError(`Failed to update channel count: ${err.message}`);
    syncChannelsCountSelect();
  } finally {
    channelsCountSelect.disabled = !isAdminUser();
  }
}
