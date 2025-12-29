function setRadioOverlayOpen(open) {
  if (!radioOverlay) return;
  const next = !!open;
  radioOverlay.classList.toggle('is-open', next);
  radioOverlay.setAttribute('aria-hidden', next ? 'false' : 'true');
  if (playerPlaylistsBtn) playerPlaylistsBtn.setAttribute('aria-expanded', next ? 'true' : 'false');
  if (playerSearchBtn) playerSearchBtn.setAttribute('aria-expanded', next ? 'true' : 'false');
  if (next) {
    document.addEventListener('keydown', handleRadioOverlayKey, true);
  } else {
    document.removeEventListener('keydown', handleRadioOverlayKey, true);
    radioResultsAbortController?.abort();
    radioResultsAbortController = null;
    radioResultsList && (radioResultsList.innerHTML = '');
    setRadioResultsStatus('Select a genre, list, or search to see stations.');
    radioActiveChannelId = null;
  }
}

function isRadioOverlayOpen() {
  return !!radioOverlay?.classList.contains('is-open');
}

function handleRadioOverlayKey(evt) {
  if (evt.key === 'Escape' && isRadioOverlayOpen()) {
    evt.stopPropagation();
    closeRadioOverlay();
  }
}

function getRadioOverlayChannel() {
  if (!radioActiveChannelId) return null;
  return getChannelById(radioActiveChannelId);
}

function openRadioOverlay(options = {}) {
  const channel = options.channel || getActiveChannel();
  if (!channel || !isRadioChannel(channel)) {
    showError('Switch to a radio channel to browse stations.');
    return;
  }
  radioActiveChannelId = channel.id;
  radioActiveTab = RADIO_TABS.includes(options.tab) ? options.tab : (radioActiveTab || 'genres');
  if (radioModalChannelName) radioModalChannelName.textContent = channel.name || channel.id;
  if (radioModalSubtitle) {
    radioModalSubtitle.textContent = `Assign a station to ${channel.name || 'this channel'}.`;
  }
  setRadioOverlayOpen(true);
  setRadioActiveTab(radioActiveTab, { focusSearch: options.tab === 'search' || options.focusSearch });
  if (options.focusSearch && radioSearchInput) {
    setTimeout(() => radioSearchInput.focus({ preventScroll: true }), 50);
  }
}

function closeRadioOverlay() {
  setRadioOverlayOpen(false);
}

function setRadioActiveTab(tabId, options = {}) {
  const normalized = RADIO_TABS.includes(tabId) ? tabId : 'genres';
  radioActiveTab = normalized;
  radioTabs.forEach(btn => {
    if (!btn?.dataset?.radioTab) return;
    const isActive = btn.dataset.radioTab === normalized;
    btn.classList.toggle('is-active', isActive);
    btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
  });
  radioPanes.forEach(pane => {
    if (!pane?.dataset?.radioPane) return;
    const match = pane.dataset.radioPane === normalized;
    pane.hidden = !match;
  });
  if (normalized === 'genres') {
    ensureRadioGenres();
  } else if (normalized === 'countries') {
    ensureRadioCountries();
  } else if (normalized === 'top') {
    loadRadioTopStations(radioTopMetric);
  } else if (normalized === 'search' && options.focusSearch && radioSearchInput) {
    setTimeout(() => radioSearchInput.focus({ preventScroll: true }), 50);
  }
}

async function ensureRadioGenres(force = false) {
  if (!radioGenreList) return;
  if (radioDataCache.genres && !force) {
    renderRadioGenres(radioDataCache.genres);
    return;
  }
  radioGenreList.textContent = 'Loading genres…';
  try {
    const res = await fetch('/api/radio/genres');
    await ensureOk(res);
    const data = await res.json();
    radioDataCache.genres = Array.isArray(data?.genres) ? data.genres : [];
    renderRadioGenres(radioDataCache.genres);
  } catch (err) {
    radioGenreList.textContent = 'Unable to load genres.';
    showError(`Failed to load radio genres: ${err.message}`);
  }
}

function renderRadioGenres(genres) {
  if (!radioGenreList) return;
  radioGenreList.innerHTML = '';
  const list = Array.isArray(genres) ? genres : [];
  list.slice(0, 60).forEach(entry => {
    if (!entry?.name) return;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = entry.stationcount ? `${entry.name} (${entry.stationcount})` : entry.name;
    btn.addEventListener('click', () => {
      runRadioSearch({ tag: entry.name }, { context: `genre “${entry.name}”` });
    });
    radioGenreList.appendChild(btn);
  });
  if (!radioGenreList.children.length) {
    const empty = document.createElement('div');
    empty.className = 'muted';
    empty.textContent = 'No genres available right now.';
    radioGenreList.appendChild(empty);
  }
}

async function ensureRadioCountries(force = false) {
  if (!radioCountrySelect) return;
  if (radioDataCache.countries && !force) {
    renderRadioCountries(radioDataCache.countries);
    return;
  }
  radioCountrySelect.innerHTML = '<option>Loading countries…</option>';
  try {
    const res = await fetch('/api/radio/countries');
    await ensureOk(res);
    const data = await res.json();
    radioDataCache.countries = Array.isArray(data?.countries) ? data.countries : [];
    renderRadioCountries(radioDataCache.countries);
  } catch (err) {
    radioCountrySelect.innerHTML = '<option>Unable to load countries</option>';
    showError(`Failed to load radio countries: ${err.message}`);
  }
}

function renderRadioCountries(countries) {
  if (!radioCountrySelect) return;
  radioCountrySelect.innerHTML = '<option value="">Select a country…</option>';
  const list = Array.isArray(countries) ? countries : [];
  list.forEach(entry => {
    if (!entry?.name) return;
    const option = document.createElement('option');
    option.value = entry.name;
    option.textContent = entry.stationcount ? `${entry.name} (${entry.stationcount})` : entry.name;
    if (entry.iso_3166_1) option.dataset.countrycode = entry.iso_3166_1;
    radioCountrySelect.appendChild(option);
  });
}

function handleRadioCountryChange() {
  if (!radioCountrySelect) return;
  const country = radioCountrySelect.value || '';
  const selectedOption = radioCountrySelect.selectedOptions?.[0];
  const code = selectedOption?.dataset?.countrycode || null;
  if (!country) {
    setRadioResultsStatus('Select a genre, list, or search to see stations.');
    radioResultsList && (radioResultsList.innerHTML = '');
    return;
  }
  const context = code ? `${country} (${code})` : country;
  runRadioSearch({ country, countrycode: code }, { context: `country ${context}` });
}

function setRadioTopMetric(metric) {
  const normalized = metric === 'clicks' ? 'clicks' : 'votes';
  radioTopMetric = normalized;
  radioTopButtons.forEach(btn => {
    if (!btn?.dataset?.radioTopMetric) return;
    const isActive = btn.dataset.radioTopMetric === normalized;
    btn.classList.toggle('is-active', isActive);
  });
}

async function loadRadioTopStations(metric = radioTopMetric) {
  setRadioTopMetric(metric);
  setRadioResultsStatus('Loading top stations…');
  radioResultsList && (radioResultsList.innerHTML = '');
  try {
    const res = await fetch(`/api/radio/top?metric=${encodeURIComponent(metric)}`);
    await ensureOk(res);
    const data = await res.json();
    const stations = Array.isArray(data?.stations) ? data.stations : [];
    renderRadioStations(stations, { context: metric === 'votes' ? 'Most voted' : 'Most popular' });
  } catch (err) {
    setRadioResultsStatus('Unable to load top stations.');
    showError(`Failed to load top stations: ${err.message}`);
  }
}

function setRadioResultsStatus(message) {
  if (!radioResultsStatus) return;
  radioResultsStatus.textContent = message || '';
}

async function runRadioSearch(filters = {}, options = {}) {
  const params = new URLSearchParams();
  if (filters.query) params.set('query', filters.query);
  if (filters.country) params.set('country', filters.country);
  if (filters.countrycode) params.set('countrycode', filters.countrycode);
  if (filters.tag) params.set('tag', filters.tag);
  if (!params.toString()) {
    setRadioResultsStatus('Enter a search term or choose a filter to see stations.');
    radioResultsList && (radioResultsList.innerHTML = '');
    return;
  }
  if (radioResults) radioResults.scrollTop = 0;
  setRadioResultsStatus(options.loadingMessage || 'Searching stations…');
  radioResultsList && (radioResultsList.innerHTML = '');
  radioResultsAbortController?.abort();
  const controller = new AbortController();
  radioResultsAbortController = controller;
  try {
    const res = await fetch(`/api/radio/search?${params.toString()}`, { signal: controller.signal });
    await ensureOk(res);
    const data = await res.json();
    const stations = Array.isArray(data?.stations) ? data.stations : [];
    renderRadioStations(stations, { context: options.context });
  } catch (err) {
    if (err.name === 'AbortError') return;
    setRadioResultsStatus('Unable to load stations right now.');
    showError(`Radio search failed: ${err.message}`);
  } finally {
    if (radioResultsAbortController === controller) radioResultsAbortController = null;
  }
}

function renderRadioStations(stations, options = {}) {
  if (!radioResultsList) return;
  radioResultsList.innerHTML = '';
  const list = Array.isArray(stations) ? stations : [];
  radioCurrentResults = list.slice();
  radioResultsContextLabel = options.context || '';
  const channel = getRadioOverlayChannel();
  if (!list.length) {
    const empty = document.createElement('div');
    empty.className = 'muted';
    empty.textContent = 'No stations found.';
    radioResultsList.appendChild(empty);
    setRadioResultsStatus('No stations found.');
    return;
  }
  list.forEach(station => {
    const card = buildRadioStationCard(station, channel);
    if (card) radioResultsList.appendChild(card);
  });
  const suffix = options.context ? ` for ${options.context}` : '';
  setRadioResultsStatus(`Showing ${list.length} station${list.length === 1 ? '' : 's'}${suffix}.`);
}

function buildRadioStationCard(station, channel) {
  if (!station) return null;
  const card = document.createElement('div');
  card.className = 'radio-station-card';
  const header = document.createElement('div');
  header.className = 'radio-station-card-header';
  const info = document.createElement('div');
  info.className = 'radio-station-info';
  const name = document.createElement('div');
  name.className = 'radio-station-name';
  name.textContent = station.name || 'Untitled station';
  info.appendChild(name);
  const meta = document.createElement('div');
  meta.className = 'radio-station-meta';
  const metaParts = [];
  if (station.country) metaParts.push(station.country);
  if (station.language) metaParts.push(station.language);
  if (station.codec) metaParts.push((station.codec || '').toUpperCase());
  if (Number.isFinite(station.bitrate) && station.bitrate > 0) metaParts.push(`${station.bitrate} kbps`);
  meta.textContent = metaParts.length ? metaParts.join(' • ') : '—';
  info.appendChild(meta);
  header.appendChild(info);
  const activeStationId = getRadioState(channel)?.station_id || null;
  const isActive = activeStationId && station.station_id && activeStationId === station.station_id;
  if (isActive) {
    const badge = document.createElement('span');
    badge.className = 'status-pill ok';
    badge.textContent = 'Currently tuned';
    header.appendChild(badge);
  }
  card.appendChild(header);
  if (Array.isArray(station.tags) && station.tags.length) {
    const tagsWrap = document.createElement('div');
    tagsWrap.className = 'radio-station-tags';
    station.tags.slice(0, 6).forEach(tag => {
      const tagEl = document.createElement('span');
      tagEl.className = 'radio-station-tag';
      tagEl.textContent = tag;
      tagsWrap.appendChild(tagEl);
    });
    card.appendChild(tagsWrap);
  }
  const actions = document.createElement('div');
  actions.className = 'radio-station-actions';
  const canTune = isAdminUser();
  const tuneBtn = document.createElement('button');
  tuneBtn.type = 'button';
  tuneBtn.className = 'small-btn';
  tuneBtn.textContent = isActive ? 'Tuned' : `Tune ${channel?.name || 'channel'}`;
  tuneBtn.disabled = !canTune || isActive || !station.stream_url;
  tuneBtn.addEventListener('click', () => {
    if (!radioActiveChannelId) {
      showError('No radio channel selected.');
      return;
    }
    tuneRadioStation(radioActiveChannelId, station, tuneBtn);
  });
  actions.appendChild(tuneBtn);
  if (station.homepage) {
    const openBtn = document.createElement('button');
    openBtn.type = 'button';
    openBtn.className = 'small-btn';
    openBtn.textContent = 'Open homepage';
    openBtn.addEventListener('click', () => window.open(station.homepage, '_blank'));
    actions.appendChild(openBtn);
  }
  card.appendChild(actions);
  return card;
}

async function tuneRadioStation(channelId, station, btn) {
  if (!channelId || !station?.station_id || !station?.stream_url) {
    showError('Select a valid station to tune.');
    return;
  }
  if (!isAdminUser()) {
    showError('Only admins can change radio stations.');
    return;
  }
  const targetBtn = btn;
  const previousLabel = targetBtn?.textContent;
  if (targetBtn) {
    targetBtn.disabled = true;
    targetBtn.textContent = 'Tuning…';
  }
  try {
    const payload = {
      station_id: station.station_id,
      name: station.name || 'Radio station',
      stream_url: station.stream_url,
      country: station.country,
      countrycode: station.countrycode,
      bitrate: station.bitrate,
      favicon: station.favicon,
      homepage: station.homepage,
      tags: Array.isArray(station.tags) ? station.tags : [],
    };
    const res = await fetch(`/api/radio/${encodeURIComponent(channelId)}/station`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    await ensureOk(res);
    await res.json();
    showSuccess(`Tuned ${station.name || 'station'} on this channel.`);
    await refreshChannels({ force: true }).catch(() => {});
    renderRadioStations(radioCurrentResults, { context: radioResultsContextLabel });
    fetchPlayerStatus();
  } catch (err) {
    showError(`Failed to tune station: ${err.message}`);
  } finally {
    if (targetBtn) {
      targetBtn.textContent = previousLabel || 'Tune channel';
      targetBtn.disabled = false;
    }
  }
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
      fetchedAt: 0,
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
  const channelId = getActiveChannelId();
  if (!channelId) {
    setPlaylistErrorMessage('Select a channel to browse playlists.');
    setPlaylistLoadingState(false);
    playlistAbortController = null;
    return;
  }
  try {
    const collected = [];
    let offset = 0;
    let hasNext = true;
    let reportedTotal = null;
    while (hasNext) {
      const params = new URLSearchParams({ limit: String(PLAYLIST_PAGE_LIMIT), offset: String(offset) });
      const res = await fetch(withChannel(`/api/spotify/playlists?${params.toString()}`, channelId), { signal: playlistAbortController.signal });
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
    const timestamp = Date.now();
    playlistsCache = collected.map((item, idx) => {
      rememberPlaylistMetadata(item, timestamp);
      return { ...item, _order: idx };
    });
    playlistsCacheFetchedAt = timestamp;
    renderPlaylistGrid(playlistsCache);
    maybeAutoSelectPlaylist();
  } catch (err) {
    if (err.name === 'AbortError') return;
    if (!playlistsCache.length && playlistGrid) playlistGrid.innerHTML = '';
    if (playlistEmpty && !playlistsCache.length) {
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
  const channelId = getActiveChannelId();
  if (!channelId) {
    setPlaylistErrorMessage('Select a channel to load tracks.');
    setPlaylistLoadingState(false);
    return;
  }
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
    const res = await fetch(withChannel(`/api/spotify/playlists/${encodeURIComponent(playlist.id)}/tracks?${params.toString()}`, channelId), { signal: playlistTracksAbortController.signal });
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
    state.fetchedAt = Date.now();
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
  const channelId = getActiveChannelId();
  if (!channelId) {
    setPlaylistErrorMessage('Select a channel to load playlist details.');
    return;
  }
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
    const res = await fetch(withChannel(`/api/spotify/playlists/${encodeURIComponent(playlist.id)}/summary`, channelId), { signal: playlistSummaryAbortController.signal });
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
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  resetPlaylistDetails();
  setPlaylistOverlayOpen(true);
  if (playlistSubtitle) playlistSubtitle.textContent = '';
  setPlaylistErrorMessage('');
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  const contextPlaylistId = activePlaylistContextId || null;
  if (contextPlaylistId) {
    playlistAutoSelectId = null;
    showPlaylistTracksForContext(contextPlaylistId);
  } else {
    showPlaylistsGrid();
  }
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
  playlistAutoSelectId = null;
  showPlaylistsGrid();
}

function selectPlaylist(playlist, options = {}) {
  if (!playlist?.id) {
    showError('Unable to open this playlist.');
    return false;
  }
  rememberPlaylistMetadata(playlist);
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
  const hasCachedTracks = !options.forceTrackReload
    && Array.isArray(state?.items)
    && state.items.length > 0
    && isTrackCacheFresh(state);
  updatePlaylistSummary(state);
  if (hasCachedTracks) renderPlaylistTracks(state);
  else if (playlistTracklist) playlistTracklist.innerHTML = '';
  updatePlaylistProgress(state);
  updatePlaylistLoadMore(state);
  playlistSummaryAbortController?.abort();
  playlistSummaryAbortController = null;
  if (!state.summary || state.summary.status === 'idle' || state.summary.status === 'error') {
    fetchPlaylistSummary(playlist);
  } else {
    updatePlaylistSummary(state);
  }
  if (!hasCachedTracks) {
    fetchPlaylistTracksPage(playlist, { offset: 0 });
  }
  return hasCachedTracks;
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
  const channelId = getActiveChannelId();
  if (!channelId) {
    setQueueErrorMessage('Select a channel to view the queue.');
    setQueueLoadingState(false);
    queueAbortController = null;
    return;
  }
  try {
    const res = await fetch(withChannel('/api/spotify/player/queue', channelId), { signal: controller.signal });
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
  const channelId = getActiveChannelId();
  if (!channelId) {
    setSearchError('Select a channel to search.');
    return;
  }
  searchHasAttempted = true;
  setSearchError('');
  setSearchLoading(true, `Searching Spotify for "${term}"…`);
  searchAbortController?.abort();
  searchAbortController = new AbortController();
  try {
    const params = new URLSearchParams({ q: term, limit: '10' });
    const res = await fetch(withChannel(`/api/spotify/search?${params.toString()}`, channelId), { signal: searchAbortController.signal });
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

async function registerControllerNode(btn) {
  const target = btn;
  const originalLabel = target ? target.textContent : '';
  if (target) {
    target.disabled = true;
    target.textContent = 'Adding…';
  }
  try {
    const res = await fetch('/api/nodes/register-controller', { method: 'POST' });
    await ensureOk(res);
    showSuccess('Controller registered as a node.');
    await fetchNodes({ force: true });
  } catch (err) {
    showError(`Failed to add server node: ${err.message}`);
  } finally {
    if (target) {
      target.disabled = false;
      target.textContent = originalLabel || 'Add server as node';
    }
  }
}

const radioCollapsiblePanels = (typeof window !== 'undefined' && window.__roomcastCollapsiblePanels)
  ? window.__roomcastCollapsiblePanels
  : Array.from(document.querySelectorAll('[data-collapsible]'));
if (typeof window !== 'undefined' && !window.__roomcastCollapsiblePanels) {
  window.__roomcastCollapsiblePanels = radioCollapsiblePanels;
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
  radioCollapsiblePanels.forEach(panel => {
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
  radioCollapsiblePanels.forEach(panel => setCollapsiblePanelState(panel, false));
}

function applyCoverArtBackground() {
  if (!coverArtBackdrop) return;
  if (lastCoverArtUrl) {
    const safeUrl = lastCoverArtUrl.replace(/"/g, '\"');
    coverArtBackdrop.style.backgroundImage = `${COVER_ART_BACKDROP_OVERLAY}, url("${safeUrl}")`;
  } else {
    coverArtBackdrop.style.backgroundImage = '';
  }
  document.body.classList.toggle('use-cover-art-background', !!lastCoverArtUrl);
}

function syncGeneralSettingsUI() {
  if (serverNameInput && document.activeElement !== serverNameInput) {
    serverNameInput.value = authState?.server_name || 'RoomCast';
  }
  if (saveServerNameBtn) {
    saveServerNameBtn.disabled = !isAdminUser();
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

function queueNodeVolumeUpdate(nodeId, value) {
  if (!nodeId) return;
  const pending = nodeVolumeUpdateTimers.get(nodeId);
  if (pending) clearTimeout(pending);
  const timer = setTimeout(() => {
    nodeVolumeUpdateTimers.delete(nodeId);
    setNodeVolume(nodeId, value, { silent: true });
  }, NODE_VOLUME_PUSH_DEBOUNCE_MS);
  nodeVolumeUpdateTimers.set(nodeId, timer);
}

function flushPendingNodeVolumeUpdate(nodeId) {
  const pending = nodeVolumeUpdateTimers.get(nodeId);
  if (!pending) return;
  clearTimeout(pending);
  nodeVolumeUpdateTimers.delete(nodeId);
}

let nodesCache = [];
let nodeSectionsCache = [];
let nodeSectionsEditMode = false;
const nodeSectionCollapsed = new Map();
const nodeVolumeSliderRefs = new Map();
const nodeVolumeUpdateTimers = new Map();
const NODE_VOLUME_PUSH_DEBOUNCE_MS = 150;
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
  return 'faders';
}
function persistEqSkinPreference(value) {
  try {
    localStorage.setItem(EQ_SKIN_STORAGE_KEY, value);
  } catch (_) {
    /* ignore storage errors */
  }
}
let eqSkin = loadEqSkinPreference();

function setNodeSections(sections) {
  nodeSectionsCache = Array.isArray(sections) ? sections.filter(s => s && typeof s === 'object') : [];
}

function getNodeSections() {
  return Array.isArray(nodeSectionsCache) ? nodeSectionsCache : [];
}

function isNodeSectionEditMode() {
  return nodeSectionsEditMode === true;
}

function toggleNodeSectionEditMode(force) {
  if (typeof force === 'boolean') {
    nodeSectionsEditMode = force;
  } else {
    nodeSectionsEditMode = !nodeSectionsEditMode;
  }
  renderNodes(nodesCache, { force: true });
}
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
const ICON_BROWSER_NODE = `
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" role="img">
    <circle cx="12" cy="12" r="9"></circle>
    <path d="M3 12h18"></path>
    <path d="M12 3a24 24 0 010 18"></path>
    <path d="M12 3a24 24 0 000 18"></path>
  </svg>`;
const ICON_CONTROLLER_NODE = `
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" role="img">
    <path d="M3 11l9-7 9 7"></path>
    <path d="M5 10v9h14v-9"></path>
    <rect x="10" y="13" width="4" height="6" rx="1"></rect>
  </svg>`;
const ICON_NETWORK_WIFI = `
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" role="img">
    <path d="M5 12.5a10 10 0 0 1 14 0"></path>
    <path d="M8.5 16a5.5 5.5 0 0 1 7 0"></path>
    <circle cx="12" cy="19" r="1.2" fill="currentColor" stroke="none"></circle>
  </svg>`;
const ICON_NETWORK_ETHERNET = `
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" role="img">
    <rect x="5" y="9" width="14" height="8" rx="2"></rect>
    <path d="M9 13h0.01"></path>
    <path d="M12 13h0.01"></path>
    <path d="M15 13h0.01"></path>
    <path d="M12 17v3"></path>
  </svg>`;
const ICON_NETWORK_SONOSNET = `
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" role="img">
    <circle cx="6.5" cy="16" r="1.6" fill="currentColor" stroke="none"></circle>
    <circle cx="12" cy="7" r="1.6" fill="currentColor" stroke="none"></circle>
    <circle cx="17.5" cy="16" r="1.6" fill="currentColor" stroke="none"></circle>
    <path d="M7.9 14.9L10.9 9.1"></path>
    <path d="M13.1 9.1L16.1 14.9"></path>
    <path d="M8.3 16h7.4"></path>
  </svg>`;
const ICON_VOLUME_ON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M11 5L6 9H3v6h3l5 4z"/><path d="M15 9a3 3 0 010 6"/></svg>`;
const ICON_VOLUME_OFF = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M11 5L6 9H3v6h3l5 4z"/><path d="M19 9l-6 6"/><path d="M13 9l6 6"/></svg>`;
const ICON_SHUFFLE = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3h4v4"/><path d="M4 20l16-16"/><path d="M4 4l5 5"/><path d="M15 15l5 5v-4"/></svg>`;
const ICON_PREV = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="15 18 9 12 15 6 15 18"/><line x1="6" y1="6" x2="6" y2="18"/></svg>`;
const ICON_NEXT = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="9 18 15 12 9 6 9 18"/><line x1="18" y1="6" x2="18" y2="18"/></svg>`;
const ICON_PLAY = `<svg viewBox="0 0 24 24" fill="currentColor"><polygon points="8,5 20,12 8,19"/></svg>`;
const ICON_PAUSE = `<svg viewBox="0 0 24 24" fill="currentColor"><rect x="7" y="5" width="4" height="14" rx="1"/><rect x="13" y="5" width="4" height="14" rx="1"/></svg>`;
const ICON_STOP = `<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>`;
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

function getNodeSocketUrl() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws/nodes`;
}

function clearNodeSocketRetry() {
  if (nodesSocketRetryTimer) {
    clearTimeout(nodesSocketRetryTimer);
    nodesSocketRetryTimer = null;
  }
}

function scheduleNodeSocketReconnect() {
  if (!nodesSocketShouldConnect || nodesSocketRetryTimer) return;
  const baseDelay = 1500;
  const maxDelay = 15000;
  const delay = Math.min(baseDelay * (2 ** nodesSocketRetryAttempt || 1), maxDelay);
  nodesSocketRetryAttempt = Math.min(nodesSocketRetryAttempt + 1, 6);
  nodesSocketRetryTimer = setTimeout(() => {
    nodesSocketRetryTimer = null;
    if (!nodesSocketShouldConnect || nodesSocket) return;
    startNodeSocket({ force: true });
  }, delay);
}

function handleNodesSocketOpen() {
  nodesSocketConnected = true;
  nodesSocketRetryAttempt = 0;
  clearNodeSocketRetry();
}

function handleNodesSocketMessage(event) {
  if (!event?.data) return;
  let payload;
  try {
    payload = JSON.parse(event.data);
  } catch (_) {
    return;
  }
  if (payload?.type === 'nodes' && Array.isArray(payload.nodes)) {
    if (Array.isArray(payload.sections)) {
      setNodeSections(payload.sections);
    }
    renderNodes(payload.nodes);
    return;
  }
  if (payload?.type === 'web_node_requests' && Array.isArray(payload.requests)) {
    setWebNodeRequests(payload.requests, { forceOpen: true });
    return;
  }
  if (payload?.type === 'web_node_request') {
    handleWebNodeRequestBroadcast(payload);
  }
}

function handleWebNodeRequestBroadcast(payload) {
  if (!payload || typeof payload !== 'object') return;
  const action = payload.action;
  if (action === 'created' && payload.request) {
    upsertWebNodeRequest(payload.request);
    return;
  }
  if (action === 'resolved') {
    const resolvedId = payload.request?.id || payload.request_id;
    if (resolvedId) removeWebNodeRequest(resolvedId);
  }
}

function handleNodesSocketClose(event) {
  if (event?.target !== nodesSocket) return;
  nodesSocketConnected = false;
  nodesSocket = null;
  if (nodesSocketShouldConnect) {
    scheduleNodeSocketReconnect();
  }
}

function handleNodesSocketError(event) {
  if (event?.target?.close) {
    try {
      event.target.close();
    } catch (_) {
      /* swallow socket close errors */
    }
  }
}

function startNodeSocket(options = {}) {
  if (!isAuthenticated()) return;
  nodesSocketShouldConnect = true;
  const force = options.force === true;
  if (nodesSocket) return;
  if (nodesSocketRetryTimer && !force) return;
  if (force) clearNodeSocketRetry();
  let socket;
  try {
    socket = new WebSocket(getNodeSocketUrl());
  } catch (_) {
    scheduleNodeSocketReconnect();
    return;
  }
  nodesSocket = socket;
  socket.addEventListener('open', handleNodesSocketOpen);
  socket.addEventListener('message', handleNodesSocketMessage);
  socket.addEventListener('close', handleNodesSocketClose);
  socket.addEventListener('error', handleNodesSocketError);
}

function stopNodeSocket() {
  nodesSocketShouldConnect = false;
  nodesSocketConnected = false;
  nodesSocketRetryAttempt = 0;
  clearNodeSocketRetry();
  if (nodesSocket) {
    try {
      nodesSocket.removeEventListener('open', handleNodesSocketOpen);
      nodesSocket.removeEventListener('message', handleNodesSocketMessage);
      nodesSocket.removeEventListener('close', handleNodesSocketClose);
      nodesSocket.removeEventListener('error', handleNodesSocketError);
      nodesSocket.close();
    } catch (_) {
      /* ignore close errors */
    }
    nodesSocket = null;
  }
}

function createNodeChannelSelector(node, options = {}) {
  if (!channelsCache.length || !node) return null;
  const wrapper = document.createElement('div');
  wrapper.className = 'node-channel-selector';
  const dot = document.createElement('span');
  dot.className = 'node-channel-dot';
  wrapper.appendChild(dot);
  const select = document.createElement('select');
  select.className = 'node-channel-select';
  select.setAttribute('aria-label', `Channel for ${node.name || 'node'}`);
  const playableChannels = getPlayerChannels();
  const hasPlayable = playableChannels.length > 0;
  const resolvedId = resolveNodeChannelId(node) || '';
  const unassignedOption = document.createElement('option');
  unassignedOption.value = '';
  unassignedOption.textContent = hasPlayable ? 'Unassigned' : 'No enabled channels';
  if (!hasPlayable) {
    unassignedOption.disabled = true;
  }
  select.appendChild(unassignedOption);
  playableChannels.forEach(channel => {
    const option = document.createElement('option');
    option.value = channel.id;
    option.textContent = channel.name || channel.id;
    select.appendChild(option);
  });
  const validResolved = resolvedId && playableChannels.some(channel => channel.id === resolvedId);
  if (validResolved) {
    select.value = resolvedId;
  } else {
    select.value = '';
  }
  select.dataset.previousChannel = select.value;
  const dotChannelId = validResolved ? resolvedId : null;
  const connecting = (node?.connection_state || '').toLowerCase() === 'connecting';
  setChannelDotConnecting(dot, dotChannelId, connecting);
  const shouldDisable = options.disabled || (!hasPlayable && select.value === '');
  select.disabled = !!shouldDisable;
  select.addEventListener('change', async () => {
    const targetChannel = select.value;
    const previous = select.dataset.previousChannel ?? '';
    if (select.disabled) {
      select.value = previous;
      setChannelDotConnecting(dot, previous || null, false);
      return;
    }
    try {
      await setNodeChannel(node.id, targetChannel, select, dot);
    } catch (_) {
      select.value = previous;
      setChannelDotConnecting(dot, previous || null, false);
    }
  });
  wrapper.appendChild(select);
  return wrapper;
}

function clampWifiPercent(value) {
  if (!Number.isFinite(value)) return null;
  return Math.max(0, Math.min(100, Math.round(value)));
}

function resolveNodeWifiState(node) {
  if (!node || typeof node !== 'object') return null;
  const percent = clampWifiPercent(Number(node?.wifi?.percent));
  if (percent === null) return null;
  let bars = 0;
  let label = 'No signal';
  for (const entry of WIFI_SIGNAL_THRESHOLDS) {
    if (percent >= entry.min) {
      bars = entry.bars;
      label = entry.label;
      break;
    }
  }
  const signalRaw = Number(node?.wifi?.signal_dbm);
  return {
    percent,
    bars,
    label,
    signalDbm: Number.isFinite(signalRaw) ? Math.round(signalRaw) : null,
    interface: typeof node?.wifi?.interface === 'string' ? node.wifi.interface : null,
  };
}

function describeNodeWifi(node) {
  const online = node?.online !== false;
  if (!online) return 'Node offline';
  const state = resolveNodeWifiState(node);
  if (!state) return 'Wi-Fi signal unavailable';
  const parts = [`Wi-Fi ${state.label}`, `${state.percent}%`];
  if (typeof state.signalDbm === 'number') {
    parts.push(`${state.signalDbm} dBm`);
  }
  if (state.interface) {
    parts.push(`via ${state.interface}`);
  }
  return parts.join(' · ');
}

function renderNodeWifiIndicator(node) {
  const indicator = document.createElement('div');
  indicator.className = 'node-wifi-indicator';
  indicator.setAttribute('role', 'img');
  const label = describeNodeWifi(node);
  indicator.setAttribute('aria-label', label);
  indicator.title = label;
  const meter = document.createElement('div');
  meter.className = 'node-wifi-meter';
  const online = node?.online !== false;
  const state = resolveNodeWifiState(node);
  const level = online && state ? state.bars : 0;
  meter.classList.add(`wifi-level-${level}`);
  if (!online) {
    meter.classList.add('is-offline');
  } else if (!state) {
    meter.classList.add('is-unknown');
  }
  for (let i = 0; i < 4; i += 1) {
    meter.appendChild(document.createElement('span'));
  }
  const accentColor = getNodeChannelAccent(node) || '#94a3b8';
  const wifiColor = !online
    ? '#f87171'
    : (state ? accentColor : '#e2e8f0');
  meter.style.setProperty('--node-wifi-color', wifiColor);
  indicator.appendChild(meter);
  return indicator;
}

function renderSonosNetworkIcon(node) {
  const info = node?.sonos_network;
  if (!info || typeof info !== 'object') return null;

  const transport = String(info.transport || '').toLowerCase();
  let label = null;
  let icon = null;

  if (transport === 'ethernet') {
    label = 'Ethernet';
    icon = ICON_NETWORK_ETHERNET;
  } else if (transport === 'sonosnet') {
    label = 'SonosNet';
    icon = ICON_NETWORK_SONOSNET;
  } else if (transport === 'wifi' || transport === 'wireless') {
    label = 'Wi-Fi';
    icon = ICON_NETWORK_WIFI;
  }

  if (!label || !icon) return null;

  const mode = typeof info.wifi_mode_string === 'string' ? info.wifi_mode_string.trim() : '';
  const fullLabel = mode ? `Sonos network: ${label} (${mode})` : `Sonos network: ${label}`;
  return createNodeTypeIcon({
    icon,
    label: fullLabel,
    className: 'node-type-network',
  });
}

function createNodeTypeIcon(options) {
  if (!options?.icon) return null;
  const iconEl = document.createElement('span');
  const classes = ['node-type-icon'];
  if (options.className) {
    classes.push(options.className);
  }
  iconEl.className = classes.join(' ');
  iconEl.innerHTML = options.icon;
  if (options.label) {
    iconEl.setAttribute('role', 'img');
    iconEl.setAttribute('aria-label', options.label);
    iconEl.title = options.label;
  }
  const colorValue = options.color || null;
  if (colorValue) {
    iconEl.style.setProperty('--node-type-color', colorValue);
  }
  return iconEl;
}

function renderNodeIdentityBadge(node) {
  if (!node) return null;
  if (node.is_controller) {
    return createNodeTypeIcon({
      icon: ICON_CONTROLLER_NODE,
      label: 'Server node',
      color: '#fde68a',
      className: 'node-type-controller',
    });
  }
  if (node.type === 'browser') {
    const accent = getNodeChannelAccent(node) || '#94a3b8';
    return createNodeTypeIcon({
      icon: ICON_BROWSER_NODE,
      label: 'Browser node',
      color: accent,
      className: 'node-type-browser',
    });
  }
  return null;
}

function resolveNodeSectionId(node, sections) {
  const sid = typeof node?.section_id === 'string' ? node.section_id : '';
  if (sid && Array.isArray(sections) && sections.some(s => s?.id === sid)) return sid;
  const fallback = Array.isArray(sections) && sections.length ? sections[0].id : null;
  return fallback || null;
}

function createNodeSectionSelector(node, options = {}) {
  const sections = Array.isArray(options.sections) ? options.sections : getNodeSections();
  if (!sections.length || !node) return null;
  const wrapper = document.createElement('div');
  wrapper.className = 'node-section-selector';
  const select = document.createElement('select');
  select.className = 'node-section-select';
  select.setAttribute('aria-label', `Section for ${node.name || 'node'}`);
  sections.forEach(section => {
    if (!section?.id) return;
    const option = document.createElement('option');
    option.value = section.id;
    option.textContent = section.name || 'Section';
    select.appendChild(option);
  });
  const resolvedId = resolveNodeSectionId(node, sections);
  if (resolvedId) {
    select.value = resolvedId;
  }
  select.dataset.previousSection = select.value;
  select.disabled = options.disabled === true;
  select.addEventListener('change', async () => {
    if (typeof setNodeSection !== 'function') return;
    await setNodeSection(node.id, select.value, select);
  });
  wrapper.appendChild(select);
  return wrapper;
}

function createSectionHeader(section, options = {}) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'node-section-header';
  const title = document.createElement('span');
  title.className = 'node-section-title';
  title.textContent = section?.name || 'Section';
  btn.appendChild(title);
  const meta = document.createElement('span');
  meta.className = 'node-section-meta';
  const count = typeof options.count === 'number' ? options.count : 0;
  meta.textContent = `${count}`;
  btn.appendChild(meta);
  const icon = document.createElement('span');
  icon.className = 'node-section-icon';
  icon.textContent = options.collapsed ? '▸' : '▾';
  btn.appendChild(icon);

  if (options.editMode) {
    const controls = document.createElement('span');
    controls.className = 'node-section-controls';

    const up = document.createElement('button');
    up.type = 'button';
    up.className = 'node-section-control-btn';
    up.textContent = '↑';
    up.disabled = options.index === 0;
    up.setAttribute('aria-label', `Move ${section?.name || 'section'} up`);
    up.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (typeof reorderNodeSections !== 'function') return;
      const order = Array.isArray(options.sectionOrder) ? options.sectionOrder.slice() : [];
      const idx = options.index;
      if (idx <= 0) return;
      const tmp = order[idx - 1];
      order[idx - 1] = order[idx];
      order[idx] = tmp;
      await reorderNodeSections(order);
    });

    const down = document.createElement('button');
    down.type = 'button';
    down.className = 'node-section-control-btn';
    down.textContent = '↓';
    down.disabled = typeof options.total === 'number' ? options.index >= options.total - 1 : false;
    down.setAttribute('aria-label', `Move ${section?.name || 'section'} down`);
    down.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (typeof reorderNodeSections !== 'function') return;
      const order = Array.isArray(options.sectionOrder) ? options.sectionOrder.slice() : [];
      const idx = options.index;
      if (typeof options.total !== 'number' || idx >= options.total - 1) return;
      const tmp = order[idx + 1];
      order[idx + 1] = order[idx];
      order[idx] = tmp;
      await reorderNodeSections(order);
    });

    controls.appendChild(up);
    controls.appendChild(down);
    btn.appendChild(controls);
  }

  return btn;
}

function commitRenderNodes(nodes) {
  nodesEl.innerHTML = '';
  nodeVolumeSliderRefs.clear();
  if (!nodes.length) {
    nodesEl.innerHTML = '<div class="muted">No nodes registered yet.</div>';
    Object.keys(camillaPendingNodes).forEach(id => delete camillaPendingNodes[id]);
    refreshNodeSettingsModal();
    return;
  }

  const sections = getNodeSections();
  const editMode = isNodeSectionEditMode();
  const resolvedSections = sections.length ? sections : [{ id: '__default__', name: 'Nodes' }];
  const sectionOrder = resolvedSections.map(s => s.id).filter(Boolean);
  const nodesBySection = new Map();
  nodes.forEach(n => {
    const sid = resolveNodeSectionId(n, resolvedSections) || '__default__';
    if (!nodesBySection.has(sid)) nodesBySection.set(sid, []);
    nodesBySection.get(sid).push(n);
  });

  resolvedSections.forEach((section, sectionIndex) => {
    const sectionId = section?.id || '__default__';
    const sectionNodes = nodesBySection.get(sectionId) || [];
    if (!sectionNodes.length && !editMode) return;

    const collapsed = nodeSectionCollapsed.get(sectionId) === true;
    const group = document.createElement('div');
    group.className = 'node-section-group';

    const header = createSectionHeader(section, {
      count: sectionNodes.length,
      collapsed,
      editMode,
      index: sectionIndex,
      total: resolvedSections.length,
      sectionOrder,
    });

    let longPressTimer = null;
    let longPressFired = false;
    header.addEventListener('pointerdown', () => {
      longPressFired = false;
      if (longPressTimer) clearTimeout(longPressTimer);
      longPressTimer = setTimeout(() => {
        longPressFired = true;
        toggleNodeSectionEditMode();
      }, 650);
    });
    const cancelLongPress = () => {
      if (longPressTimer) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    };
    header.addEventListener('pointerup', cancelLongPress);
    header.addEventListener('pointercancel', cancelLongPress);
    header.addEventListener('pointerleave', cancelLongPress);
    header.addEventListener('click', (event) => {
      if (longPressFired) {
        event.preventDefault();
        event.stopPropagation();
        longPressFired = false;
        return;
      }
      const next = !(nodeSectionCollapsed.get(sectionId) === true);
      nodeSectionCollapsed.set(sectionId, next);
      renderNodes(nodesCache, { force: true });
    });

    group.appendChild(header);

    const body = document.createElement('div');
    body.className = 'node-section-body';
    body.hidden = collapsed;
    group.appendChild(body);
    nodesEl.appendChild(group);

    sectionNodes.forEach(n => {
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
    const identityBadge = renderNodeIdentityBadge(n);
    if (identityBadge) {
      title.appendChild(identityBadge);
    }
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
    const isSonos = n.type === 'sonos';
    const paired = isSonos ? true : !!n.paired;
    const configured = (isBrowser || isSonos) ? true : !!n.configured;
    const online = isBrowser ? true : n.online !== false;
    const restarting = !!n.restarting;
    const updateAvailable = hasAgentUpdate(n);
    const updating = !!n.updating;
    const disableNodeControls = isBrowser ? false : (!online || restarting || (!isSonos && (!paired || !configured)));

    let eqBtn = null;
    if (isBrowser || isSonos || n.type === 'agent') {
      eqBtn = document.createElement('button');
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
    }

    if (editMode) {
      const sectionSelector = createNodeSectionSelector(n, { sections: resolvedSections, disabled: false });
      if (sectionSelector) {
        gearWrap.insertBefore(sectionSelector, eqBtn || gearBtn);
      }
    }
    const channelSelector = createNodeChannelSelector(n, { disabled: disableNodeControls });
    if (channelSelector) {
      gearWrap.insertBefore(channelSelector, eqBtn || gearBtn);
    }
    if (!isBrowser && !isSonos) {
      const wifiIndicator = renderNodeWifiIndicator(n);
      title.appendChild(wifiIndicator);
    }
    if (isSonos) {
      const netIcon = renderSonosNetworkIcon(n);
      if (netIcon) {
        title.appendChild(netIcon);
      }
    }
    if (!isBrowser && !isSonos && !paired) {
      const pairPill = document.createElement('span');
      pairPill.className = 'status-pill warn';
      pairPill.textContent = 'Pairing required';
      statusRow.appendChild(pairPill);
    }
    if (!isBrowser && !isSonos) {
      if (!configured) {
        const cfgPill = document.createElement('span');
        cfgPill.className = 'status-pill warn';
        cfgPill.textContent = 'Needs config';
        statusRow.appendChild(cfgPill);
      }
    }
    if (!isBrowser && !isSonos && (updateAvailable || updating)) {
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
    if (isSonos && typeof n.connection_error === 'string' && n.connection_error.trim()) {
      const errPill = document.createElement('button');
      errPill.type = 'button';
      errPill.className = 'status-pill err';
      errPill.textContent = 'Error';
      errPill.title = n.connection_error.trim();
      errPill.setAttribute('aria-label', `Show error details for ${n.name}`);
      errPill.addEventListener('click', (event) => {
        event.stopPropagation();
        openNodeSettingsModal(n.id);
      });
      statusRow.appendChild(errPill);
    }
    wrapper.appendChild(statusRow);

    const volRow = document.createElement('div');
    volRow.style.display = 'grid';
    volRow.style.gridTemplateColumns = 'auto minmax(0, 1fr) auto';
    volRow.style.alignItems = 'center';
    volRow.style.gap = '8px';
    const muteBtn = document.createElement('button');
    muteBtn.className = 'node-mute-btn';
    muteBtn.disabled = disableNodeControls;
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
    volInput.disabled = disableNodeControls;
    volInput.style.width = '100%';
    const nodeVolumeColor = getNodeChannelAccent(n) || '#94a3b8';
    applyRangeAccent(volInput, nodeVolumeColor);
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
      queueNodeVolumeUpdate(n.id, volInput.value);
    });
    volInput.addEventListener('change', () => {
      flushPendingNodeVolumeUpdate(n.id);
      setNodeVolume(n.id, volInput.value);
    });
    volRow.appendChild(volInput);
    nodeVolumeSliderRefs.set(n.id, volInput);
    if (volumeMeta) {
      volRow.appendChild(volumeMeta);
    }
    wrapper.appendChild(volRow);

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

    body.appendChild(wrapper);
    });
  });

  const nodeIds = new Set(nodes.map(n => n.id));
  Object.keys(camillaPendingNodes).forEach(id => {
    if (!nodeIds.has(id)) delete camillaPendingNodes[id];
  });
  nodeVolumeUpdateTimers.forEach((timer, nodeId) => {
    if (nodeIds.has(nodeId)) return;
    clearTimeout(timer);
    nodeVolumeUpdateTimers.delete(nodeId);
  });
  refreshNodeSettingsModal();
}

function refreshNodeVolumeAccents() {
  if (!nodeVolumeSliderRefs.size || !nodesCache.length) return;
  const nodeMap = new Map(nodesCache.map(node => [node.id, node]));
  nodeVolumeSliderRefs.forEach((slider, nodeId) => {
    if (!slider) return;
    const node = nodeMap.get(nodeId);
    if (!node) return;
    const color = getNodeChannelAccent(node) || '#94a3b8';
    applyRangeAccent(slider, color);
    setRangeProgress(slider, slider.value, slider.max || 100);
  });
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
    const isSonos = typeof item.url === 'string' && item.url.startsWith('sonos://');
    const row = document.createElement('div');
    row.className = 'panel discover-row';
    row.style.marginBottom = '8px';
    const title = document.createElement('div');
    const versionLabel = isSonos ? 'Sonos speaker' : (item.version ? `Agent ${item.version}` : 'Version unknown');
    title.innerHTML = `<strong>${item.host}</strong> <span class="muted">${item.url}</span><div class="label">${versionLabel}</div>`;
    const nameInput = document.createElement('input');
    const existing = findNodeByFingerprint(item.fingerprint);
    if (existing) {
      nameInput.value = existing.name || existing.id || `Node ${item.host}`;
      nameInput.disabled = true;
    } else {
      nameInput.value = isSonos ? (item.host || 'Sonos speaker') : `Node ${item.host}`;
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
  if (!isAuthenticated()) return;
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

