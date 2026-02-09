function showPlaylistsGrid(options = {}) {
  playlistAutoSelectId = null;
  setPlaylistView('playlists');
  if (playlistSubtitle) playlistSubtitle.textContent = '';
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  setPlaylistErrorMessage('');
  if (playlistGrid) playlistGrid.innerHTML = '';
  if (playlistEmpty) {
    playlistEmpty.hidden = true;
    playlistEmpty.textContent = '';
  }
  setPlaylistLoadingState(true, 'Loading playlists…');
  ensurePlaylistsLoaded(options);
}

async function fetchPlaylistDetails(playlistId) {
  if (!playlistId) return null;
  const channelId = getActiveChannelId();
  if (!channelId) {
    setPlaylistErrorMessage('Select a channel to load playlists.');
    return null;
  }
  const res = await fetch(withChannel(`/api/spotify/playlists/${encodeURIComponent(playlistId)}`, channelId));
  await ensureOk(res);
  const data = await res.json();
  const playlist = data?.playlist || null;
  if (playlist) rememberPlaylistMetadata(playlist);
  return playlist;
}

async function ensurePlaylistDetails(playlistId) {
  const cached = getCachedPlaylistMetadata(playlistId);
  if (cached) return cached;
  return fetchPlaylistDetails(playlistId);
}

async function showPlaylistTracksForContext(playlistId) {
  if (!playlistId) {
    showPlaylistsGrid();
    return;
  }
  setPlaylistView('tracks');
  setPlaylistErrorMessage('');
  if (playlistTracklist) playlistTracklist.innerHTML = '';
  setPlaylistLoadingState(true, 'Loading playlist…');
  try {
    const playlist = await ensurePlaylistDetails(playlistId);
    if (!playlist) throw new Error('Unable to load playlist right now.');
    const usedCache = selectPlaylist(playlist);
    if (usedCache) setPlaylistLoadingState(false);
  } catch (err) {
    setPlaylistErrorMessage(err?.message || 'Unable to load playlist right now.');
    showPlaylistsGrid();
  }
}

function isTrackCacheFresh(state) {
  if (!state) return false;
  return isCacheFresh(state.fetchedAt, PLAYLIST_TRACK_CACHE_TTL_MS);
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

function playlistIdentity(item) {
  const playlistId = item?.id || extractSpotifyPlaylistId(item?.uri);
  return playlistId || `${item?.uri || ''}:${item?.name || ''}`;
}

function createPlaylistCard(item) {
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
  card.addEventListener('click', () => {
    if (playlistPickerMode && typeof playlistPickerOnSelect === 'function') {
      playlistPickerOnSelect(item);
      return;
    }
    selectPlaylist(item);
  });
  syncPlaylistHighlightForElement(card);
  return card;
}

function renderPlaylistSection(title, items) {
  if (!playlistGrid || !Array.isArray(items) || !items.length) return;
  const section = document.createElement('section');
  section.className = 'playlist-section';
  section.setAttribute('aria-label', title);

  const heading = document.createElement('h3');
  heading.className = 'playlist-section-title';
  heading.textContent = title;

  const sectionGrid = document.createElement('div');
  sectionGrid.className = 'playlist-grid-section';
  sectionGrid.setAttribute('role', 'list');

  items.forEach(item => {
    sectionGrid.appendChild(createPlaylistCard(item));
  });

  section.appendChild(heading);
  section.appendChild(sectionGrid);
  playlistGrid.appendChild(section);
}

function renderPlaylistGrid(items = playlistsCache) {
  if (!playlistGrid) return;
  playlistGrid.innerHTML = '';
  const addedList = Array.isArray(items) ? items : [];
  const recentlyPlayedList = Array.isArray(playlistsRecentlyPlayedCache) ? playlistsRecentlyPlayedCache : [];
  const filterMatches = item => {
    if (!playlistSearchTerm) return true;
    const haystack = `${item?.name || ''} ${item?.owner || ''} ${item?.description || ''}`.toLowerCase();
    return haystack.includes(playlistSearchTerm);
  };

  let totalVisible = 0;
  let totalSourceCount = 0;
  if (playlistSortOrder === 'recent_played') {
    const recentFiltered = sortPlaylists(recentlyPlayedList.filter(filterMatches));
    renderPlaylistSection('Recently played', recentFiltered);
    totalVisible = recentFiltered.length;
    totalSourceCount = recentlyPlayedList.length;
    if (playlistSubtitle) {
      playlistSubtitle.textContent = playlistsRecentlyPlayedScopeMissing
        ? 'Requires Spotify permission: recently played history'
        : 'Recently played playlists';
    }
  } else if (playlistSortOrder === 'name') {
    const merged = [];
    const seen = new Set();
    [...recentlyPlayedList, ...addedList].forEach(item => {
      const key = playlistIdentity(item);
      if (!key || seen.has(key)) return;
      seen.add(key);
      merged.push(item);
    });
    const allFiltered = sortPlaylists(merged.filter(filterMatches));
    renderPlaylistSection('All playlists', allFiltered);
    totalVisible = allFiltered.length;
    totalSourceCount = merged.length;
    if (playlistSubtitle) playlistSubtitle.textContent = 'All playlists';
  } else {
    const recentFiltered = sortPlaylists(recentlyPlayedList.filter(filterMatches));
    const recentIds = new Set(recentFiltered.map(playlistIdentity));
    const addedFiltered = sortPlaylists(addedList.filter(item => !recentIds.has(playlistIdentity(item)) && filterMatches(item)));
    renderPlaylistSection('Recently played', recentFiltered);
    renderPlaylistSection('Recently added', addedFiltered);
    totalVisible = recentFiltered.length + addedFiltered.length;
    totalSourceCount = addedList.length + recentlyPlayedList.length;
    if (playlistSubtitle) playlistSubtitle.textContent = 'Recently played and recently added playlists';
  }
  updatePlaylistCardHighlights();
  if (playlistEmpty) {
    if (!totalSourceCount) {
      playlistEmpty.hidden = false;
      if (playlistSortOrder === 'recent_played' && playlistsRecentlyPlayedScopeMissing) {
        playlistEmpty.textContent = 'Spotify permission missing for recently played. Sign out and sign in to Spotify again.';
      } else {
        playlistEmpty.textContent = playlistSortOrder === 'recent_played'
          ? 'No recently played playlists yet.'
          : 'No playlists available yet.';
      }
    } else if (!totalVisible) {
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
  if (playlistSortOrder === 'recent_played') {
    sorted.sort((a, b) => {
      const aTs = Date.parse(a?.recent_played_at || '');
      const bTs = Date.parse(b?.recent_played_at || '');
      const safeA = Number.isFinite(aTs) ? aTs : -1;
      const safeB = Number.isFinite(bTs) ? bTs : -1;
      if (safeA !== safeB) return safeB - safeA;
      return (a?._order ?? 0) - (b?._order ?? 0);
    });
    return sorted;
  }
  sorted.sort((a, b) => (a?._order ?? 0) - (b?._order ?? 0));
  return sorted;
}
