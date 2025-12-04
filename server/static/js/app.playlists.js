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

