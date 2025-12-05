function setPlayerIdleState(message = 'Player unavailable', options = {}) {
  if (!playerPanel) return;
  optimisticSeekState = null;
  radioPlaybackState = null;
  if (playerTick) {
    clearInterval(playerTick);
    playerTick = null;
  }
  const forceClear = options.forceClear === true;
  const snapshot = forceClear ? null : getPlayerSnapshot();
  if (snapshot) {
    playerStatus = {
      active: false,
      is_playing: false,
      progress_ms: 0,
      device: {},
      context: snapshot.context,
      item: snapshot.item,
      shuffle_state: snapshot.shuffle_state,
      repeat_state: snapshot.repeat_state,
      allowResume: true,
      __fromSnapshot: true,
    };
    renderPlayer(playerStatus);
    return;
  }
  playerStatus = null;
  playerPanel.style.display = 'flex';
  playerTitle.textContent = message;
  playerArtist.textContent = '';
  playerSeek.disabled = true;
  playerTimeCurrent.textContent = '0:00';
  playerTimeTotal.textContent = '0:00';
  playerArt.style.display = 'none';
  playerArt.alt = '';
  setPlayerArtInteractivity(false);
  lastCoverArtUrl = null;
  applyCoverArtBackground();
  setRangeProgress(playerSeek, 0, playerSeek.max || 1);
  setPlayButtonIcon(false);
  playerPrev.disabled = true;
  playerPlay.disabled = true;
  playerNext.disabled = true;
  if (playerPlay) playerPlay.dataset.radioChannelId = '';
  if (playerShuffleBtn) playerShuffleBtn.disabled = true;
  if (playerRepeatBtn) playerRepeatBtn.disabled = true;
  setShuffleActive(false);
  setRepeatMode('off');
  setTakeoverBannerVisible(false);
}

async function fetchPlayerStatus() {
  if (!isAuthenticated()) return;
  const channel = getActiveChannel();
  if (!channel) {
    setPlayerIdleState('Select a channel to control playback', { forceClear: true });
    return;
  }
  if (isRadioChannel(channel)) {
    await fetchRadioPlaybackStatus(channel);
    return;
  }
  await fetchSpotifyPlayerStatus(channel.id);
}

async function fetchSpotifyPlayerStatus(channelId) {
  if (!channelId) return;
  try {
    const res = await fetch(withChannel('/api/spotify/player/status', channelId));
    await ensureOk(res);
    playerStatus = await res.json();
    const serverSnapshot = hydrateServerSnapshot(playerStatus?.snapshot);
    if (serverSnapshot) {
      delete playerStatus.snapshot;
    }
    const cachedSnapshot = !playerStatus?.active ? getPlayerSnapshot() : null;
    if (!playerStatus?.active && cachedSnapshot) {
      playerStatus = {
        ...playerStatus,
        item: playerStatus?.item || cachedSnapshot.item,
        context: playerStatus?.context || cachedSnapshot.context,
        shuffle_state: playerStatus?.shuffle_state ?? cachedSnapshot.shuffle_state,
        repeat_state: playerStatus?.repeat_state ?? cachedSnapshot.repeat_state,
        allowResume: true,
        __fromSnapshot: true,
      };
    }
    renderPlayer(playerStatus);
    markSpotifyHealthy();
  } catch (err) {
    setPlayerIdleState('Player unavailable');
    reportSpotifyError(err);
  }
}

async function fetchRadioPlaybackStatus(channel) {
  if (!channel?.id) return;
  try {
    const res = await fetch(`/api/radio/status/${encodeURIComponent(channel.id)}`);
    await ensureOk(res);
    const payload = await res.json();
    renderRadioPlayer(channel, payload);
  } catch (err) {
    renderRadioPlayer(channel, null, { error: err.message || 'Radio status unavailable' });
  }
}

function describeRadioRuntimeState(state) {
  if (!state) return '';
  switch (state) {
    case 'playing':
      return 'Live';
    case 'connecting':
      return 'Connecting';
    case 'buffering':
      return 'Buffering';
    case 'error':
      return 'Error';
    case 'idle':
    default:
      return state.charAt(0).toUpperCase() + state.slice(1);
  }
}

function renderRadioPlayer(channel, payload, options = {}) {
  if (!playerPanel) return;
  optimisticSeekState = null;
  playerStatus = null;
  playerPanel.style.display = 'flex';
  const radioState = payload?.radio_state || getRadioState(channel) || {};
  if (channel?.id) {
    updateChannelRadioState(channel.id, radioState);
  }
  const runtime = payload?.runtime || {};
  const enabled = payload?.enabled ?? channel?.enabled ?? true;
  const hasStation = !!radioState.stream_url;
  const playbackEnabled = radioState.playback_enabled !== false;
  const canToggle = enabled && hasStation;
  const runtimeLabel = describeRadioRuntimeState(runtime.state);
  let titleBase = radioState.station_name || channel?.name || 'Radio channel';
  if (!hasStation) titleBase = channel?.name || 'Radio channel';
  const runtimeMetadata = runtime.metadata && typeof runtime.metadata === 'object' ? runtime.metadata : {};
  const stateMetadata = radioState.last_metadata && typeof radioState.last_metadata === 'object'
    ? radioState.last_metadata
    : {};
  const metadata = Object.keys(runtimeMetadata).length || Object.keys(stateMetadata).length
    ? { ...runtimeMetadata, ...stateMetadata }
    : {};
  const metadataParts = [];
  if (metadata.artist) metadataParts.push(metadata.artist);
  if (metadata.title) metadataParts.push(metadata.title);
  const streamTitle = metadata.streamTitle || metadata.StreamTitle;
  if (streamTitle) metadataParts.push(streamTitle);
  if (!metadataParts.length && metadata.text) metadataParts.push(metadata.text);
  let subtitle = options.error || metadataParts.join(' – ');
  if (!subtitle) {
    const fallbackParts = [];
    if (radioState.station_country) fallbackParts.push(radioState.station_country);
    if (radioState.bitrate) fallbackParts.push(`${radioState.bitrate} kbps`);
    if (runtime.message) fallbackParts.push(runtime.message);
    subtitle = fallbackParts.join(' • ');
  }
  if (!enabled) subtitle = 'Enable this channel in Settings to start playback.';
  else if (!hasStation && !options.error) subtitle = 'Select a station to start playback.';
  if (!subtitle) subtitle = hasStation ? 'Streaming radio' : 'Radio offline';
  playerTitle.textContent = runtimeLabel && hasStation ? `${titleBase} (${runtimeLabel})` : titleBase;
  playerArtist.textContent = subtitle;
  if (radioState.station_favicon) {
    playerArt.src = radioState.station_favicon;
    playerArt.alt = radioState.station_name ? `${radioState.station_name} artwork` : 'Station artwork';
    playerArt.style.display = 'block';
  } else {
    playerArt.style.display = 'none';
    playerArt.alt = '';
  }
  setPlayerArtInteractivity(false);
  lastCoverArtUrl = radioState.station_favicon || null;
  applyCoverArtBackground();
  playerSeek.disabled = true;
  playerSeek.value = 0;
  setRangeProgress(playerSeek, 0, playerSeek.max || 1);
  playerTimeCurrent.textContent = '—';
  playerTimeTotal.textContent = hasStation ? 'Live' : '0:00';
  let buttonLabel;
  if (!enabled) buttonLabel = 'Enable this channel in Settings to start radio';
  else if (!hasStation) buttonLabel = 'Select a station to start radio';
  else buttonLabel = playbackEnabled ? 'Stop radio' : 'Play radio';
  setPlayButtonIcon(playbackEnabled && hasStation && enabled, { variant: 'radio', label: buttonLabel });
  if (playerPlay) {
    playerPlay.disabled = !canToggle;
    playerPlay.dataset.radioChannelId = channel?.id || '';
  }
  playerPrev.disabled = true;
  playerNext.disabled = true;
  if (playerShuffleBtn) {
    playerShuffleBtn.disabled = true;
    setShuffleActive(false);
  }
  if (playerRepeatBtn) {
    playerRepeatBtn.disabled = true;
    setRepeatMode('off');
  }
  if (playerTick) {
    clearInterval(playerTick);
    playerTick = null;
  }
  setTakeoverBannerVisible(false);
  radioPlaybackState = {
    channelId: channel?.id || null,
    playbackEnabled,
    hasStation,
    enabled: !!enabled,
    runtimeState: runtime?.state || 'idle',
  };
}

const PLAYER_STOP_ALL_HOLD_MS = 1500;
const PLAYER_SEEK_OPTIMISTIC_WINDOW_MS = 3000;
const PLAYER_SEEK_TOLERANCE_MS = 750;
let playerPlayHoldTimer = null;
let playerPlayHoldTriggered = false;
let optimisticSeekState = null;

function cancelPlayerHoldToStop(options = {}) {
  if (playerPlayHoldTimer) {
    clearTimeout(playerPlayHoldTimer);
    playerPlayHoldTimer = null;
  }
  if (options.resetHold) {
    playerPlayHoldTriggered = false;
  }
}

function armPlayerHoldToStop() {
  if (!playerPlay || playerPlay.disabled) return;
  cancelPlayerHoldToStop();
  playerPlayHoldTriggered = false;
  playerPlayHoldTimer = setTimeout(async () => {
    playerPlayHoldTimer = null;
    playerPlayHoldTriggered = true;
    await confirmStopAllPlayback();
  }, PLAYER_STOP_ALL_HOLD_MS);
}

function clampSeekProgress(value, duration) {
  const safeValue = Number.isFinite(value) ? value : 0;
  if (!Number.isFinite(duration) || duration <= 0) return Math.max(0, safeValue);
  return Math.min(Math.max(safeValue, 0), duration);
}

function resolveOptimisticSeekProgress(progress, duration, status) {
  if (!optimisticSeekState) return { progress, pending: false };
  if (!status?.active || !playerSeek) {
    optimisticSeekState = null;
    return { progress, pending: false };
  }
  const activeChannelId = getActiveChannelId();
  if (!activeChannelId || optimisticSeekState.channelId !== activeChannelId) {
    optimisticSeekState = null;
    return { progress, pending: false };
  }
  const now = Date.now();
  if (now - optimisticSeekState.requestedAt > PLAYER_SEEK_OPTIMISTIC_WINDOW_MS) {
    optimisticSeekState = null;
    return { progress, pending: false };
  }
  if (progress >= (optimisticSeekState.requestedProgress - PLAYER_SEEK_TOLERANCE_MS)) {
    optimisticSeekState = null;
    return { progress, pending: false };
  }
  const capped = clampSeekProgress(optimisticSeekState.requestedProgress, duration);
  return { progress: capped, pending: true };
}

function noteOptimisticSeek(requestedProgress) {
  if (!playerSeek) return;
  const channelId = getActiveChannelId();
  if (!channelId) {
    optimisticSeekState = null;
    return;
  }
  const duration = Number(playerSeek.max) || playerStatus?.item?.duration_ms || 0;
  const clamped = clampSeekProgress(Number(requestedProgress) || 0, duration);
  optimisticSeekState = {
    channelId,
    requestedProgress: clamped,
    requestedAt: Date.now(),
  };
  playerSeek.value = clamped;
  setRangeProgress(playerSeek, clamped, duration || 1);
  playerTimeCurrent.textContent = msToTime(clamped);
  if (playerStatus) {
    playerStatus.progress_ms = clamped;
  }
}

async function confirmStopAllPlayback() {
  const confirmed = await openConfirmDialog({
    title: 'Stop all playback?',
    message: 'This will pause Spotify and stop radio streams on every channel.',
    confirmLabel: 'Stop all playback',
    cancelLabel: 'Keep playing',
    tone: 'danger',
  });
  if (!confirmed) return;
  await stopAllPlaybackStreams();
}

async function stopAllPlaybackStreams() {
  try {
    const res = await fetch('/api/playback/stop-all', { method: 'POST' });
    await ensureOk(res);
    const data = await res.json();
    const radioCount = Array.isArray(data?.radio_stopped) ? data.radio_stopped.length : 0;
    const spotifyCount = Array.isArray(data?.spotify_stopped) ? data.spotify_stopped.length : 0;
    const totalStopped = radioCount + spotifyCount;
    if (data?.radio_states && typeof data.radio_states === 'object') {
      Object.entries(data.radio_states).forEach(([cid, state]) => updateChannelRadioState(cid, state));
    }
    const summaryParts = [];
    if (spotifyCount) summaryParts.push(`${spotifyCount} Spotify channel${spotifyCount === 1 ? '' : 's'}`);
    if (radioCount) summaryParts.push(`${radioCount} radio channel${radioCount === 1 ? '' : 's'}`);
    const summaryText = summaryParts.length ? summaryParts.join(' and ') : 'No active playback';
    const hasErrors = data?.errors && Object.keys(data.errors).length > 0;
    if (hasErrors) {
      const errorSummary = Object.entries(data.errors)
        .map(([cid, detail]) => `${cid}: ${detail}`)
        .join('; ');
      showError(`Stopped ${summaryText}, but some channels failed: ${errorSummary}`);
    } else if (totalStopped) {
      showSuccess(`Stopped ${summaryText}.`);
    } else {
      showSuccess('No active playback to stop.');
    }
    await fetchPlayerStatus();
    await refreshChannels({ force: true }).catch(() => {});
  } catch (err) {
    showError(`Failed to stop playback: ${err.message}`);
  }
}

function renderPlayer(status) {
  radioPlaybackState = null;
  const item = status?.item || {};
  const active = !!status?.active;
  const resumeAvailable = !!status?.allowResume && !active;
  const showMeta = active || resumeAvailable;
  const activeChannel = getActiveChannel();
  const channelLabel = (status?.channel_name || activeChannel?.name || authState?.server_name || '').trim();
  updateActivePlaylistContext(status);
  updateTakeoverBanner(status);
  activeDeviceId = active && status?.device?.id ? status.device.id : null;
  if (!status?.__fromSnapshot && active && status?.device_is_roomcast !== false) {
    rememberPlayerSnapshot(status);
  }
  playerPanel.style.display = 'flex';
  playerTitle.textContent = showMeta ? (item.name || '—') : 'No active playback';
  const artistsRaw = Array.isArray(item.artists)
    ? item.artists.map(a => a?.name).filter(Boolean).join(', ')
    : (item.artists || '');
  if (showMeta) {
    if (resumeAvailable && channelLabel) {
      playerArtist.textContent = channelLabel;
    } else {
      playerArtist.textContent = artistsRaw || '—';
    }
  } else {
    playerArtist.textContent = channelLabel || '';
  }
  const art = showMeta ? (item.album?.images?.[1]?.url || item.album?.images?.[0]?.url) : null;
  if (art) {
    playerArt.src = art;
    playerArt.alt = item?.name ? `${item.name} cover art` : 'Album art';
    playerArt.style.display = 'block';
    setPlayerArtInteractivity(active);
  } else {
    playerArt.style.display = 'none';
    playerArt.alt = '';
    setPlayerArtInteractivity(false);
  }
  lastCoverArtUrl = art || null;
  applyCoverArtBackground();
  const duration = item.duration_ms || 0;
  const baseProgress = status?.progress_ms || 0;
  const { progress: displayProgress } = resolveOptimisticSeekProgress(baseProgress, duration, status);
  playerSeek.max = duration || 1;
  playerSeek.value = displayProgress;
  setRangeProgress(playerSeek, displayProgress, duration || 1);
  playerTimeCurrent.textContent = msToTime(displayProgress);
  status.progress_ms = displayProgress;
  playerTimeTotal.textContent = msToTime(duration);
  const playing = !!status?.is_playing && active;
  setPlayButtonIcon(playing);
  playerPrev.disabled = !active;
  playerPlay.disabled = !showMeta;
  if (playerPlay && playerPlay.dataset.radioChannelId) {
    playerPlay.dataset.radioChannelId = '';
  }
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

  playPendingPlayerPanelEntryAnimation();

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

async function handleRadioPlayToggle(channel) {
  if (!channel || !channel.id) {
    showError('Select a radio channel before toggling playback.');
    return;
  }
  const snapshot = resolveRadioPlaybackSnapshot(channel);
  if (!snapshot.hasStation) {
    showError('Select a station to start radio playback.');
    return;
  }
  if (!snapshot.enabled) {
    showError('Enable this channel in Settings to control radio playback.');
    return;
  }
  const action = snapshot.playbackEnabled ? 'stop' : 'start';
  if (playerPlay) {
    playerPlay.disabled = true;
  }
  try {
    const res = await fetch(`/api/radio/${encodeURIComponent(channel.id)}/playback`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    await ensureOk(res);
    const data = await res.json();
    updateChannelRadioState(channel.id, data?.radio_state || null);
  } catch (err) {
    if (playerPlay) playerPlay.disabled = false;
    showError(`Failed to ${action === 'start' ? 'start' : 'stop'} radio: ${err.message}`);
    return;
  }
  await fetchRadioPlaybackStatus(getChannelById(channel.id) || channel);
}

function buildPlayerActionPath(path, body, channelId) {
  const hasDevice = body && typeof body === 'object' && Object.prototype.hasOwnProperty.call(body, 'device_id');
  let targetPath = withChannel(path, channelId);
  if (hasDevice) return targetPath;
  const deviceId = getActiveDeviceId();
  if (!deviceId) return targetPath;
  const separator = targetPath.includes('?') ? '&' : '?';
  return `${targetPath}${separator}device_id=${encodeURIComponent(deviceId)}`;
}

function parseSpotifyErrorDetail(detail) {
  if (!detail || typeof detail !== 'string') return {};
  const trimmed = detail.trim();
  if (!trimmed.startsWith('{')) return {};
  try {
    const parsed = JSON.parse(trimmed);
    const error = parsed?.error || {};
    return {
      message: typeof error.message === 'string' ? error.message : null,
      reason: typeof error.reason === 'string' ? error.reason.toLowerCase() : null,
    };
  } catch (_) {
    return {};
  }
}

async function activateRoomcastDevice(play = false) {
  const channelId = getActiveChannelId();
  if (!channelId) {
    showError('Select a channel to activate playback.');
    return;
  }
  const payload = { play: !!play };
  const res = await fetch(withChannel('/api/spotify/player/activate-roomcast', channelId), {
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

async function prepareRoomcastDeviceForResume(resumePayload) {
  if (!resumePayload) return;
  if (playerStatus?.active) return;
  if (playerStatus?.device_is_roomcast === false) return;
  const deviceId = getActiveDeviceId();
  if (deviceId) return;
  try {
    await activateRoomcastDevice(false);
  } catch (err) {
    console.warn('RoomCast device activation before resume failed:', err);
  }
}

async function playerAction(path, body, options = {}) {
  const channelId = getActiveChannelId();
  if (!channelId) {
    showError('Select a channel before controlling playback.');
    return;
  }
  const allowRoomcastFallback = options.roomcastFallback !== false;
  let attemptedRoomcastActivation = false;
  while (true) {
    try {
      const targetPath = buildPlayerActionPath(path, body, channelId);
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
      const meta = parseSpotifyErrorDetail(detail);
      const looksLikeNoDevice = normalizedDetail.includes('no active device');
      const looksLikeRestriction = normalizedDetail.includes('restriction') || meta.reason === 'unknown';
      const canActivateRoomcast = allowRoomcastFallback
        && !attemptedRoomcastActivation
        && (
          (res.status === 404 && looksLikeNoDevice)
          || (res.status === 403 && looksLikeRestriction)
        );
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
    const channelId = getSettingsChannelId();
    if (!channelId) {
      showError('No channels available to link.');
      return;
    }
    const res = await fetch(withChannel('/api/spotify/auth-url', channelId));
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
  const channelId = getSettingsChannelId();
  if (!channelId) {
    showError('No channels available to update.');
    return;
  }
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
    const res = await fetch(withChannel('/api/config/spotify', channelId), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    await ensureOk(res);
    spClientSecret.value = '';
    const targetChannel = getChannelById(channelId);
    const channelName = targetChannel?.name ? ` for ${targetChannel.name}` : '';
    showSuccess(`Spotify config saved${channelName}. Librespot will reload and connect.`);
    await pollLibrespotStatus(channelId);
  } catch (err) {
    showError(`Failed to save Spotify config: ${err.message}`);
  } finally {
    saveSpotifyBtn.disabled = false;
    spotifySpinner.style.display = 'none';
  }
}

async function saveServerName() {
  if (!isAdminUser()) {
    showError('Only admins can rename the server.');
    return;
  }
  const value = (serverNameInput?.value || '').trim();
  if (!value) {
    showError('Server name cannot be empty.');
    return;
  }
  try {
    saveServerNameBtn.disabled = true;
    const res = await fetch('/api/server/name', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ server_name: value }),
    });
    await ensureOk(res);
    const data = await res.json();
    authState.server_name = data.server_name || value;
    setServerBranding(authState.server_name);
    syncGeneralSettingsUI();
    showSuccess('Server name updated.');
  } catch (err) {
    showError(`Failed to update server name: ${err.message}`);
  } finally {
    saveServerNameBtn.disabled = false;
  }
}

async function fetchUsersList(force = false) {
  if (!usersListEl) return;
  if (!isAdminUser()) {
    usersCache = [];
    usersLoaded = false;
    renderUsersList();
    return;
  }
  if (usersLoaded && !force) {
    renderUsersList();
    return;
  }
  try {
    const res = await fetch('/api/users');
    await ensureOk(res);
    const data = await res.json();
    usersCache = Array.isArray(data?.users) ? data.users : [];
    usersLoaded = true;
    renderUsersList();
  } catch (err) {
    showError(`Failed to load users: ${err.message}`);
  }
}

function renderUsersList() {
  if (!usersListEl) return;
  const canManage = isAdminUser();
  if (usersPanelNote) {
    usersPanelNote.textContent = canManage
      ? 'Admins can add members or other admins. Password changes are immediate.'
      : 'Only admins can manage accounts.';
  }
  if (addUserForm) addUserForm.hidden = !canManage;
  if (!canManage) {
    usersListEl.innerHTML = '';
    usersListEl.hidden = true;
    return;
  }
  usersListEl.hidden = false;
  if (!usersCache.length) {
    usersListEl.innerHTML = '<div class="muted">No additional users yet.</div>';
    return;
  }
  usersListEl.innerHTML = '';
  usersCache.forEach(user => {
    const form = document.createElement('form');
    form.className = 'user-row';
    form.dataset.userId = user.id;

    const usernameWrap = document.createElement('div');
    const usernameLabel = document.createElement('label');
    usernameLabel.textContent = authState?.user?.id === user.id ? 'Username (you)' : 'Username';
    const usernameInput = document.createElement('input');
    usernameInput.value = user.username || '';
    usernameInput.required = true;
    usernameWrap.appendChild(usernameLabel);
    usernameWrap.appendChild(usernameInput);

    const roleWrap = document.createElement('div');
    const roleLabel = document.createElement('label');
    roleLabel.textContent = 'Role';
    const roleSelect = document.createElement('select');
    ['admin', 'member'].forEach(value => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value === 'admin' ? 'Admin' : 'Member';
      roleSelect.appendChild(option);
    });
    roleSelect.value = user.role || 'member';
    roleWrap.appendChild(roleLabel);
    roleWrap.appendChild(roleSelect);

    const passwordWrap = document.createElement('div');
    const passwordLabel = document.createElement('label');
    passwordLabel.textContent = 'New password';
    const passwordInput = document.createElement('input');
    passwordInput.type = 'password';
    passwordInput.placeholder = 'Leave blank to keep current';
    passwordInput.autocomplete = 'new-password';
    passwordWrap.appendChild(passwordLabel);
    passwordWrap.appendChild(passwordInput);

    const actions = document.createElement('div');
    actions.className = 'user-row-actions';
    const saveBtn = document.createElement('button');
    saveBtn.type = 'submit';
    saveBtn.textContent = 'Save';
    const deleteBtn = document.createElement('button');
    deleteBtn.type = 'button';
    deleteBtn.className = 'small-btn';
    deleteBtn.textContent = 'Delete';
    actions.appendChild(saveBtn);
    actions.appendChild(deleteBtn);

    form.appendChild(usernameWrap);
    form.appendChild(roleWrap);
    form.appendChild(passwordWrap);
    form.appendChild(actions);

    form.addEventListener('submit', async evt => {
      evt.preventDefault();
      const payload = {};
      const nextName = usernameInput.value.trim();
      if (nextName && nextName !== user.username) payload.username = nextName;
      if (roleSelect.value !== user.role) payload.role = roleSelect.value;
      if (passwordInput.value) payload.password = passwordInput.value;
      if (!Object.keys(payload).length) {
        showSuccess('Nothing to update.');
        passwordInput.value = '';
        return;
      }
      try {
        saveBtn.disabled = true;
        const res = await fetch(`/api/users/${encodeURIComponent(user.id)}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        await ensureOk(res);
        const data = await res.json();
        const updated = data?.user;
        if (updated) {
          usersCache = usersCache.map(u => (u.id === updated.id ? updated : u));
          if (authState?.user?.id === updated.id) {
            authState.user = updated;
            updateUserStatusUI();
          }
        }
        showSuccess('User updated.');
        fetchUsersList(true);
      } catch (err) {
        showError(`Failed to update user: ${err.message}`);
      } finally {
        saveBtn.disabled = false;
        passwordInput.value = '';
      }
    });

    deleteBtn.addEventListener('click', async () => {
      const confirmed = window.confirm(`Remove user "${user.username}"?`);
      if (!confirmed) return;
      try {
        deleteBtn.disabled = true;
        const res = await fetch(`/api/users/${encodeURIComponent(user.id)}`, { method: 'DELETE' });
        await ensureOk(res);
        const data = await res.json();
        usersCache = usersCache.filter(u => u.id !== user.id);
        usersLoaded = false;
        showSuccess('User removed.');
        if (data?.self_removed) {
          stopDataPolling();
          await refreshAuthState();
          return;
        }
        fetchUsersList(true);
      } catch (err) {
        showError(`Failed to delete user: ${err.message}`);
      } finally {
        deleteBtn.disabled = false;
      }
    });

    usersListEl.appendChild(form);
  });
}

function resetUsersState() {
  usersCache = [];
  usersLoaded = false;
  if (usersListEl) {
    usersListEl.innerHTML = '';
  }
}

async function pollLibrespotStatus(channelId = getSettingsChannelId()) {
  if (!channelId) return;
  for (let i = 0; i < 8; i++) {
    await new Promise(r => setTimeout(r, 1200));
    await fetchLibrespotStatus(channelId);
    const text = librespotStatus.innerText || '';
    if (!text.includes('starting') && !text.includes('waiting') && !text.includes('unknown')) break;
  }
}

async function openSettings() {
  settingsOverlay.style.display = 'flex';
  collapseAllPanels();
  try {
    await refreshChannels();
  } catch (_) {
    /* channel errors already surfaced via toast */
  }
  spotifySettingsChannelId = getActiveChannelId();
  populateSpotifyChannelSelect();
  syncGeneralSettingsUI();
  fetchSpotifyConfig();
  fetchLibrespotStatus();
  fetchStatus();
  fetchUsersList();
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
if (spotifyChannelSelect) {
  spotifyChannelSelect.addEventListener('change', () => {
    spotifySettingsChannelId = spotifyChannelSelect.value || null;
    fetchSpotifyConfig();
    fetchLibrespotStatus();
  });
}
if (saveServerNameBtn) {
  saveServerNameBtn.addEventListener('click', saveServerName);
}
openSettingsBtn.addEventListener('click', openSettings);
closeSettingsBtn.addEventListener('click', closeSettings);
closeDiscoverBtn.addEventListener('click', closeDiscover);
if (logoutButton) {
  logoutButton.addEventListener('click', async () => {
    logoutButton.disabled = true;
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
    } catch (_) {
      /* ignore logout errors */
    } finally {
      logoutButton.disabled = false;
      stopDataPolling();
      resetUsersState();
      await refreshAuthState();
    }
  });
}
if (loginForm) {
  const submitBtn = loginForm.querySelector('button[type="submit"]');
  loginForm.addEventListener('submit', async evt => {
    evt.preventDefault();
    const username = loginUsername?.value.trim();
    const password = loginPassword?.value || '';
    if (!username || !password) {
      setInlineMessage(loginError, 'Enter username and password.');
      return;
    }
    setInlineMessage(loginError, '');
    try {
      if (submitBtn) submitBtn.disabled = true;
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      await ensureOk(res);
      await refreshAuthState();
      loginPassword.value = '';
    } catch (err) {
      setInlineMessage(loginError, err.message);
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  });
}
if (onboardingForm) {
  const submitBtn = onboardingForm.querySelector('button[type="submit"]');
  onboardingForm.addEventListener('submit', async evt => {
    evt.preventDefault();
    const serverName = onboardingServerName?.value.trim() || 'RoomCast';
    const username = onboardingUsername?.value.trim();
    const password = onboardingPassword?.value || '';
    if (!serverName || !username || !password) {
      setInlineMessage(onboardingError, 'All fields are required.');
      return;
    }
    setInlineMessage(onboardingError, '');
    try {
      if (submitBtn) submitBtn.disabled = true;
      const res = await fetch('/api/auth/initialize', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ server_name: serverName, username, password }),
      });
      await ensureOk(res);
      onboardingPassword.value = '';
      await refreshAuthState();
    } catch (err) {
      setInlineMessage(onboardingError, err.message);
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  });
}
if (addUserForm) {
  const submitBtn = addUserForm.querySelector('button[type="submit"]');
  addUserForm.addEventListener('submit', async evt => {
    evt.preventDefault();
    if (!isAdminUser()) {
      showError('Only admins can add users.');
      return;
    }
    const username = newUserUsername?.value.trim();
    const password = newUserPassword?.value || '';
    const role = newUserRole?.value || 'member';
    if (!username || !password) {
      showError('Enter username and password for the new user.');
      return;
    }
    try {
      if (submitBtn) submitBtn.disabled = true;
      const res = await fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password, role }),
      });
      await ensureOk(res);
      newUserUsername.value = '';
      newUserPassword.value = '';
      newUserRole.value = 'member';
      usersLoaded = false;
      showSuccess('User created.');
      fetchUsersList(true);
    } catch (err) {
      showError(`Failed to create user: ${err.message}`);
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
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
if (addControllerNodeBtn) {
  addControllerNodeBtn.addEventListener('click', async () => {
    setAddNodeMenuOpen(false);
    await registerControllerNode(addControllerNodeBtn);
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
if (radioOverlay) {
  radioOverlay.addEventListener('click', evt => {
    if (evt.target === radioOverlay) closeRadioOverlay();
  });
}
if (radioCloseBtn) {
  radioCloseBtn.addEventListener('click', closeRadioOverlay);
}
radioTabs.forEach(btn => {
  if (!btn?.dataset?.radioTab) return;
  btn.addEventListener('click', () => {
    setRadioActiveTab(btn.dataset.radioTab, { focusSearch: btn.dataset.radioTab === 'search' });
  });
});
radioTopButtons.forEach(btn => {
  if (!btn?.dataset?.radioTopMetric) return;
  btn.addEventListener('click', () => {
    loadRadioTopStations(btn.dataset.radioTopMetric);
  });
});
if (radioCountrySelect) {
  radioCountrySelect.addEventListener('change', handleRadioCountryChange);
}
if (radioSearchForm) {
  radioSearchForm.addEventListener('submit', evt => {
    evt.preventDefault();
    const query = (radioSearchInput?.value || '').trim();
    if (!query) {
      setRadioResultsStatus('Enter a keyword to search stations.');
      if (radioResultsList) radioResultsList.innerHTML = '';
      return;
    }
    runRadioSearch({ query }, { context: `search “${query}”` });
  });
}
if (playerPlaylistsBtn) playerPlaylistsBtn.setAttribute('aria-expanded', 'false');
if (playerPlaylistsBtn) playerPlaylistsBtn.addEventListener('click', handlePlayerPlaylistsClick);
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
  playerSearchBtn.addEventListener('click', handlePlayerSearchClick);
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

if (playerPlay) {
  playerPlay.addEventListener('pointerdown', event => {
    if (playerPlay.disabled) return;
    if (typeof event.button === 'number' && event.button !== 0) return;
    armPlayerHoldToStop();
  });
  playerPlay.addEventListener('pointerup', () => {
    cancelPlayerHoldToStop();
  });
  playerPlay.addEventListener('pointerleave', () => {
    cancelPlayerHoldToStop({ resetHold: true });
  });
  playerPlay.addEventListener('pointercancel', () => {
    cancelPlayerHoldToStop({ resetHold: true });
  });
  playerPlay.addEventListener('blur', () => {
    cancelPlayerHoldToStop({ resetHold: true });
  });
  playerPlay.addEventListener('keydown', event => {
    if (playerPlay.disabled) return;
    if (event.repeat) return;
    if (event.code === 'Space' || event.code === 'Enter') {
      armPlayerHoldToStop();
    }
  });
  playerPlay.addEventListener('keyup', event => {
    if (event.code === 'Space' || event.code === 'Enter') {
      cancelPlayerHoldToStop();
    }
  });
}

playerPrev.addEventListener('click', () => {
  if (playerPrev.disabled) return;
  playerAction('/api/spotify/player/previous');
});
playerPlay.addEventListener('click', async () => {
  cancelPlayerHoldToStop();
  if (playerPlay.disabled) return;
  if (playerPlayHoldTriggered) {
    playerPlayHoldTriggered = false;
    return;
  }
  const channel = getActiveChannel();
  if (isRadioChannel(channel)) {
    handleRadioPlayToggle(channel);
    return;
  }
  if (playerStatus?.is_playing) playerAction('/api/spotify/player/pause');
  else {
    const resumePayload = playerStatus?.allowResume ? buildPlayerResumePayload() : null;
    await prepareRoomcastDeviceForResume(resumePayload);
    playerAction('/api/spotify/player/play', resumePayload || undefined);
  }
});
playerNext.addEventListener('click', () => {
  if (playerNext.disabled) return;
  playerAction('/api/spotify/player/next');
});
playerSeek.addEventListener('input', () => {
  const value = Number(playerSeek.value);
  const safeValue = Number.isFinite(value) ? value : 0;
  setRangeProgress(playerSeek, safeValue, playerSeek.max || 1);
  playerTimeCurrent.textContent = msToTime(safeValue);
});
playerSeek.addEventListener('change', () => {
  const parsed = Number(playerSeek.value);
  const targetPosition = Number.isFinite(parsed) ? parsed : 0;
  noteOptimisticSeek(targetPosition);
  playerAction('/api/spotify/player/seek', { position_ms: targetPosition });
});
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

bootstrapAuth();

if (document.readyState === 'complete' || document.readyState === 'interactive') {
  registerRoomcastServiceWorker();
} else {
  window.addEventListener('load', registerRoomcastServiceWorker, { once: true });
}
