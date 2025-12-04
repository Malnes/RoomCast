async function fetchNodes(options = {}) {
  if (!isAuthenticated()) return;
  try {
    const res = await fetch('/api/nodes');
    await ensureOk(res);
    const data = await res.json();
    renderNodes(data.nodes || [], options);
  } catch (err) {
    showError(`Failed to load nodes: ${err.message}`);
  }
}

async function fetchSpotifyConfig(targetChannelId = getSettingsChannelId()) {
  if (!targetChannelId) {
    if (spotifyLinkStatus) {
      spotifyLinkStatus.textContent = 'No channels available';
      spotifyLinkStatus.className = 'status-pill warn';
    }
    return;
  }
  try {
    const res = await fetch(withChannel('/api/config/spotify', targetChannelId));
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

async function fetchLibrespotStatus(targetChannelId = getSettingsChannelId()) {
  if (!targetChannelId) {
    if (librespotStatus) librespotStatus.innerText = 'Status: no channel selected';
    return;
  }
  try {
    const res = await fetch(withChannel('/api/librespot/status', targetChannelId));
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

async function setNodeChannel(nodeId, channelId, selectEl, dotEl) {
  if (!channelId) return;
  const previous = selectEl?.dataset?.previousChannel || null;
  if (previous && previous === channelId) return;
  if (selectEl) selectEl.disabled = true;
  try {
    const res = await fetch(`/api/nodes/${nodeId}/channel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_id: channelId }),
    });
    await ensureOk(res);
    showSuccess('Channel updated');
    if (selectEl) {
      selectEl.dataset.previousChannel = channelId;
    }
    updateChannelDotColor(dotEl, channelId);
    await fetchNodes({ force: true });
  } catch (err) {
    showError(`Failed to update channel: ${err.message}`);
    if (selectEl && previous) {
      selectEl.value = previous;
      selectEl.dataset.previousChannel = previous;
    }
    if (dotEl) updateChannelDotColor(dotEl, previous);
    throw err;
  } finally {
    if (selectEl) selectEl.disabled = false;
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

async function openNodeTerminal(nodeId, btn) {
  const originalLabel = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Opening terminal…';
  }
  try {
    const res = await fetch(`/api/nodes/${nodeId}/terminal-session`, { method: 'POST' });
    await ensureOk(res);
    const data = await res.json();
    const token = data?.token;
    if (!token) throw new Error('No terminal token returned');
    const targetUrl = data?.page_url || `/static/terminal.html?token=${encodeURIComponent(token)}`;
    const finalUrl = new URL(targetUrl, window.location.origin);
    const opened = window.open(finalUrl.toString(), '_blank', 'noopener');
    if (!opened) {
      showError('Pop-up blocked. Allow pop-ups for RoomCast to open the terminal.');
    }
  } catch (err) {
    showError(`Failed to open terminal: ${err.message}`);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalLabel || 'Open terminal';
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
  limitsTitle.textContent = 'Volume limit';
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

    const terminalBtn = createNodeSettingsAction('Open terminal', (btn) => openNodeTerminal(node.id, btn), { secondary: true, disabled: restarting || updating || !online });
    actionStack.appendChild(terminalBtn);

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

function rememberPlayerSnapshot(status) {
  const item = status?.item;
  if (!item || (!item.name && !item.uri)) return;
  lastPlayerSnapshot = {
    item,
    context: status?.context || null,
    capturedAt: Date.now(),
    shuffle_state: status?.shuffle_state ?? false,
    repeat_state: status?.repeat_state ?? 'off',
  };
}

function hydrateServerSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return null;
  const item = snapshot.item;
  if (!item || (!item.name && !item.uri)) return null;
  const captured = typeof snapshot.captured_at === 'number'
    ? snapshot.captured_at * 1000
    : Date.now();
  lastPlayerSnapshot = {
    item,
    context: snapshot.context || null,
    capturedAt: captured,
    shuffle_state: snapshot.shuffle_state ?? false,
    repeat_state: snapshot.repeat_state ?? 'off',
  };
  return lastPlayerSnapshot;
}

function getPlayerSnapshot() {
  if (!lastPlayerSnapshot) return null;
  if (Date.now() - lastPlayerSnapshot.capturedAt > PLAYER_SNAPSHOT_TTL_MS) {
    lastPlayerSnapshot = null;
    return null;
  }
  return lastPlayerSnapshot;
}

function buildPlayerResumePayload() {
  const snapshot = getPlayerSnapshot();
  if (!snapshot) return null;
  const contextUri = snapshot.context?.uri;
  const trackUri = snapshot.item?.uri;
  if (contextUri) {
    const payload = { context_uri: contextUri };
    if (trackUri) payload.offset = { uri: trackUri };
    return payload;
  }
  if (trackUri) return { uris: [trackUri] };
  return null;
}

