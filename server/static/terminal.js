const termContainer = document.getElementById('terminal');
const statusEl = document.getElementById('status');
const params = new URLSearchParams(window.location.search);
const token = params.get('token');

function setStatus(message, isError = false) {
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.classList.toggle('error', !!isError);
}

if (!token) {
  setStatus('Missing terminal token.', true);
  throw new Error('Missing terminal token');
}

const term = new window.Terminal({
  convertEol: true,
  cursorBlink: true,
  fontFamily: 'JetBrains Mono, SFMono-Regular, Menlo, monospace',
  fontSize: 14,
  theme: {
    background: '#020617',
    foreground: '#f8fafc',
    cursor: '#22d3ee',
    black: '#0f172a',
    red: '#f87171',
    green: '#4ade80',
    yellow: '#facc15',
    blue: '#60a5fa',
    magenta: '#f472b6',
    cyan: '#22d3ee',
    white: '#e2e8f0',
  },
});
const fitAddon = new window.FitAddon.FitAddon();
term.loadAddon(fitAddon);
term.open(termContainer);

function fitTerminal() {
  try {
    fitAddon.fit();
  } catch (_) {
    /* ignore fit failures */
  }
}

fitTerminal();
term.focus();

const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
const wsUrl = `${wsProtocol}://${window.location.host}/ws/terminal/${encodeURIComponent(token)}`;
const socket = new WebSocket(wsUrl);

function sendMessage(payload) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

function sendResize() {
  fitTerminal();
  sendMessage({ type: 'resize', cols: term.cols, rows: term.rows });
}

socket.addEventListener('open', () => {
  setStatus('Connected');
  sendResize();
});

socket.addEventListener('message', (event) => {
  let payload;
  try {
    payload = JSON.parse(event.data);
  } catch (_) {
    return;
  }
  switch (payload.type) {
    case 'output':
      term.write(payload.data || '');
      break;
    case 'error':
      setStatus(payload.message || 'Terminal error', true);
      break;
    case 'exit':
      setStatus(`Session ended (code ${payload.code ?? '0'})`);
      break;
    default:
      break;
  }
});

socket.addEventListener('close', () => {
  setStatus('Disconnected');
});

socket.addEventListener('error', () => {
  setStatus('Unable to connect to terminal', true);
});

term.onData((data) => {
  sendMessage({ type: 'input', data });
});

window.addEventListener('resize', () => {
  clearTimeout(window.__terminalResizeTimer);
  window.__terminalResizeTimer = setTimeout(() => {
    sendResize();
  }, 150);
});

window.addEventListener('beforeunload', () => {
  sendMessage({ type: 'close' });
});
