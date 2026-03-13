/* ── State ── */
let currentUnit = 'cm';
let history = JSON.parse(localStorage.getItem('calc_history') || '[]');
let netlifyIdentity = window.netlifyIdentity;

/* ── Bootstrap ── */
document.addEventListener('DOMContentLoaded', () => {
  initAuth();
});

/* ═══════════════════════════════════════════
   AUTH
═══════════════════════════════════════════ */
function initAuth() {
  // Check if Netlify Identity is available (deployed on Netlify)
  if (netlifyIdentity) {
    netlifyIdentity.on('init', (user) => {
      hideLoading();
      if (user) {
        showApp(user);
      } else {
        showAuth();
      }
    });

    netlifyIdentity.on('login', (user) => {
      netlifyIdentity.close();
      showApp(user);
    });

    netlifyIdentity.on('logout', () => {
      showCalculator(false);
      showAuth();
    });

    netlifyIdentity.init();
  } else {
    // Fallback: local auth for development/preview without Netlify Identity
    hideLoading();
    const user = getLocalUser();
    if (user) {
      showApp(user);
    } else {
      showAuth();
    }
  }
}

/* ── Local Auth Fallback (dev mode) ── */
function getLocalUser() {
  try {
    const data = sessionStorage.getItem('local_user');
    return data ? JSON.parse(data) : null;
  } catch { return null; }
}

function setLocalUser(user) {
  sessionStorage.setItem('local_user', JSON.stringify(user));
}

function clearLocalUser() {
  sessionStorage.removeItem('local_user');
}

function getLocalUsers() {
  try {
    return JSON.parse(localStorage.getItem('local_users') || '{}');
  } catch { return {}; }
}

function saveLocalUsers(users) {
  localStorage.setItem('local_users', JSON.stringify(users));
}

/* ── Tab switching ── */
function showTab(tab) {
  const loginForm = document.getElementById('login-form');
  const registerForm = document.getElementById('register-form');
  const loginTab = document.getElementById('login-tab');
  const registerTab = document.getElementById('register-tab');

  clearMessages();

  if (tab === 'login') {
    loginForm.classList.remove('hidden');
    registerForm.classList.add('hidden');
    loginTab.classList.add('active');
    registerTab.classList.remove('active');
  } else {
    loginForm.classList.add('hidden');
    registerForm.classList.remove('hidden');
    loginTab.classList.remove('active');
    registerTab.classList.add('active');
  }
}

function clearMessages() {
  ['login-error', 'register-error', 'register-success'].forEach(id => {
    const el = document.getElementById(id);
    el.classList.add('hidden');
    el.textContent = '';
  });
}

/* ── Login ── */
async function handleLogin(e) {
  e.preventDefault();
  clearMessages();

  const email = document.getElementById('login-email').value.trim();
  const password = document.getElementById('login-password').value;

  setButtonLoading('login-btn', true);

  try {
    if (netlifyIdentity) {
      // Use Netlify Identity
      await netlifyIdentity.login({ email, password });
    } else {
      // Local fallback
      await delay(400);
      const users = getLocalUsers();
      const user = users[email];

      if (!user || user.password !== hashSimple(password)) {
        throw new Error('Email o password non corretti.');
      }

      setLocalUser({ email, name: user.name });
      showApp({ email, user_metadata: { full_name: user.name } });
    }
  } catch (err) {
    showError('login-error', err.message || 'Errore durante il login. Riprova.');
  } finally {
    setButtonLoading('login-btn', false);
  }
}

/* ── Register ── */
async function handleRegister(e) {
  e.preventDefault();
  clearMessages();

  const name = document.getElementById('register-name').value.trim();
  const email = document.getElementById('register-email').value.trim();
  const password = document.getElementById('register-password').value;
  const confirm = document.getElementById('register-confirm').value;

  if (password !== confirm) {
    showError('register-error', 'Le password non coincidono.');
    return;
  }

  if (password.length < 8) {
    showError('register-error', 'La password deve avere almeno 8 caratteri.');
    return;
  }

  setButtonLoading('register-btn', true);

  try {
    if (netlifyIdentity) {
      // Use Netlify Identity
      await netlifyIdentity.signup({ email, password, data: { full_name: name } });
      showSuccess('register-success', 'Registrazione completata! Controlla la tua email per confermare l\'account, poi accedi.');
      document.getElementById('register-form').reset();
    } else {
      // Local fallback
      await delay(400);
      const users = getLocalUsers();

      if (users[email]) {
        throw new Error('Email già registrata. Prova ad accedere.');
      }

      users[email] = { name, password: hashSimple(password) };
      saveLocalUsers(users);

      showSuccess('register-success', 'Registrazione completata! Ora puoi accedere.');
      document.getElementById('register-form').reset();
      setTimeout(() => showTab('login'), 1800);
    }
  } catch (err) {
    showError('register-error', err.message || 'Errore durante la registrazione. Riprova.');
  } finally {
    setButtonLoading('register-btn', false);
  }
}

/* ── Logout ── */
function handleLogout() {
  if (netlifyIdentity) {
    netlifyIdentity.logout();
  } else {
    clearLocalUser();
    showCalculator(false);
    showAuth();
  }
}

/* ── Show/Hide sections ── */
function showAuth() {
  document.getElementById('auth-section').classList.remove('hidden');
  document.getElementById('app-section').classList.add('hidden');
}

function showApp(user) {
  document.getElementById('auth-section').classList.add('hidden');
  document.getElementById('app-section').classList.remove('hidden');

  // Set user name in topbar
  const name = (user.user_metadata && user.user_metadata.full_name) ||
               user.name ||
               user.email ||
               'Utente';
  document.getElementById('user-name').textContent = name;

  // Load history for this user
  const userKey = 'calc_history_' + (user.email || 'local');
  history = JSON.parse(localStorage.getItem(userKey) || '[]');
  renderHistory();
}

function showCalculator(show) {
  // used for local fallback logout
}

function hideLoading() {
  document.getElementById('loading-overlay').classList.add('hidden');
}

/* ═══════════════════════════════════════════
   CALCULATOR
═══════════════════════════════════════════ */
function handleCalculate(e) {
  e.preventDefault();

  const width = parseFloat(document.getElementById('width').value);
  const height = parseFloat(document.getElementById('height').value);

  if (isNaN(width) || isNaN(height) || width <= 0 || height <= 0) {
    return;
  }

  const area = width * height;
  const perimeter = 2 * (width + height);

  // Show result
  const resultBox = document.getElementById('result-box');
  const resultValue = document.getElementById('result-value');
  const resultDetails = document.getElementById('result-details');

  resultValue.textContent = formatNumber(area) + ' ' + currentUnit + '²';
  resultDetails.textContent =
    `Larghezza: ${formatNumber(width)} ${currentUnit}  •  Altezza: ${formatNumber(height)} ${currentUnit}  •  Perimetro: ${formatNumber(perimeter)} ${currentUnit}`;

  resultBox.classList.remove('hidden');

  // Save to history
  addToHistory(width, height, area, currentUnit);
}

function addToHistory(w, h, area, unit) {
  const user = netlifyIdentity ? netlifyIdentity.currentUser() : getLocalUser();
  const userKey = 'calc_history_' + ((user && (user.email || 'local')) || 'local');

  const entry = {
    w, h, area, unit,
    time: new Date().toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' })
  };

  history.unshift(entry);
  if (history.length > 10) history.pop(); // keep last 10

  localStorage.setItem(userKey, JSON.stringify(history));
  renderHistory();
}

function renderHistory() {
  const list = document.getElementById('history-list');
  const clearBtn = document.getElementById('clear-history-btn');

  if (history.length === 0) {
    list.innerHTML = '<p class="empty-history">Nessun calcolo ancora.</p>';
    clearBtn.style.display = 'none';
    return;
  }

  clearBtn.style.display = 'block';

  list.innerHTML = history.map(entry => `
    <div class="history-item">
      <div class="history-item-dims">
        ${formatNumber(entry.w)} × ${formatNumber(entry.h)} ${entry.unit} &nbsp;·&nbsp; ${entry.time}
      </div>
      <div class="history-item-area">${formatNumber(entry.area)} ${entry.unit}²</div>
    </div>
  `).join('');
}

function clearHistory() {
  history = [];
  const user = netlifyIdentity ? netlifyIdentity.currentUser() : getLocalUser();
  const userKey = 'calc_history_' + ((user && (user.email || 'local')) || 'local');
  localStorage.removeItem(userKey);
  renderHistory();
}

/* ── Rectangle Preview ── */
function updatePreview() {
  const width = parseFloat(document.getElementById('width').value) || 0;
  const height = parseFloat(document.getElementById('height').value) || 0;

  const maxW = 200, maxH = 160, minW = 40, minH = 40;
  const max = Math.max(width, height, 1);

  const w = Math.max(minW, Math.min(maxW, (width / max) * maxW));
  const h = Math.max(minH, Math.min(maxH, (height / max) * maxH));

  const rect = document.getElementById('rect-visual');
  rect.style.width = w + 'px';
  rect.style.height = h + 'px';

  document.getElementById('label-width').textContent =
    width > 0 ? formatNumber(width) + ' ' + currentUnit : '—';
  document.getElementById('label-height').textContent =
    height > 0 ? formatNumber(height) + ' ' + currentUnit : '—';
}

/* ── Unit selector ── */
function setUnit(btn, unit) {
  currentUnit = unit;
  document.querySelectorAll('.unit-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  // Update displayed units in inputs
  document.querySelectorAll('.input-with-unit .unit').forEach(el => {
    el.textContent = unit;
  });

  updatePreview();

  // Hide result so user recalculates with new unit
  document.getElementById('result-box').classList.add('hidden');
}

/* ═══════════════════════════════════════════
   UTILITIES
═══════════════════════════════════════════ */
function formatNumber(n) {
  if (Number.isInteger(n)) return n.toString();
  // Up to 4 decimal places, strip trailing zeros
  return parseFloat(n.toFixed(4)).toString();
}

function showError(id, msg) {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.classList.remove('hidden');
}

function showSuccess(id, msg) {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.classList.remove('hidden');
}

function setButtonLoading(btnId, loading) {
  const btn = document.getElementById(btnId);
  const text = btn.querySelector('.btn-text');
  const loader = btn.querySelector('.btn-loader');
  btn.disabled = loading;
  text.classList.toggle('hidden', loading);
  loader.classList.toggle('hidden', !loading);
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Very simple (non-cryptographic) hash for local-only dev fallback
function hashSimple(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const chr = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + chr;
    hash |= 0;
  }
  return hash.toString(16);
}
