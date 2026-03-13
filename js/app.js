/* ── State ── */
let currentUnit = 'cm';
let history = JSON.parse(localStorage.getItem('calc_history') || '[]');

/* ── Bootstrap ── */
document.addEventListener('DOMContentLoaded', () => {
  renderHistory();
});

/* ═══════════════════════════════════════════
   CALCULATOR
═══════════════════════════════════════════ */
function handleCalculate(e) {
  e.preventDefault();

  const width = parseFloat(document.getElementById('width').value);
  const height = parseFloat(document.getElementById('height').value);

  if (isNaN(width) || isNaN(height) || width <= 0 || height <= 0) return;

  const area = width * height;
  const perimeter = 2 * (width + height);

  document.getElementById('result-value').textContent = formatNumber(area) + ' ' + currentUnit + '²';
  document.getElementById('result-details').textContent =
    `Larghezza: ${formatNumber(width)} ${currentUnit}  •  Altezza: ${formatNumber(height)} ${currentUnit}  •  Perimetro: ${formatNumber(perimeter)} ${currentUnit}`;
  document.getElementById('result-box').classList.remove('hidden');

  addToHistory(width, height, area, currentUnit);
}

function addToHistory(w, h, area, unit) {
  const entry = {
    w, h, area, unit,
    time: new Date().toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' })
  };
  history.unshift(entry);
  if (history.length > 10) history.pop();
  localStorage.setItem('calc_history', JSON.stringify(history));
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
  localStorage.removeItem('calc_history');
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

  document.getElementById('rect-visual').style.width = w + 'px';
  document.getElementById('rect-visual').style.height = h + 'px';

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
  document.querySelectorAll('.input-with-unit .unit').forEach(el => el.textContent = unit);
  updatePreview();
  document.getElementById('result-box').classList.add('hidden');
}

/* ── Utilities ── */
function formatNumber(n) {
  if (Number.isInteger(n)) return n.toString();
  return parseFloat(n.toFixed(4)).toString();
}
