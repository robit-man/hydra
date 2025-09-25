import { qs } from './utils.js';

function makeTransportButtonUpdater({ CFG, Net }) {
  return function updateTransportButton() {
    const btn = qs('#transportToggle');
    if (!btn) return;
    btn.innerHTML = '';

    const dot = document.createElement('span');
    dot.className = 'dot';
    const label = document.createElement('span');

    if (CFG.transport === 'nkn') {
      btn.classList.add('active');
      label.textContent = 'NKN';
      if (Net?.nkn?.ready) {
        dot.classList.add('ok');
        btn.title = Net.nkn.addr || 'NKN relay address';
      } else {
        dot.classList.add(Net?.nkn?.client ? 'warn' : 'err');
        btn.title = 'NKN relay connecting';
      }
    } else {
      btn.classList.remove('active');
      label.textContent = 'HTTP';
      dot.classList.add('err');
      btn.title = 'HTTP transport';
    }

    btn.append(dot, label);
  };
}

export { makeTransportButtonUpdater };
