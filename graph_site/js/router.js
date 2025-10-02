import { CFG, saveCFG } from './config.js';
import { qs, qsa, log } from './utils.js';

const Router = {
  ports: new Map(),
  wires: CFG.wires.slice(),
  sel: null,
  onSend: null,

  register(key, el, onrecv) {
    this.ports.set(key, { el, onrecv });
    const fromTypeMatch = /^([a-z]+):([^:]+):(.+)$/.exec(key);
    if (fromTypeMatch) {
      this.ports.set(`${fromTypeMatch[2]}:in:${fromTypeMatch[3]}`, { el, onrecv });
    }
    const toMatch = /^([^:]+):in:(.+)$/.exec(key);
    if (toMatch && el) {
      const typeGuess = el.closest('.node')?.querySelector('.title')?.textContent?.trim()?.toLowerCase();
      if (typeGuess) this.ports.set(`${typeGuess}:${toMatch[1]}:${toMatch[2]}`, { el, onrecv });
    }
    if (!el) return;
    el.addEventListener('click', () => {
      if (!this.sel) {
        this.sel = key;
        el.classList.add('sel');
        return;
      }
      const from = this.sel;
      this.clearSel();
      if (from === key) return;
      if (!this.wires.find((w) => w.from === from && w.to === key)) {
        this.wires.push({ from, to: key });
        CFG.wires = this.wires.slice();
        saveCFG();
        this.render();
        log(`wired ${from} → ${key}`);
      }
    });
  },

  sendFrom(nodeId, portName, payload) {
    this.send(`${nodeId}:out:${portName}`, payload);
  },

  clearSel() {
    qsa('.wp-port.sel').forEach((el) => el.classList.remove('sel'));
    this.sel = null;
  },

  send(from, payload) {
    for (const w of this.wires) {
      if (w.from === from) {
        try {
          this.onSend?.(from, w.to, payload);
        } catch (err) {
          log(`wire hook error ${w.from}→${w.to}: ${err.message}`);
        }
        const port = this.ports.get(w.to);
        try {
          port && port.onrecv && port.onrecv(payload, from, w.to);
        } catch (err) {
          log(`wire error ${w.from}→${w.to}: ${err.message}`);
        }
      }
    }
  },

  render() {
    const box = qs('#wiresBox');
    if (!box) return;
    box.textContent = this.wires.length
      ? this.wires.map((w, idx) => `${idx + 1}. ${w.from} → ${w.to}`).join('\n')
      : '(no wires)';
  },

  clear() {
    this.wires.length = 0;
    CFG.wires = [];
    saveCFG();
    this.render();
  }
};

export { Router };
