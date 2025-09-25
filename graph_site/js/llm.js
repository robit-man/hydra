function createLLM({
  getNode,
  NodeStore,
  Router,
  Net,
  CFG,
  createSentenceMux,
  makeNdjsonPump,
  stripEOT,
  log
}) {
  async function onPrompt(nodeId, payload) {
    const node = getNode(nodeId);
    if (!node) return;
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec.config || {};
    const base = (cfg.base || '').trim();
    const api = (cfg.api || '').trim();
    const relay = (cfg.relay || '').trim();
    const model = (cfg.model || '').trim();
    const viaNkn = CFG.transport === 'nkn';
    const stream = !!cfg.stream;
    const sysUse = !!cfg.useSystem;
    const sysTxt = (cfg.system || '').trim();
    const memOn = !!cfg.memoryOn;
    const persist = !!cfg.persistMemory;
    const maxTurns = Number.isFinite(cfg.maxTurns) ? cfg.maxTurns : 16;

    const text = String(payload && (payload.text ?? payload.prompt ?? payload) || '');
    const memory = Array.isArray(cfg.memory) ? cfg.memory.slice() : [];

    function buildMessages(latest) {
      const msgs = [];
      let startIdx = 0;
      if (sysUse && sysTxt) msgs.push({ role: 'system', content: sysTxt });
      if (memOn) {
        if (memory.length && memory[0].role === 'system') {
          if (!msgs.length) msgs.push(memory[0]);
          startIdx = 1;
        }
        for (let i = startIdx; i < memory.length; i++) msgs.push(memory[i]);
      }
      if (latest?.trim()) msgs.push({ role: 'user', content: latest.trim() });
      if (memOn) {
        let userCount = msgs.filter((m) => m.role === 'user').length;
        while (userCount > maxTurns) {
          const sysIndex = msgs[0]?.role === 'system' ? 1 : 0;
          const userIdx = msgs.findIndex((m, idx) => idx >= sysIndex && m.role === 'user');
          if (userIdx < 0) break;
          msgs.splice(userIdx, 1);
          if (msgs[userIdx]?.role === 'assistant') msgs.splice(userIdx, 1);
          userCount--;
        }
      }
      return msgs;
    }

    const messages = buildMessages(text);
    const mux = createSentenceMux(250);
    let full = '';

    const outEl = node.el?.querySelector('[data-llm-out]');
    let uiLines = [];
    let uiBuf = '';
    const setOut = (value) => {
      if (!outEl) return;
      outEl.textContent = value || '';
      outEl.scrollTop = outEl.scrollHeight;
    };
    setOut('');
    uiLines = [];
    uiBuf = '';

    const emitSentenceFinal = (s) => {
      Router.sendFrom(nodeId, 'final', { nodeId, text: s, eos: true });
    };
    const emitSentenceDelta = (s) => {
      Router.sendFrom(nodeId, 'delta', { nodeId, type: 'text', text: s, eos: true });
    };
    const emitBoth = (s) => {
      emitSentenceDelta(s);
      emitSentenceFinal(s);
      const line = String(s || '').trim();
      if (line) {
        uiLines.push(line);
        uiBuf = '';
        setOut(uiLines.join('\n'));
      }
    };

    try {
      if (stream) {
        const pump = makeNdjsonPump((line) => {
          try {
            const obj = JSON.parse(line);
            let delta =
              (obj.message && typeof obj.message.content === 'string' && obj.message.content) ||
              (typeof obj.response === 'string' && obj.response) ||
              (typeof obj.delta === 'string' && obj.delta) || '';
            if (!delta && (obj.done || obj.complete) && typeof obj.final === 'string') delta = obj.final;
            if (!delta && (obj.done || obj.complete) && obj.message && typeof obj.message.content === 'string') delta = obj.message.content;
            if (delta) {
              const clean = stripEOT(delta);
              full += clean;
              uiBuf += clean;
              setOut(uiLines.length ? `${uiLines.join('\n')}\n${uiBuf}` : uiBuf);
              mux.push(clean, emitBoth);
            }
          } catch (err) {
            // ignore parse errors per chunk
          }
        });

        if (viaNkn) {
          let expected = null;
          const stash = new Map();
          const seen = new Set();
          const flushReorder = () => {
            while (expected != null && stash.has(expected)) {
              pump.push(`${stash.get(expected)}\n`);
              stash.delete(expected);
              expected++;
            }
          };
          await Net.nknStream(
            {
              url: base.replace(/\/+$/, '') + '/api/chat',
              method: 'POST',
              headers: Net.auth({ 'X-Relay-Stream': 'chunks', Accept: 'application/x-ndjson' }, api),
              json: { model, messages, stream: true },
              timeout_ms: 180000
            },
            relay,
            {
              onBegin: () => {},
              onLine: (line, seqRaw) => {
                const seq = seqRaw | 0;
                if (seen.has(seq)) return;
                seen.add(seq);
                if (expected == null) expected = seq;
                if (seq === expected) {
                  pump.push(`${line}\n`);
                  expected++;
                  flushReorder();
                } else if (seq > expected) {
                  stash.set(seq, line);
                }
              },
              onChunk: (bytes) => pump.push(bytes),
              onEnd: () => {
                flushReorder();
                pump.flush();
                mux.flush(emitBoth);
              }
            },
            180000
          );
        } else {
          const res = await fetch(base.replace(/\/+$/, '') + '/api/chat', {
            method: 'POST',
            headers: Net.auth({ Accept: 'application/x-ndjson', 'Content-Type': 'application/json' }, api),
            body: JSON.stringify({ model, messages, stream: true })
          });
          if (!res.ok || !res.body) throw new Error(`${res.status} ${res.statusText}`);
          const reader = res.body.getReader();
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            if (value && value.byteLength) pump.push(value);
          }
          pump.flush();
          mux.flush(emitBoth);
        }
      } else {
        const data = await Net.postJSON(base, '/api/chat', { model, messages, stream: false }, api, viaNkn, relay, 120000);
        full = stripEOT((data?.message?.content) || (data?.response) || '') || '';
        if (full) {
          uiLines = [full.trim()];
          uiBuf = '';
          setOut(uiLines.join('\n'));
          mux.push(`${full}\n`, emitBoth);
          mux.flush(emitBoth);
        }
      }

      if (cfg.memoryOn) {
        const newMemory = (memory || []).slice();
        if (sysUse && sysTxt && !(newMemory.length && newMemory[0].role === 'system')) {
          newMemory.unshift({ role: 'system', content: sysTxt });
        }
        if (text?.trim()) newMemory.push({ role: 'user', content: text.trim() });
        const finalText = full.trim();
        if (finalText) newMemory.push({ role: 'assistant', content: finalText });
        let pairs = 0;
        const pruned = [];
        let i = newMemory.length && newMemory[0].role === 'system' ? 1 : 0;
        for (let k = newMemory.length - 1; k >= i; k--) {
          pruned.push(newMemory[k]);
          if (newMemory[k].role === 'user') {
            pairs++;
            if (pairs >= maxTurns) break;
          }
        }
        pruned.reverse();
        const out = newMemory[0] && newMemory[0].role === 'system' ? [newMemory[0], ...pruned] : pruned;
        NodeStore.update(nodeId, { memory: out });
        Router.sendFrom(nodeId, 'memory', { type: 'updated', size: out.length });
      }
    } catch (err) {
      log(`[llm ${nodeId}] ${err.message}`);
    }
  }

  function onSystem(nodeId, payload) {
    const text = String(payload && (payload.text ?? payload.prompt ?? payload) || '').trim();
    NodeStore.update(nodeId, { system: text, useSystem: true });
  }

  return { onPrompt, onSystem };
}

export { createLLM };
