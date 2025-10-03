const EOT_TOKENS = ["<|eot_id|>", "</s>"];

function stripEOT(text) {
  let out = text || '';
  for (const token of EOT_TOKENS) {
    if (token) out = out.split(token).join('');
  }
  return out;
}

function createSentenceMux(stableMs = 250) {
  let carry = '';
  let pending = '';
  let timer = null;
  const SENT_RE = /([\s\S]*?)(?:([.!?;:](?:["')\]]+)*)\s+|(\n{2,}|\r?\n[-*•]\s+)|((?:\p{Extended_Pictographic}|\p{Emoji_Presentation})+\s+))/u;

  function arm(emit) {
    clear();
    timer = setTimeout(() => {
      if (pending) {
        emit(pending);
        pending = '';
      }
    }, stableMs);
  }

  function clear() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  return {
    push(delta, emit) {
      if (!delta) return;
      carry += delta;
      const produced = [];
      let guard = 0;
      while (guard++ < 1000) {
        const match = SENT_RE.exec(carry);
        if (!match) break;
        const head = (match[1] || '').trim();
        const punctuation = (match[2] || '').trim();
        const bullet = (match[3] || '').trim();
        const emoji = (match[4] || '').trim();
        const boundary = punctuation || bullet || emoji;
        if (!boundary) break;
        const sentence = (head + (punctuation ? punctuation : '')).trim();
        const cut = match.index + (match[0] || '').length;
        carry = carry.slice(cut);
        if (sentence) produced.push(sentence);
      }
      if (produced.length) {
        if (pending) emit(pending);
        for (let i = 0; i < produced.length - 1; i++) emit(produced[i]);
        pending = produced[produced.length - 1];
        arm(emit);
        return;
      }
      if (pending && /\S/.test(carry)) {
        emit(pending);
        pending = '';
        clear();
        return;
      }
      if (pending) arm(emit);
    },
    flush(emit) {
      clear();
      if (pending) {
        emit(pending);
        pending = '';
      }
      const tail = carry.trim();
      if (tail) emit(tail);
      carry = '';
    }
  };
}

function makeNdjsonPump(onLine) {
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  let inString = false;
  let escaped = false;
  let depth = 0;

  function feed(text) {
    buffer += text;
    let start = 0;
    for (let i = 0; i < buffer.length; i++) {
      const ch = buffer[i];
      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }
        if (ch === '\\') {
          escaped = true;
          continue;
        }
        if (ch === '"') {
          inString = false;
        }
        continue;
      }
      if (ch === '"') {
        inString = true;
        continue;
      }
      if (ch === '{') {
        depth++;
        continue;
      }
      if (ch === '}') {
        depth--;
        if (depth === 0) {
          let slice = buffer.slice(start, i + 1).trim();
          if (slice.startsWith('data:')) slice = slice.slice(5).trim();
          if (slice && slice !== '[DONE]') onLine(slice);
          start = i + 1;
        }
        continue;
      }
      if (ch === '\n') {
        const line = buffer.slice(start, i).trim();
        if (line === '[DONE]' || line === 'data: [DONE]') start = i + 1;
      }
    }
    buffer = buffer.slice(start);
  }

  return {
    push(chunk) {
      const str = (chunk instanceof Uint8Array || chunk instanceof ArrayBuffer)
        ? decoder.decode(chunk, { stream: true })
        : String(chunk);
      feed(str);
    },
    flush() {
      const tail = buffer.trim();
      if (tail && depth === 0) {
        let slice = tail;
        if (slice.startsWith('data:')) slice = slice.slice(5).trim();
        if (slice && slice !== '[DONE]') onLine(slice);
      }
      buffer = '';
      inString = false;
      escaped = false;
      depth = 0;
    }
  };
}

export { createSentenceMux, stripEOT, makeNdjsonPump };
