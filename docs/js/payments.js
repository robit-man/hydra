const ERC20_ABI = [
  'function decimals() view returns (uint8)',
  'function symbol() view returns (string)',
  'function transfer(address to, uint256 value) returns (bool)',
  'event Transfer(address indexed from, address indexed to, uint256 value)'
];

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const DEFAULT_UNLOCK_TTL_MS = 5 * 60 * 1000;

let ethersPromise = null;

function loadEthers() {
  if (!ethersPromise) {
    ethersPromise = import('https://cdn.jsdelivr.net/npm/ethers@6.10.0/+esm');
  }
  return ethersPromise;
}

const nowMs = () => Date.now();
const nowSeconds = () => Math.floor(nowMs() / 1000);

function randomId(prefix = 'inv') {
  try {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
  } catch (_) {
    // ignore
  }
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeKey(value) {
  if (value == null) return null;
  let str = typeof value === 'string' ? value : String(value);
  try {
    str = str.normalize('NFKC');
  } catch (_) {
    // ignore unicode normalization failures
  }
  const trimmed = str.trim();
  return trimmed || null;
}

function resolveControlType(payload) {
  if (!payload || typeof payload !== 'object') return '';
  const fields = ['type', 'event', 'kind'];
  for (const key of fields) {
    const val = payload[key];
    if (typeof val === 'string' && val.toLowerCase().startsWith('payment.')) {
      return val.toLowerCase();
    }
  }
  return '';
}

function resolveRemoteKey(payload) {
  if (!payload || typeof payload !== 'object') return null;
  const meta = payload.meta || payload.__meta || payload.headers || {};
  const candidates = [
    meta.from,
    meta.sender,
    meta.address,
    payload.from,
    payload.sender,
    payload.source,
    payload.addr,
    payload.address,
    payload.peer,
    payload.target
  ];
  for (const candidate of candidates) {
    const normalized = normalizeKey(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function getUnlockTtlMs(config) {
  const ttl = Number(config?.unlockTTL);
  if (Number.isFinite(ttl) && ttl > 0) return ttl * 1000;
  return DEFAULT_UNLOCK_TTL_MS;
}

function isSellerMode(mode) {
  const text = String(mode || 'seller').toLowerCase();
  return text === 'seller' || text === 'both';
}

function isBuyerMode(mode) {
  const text = String(mode || 'seller').toLowerCase();
  return text === 'buyer' || text === 'both';
}

function shortenKey(key, length = 10) {
  const normalized = normalizeKey(key);
  if (!normalized) return '(unknown)';
  if (normalized.length <= length) return normalized;
  return `${normalized.slice(0, Math.max(3, Math.floor(length / 2)))}...${normalized.slice(-Math.max(3, Math.floor(length / 2)))}`;
}

function formatChain(chainId) {
  const num = Number(chainId);
  if (!Number.isFinite(num) || num <= 0) return 'unknown';
  return `chain ${num}`;
}

function createPayments({ Router, NodeStore, CFG, setBadge, log }) {
  const nodeState = new Map();

  function ensureState(nodeId) {
    if (!nodeState.has(nodeId)) {
      nodeState.set(nodeId, {
        queue: new Map(),
        unlocked: new Map(),
        pendingInvoices: new Map(),
        incomingInvoices: new Map(),
        remoteInvoices: new Map(),
        remoteIncoming: new Map(),
        requestedRemotes: new Set(),
        tokenMeta: new Map(),
        wallet: null,
        ui: null,
        mounted: false
      });
    }
    return nodeState.get(nodeId);
  }

  function getConfig(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'Payments');
    return rec?.config || {};
  }

  function emitEvent(nodeId, payload) {
    if (!payload || typeof payload !== 'object') return;
    try {
      Router.sendFrom(nodeId, 'events', {
        nodeId,
        ts: Date.now(),
        ...payload
      });
    } catch (err) {
      log?.(`[payments] event emit failed: ${err?.message || err}`);
    }
  }

  function isUnlocked(st, remoteKey, ttlMs) {
    if (!remoteKey) return true;
    const entry = st.unlocked.get(remoteKey);
    if (!entry) return false;
    if (Date.now() <= entry) return true;
    st.unlocked.delete(remoteKey);
    return false;
  }

  function markUnlocked(nodeId, remoteKey) {
    if (!remoteKey) return;
    const st = ensureState(nodeId);
    const cfg = getConfig(nodeId);
    const ttl = getUnlockTtlMs(cfg);
    st.unlocked.set(remoteKey, Date.now() + ttl);
  }

  function enqueuePayload(nodeId, remoteKey, portName, payload) {
    const st = ensureState(nodeId);
    const key = remoteKey || '__local__';
    const existing = st.queue.get(key) || [];
    existing.push({ port: portName, payload });
    st.queue.set(key, existing);
  }

  function flushQueue(nodeId, remoteKey) {
    const st = ensureState(nodeId);
    const key = remoteKey || '__local__';
    const queued = st.queue.get(key);
    if (!queued || !queued.length) return;
    st.queue.delete(key);
    queued.forEach((entry) => {
      try {
        Router.sendFrom(nodeId, 'output', entry.payload);
      } catch (err) {
        log?.(`[payments] forward failed: ${err?.message || err}`);
      }
    });
  }

  function updateSummary(nodeId) {
    const st = ensureState(nodeId);
    const ui = st.ui;
    if (!ui?.summary) return;
    const cfg = getConfig(nodeId);
    const modeText = String(cfg.mode || 'seller').toUpperCase();
    const asset = cfg.asset || ZERO_ADDRESS;
    const amount = cfg.amount || '1';
    const chain = cfg.chainId ? ` • ${formatChain(cfg.chainId)}` : '';
    ui.summary.textContent = `Mode: ${modeText} • ${amount} ${asset.slice(0, 8)}${chain}`;
  }

  function updateWalletStatus(nodeId) {
    const st = ensureState(nodeId);
    const ui = st.ui;
    if (!ui?.wallet) return;
    const wallet = st.wallet;
    if (!wallet) {
      ui.wallet.textContent = 'Wallet: not connected';
      return;
    }
    const shortAddr = `${wallet.address.slice(0, 6)}...${wallet.address.slice(-4)}`;
    ui.wallet.textContent = `Wallet: ${shortAddr} • ${formatChain(wallet.chainId)}`;
  }

  function renderInvoices(nodeId) {
    const st = ensureState(nodeId);
    const ui = st.ui;
    if (!ui?.invoices) return;
    const container = ui.invoices;
    container.innerHTML = '';

    const entries = [];
    st.pendingInvoices.forEach((record) => {
      entries.push({ role: 'seller', record });
    });
    st.incomingInvoices.forEach((record) => {
      entries.push({ role: 'buyer', record });
    });
    entries.sort((a, b) => {
      const aTs = a.record?.invoice?.createdAt || 0;
      const bTs = b.record?.invoice?.createdAt || 0;
      return bTs - aTs;
    });

    if (!entries.length) {
      const empty = document.createElement('div');
      empty.className = 'payment-invoice-empty';
      empty.textContent = 'No payment activity yet.';
      container.appendChild(empty);
      return;
    }

    entries.forEach(({ role, record }) => {
      const invoice = record?.invoice || {};
      const wrapper = document.createElement('div');
      wrapper.className = 'payment-invoice';
      if (record.status) wrapper.dataset.status = record.status;
      wrapper.dataset.invoiceId = invoice.id || '';
      wrapper.dataset.role = role;

      const title = document.createElement('div');
      title.className = 'payment-invoice-title';
      const remoteLabel =
        role === 'seller'
          ? `to ${shortenKey(record.remoteKey)}`
          : `from ${shortenKey(record.remoteKey)}`;
      title.textContent = `Invoice ${remoteLabel}`;
      wrapper.appendChild(title);

      const details = document.createElement('div');
      details.className = 'payment-invoice-details';
      const amount = invoice.amount || '0';
      const asset = (invoice.asset || '').slice(0, 8) || 'asset';
      details.textContent = `${amount} ${asset} • ${formatChain(invoice.chainId)}`;
      wrapper.appendChild(details);

      const status = document.createElement('div');
      status.className = 'payment-invoice-status';
      status.textContent = `Status: ${record.status || 'pending'}`;
      if (record.error) status.textContent += ` (${record.error})`;
      if (record.txHash) status.textContent += ` • tx ${record.txHash.slice(0, 10)}...`;
      wrapper.appendChild(status);

      if (role === 'buyer' && record.status === 'pending') {
        const actions = document.createElement('div');
        actions.className = 'payment-invoice-actions';
        const payBtn = document.createElement('button');
        payBtn.textContent = 'Pay now';
        payBtn.className = 'secondary';
        payBtn.dataset.paymentPay = invoice.id || '';
        actions.appendChild(payBtn);
        wrapper.appendChild(actions);
      }

      container.appendChild(wrapper);
    });
  }

  async function connectWallet(nodeId) {
    if (typeof window === 'undefined' || !window.ethereum) {
      setBadge?.('Payments: MetaMask provider not detected', false);
      throw new Error('MetaMask not available');
    }
    const st = ensureState(nodeId);
    const ethersMod = await loadEthers();
    const provider = new ethersMod.BrowserProvider(window.ethereum);
    await provider.send('eth_requestAccounts', []);
    const signer = await provider.getSigner();
    const address = await signer.getAddress();
    const network = await provider.getNetwork();
    st.wallet = {
      provider,
      signer,
      address,
      chainId: Number(network.chainId)
    };
    updateWalletStatus(nodeId);
    return st.wallet;
  }

  async function ensureWallet(nodeId) {
    const st = ensureState(nodeId);
    if (st.wallet) return st.wallet;
    return await connectWallet(nodeId);
  }

  async function ensureChain(nodeId, targetChainId) {
    if (!targetChainId) return;
    const st = ensureState(nodeId);
    const wallet = await ensureWallet(nodeId);
    const desired = `0x${Number(targetChainId).toString(16)}`;
    if (wallet.chainId === Number(targetChainId)) return;
    try {
      await wallet.provider.send('wallet_switchEthereumChain', [{ chainId: desired }]);
    } catch (switchErr) {
      // attempt to add chain if not recognized
      if (switchErr?.code === 4902) {
        try {
          await wallet.provider.send('wallet_addEthereumChain', [
            { chainId: desired, rpcUrls: [], nativeCurrency: { name: 'Native', symbol: 'ETH', decimals: 18 }, chainName: `Chain ${targetChainId}` }
          ]);
        } catch (addErr) {
          throw addErr;
        }
      } else {
        throw switchErr;
      }
    }
    const net = await wallet.provider.getNetwork();
    wallet.chainId = Number(net.chainId);
    updateWalletStatus(nodeId);
  }

  async function getTokenMeta(nodeId, asset) {
    const st = ensureState(nodeId);
    if (!asset || typeof asset !== 'string') throw new Error('Invalid token address');
    const lower = asset.toLowerCase();
    if (st.tokenMeta.has(lower)) return st.tokenMeta.get(lower);
    const wallet = await ensureWallet(nodeId);
    const ethersMod = await loadEthers();
    const contract = new ethersMod.Contract(asset, ERC20_ABI, wallet.signer || wallet.provider);
    let decimals = 6;
    let symbol = 'TOKEN';
    try {
      decimals = Number(await contract.decimals());
    } catch (_) {
      decimals = 18;
    }
    try {
      symbol = await contract.symbol();
    } catch (_) {
      symbol = 'TOKEN';
    }
    const meta = { decimals, symbol };
    st.tokenMeta.set(lower, meta);
    return meta;
  }

  async function sendPayment(nodeId, invoice) {
    if (!invoice) throw new Error('Missing invoice');
    const st = ensureState(nodeId);
    const wallet = await ensureWallet(nodeId);
    await ensureChain(nodeId, invoice.chainId || wallet.chainId);
    const amountStr = String(invoice.amount || '0');
    const asset = invoice.asset || ZERO_ADDRESS;
    if (!asset || asset === ZERO_ADDRESS) throw new Error('Unsupported asset');
    const meta = await getTokenMeta(nodeId, asset);
    const ethersMod = await loadEthers();
    const value = ethersMod.parseUnits(amountStr, meta.decimals);
    const contract = new ethersMod.Contract(asset, ERC20_ABI, wallet.signer);
    const tx = await contract.transfer(invoice.receiver, value);
    setBadge?.(`Payment submitted (${tx.hash.slice(0, 10)}...)`);
    const receipt = await tx.wait();
    const info = {
      type: 'payment.receipt',
      invoiceId: invoice.id,
      txHash: receipt.hash,
      chainId: wallet.chainId,
      asset,
      amount: amountStr,
      from: wallet.address,
      to: invoice.receiver,
      target: invoice.to,
      graphId: CFG.graphId || '',
      ts: Date.now()
    };
    emitEvent(nodeId, info);
    return info;
  }

  async function verifyReceipt(nodeId, payload, invoice) {
    const st = ensureState(nodeId);
    const wallet = await ensureWallet(nodeId);
    await ensureChain(nodeId, invoice.chainId || wallet.chainId);
    const ethersMod = await loadEthers();
    const txHash = payload.txHash || payload.hash;
    if (!txHash) throw new Error('Receipt missing transaction hash');
    const receipt = await wallet.provider.getTransactionReceipt(txHash);
    if (!receipt) throw new Error('Transaction not yet available');
    if (receipt.status !== 1) throw new Error('Transaction failed');
    const asset = (invoice.asset || '').toLowerCase();
    if (!asset || asset === ZERO_ADDRESS) throw new Error('Unsupported asset');

    const meta = await getTokenMeta(nodeId, asset);
    const expectedValue = ethersMod.parseUnits(String(invoice.amount || '0'), meta.decimals);
    const iface = new ethersMod.Interface(ERC20_ABI);
    const targetAddr = (invoice.receiver || '').toLowerCase();
    if (!targetAddr) throw new Error('Invoice missing receiver');

    let matched = false;
    for (const logItem of receipt.logs) {
      if (logItem.address.toLowerCase() !== asset) continue;
      let parsed;
      try {
        parsed = iface.parseLog(logItem);
      } catch (_) {
        continue;
      }
      if (parsed?.name !== 'Transfer') continue;
      const toAddr = parsed.args?.[1]?.toLowerCase?.();
      const value = parsed.args?.[2];
      if (!toAddr || value == null) continue;
      if (toAddr === targetAddr && BigInt(value) >= BigInt(expectedValue)) {
        matched = true;
        break;
      }
    }
    if (!matched) throw new Error('Transfer to receiver not detected');
    return true;
  }

  function mount(nodeId, panelEl) {
    const st = ensureState(nodeId);
    if (!panelEl || st.mounted) return;
    panelEl.innerHTML = `
      <div class="payment-summary" data-payment-summary>—</div>
      <div class="payment-wallet" data-payment-wallet>Wallet: not connected</div>
      <div class="payment-actions">
        <button type="button" class="secondary" data-payment-connect>Connect Wallet</button>
        <button type="button" class="ghost" data-payment-refresh>Refresh</button>
        <button type="button" class="ghost" data-payment-clear>Clear Unlocks</button>
      </div>
      <div class="payment-invoices" data-payment-invoices></div>
    `;
    const summary = panelEl.querySelector('[data-payment-summary]');
    const wallet = panelEl.querySelector('[data-payment-wallet]');
    const connectBtn = panelEl.querySelector('[data-payment-connect]');
    const refreshBtn = panelEl.querySelector('[data-payment-refresh]');
    const clearBtn = panelEl.querySelector('[data-payment-clear]');
    const invoices = panelEl.querySelector('[data-payment-invoices]');

    st.ui = {
      panel: panelEl,
      summary,
      wallet,
      connectBtn,
      refreshBtn,
      clearBtn,
      invoices
    };

    if (connectBtn && !connectBtn._paymentsBound) {
      connectBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        try {
          await connectWallet(nodeId);
          setBadge?.('Payments: wallet connected');
        } catch (err) {
          setBadge?.(`Payments: ${err?.message || err}`, false);
        }
      });
      connectBtn._paymentsBound = true;
    }

    if (refreshBtn && !refreshBtn._paymentsBound) {
      refreshBtn.addEventListener('click', (e) => {
        e.preventDefault();
        updateSummary(nodeId);
        updateWalletStatus(nodeId);
        renderInvoices(nodeId);
      });
      refreshBtn._paymentsBound = true;
    }

    if (clearBtn && !clearBtn._paymentsBound) {
      clearBtn.addEventListener('click', (e) => {
        e.preventDefault();
        st.unlocked.clear();
        setBadge?.('Payments: unlock cache cleared');
      });
      clearBtn._paymentsBound = true;
    }

    if (panelEl && !panelEl._paymentsBound) {
      panelEl.addEventListener('click', async (event) => {
        const payBtn = event.target.closest('[data-payment-pay]');
        if (payBtn) {
          event.preventDefault();
          const invoiceId = payBtn.dataset.paymentPay;
          if (!invoiceId) return;
          const record = st.incomingInvoices.get(invoiceId);
          if (!record) return;
          record.status = 'paying';
          renderInvoices(nodeId);
          try {
            const info = await sendPayment(nodeId, record.invoice);
            record.status = 'paid';
            record.txHash = info.txHash;
            st.incomingInvoices.set(invoiceId, record);
            markUnlocked(nodeId, record.remoteKey);
            flushQueue(nodeId, record.remoteKey);
            renderInvoices(nodeId);
            setBadge?.('Payment sent');
          } catch (err) {
            record.status = 'failed';
            record.error = err?.message || String(err);
            st.incomingInvoices.set(invoiceId, record);
            renderInvoices(nodeId);
            setBadge?.(`Payment failed: ${record.error}`, false);
          }
        }
      });
      panelEl._paymentsBound = true;
    }

    st.mounted = true;
    updateSummary(nodeId);
    updateWalletStatus(nodeId);
    renderInvoices(nodeId);
  }

  function dispose(nodeId) {
    const st = nodeState.get(nodeId);
    if (!st) return;
    if (st.ui?.panel) {
      st.ui.panel.innerHTML = '';
    }
    nodeState.delete(nodeId);
  }

  function ensureInvoiceForRemote(nodeId, remoteKey, payload = {}) {
    const key = normalizeKey(remoteKey);
    if (!key) return;
    const st = ensureState(nodeId);
    if (st.remoteInvoices.has(key)) return st.remoteInvoices.get(key);
    const cfg = getConfig(nodeId);
    if (!isSellerMode(cfg.mode)) return;
    const receiver = normalizeKey(cfg.receiver) || st.wallet?.address;
    if (!receiver) {
      setBadge?.('Payments: receiver address required for invoices', false);
      return null;
    }
    const invoice = {
      id: randomId('invoice'),
      amount: String(payload.amount || cfg.amount || '0'),
      asset: payload.asset || cfg.asset || ZERO_ADDRESS,
      chainId: Number(payload.chainId || cfg.chainId) || st.wallet?.chainId || 0,
      receiver,
      memo: payload.memo || cfg.memo || '',
      from: receiver,
      to: key,
      nodeId,
      graphId: CFG.graphId || '',
      createdAt: Date.now()
    };
    st.pendingInvoices.set(invoice.id, {
      invoice,
      remoteKey: key,
      status: 'awaiting'
    });
    st.remoteInvoices.set(key, invoice.id);
    renderInvoices(nodeId);
    emitEvent(nodeId, {
      type: 'payment.invoice',
      invoice,
      target: key
    });
    return invoice.id;
  }

  function handleInvoiceReceived(nodeId, payload, remoteKey) {
    const st = ensureState(nodeId);
    const cfg = getConfig(nodeId);
    if (!isBuyerMode(cfg.mode)) return;
    if (!payload || typeof payload !== 'object') return;
    const invoice = {
      ...payload,
      id: payload.id || randomId('invoice'),
      to: remoteKey || payload.to,
      createdAt: payload.createdAt || Date.now()
    };
    const key = normalizeKey(remoteKey || invoice.from || invoice.to);
    st.incomingInvoices.set(invoice.id, {
      invoice,
      remoteKey: key,
      status: 'pending'
    });
    if (key) st.remoteIncoming.set(key, invoice.id);
    renderInvoices(nodeId);
    setBadge?.(`Invoice received from ${shortenKey(key || remoteKey)}`);
  }

  async function handleReceipt(nodeId, payload, remoteKey) {
    const st = ensureState(nodeId);
    const cfg = getConfig(nodeId);
    if (!isSellerMode(cfg.mode)) return;
    const lookupKey = normalizeKey(remoteKey || payload?.from || payload?.target);
    const invoiceId =
      payload?.invoiceId ||
      (lookupKey ? st.remoteInvoices.get(lookupKey) : null) ||
      null;
    const record = invoiceId ? st.pendingInvoices.get(invoiceId) : null;
    if (!record) {
      setBadge?.('Payments: receipt without matching invoice', false);
      return;
    }
    const targetKey = lookupKey || record.remoteKey;
    record.status = 'verifying';
    renderInvoices(nodeId);
    try {
      await verifyReceipt(nodeId, payload, record.invoice);
      record.status = 'paid';
      record.txHash = payload.txHash;
      st.pendingInvoices.set(record.invoice.id, record);
      markUnlocked(nodeId, targetKey);
      flushQueue(nodeId, targetKey);
      renderInvoices(nodeId);
      emitEvent(nodeId, {
        type: 'payment.status',
        status: 'unlocked',
        invoiceId: record.invoice.id,
        target: targetKey
      });
      setBadge?.(`Payment confirmed from ${shortenKey(targetKey || remoteKey)}`);
    } catch (err) {
      record.status = 'failed';
      record.error = err?.message || String(err);
      st.pendingInvoices.set(record.invoice.id, record);
      renderInvoices(nodeId);
      setBadge?.(`Payment verification failed: ${record.error}`, false);
    }
  }

  function handleStatus(nodeId, payload, remoteKey) {
    if (!payload || typeof payload !== 'object') return;
    const status = String(payload.status || '').toLowerCase();
    if (!status) return;
    const key = normalizeKey(remoteKey || payload.from || payload.target);
    if (status === 'unlocked') {
      markUnlocked(nodeId, key);
      flushQueue(nodeId, key);
    }
  }

  function handleRequest(nodeId, payload, remoteKey) {
    const cfg = getConfig(nodeId);
    if (!isSellerMode(cfg.mode)) return;
    ensureInvoiceForRemote(nodeId, remoteKey, payload);
  }

  function maybeRequestInvoice(nodeId, remoteKey) {
    const st = ensureState(nodeId);
    const cfg = getConfig(nodeId);
    if (!isBuyerMode(cfg.mode)) return;
    const key = normalizeKey(remoteKey);
    if (!key) return;
    if (st.requestedRemotes.has(key)) return;
    st.requestedRemotes.add(key);
    emitEvent(nodeId, {
      type: 'payment.request',
      target: key,
      amount: cfg.amount,
      asset: cfg.asset,
      chainId: cfg.chainId
    });
  }

  function handleControl(nodeId, payload, remoteKey) {
    const type = resolveControlType(payload);
    switch (type) {
      case 'payment.request':
        handleRequest(nodeId, payload, remoteKey);
        break;
      case 'payment.invoice':
        handleInvoiceReceived(nodeId, payload.invoice || payload, remoteKey || payload.from);
        break;
      case 'payment.receipt':
        handleReceipt(nodeId, payload, remoteKey || payload.from);
        break;
      case 'payment.status':
        handleStatus(nodeId, payload, remoteKey || payload.from);
        break;
      default:
        break;
    }
  }

  function handleIngress(nodeId, portName, payload) {
    const st = ensureState(nodeId);
    const cfg = getConfig(nodeId);
    const controlType = resolveControlType(payload);
    const remoteKey = resolveRemoteKey(payload);
    if (controlType) {
      handleControl(nodeId, payload, remoteKey);
      return;
    }
    if (!remoteKey) {
      Router.sendFrom(nodeId, 'output', payload);
      return;
    }
    if (isUnlocked(st, remoteKey, getUnlockTtlMs(cfg))) {
      Router.sendFrom(nodeId, 'output', payload);
      return;
    }
    enqueuePayload(nodeId, remoteKey, portName, payload);
    emitEvent(nodeId, { type: 'payment.required', target: remoteKey });
    if (isSellerMode(cfg.mode)) {
      ensureInvoiceForRemote(nodeId, remoteKey, {});
    } else if (isBuyerMode(cfg.mode)) {
      maybeRequestInvoice(nodeId, remoteKey);
    }
  }

  function onInput(nodeId, portName, payload) {
    if (portName === 'control') {
      const remoteKey = resolveRemoteKey(payload);
      handleControl(nodeId, payload, remoteKey);
      return;
    }
    if (portName === 'input' || portName === 'ingress' || portName === 'default') {
      handleIngress(nodeId, portName, payload);
      return;
    }
    // allow sending control payloads via other ports
    const controlType = resolveControlType(payload);
    if (controlType) {
      const remoteKey = resolveRemoteKey(payload);
      handleControl(nodeId, payload, remoteKey);
      return;
    }
    Router.sendFrom(nodeId, 'output', payload);
  }

  function init(nodeId) {
    updateSummary(nodeId);
    updateWalletStatus(nodeId);
    renderInvoices(nodeId);
  }

  function refresh(nodeId) {
    updateSummary(nodeId);
    renderInvoices(nodeId);
  }

  return {
    mount,
    init,
    dispose,
    onInput,
    refresh
  };
}

export { createPayments };
