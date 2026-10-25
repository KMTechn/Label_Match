
'use strict';
window.AppFrame = (() => {
  const SVG = 'http://www.w3.org/2000/svg';
  const PATHS = {
    check: ['M5 12.5l4.5 4.5L19 7.5'],
    alert: ['M12 8v5', 'M12 16.5v.01', 'M10.3 3.9L2.6 17.5a2 2 0 0 0 1.7 3h15.4a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z'],
    swap: ['M7 7h11l-3-3', 'M17 17H6l3 3'],
    idle: ['M8 12h8'],
    now: ['M3 12a9 9 0 1 0 18 0a9 9 0 1 0 -18 0', 'M12 12v.01'],
    info: ['M12 11v5', 'M12 7.5v.01'],
    sound: ['M11 5L6 9H3v6h3l5 4z', 'M15.5 8.5a5 5 0 0 1 0 7', 'M18.5 6a8.5 8.5 0 0 1 0 12'],
  };

  function icon(kind, size = 22) {
    const svg = document.createElementNS(SVG, 'svg');
    for (const [key, value] of Object.entries({ viewBox: '0 0 24 24', width: size, height: size, fill: 'none', stroke: 'currentColor',
      'stroke-width': 2, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' })) svg.setAttribute(key, value);
    for (const d of PATHS[kind]) {
      const path = document.createElementNS(SVG, 'path');
      path.setAttribute('d', d);
      svg.append(path);
    }
    return svg;
  }

  function keepTogether(text) {
    return String(text);
  }
  function workerLabel(worker) {
    return String(worker?.label || '').replace(/^(작업자\s+(\d+))\s+#\2$/, '$1');
  }
  function textParts(parts, className = '', wholeFirst = false) {
    const values = parts.filter(value => value != null).map(value => String(value).trim()).filter(Boolean);
    return values.flatMap((value, i) => {
      const text = value + (i < values.length - 1 ? '\u00a0·' : '');
      return [i || wholeFirst ? el('span', className, text) : text, ...(i < values.length - 1 ? [' '] : [])];
    });
  }
  function joinText() {
    for (const span of document.querySelectorAll('.text-join')) {
      const parent = span.parentNode;
      span.replaceWith(...span.childNodes);
      parent.normalize();
    }
    const groups = [], byBlock = new Map(), walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walk.nextNode())) {
      let block = node.parentElement;
      if (block.closest('script, style, svg, input, textarea, [contenteditable], .ca-count__item, .result__time')) { byBlock.clear(); continue; }
      while (block.parentElement && !block.matches('button, a, [role="button"]') && /^(inline|contents)/.test(getComputedStyle(block).display)) block = block.parentElement;
      if (!byBlock.has(block)) { const group = []; byBlock.set(block, group); groups.push(group); }
      byBlock.get(block).push(node);
    }
    for (const nodes of groups) {
      const text = nodes.map(n => n.data).join('');
      const joined = [...text.matchAll(/(?<!\S)(?:(?:[가-힣]+[ \t]+\d[\d,.]*(?=[ \t]*(?:번째|[개건줄장명분초%]|$|[·—:!?]))(?:[ \t]*(?:번째|[개건줄장명분초%]))?|\d[\d,.]*[ \t]+(?:번째|[개건줄장명분초%]))(?:[ \t]+(?:·|PIN\b))*|\S+[ \t]+(?:·|PIN\b))\S*(?!\S)/g)];
      const at = pos => {
        let i = 0;
        while (pos >= nodes[i].length) pos -= nodes[i++].length;
        return [nodes[i], pos];
      };
      for (const match of joined.reverse()) {
        const start = match.index, end = start + match[0].length;
        const [first, a] = at(start), [last, b] = at(end - 1), range = new Range();
        range.setStart(first, a);
        range.setEnd(last, b + 1);
        if (first !== last) {
          const common = range.commonAncestorContainer;
          let head = first, tail = last;
          while (head.parentNode !== common) head = head.parentNode;
          while (tail.parentNode !== common) tail = tail.parentNode;
          if (head !== first) range.setStartBefore(head);
          if (tail !== last) range.setEndAfter(tail);
        }
        const span = el('span', 'text-join');
        range.surroundContents(span);
      }
    }
  }
  function el(tag, cls, text) {
    const node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text != null) node.textContent = keepTogether(text);
    return node;
  }
  const $ = (sel) => document.querySelector(sel);

  const codeNodes = new Set();
  let codeFrame = 0;
  const codeObserver = new ResizeObserver(queueCodes);
  function queueCodes() {
    if (codeFrame) return;
    codeFrame = requestAnimationFrame(() => {
      codeFrame = 0;
      for (const node of codeNodes) if (!node.isConnected) { codeObserver.unobserve(node); codeNodes.delete(node); }
      for (const node of document.querySelectorAll('[data-code-full]')) {
        if (!codeNodes.has(node)) { codeNodes.add(node); codeObserver.observe(node); }
        if (!node.getClientRects().length || !node.clientWidth) continue;
        const full = node.dataset.codeFull;
        const fits = () => {
          const range = document.createRange(); range.selectNodeContents(node);
          const style = getComputedStyle(node);
          return range.getBoundingClientRect().width <= node.clientWidth - (parseFloat(style.paddingLeft) || 0) - (parseFloat(style.paddingRight) || 0) - 1;
        };
        const group = node.closest('.result__code-group'), time = group?.querySelector('.result__time');
        const minimum = time ? 4 : 6;
        if (time) {
          group.classList.remove('result__code-group--stack');
          node.textContent = /…|\.{3}/.test(full) ? full : `…${full.slice(-minimum)}`;
          if (!fits()) group.classList.add('result__code-group--stack');
        }
        node.textContent = full;
        if (fits() || full.length <= minimum || /…|\.{3}/.test(full)) continue;
        let low = minimum, high = full.length - 1;
        while (low < high) {
          const n = Math.ceil((low + high) / 2);
          node.textContent = `…${full.slice(-n)}`;
          if (fits()) low = n; else high = n - 1;
        }
        node.textContent = `…${full.slice(-low)}`;
      }
      for (const time of document.querySelectorAll('.result__time')) {
        if (time.closest('.result__code-group')) continue;
        time.classList.remove('result__time--newline');
        const separator = time.querySelector('.result__time-separator');
        if (separator.textContent && time.getClientRects().length && time.getBoundingClientRect().left <= time.parentElement.getBoundingClientRect().left + 1) time.classList.add('result__time--newline');
      }
    });
  }
  function code(value, cls = '') {
    const full = String(value == null ? '' : value);
    const node = el('span', `app-code ${cls}`.trim());
    node.dataset.codeFull = full;
    node.textContent = full; node.title = full; node.setAttribute('aria-label', full);
    queueCodes();
    return node;
  }
  function exchange(host, value) {
    let box = host.querySelector('.app-exchange');
    if (!box && !value) return;
    if (!box) { box = el('section', 'app-exchange'); host.append(box); }
    box.hidden = !value;
    if (!value) return;
    box.setAttribute('aria-label', '제품 교환');
    const fields = el('div', 'app-exchange__fields');
    for (const [name, title] of [['remove', '빼는 제품(불량)'], ['insert', '넣는 제품(양품)']]) {
      const active = value.phase === name;
      const bad = active && value.error;
      const field = el('div', `app-exchange__field${active ? ' is-current' : ''}${bad ? ' is-error' : ''}`);
      if (active) field.setAttribute('aria-current', 'step');
      field.append(el('h3', '', title), value[name] ? code(value[name]) : el('p', 'app-exchange__empty', '스캔하세요'),
        el('p', 'app-exchange__state', bad ? '오류' : value[name] ? '확인' : '대기'));
      fields.append(field);
    }
    box.replaceChildren(fields);
    if (value.phase === 'done') box.append(el('p', 'app-exchange__done', '교환 완료'));
  }
  function workers(host, names) {
    const key = JSON.stringify(names);
    if (host.pickerKey === key) { host.pickerFit(); return; }
    host.pickerKey = key;
    host.pickerObserver?.disconnect();
    host.classList.add('worker-picker');
    const cols = names.length <= 6 ? 2 : names.length <= 12 ? 3 : 4;
    host.style.setProperty('--worker-cols', cols);
    const grid = el('div', 'worker-picker__grid');
    const pager = el('div', 'worker-picker__pager');
    const prev = el('button', '', '이전'), next = el('button', '', '다음'), count = el('span');
    prev.type = next.type = 'button'; prev.dataset.workerPrev = ''; next.dataset.workerNext = '';
    pager.append(prev, count, next);
    const initial = (name) => {
      const n = (name || '').charCodeAt(0) - 0xac00;
      return n >= 0 && n < 11172 ? 'ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ'[Math.floor(n / 588)] : (name || '').slice(0, 1).toUpperCase();
    };
    let query = '', letter = '', page = 0, capacity = cols * 6, layout = '', pending = false;
    let found = names;
    const draw = () => {
      found = names.filter(n => (!letter || initial(n.label) === letter) && (!query || `${n.label} ${n.id || ''}`.toLocaleLowerCase().includes(query)));
      const pages = Math.max(1, Math.ceil(found.length / capacity));
      page = Math.min(page, pages - 1);
      grid.replaceChildren(...found.slice(page * capacity, (page + 1) * capacity).map(n => {
        const b = el('button', `worker-choice${n.recent ? ' is-recent' : ''}`);
        b.type = 'button'; b.dataset.go = n.go; b.title = n.label;
        b.append(el('span', 'worker-choice__name', n.label));
        if (n.note) b.append(el('small', 'worker-choice__note', n.note));
        return b;
      }));
      if (!found.length) grid.append(el('p', 'worker-picker__empty', '일치하는 작업자가 없습니다'));
      pager.hidden = pages === 1; prev.disabled = page === 0; next.disabled = page === pages - 1;
      count.textContent = `${page + 1} / ${pages} · ${found.length}명`;
      fit();
    };
    const fit = () => {
      if (pending) return;
      pending = true;
      requestAnimationFrame(() => {
        pending = false;
        if (!host.isConnected || !host.checkVisibility()) return;
        const before = capacity;
        const signature = `${innerWidth}/${innerHeight}/${document.documentElement.dataset.textLevel}/${query}/${letter}`;
        if (signature !== layout) { layout = signature; capacity = Math.max(cols, Math.min(found.length, cols * 6)); }
        const rows = [...grid.querySelectorAll('button')];
        if (!rows.length) return;
        const rowHeight = Math.max(64, ...rows.map(b => b.getBoundingClientRect().height));
        const gap = parseFloat(getComputedStyle(grid).rowGap) || 0;
        let reserve = 16;
        let block = host;
        while (block.parentElement) {
          const parent = block.parentElement, style = getComputedStyle(parent);
          const after = [...parent.children].slice([...parent.children].indexOf(block) + 1).filter(n => n.checkVisibility());
          reserve += after.reduce((n, node) => n + node.getBoundingClientRect().height + (parseFloat(getComputedStyle(node).marginTop) || 0) + (parseFloat(getComputedStyle(node).marginBottom) || 0) + (parseFloat(style.rowGap) || 0), 0);
          reserve += parseFloat(style.paddingBottom) || 0;
          if (parent.matches('.sheet, .ca-pane, .iw-gate')) break;
          block = parent;
        }
        const sheet = host.closest('.sheet');
        const bottom = Math.min($('[data-status]').getBoundingClientRect().top, sheet ? sheet.getBoundingClientRect().bottom : innerHeight);
        const available = bottom - grid.getBoundingClientRect().top - reserve;
        const allFit = found.length <= Math.floor((available + gap) / (rowHeight + gap)) * cols;
        const room = Math.max(1, Math.floor((available - (allFit ? 0 : 68) + gap) / (rowHeight + gap))) * cols;
        capacity = Math.min(capacity, room);
        if (capacity !== before) draw();
      });
    };
    host.pickerFit = fit;
    host.replaceChildren();
    if (names.length > 24) {
      const tools = el('div', 'worker-picker__tools');
      const search = el('input'); search.type = 'search'; search.placeholder = '작업자 찾기'; search.setAttribute('aria-label', '작업자 찾기');
      search.addEventListener('input', () => { query = search.value.trim().toLocaleLowerCase(); page = 0; draw(); });
      search.addEventListener('keydown', e => {
        const buttons = grid.querySelectorAll('button');
        if (e.key === 'ArrowDown' && buttons.length) { e.preventDefault(); buttons[0].focus(); }
        if (e.key === 'Enter' && !e.isComposing && found.length === 1) { e.preventDefault(); buttons[0].click(); }
      });
      const letters = el('div', 'worker-picker__letters'); letters.setAttribute('aria-label', '첫 글자로 찾기');
      for (const value of ['', ...new Set(names.map(n => initial(n.label)))]) {
        const b = el('button', '', value || '전체'); b.type = 'button'; b.setAttribute('aria-pressed', String(!value));
        b.addEventListener('click', () => { letter = value; page = 0; for (const x of letters.children) x.setAttribute('aria-pressed', String(x === b)); draw(); });
        letters.append(b);
      }
      tools.append(search, letters); host.append(tools);
    }
    host.append(grid, pager);
    prev.addEventListener('click', () => { page--; draw(); }); next.addEventListener('click', () => { page++; draw(); });
    host.pickerObserver = new ResizeObserver(fit); host.pickerObserver.observe(host);
    draw();
  }
  function startup(host, value) {
    host.classList.add('app-start');
    host.classList.toggle('app-start--error', !!value.error);
    const drawing = document.createElementNS(SVG, 'svg');
    for (const [k, v] of Object.entries({viewBox: '0 0 160 64', fill: 'none', stroke: 'currentColor', 'stroke-width': 2, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true'})) drawing.setAttribute(k, v);
    const path = document.createElementNS(SVG, 'path');
    path.setAttribute('d', 'M8 48h32V24l24-12 24 12v24H40m0-24 24 12 24-12M64 36v24m32-12h24V24l16-8 16 8v24h-32m0-24 16 8 16-8m-16 8v24');
    path.setAttribute('pathLength', '1'); drawing.append(path);
    drawing.classList.toggle('is-busy', !!value.busy && !value.error);
    const title = el('p', 'app-start__name', value.name);
    const status = el('h1', 'app-start__status', value.status || '프로그램을 준비하는 중입니다');
    const sub = el('p', 'app-start__sub', value.sub || '');
    host.replaceChildren(title, drawing, status, sub);
    if (value.retry) {
      const b = el('button', 'act', '다시 시도'); b.type = 'button'; b.dataset.go = `command:${value.retry}`; host.append(b);
    }
  }
  function today(host, rows) {
    host.classList.add('today-items');
    const max = Math.max(1, ...rows.map(r => r.products || 0));
    host.replaceChildren(...rows.map(r => {
      const card = el('section', 'today-items__card');
      card.append(el('h3', '', r.item));
      const counts = el('p', 'today-items__counts');
      if (r.products != null) counts.append(el('strong', '', `제품 ${r.products}개`));
      counts.append(el('span', '', `트레이 ${r.trays}개`));
      const bar = el('div', 'today-items__bar'); bar.setAttribute('aria-hidden', 'true');
      const fill = el('i'); fill.style.width = `${100 * (r.products || 0) / max}%`; bar.append(fill); bar.hidden = r.products == null;
      card.append(counts, bar); return card;
    }));
  }
  function menu(host, pins, extra = []) {
    $('[data-admin-btn]').textContent = pins && Object.values(pins).includes(false) ? '메뉴 ▾' : '관리자 ▾';
    if (!host.menuOriginal) {
      host.menuOriginal = [...host.childNodes];
      host.menuBadges = new Map([...host.querySelectorAll('.admin__pin')].map(n => [n, n.hidden]));
    }
    if (!pins) {
      host.replaceChildren(...host.menuOriginal);
      for (const [node, hidden] of host.menuBadges) node.hidden = hidden;
      for (const node of host.querySelectorAll('[data-part-pin]')) node.remove();
      for (const node of host.querySelectorAll('.admin__note')) node.hidden = false;
      return;
    }
    const buttons = host.menuOriginal.filter(n => n.nodeType === 1 && n.tagName === 'BUTTON').concat(extra);
    host.replaceChildren(...host.menuOriginal.filter(n => n.nodeType === 1 && n.tagName !== 'BUTTON' && !n.classList.contains('admin__group')));
    for (const node of host.querySelectorAll('.admin__note')) node.hidden = true;
    for (const [value, label] of [[false, '작업'], [true, '관리자']]) {
      const group = buttons.filter(b => (pins[b.dataset.open || b.dataset.go] !== false) === value);
      if (!group.length) continue;
      const heading = el('p', 'admin__group', label); heading.hidden = group.every(b => b.hidden); host.append(heading);
      for (const b of group) {
        const pin = pins[b.dataset.open || b.dataset.go];
        let badge = b.querySelector('.admin__pin');
        if (pin === true && !badge) { badge = el('span', 'admin__pin', 'PIN'); badge.dataset.partPin = ''; b.append(badge); }
        if (badge && typeof pin === 'boolean') badge.hidden = !pin;
        host.append(b);
      }
    }
  }

  const BAND_ICON = { warn: 'alert', bad: 'alert', strong: 'alert', quiet: 'info' };
  function band(alert) {
    const kind = alert.kind || 'warn';
    const node = el('div', kind === 'warn' ? 'band' : `band band--${kind}`);
    node.setAttribute('role', kind === 'bad' || kind === 'strong' ? 'alert' : 'status');
    const message = el('span', 'band__text');
    const detail = alert.text.indexOf('\n코드: ');
    if (alert.detail) message.append(el('span', 'band__summary', keepTogether(alert.text)), el('small', 'band__code band__detail', alert.detail));
    else if (detail < 0) keepTogether(alert.text).split(/( — |\n)/).forEach((text, i) => message.append(el('span', i % 2 ? 'band__sep' : 'band__chunk', text)));
    else message.append(keepTogether(alert.text.slice(0, detail)), el('small', 'band__code', alert.text.slice(detail + 1)));
    node.append(icon(BAND_ICON[kind] || 'alert', 20), message);
    if (alert.choices) {
      const row = el('div', 'band__choices');
      for (const choice of alert.choices) {
        const button = el('button', choice.default ? 'is-default' : '', choice.text);
        button.type = 'button';
        if (choice.go) button.dataset.go = choice.go;
        row.append(button);
      }
      node.append(row);
    }
    if (alert.tail) node.append(el('span', 'band__tail', alert.tail));
    return node;
  }

  const RESULT_ICON = { ok: 'check', bad: 'alert', warn: 'alert', saved: 'now', idle: 'idle' };
  const DUPLICATE_SCAN = '중복 스캔 — 이미 스캔한 제품입니다';
  const DUPLICATE_RESULTS = {
    ca: [DUPLICATE_SCAN], defect_inspection: [DUPLICATE_SCAN], inspection: [DUPLICATE_SCAN],
    rework_worker: ['이미 스캔한 제품 · 확인 중'],
  };
  function drawResult(node, result, codeSub = false) {
    node.classList.remove('result--ok', 'result--bad', 'result--warn', 'result--saved', 'result--idle');
    node.classList.add('result', `result--${result.kind}`);
    node.classList.toggle('result--duplicate', !!DUPLICATE_RESULTS[current?.app]?.includes(result.text));
    const headline = result.text.split(' — ')[0];
    const mismatch = ['inspection', 'defect_inspection'].includes(current?.app) && headline === '다른 품목입니다'
      || current?.app === 'label_match' && result.text === '이 현품표에 없는 제품입니다';
    node.classList.toggle('result--mismatch', mismatch);
    const body = el('div');
    if (mismatch) {
      body.append(el('strong', 'result__headline', headline), el('span', 'result__next', '이 제품은 따로 두고 다음 제품을 스캔하세요'));
      const context = result.text.slice(headline.length).trimStart();
      if (context) body.append(el('small', 'result__context', context));
    } else body.append(result.text);
    if (result.sub) {
      const sub = el('small');
      const isTime = text => /^\d{2}:\d{2}:\d{2}$/.test(text);
      const timeNode = (value, separator = '') => {
        const time = el('span', 'result__time');
        time.append(el('span', 'result__time-separator', separator), value);
        return time;
      };
      if (codeSub && /^[\x20-\x7e…]+$/.test(result.sub) && !/\d{2}:\d{2}:\d{2}/.test(result.sub)) sub.append(code(result.sub));
      else if (codeSub) {
        const parts = result.sub.split(/(\d{2}:\d{2}:\d{2}|…[!-\x7e]{4,}|[!-\x7e…]{6,})/g);
        parts.forEach((text, i) => {
          if (text == null) return;
          if (!(i % 2)) { sub.append(isTime(parts[i + 1] || '') ? text.replace(/\s*·\s*$/, '') : text); return; }
          if (isTime(text)) {
            sub.append(timeNode(text, parts[i - 1].match(/\s*·\s*$/)?.[0] || ''));
            return;
          }
          const group = el('span', 'result__code-group'); group.append(code(text));
          if (isTime(parts[i + 2] || '') && /^\s*·\s*$/.test(parts[i + 1])) {
            group.append(timeNode(parts[i + 2], parts[i + 1]));
            parts[i + 1] = ''; parts[i + 2] = null;
          }
          else if (/^\s*·/.test(parts[i + 1] || '')) {
            group.append('\u00a0·');
            parts[i + 1] = parts[i + 1].replace(/^\s*·/, '');
          }
          sub.append(group);
        });
      }
      else sub.textContent = result.sub;
      body.append(sub);
    }
    if (result.sound) {
      const sound = el('span', 'result__sound');
      sound.append(icon('sound', 18), `소리: ${result.sound}`);
      body.append(sound);
    }
    node.replaceChildren(icon(RESULT_ICON[result.kind] || 'idle', 26), body);
  }

  function drawFrame(s, ui) {
    const starting = s.view === 'start' || !!s.start;
    $('.top').classList.toggle('top--starting', starting && !!document.querySelector('.ca-start, .lm-start, .iw-gate'));
    $('[data-worker]').textContent = starting ? '' : s.worker ? workerLabel(s.worker) : s.app ? '작업자를 선택하세요' : '';
    $('[data-worker]').title = starting ? '' : s.worker?.label || '';
    const admin = $('[data-admin]');
    if (admin) {
      admin.hidden = !ui.admin_open;
      $('[data-admin-btn]').setAttribute('aria-expanded', String(!!ui.admin_open));
    }
    const focusLost = ui.focus === 'off';
    const conn = s.conn || {};
    const queue = s.queue || { pending: 0, failed: 0 };
    const alerts = (focusLost ? [{ kind: 'strong', text: '창을 한 번 누르세요 — 지금은 스캔이 들어오지 않습니다' }] : [])
      .concat(ui.stopped ? [{ kind: 'bad', text: ui.stopped }] : [], (ui.notices || []).map((text, i) => ({ kind: 'warn', text, choices: [{ text: '확인', go: `dismiss:${i}` }] })), s.alerts || []);
    const bands = alerts.map(band);
    if (conn.state === 'offline') {
      const offline = band({ text: `오프라인 저장 — 이 PC에 저장했습니다\n연결되면 자동 전송합니다\n전송 대기 ${queue.pending}건` });
      offline.classList.add('band--offline');
      offline.dataset.offlineBand = '';
      if (queue.failed > 0) {
        const review = el('span', 'band__review', `관리자 확인 ${queue.failed}건`);
        review.dataset.offlineReview = '';
        offline.append(review);
      }
      bands.unshift(offline);
    }
    if (ui.recovery) {
      const recovery = band({ text: '지난번 작업을 이어서 엽니다 — 스캔 기록은 그대로 있습니다\n마지막으로 스캔한 제품을 확인하고 계속 스캔하세요' });
      recovery.dataset.recoveryBand = '';
      recovery.querySelector('.band__text').style.whiteSpace = 'pre-line';
      bands.splice(conn.state === 'offline' ? 1 : 0, 0, recovery);
    }
    if (s.practice === true) {
      const practice = band({ text: '연습용 PC — 실제 기록이 아닙니다' });
      practice.dataset.practiceBand = '';
      practice.classList.add('band--practice');
      bands.unshift(practice);
    }
    $('[data-bands]').replaceChildren(...bands);

    const SERVER = {
      review: ['dot dot--amber', `관리자 확인 ${queue.failed}건`],
      offline: ['dot dot--amber', `회사 서버 연결 안 됨 · 오프라인 저장 · 전송 대기 ${queue.pending}건`],
      late: ['dot dot--amber', `회사 서버 연결됨 · 전송 대기 ${queue.pending}건`],
      first: ['dot dot--off', '회사 서버: 첫 작업 때 확인'],
      ok: ['dot', `회사 서버 연결됨 · 전송 대기 ${queue.pending}건`],
    };
    const [dot, text] = s.app ? SERVER[conn.state] || SERVER.ok : ['dot dot--off', '시작 준비 중'];
    $('[data-status]').className = conn.state === 'offline' ? 'status status--offline' : 'status';
    $('[data-server]').replaceChildren(el('span', dot), conn.label || text);
    const update = $('[data-update]');
    if (update) {
      const ready = !!(s.update && s.update.state === 'ready');
      update.hidden = !ready;
      update.textContent = ready ? `업데이트 ${s.update.version} 준비됨 — PC 를 끄거나 켤 때 바뀝니다` : '';
    }
    $('[data-scan]').replaceChildren(el('span', focusLost ? 'dot dot--amber' : 'dot'), focusLost ? '스캔 멈춤 — 창을 누르세요' : '스캔 받는 중');
    $('[data-clock]').textContent = ui.clock || '';
  }

  const ui = { focus: 'on', admin_open: false, sheet: null, stopped: '', clock: '', notices: [] };
  const formEdits = new WeakMap();
  const LOCAL_PUSH = { pedal: 1, focus_state: 1 };
  const FN_KEY = /^(F([1-9]|1[01])|Escape|Delete|Ctrl\+[A-Z])$/;
  let app = null, current = null, shown = -Infinity, asked = 0, scanning = false, hostActive = true, readySent = false;

  let restored = false, recoveryDismissed = false;
  let textKey = '', textLevel = 0;
  const textScales = [1, 1.15, 1.3, 1.5], textNames = ['보통', '크게', '더 크게', '아주 크게'];
  function textSize(s, migrate) {
    let readFailed = false;
    const w = s.worker;
    let who = 'pc';
    if (w && w.id != null && w.id !== '') who = `id:${w.id}`;
    else if (w && w.label) {
      let hash = 2166136261;
      for (const ch of w.label) hash = Math.imul(hash ^ ch.codePointAt(0), 16777619);
      who = `h:${(hash >>> 0).toString(16)}`;
    }
    const key = `kmtech.app.text:${s.app || s.profile || 'app'}:${who}`;
    if (key !== textKey) {
      textKey = key;
      document.documentElement.removeAttribute('data-text-expanded');
      $('[data-text-extra]').setAttribute('aria-expanded', 'false');
      try { textLevel = Math.max(0, textNames.indexOf(localStorage.getItem(key))); } catch { textLevel = 0; readFailed = true; }
    }
    if (migrate && !readFailed && s.text_size_hint === 'large') {
      try {
        if (!textNames.includes(localStorage.getItem(key))) {
          localStorage.setItem(key, textNames[1]);
          textLevel = 1;
        }
      } catch {}
    }
    document.documentElement.style.setProperty('--app-font-scale', textScales[textLevel]);
    document.documentElement.dataset.textLevel = textLevel;
    $('[data-text-size]').textContent = `글자: ${textNames[textLevel]}`;
  }
  let headerMoves = [], headerWorkHeading = null;
  function restoreHeaderLinks() {
    for (const { node, marker, hidden } of headerMoves) {
      marker.replaceWith(node);
      if (typeof hidden === 'boolean') node.hidden = hidden;
    }
    headerMoves = [];
    if (headerWorkHeading) { headerWorkHeading.remove(); headerWorkHeading = null; }
  }
  function fitHeader(focusedHeader = null) {
    const active = document.activeElement;
    let focused = focusedHeader || (headerMoves.some(({ node }) => node === active) ||
      active?.matches('.top__nav .link:not(.link--admin)') ? active : null);
    // Measure the original navigation, even when its buttons are currently in the menu.
    restoreHeaderLinks();
    const top = $('.top'), tag = $('[data-tag]'), desktop = window.matchMedia('(min-width: 601px)').matches;
    top.classList.remove('top--compact');
    if (desktop &&
        (top.scrollWidth > top.clientWidth || tag.scrollWidth > tag.clientWidth)) {
      top.classList.add('top--compact');
    }
    const nav = top.querySelector('.top__nav'), host = $('[data-admin]');
    const links = [...nav.querySelectorAll('.link:not(.link--admin)')].filter(node =>
      !nav.hidden && !node.hidden && node.textContent.trim() && getComputedStyle(node).display === 'none');
    if (links.length) {
      const action = node => node.dataset.open ? `open:${node.dataset.open}` : node.dataset.go;
      const duplicates = [...host.querySelectorAll('button')].filter(node =>
        links.some(link => action(link) && action(link) === action(node)));
      if (duplicates.includes(active)) focused = links.find(link => action(link) === action(active));
      const work = [...host.children].find(node => node.classList.contains('admin__group') && node.textContent === '작업');
      const workers = [];
      if (work) {
        for (let node = work.nextElementSibling; node && !node.classList.contains('admin__group'); node = node.nextElementSibling) {
          if (node.tagName === 'BUTTON' && !duplicates.includes(node)) workers.push(node);
        }
      } else headerWorkHeading = el('p', 'admin__group', '작업');
      if (workers.includes(active)) focused = active;
      // Move the original nodes: app state, listeners and routes keep one owner.
      for (const node of [...(work ? [work] : []), ...links, ...workers, ...duplicates]) {
        const marker = document.createComment('header navigation');
        node.before(marker); headerMoves.push({ node, marker, hidden: node === work ? node.hidden : undefined });
      }
      if (work) work.hidden = false;
      for (const node of duplicates) node.remove();
      host.prepend(work || headerWorkHeading, ...links, ...workers);
    }
    if (focused && (document.activeElement === document.body || document.activeElement === focused)) {
      const target = focused.checkVisibility() ? focused : $('[data-admin-btn]');
      if (document.activeElement !== target) target.focus({ preventScroll: true });
    }
  }
  function paint(migrate = false) {
    const focusedHeader = headerMoves.some(({ node }) => node === document.activeElement) ? document.activeElement : null;
    // App renderers still update the links through their original header selectors.
    restoreHeaderLinks();
    resetNotices();
    textSize(current || {}, migrate);
    drawFrame(current || {}, ui);
    if (window.AppNotice) window.AppNotice.render(current || {}, { send: (name, args) => bridge('command', name, args), take, refresh, current: () => current });
    if (current) app.render(current, ui);
    const tag = $('[data-tag]');
    if (tag) tag.title = tag.textContent;
    if (window.AppSheet) window.AppSheet.render(partial() ? null : shownSheet());
    if (current && app.afterPaint) app.afterPaint(current, ui);
    layoutNotices();
    drawPartial();
    joinText();
    queueCodes();
    fitHeader(focusedHeader);
  }
  let rail;
  function resetNotices() {
    if (!rail) return;
    const bands = $('[data-bands]'), notice = $('[data-notice-bar]');
    const practice = rail.querySelector('[data-practice-band]');
    if (practice) bands.append(practice);
    document.body.insertBefore(notice, $('main'));
    document.body.insertBefore(bands, $('main'));
    document.body.removeAttribute('data-notice-rail');
    document.body.removeAttribute('data-notice-wide');
    rail.hidden = true;
  }
  function layoutNotices() {
    resetNotices();
    const bands = $('[data-bands]'), notice = $('[data-notice-bar]');
    const work = [...document.querySelectorAll('main.work')].find(n => n.checkVisibility()), count = work?.querySelector('[data-count]');
    if (!notice || innerWidth <= 600 || !count || bands.children.length + (notice.hidden ? 0 : 1) < 3) return;
    const font = () => parseFloat(getComputedStyle(count).fontSize);
    const stacked = font(), wasHidden = notice.hidden;
    bands.hidden = true; notice.hidden = true;
    const full = font();
    bands.hidden = false; notice.hidden = wasHidden;
    const card = count.closest('[data-count-card]');
    const overflow = card.scrollHeight > card.clientHeight + 1;
    if (stacked + .01 >= full && !overflow) return;
    if (!rail) { rail = el('aside', 'notice-rail'); document.body.insertBefore(rail, work); }
    rail.hidden = false;
    const practice = bands.querySelector('[data-practice-band]');
    rail.replaceChildren(bands, notice, ...(practice ? [practice] : []));
    document.body.dataset.noticeRail = '';
    document.body.toggleAttribute('data-notice-wide', card.matches('.ca-count') && count.scrollWidth > card.clientWidth);
  }
  const partial = () => current && current.confirm && current.confirm.kind === 'partial_submit' ? current.confirm : null;
  function drawPartial() {
    let layer = $('[data-partial]');
    const confirm = partial();
    if (!layer && !confirm) return;
    if (!layer) {
      layer = el('div', 'partial-layer');
      layer.dataset.partial = '';
      layer.hidden = true;
      const card = el('section', 'partial');
      card.setAttribute('role', 'dialog');
      card.setAttribute('aria-modal', 'true');
      card.setAttribute('aria-labelledby', 'partial-title');
      card.setAttribute('aria-describedby', 'partial-body partial-note');
      const title = el('h2', '', '수량 부족 — 부분 제출하시겠습니까?'); title.id = 'partial-title';
      const body = el('p', 'partial__body'); body.id = 'partial-body';
      const buttons = el('div', 'partial__buttons');
      for (const [text, name] of [['부분 제출', 'partial_submit'], ['계속 작업', 'partial_continue']]) {
        const button = el('button', name === 'partial_submit' ? 'act' : 'act act--quiet', text);
        button.type = 'button'; button.dataset.go = `command:${name}`;
        buttons.append(button);
      }
      const note = el('p', 'partial__note', '현품표 한 번 더·Enter = 부분 제출 / 제품 스캔·Esc = 계속 작업'); note.id = 'partial-note';
      card.append(title, body, buttons, note); layer.append(card); document.body.append(layer);
    }
    const opening = layer.hidden;
    layer.hidden = !confirm;
    if (confirm) {
      $('#partial-body').textContent = `${confirm.total}개 중 ${confirm.done}개(${confirm.short}개 부족)`;

      if (opening && document.activeElement !== document.body) document.activeElement.blur();
    }
  }
  function shownSheet() {
    const own = current && current.sheet;
    return own && own.modal ? own : ui.sheet || own || null;
  }
  const fromProcess = () => !!current && !!current.sheet && shownSheet() === current.sheet;
  function closeSheet() {
    const sheet = shownSheet();
    if (sheet && sheet.cancel) { go(`command:${sheet.cancel}`); return; }
    if (sheet && fromProcess()) return;
    open(null);
  }
  function take(s) {
    if (!s || typeof s !== 'object') return;
    const seq = s.last_request_seq ?? s.last_seq;
    if (typeof seq === 'number') {
      if (seq < shown) return;
      shown = seq;
    }
    if (s.recovery_notice?.restored !== true) recoveryDismissed = false;
    else if (!restored) recoveryDismissed = false;
    restored = s.recovery_notice?.restored === true;
    ui.recovery = restored && !recoveryDismissed;
    current = s;
    if (ui.sheet && ui.sheet.pending && s.sheet && !s.sheet.modal) ui.sheet = null;
    if (!scanning && window.AppScan) {
      scanning = true;
      window.AppScan.start(s.profile || s.app, typeof seq === 'number' ? seq : 0, { recovery: s.recovery });
    }
    paint(true);
    if (!readySent) {
      readySent = true;
      KMShell.call('ready', [{ readyAt: performance.timeOrigin + performance.now(), transport: KMShell.status().mode, app: s.app }]).catch(() => {});
    }
  }
  function bridge(kind, name, args) {
    return KMShell.call(kind, [name, args]).then((reply) => {
      if (reply && reply.result && typeof reply.result === 'object' && 'ok' in reply.result) return { ...reply.result, state: reply.result.state || reply.state };
      if (reply && reply.ok === false && !reply.result) return { ok: false, result: { kind: 'warn', text: '처리하지 못했습니다\n다시 시도하세요' }, state: reply.state };
      return reply;
    });
  }
  function refresh() {
    const n = ++asked;
    return KMShell.call('state', []).then((s) => { if (n === asked) take(s); });
  }
  function answer(reply) {
    if (reply && reply.state) take(reply.state);
    else refresh().catch(() => {});
    return reply;
  }
  function open(name, target) {
    const closing = !name && (ui.sheet || ui.admin_open);
    const sheet = name && name !== 'admin' && app.sheets ? app.sheets[name] : null;
    ui.admin_open = name === 'admin';
    ui.sheet = sheet ? { ...sheet, name, target, pin: sheet.kind === 'pin' ? 'enter' : undefined } : null;
    paint();
    if (closing) {
      const previous = document.body.getAttribute('tabindex');
      document.body.tabIndex = -1; document.body.focus({ preventScroll: true });
      if (previous == null) document.body.removeAttribute('tabindex'); else document.body.setAttribute('tabindex', previous);
    }
    if (sheet && sheet.load) {
      bridge('command', sheet.load, target === undefined ? {} : { id: target }).then((reply) => {
        if (ui.sheet && ui.sheet.name === name && reply && reply.sheet) { ui.sheet = { ...ui.sheet, ...reply.sheet }; paint(); }
      }, () => {});
    }
  }
  function go(value) {
    const [kind, rest = ''] = String(value).split(/:(.*)/);
    if (kind === 'close') closeSheet();
    else if (kind === 'retry' && ui.sheet) { ui.sheet = { ...ui.sheet, pin: 'enter', result: undefined }; paint(); }
    else if (kind === 'dismiss') { ui.notices = ui.notices.filter((_, i) => String(i) !== rest); paint(); }
    else if (kind === 'open') { const [sheet, id] = rest.split(/:(.*)/); open(sheet, id); }
    else if (kind === 'command') {
      const [name, id] = rest.split(/:(.*)/);
      if (!fromProcess() && (ui.sheet || ui.admin_open)) open(null);
      bridge('command', name, id === undefined ? {} : { id }).then(answer, () => refresh().catch(() => {}));
    }
  }
  const PIN_FAST_MS = 50;
  function pinField(event) {
    const sheet = shownSheet();
    return sheet && sheet.kind === 'pin' && sheet.pin === 'enter' && event.target.matches('[data-pin], [data-pin-id]') ? event.target : null;
  }
  function pinIsScan(input) {
    const times = input.pinTimes || [];
    const tail = times.slice(times.lastIndexOf(null) + 1);
    if (tail.length < 5) return false;
    const gaps = tail.slice(1).map((t, i) => t - tail[i]).sort((a, b) => a - b), n = gaps.length;
    return (gaps[Math.floor((n - 1) / 2)] + gaps[Math.floor(n / 2)]) / 2 < PIN_FAST_MS;
  }
  function pinHint(text, scan = false) {
    const box = $('[data-sheet-body]');
    let hint = box.querySelector('.pinform__error');
    if (!hint) {
      hint = el('p', 'pinform__error');
      hint.setAttribute('role', 'alert');
      box.querySelector('.pinform').append(hint);
    }
    hint.textContent = text;
    hint.style.visibility = scan ? 'hidden' : '';
  }
  function pinClearScan(input) {
    if (!(input.pinUntil > performance.now())) pinHint(pinIsScan(input) ? '칸에 들어간 스캔은 지웠습니다\nPIN 창을 닫은 뒤 스캔하세요' : 'PIN 칸에 스캔이 들어가 지웠습니다\nPIN 은 손으로 넣으세요', true);
    input.value = '';
    input.pinTimes = [];
    input.pinUntil = performance.now() + 300;
    window.AppSheet.blocked();
  }
  function submitPin(event) {
    const sheet = ui.sheet;
    if (sheet !== shownSheet()) return;
    if (!sheet || sheet.kind !== 'pin' || sheet.pin !== 'enter') return;
    const box = $('[data-sheet-body]');
    const pin = box.querySelector('[data-pin]');
    const id = box.querySelector('[data-pin-id]');
    const adminId = id ? id.value.trim() : '';
    const fields = [id, pin].filter(Boolean);
    if (event.isComposing || event.keyCode === 229 || fields.some(input => input.pinComposing)) return;
    const fastFields = fields.filter(input => pinIsScan(input) || (input === pin && input.value.length > 6));
    for (const input of fastFields) pinClearScan(input);
    const held = fields.find(input => input.pinUntil > performance.now());
    if (held) { window.AppSheet.blocked(); held.focus(); return; }
    if (!adminId && id && id.pinUntil) { window.AppSheet.blocked(); id.focus(); return; }
    if (!adminId || !pin || !/^\d{6}$/.test(pin.value)) {
      if (pin) { pin.value = ''; pin.pinTimes = []; }
      pinHint('관리자 ID 와 PIN 6자리를 넣으세요');
      const focus = !adminId ? id : pin;
      if (focus) focus.focus();
      return;
    }
    const values = {};
    for (const input of box.querySelectorAll('[data-field]')) values[input.dataset.field] = input.value.trim();
    const args = { ...values, ...(sheet.target === undefined ? {} : { target: sheet.target }), admin_id: adminId, pin: pin.value };
    pin.value = ''; pin.pinTimes = [];
    ui.sheet = { ...sheet, pin: 'checking', admin_id: args.admin_id, values };
    paint();
    const done = (reply) => {
      if (ui.sheet && ui.sheet.name === sheet.name) {
        ui.sheet = reply.ok && reply.sheet ? { ...reply.sheet, name: sheet.name } : { ...ui.sheet, pin: reply.ok ? 'done' : 'denied', result: reply.result };
      }
      paint();
    };
    bridge('admin', sheet.name, args).then((reply) => {
      done(reply || {});
      answer(reply);
    }, () => done({ ok: false, result: { kind: 'warn', text: 'PIN 을 확인하지 못했습니다', sub: '잠시 후 다시 넣으세요 — 작업은 계속할 수 있습니다' } }));
  }
  function submitForm() {
    const sheet = shownSheet();
    if (!sheet || sheet.kind !== 'form' || sheet.sending) return;
    const values = {}, kept = {}, scanned = new Set();
    const fields = () => [...$('[data-sheet-body]').querySelectorAll('[data-field]')];
    const focusField = document.activeElement?.dataset.field;
    for (const input of fields()) {
      values[input.dataset.field] = input.type === 'password' ? input.value : input.value.trim();
      if (input.type !== 'password') kept[input.dataset.field] = values[input.dataset.field];
      if (input.matches('[data-scan-into]') && input.scanFromScanner) scanned.add(input.dataset.field);
      if (input.type === 'password') input.value = '';
    }
    let pending = sheet;
    if (!fromProcess()) {
      pending = ui.sheet = { ...sheet, sending: true, values: kept, error: '' };
      paint();
    }
    const read = () => fields().map(node => ({node, value:node.value, edit:formEdits.get(node)}));
    let snapshot = read();
    const owns = () => shownSheet() === pending && snapshot.every(item => item.node.isConnected && item.node.value === item.value && formEdits.get(item.node) === item.edit);
    const focusScan = () => fields().find(input => input.dataset.field === (scanned.has(focusField) ? focusField : [...scanned][0]))?.focus();
    const failedProcess = (reply) => {
      if (!owns()) return;
      for (const input of fields()) {
        if (scanned.has(input.dataset.field)) { input.value = ''; input.scanFromScanner = false; }
        if (input.type !== 'password') kept[input.dataset.field] = input.value;
      }
      pending.values = { ...pending.values, ...kept };
      snapshot = read(); focusScan();
      const apply = (s) => {
        if (!owns()) return;
        const next = s?.sheet;
        const same = next?.kind === 'form' && ['name','target','command','fields'].every(key => JSON.stringify(next[key]) === JSON.stringify(sheet[key]));
        const view = same ? { ...next, values: { ...next.values, ...kept } } : next;
        take(same ? { ...s, sheet: view } : s);
        if (same && shownSheet() === view) focusScan();
      };
      if (reply?.state) apply(reply.state);
      else { const n = ++asked; KMShell.call('state', []).then(s => { if (n === asked) apply(s); }).catch(() => {}); }
    };
    if (pending === sheet) {
      bridge('command', sheet.command || sheet.name, values).then(reply => {
        if (!reply || !reply.ok) failedProcess(reply); else answer(reply);
      }, () => failedProcess());
      return;
    }
    const settle = (reply) => {
      if (!owns()) return false;
      if (reply.ok && reply.sheet) ui.sheet = { ...reply.sheet, name: sheet.name };
      else if (reply.ok) ui.sheet = reply.result ? { ...sheet, kind: 'result', result: reply.result, actions: [{ text: '작업 화면으로', go: 'close' }] } : null;
      else {
        for (const name of scanned) kept[name] = '';
        ui.sheet = { ...sheet, values: kept, error: (reply.result && reply.result.text) || '전송하지 못했습니다\n다시 누르세요' };
      }
      paint();
      if (!reply.ok) focusScan();
      return true;
    };
    bridge('command', sheet.command || sheet.name, sheet.target === undefined ? values : { ...values, target: sheet.target }).then((reply) => {
      if (settle(reply || {})) answer(reply);
    }, () => settle({}));
  }
  function focusChanged() {
    ui.focus = hostActive && document.hasFocus() && document.visibilityState === 'visible' ? 'on' : 'off';
    paint();
  }
  function tick() {
    const now = new Date();
    ui.clock = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    $('[data-clock]').textContent = ui.clock;
  }

  window.KMHost = {
    push(event) {
      if (!event || typeof event !== 'object') return;
      if (app && app.onPush) app.onPush(event, ui);
      if (event.kind === 'alert' && event.message) {
        ui.notices = [...ui.notices, String(event.message)].slice(-3);
        paint();
      }
      if (event.state) take(event.state);
      else if (!LOCAL_PUSH[event.kind]) refresh().catch(() => {});
    },
    focus(state) {
      if (state === 'page_focus') { focusChanged(); return; }
      if (state !== 'win_activated' && state !== 'win_deactivated') return;
      hostActive = state === 'win_activated';
      if (window.AppScan) window.AppScan.focusEvent(hostActive ? 'focus' : 'blur');
      focusChanged();
    },
    transportStopped(message) { ui.stopped = String(message || '프로그램과 화면 연결이 끊겼습니다\n현장 관리자에게 알리세요'); paint(); },
    control(op) {
      if (op === 'get_state') return { active: hostActive && document.hasFocus() && document.visibilityState === 'visible' };
      throw new Error('unknown control operation');
    },
  };

  function start(definition) {
    app = definition;
    document.fonts.ready.then(() => { queueCodes(); fitHeader(); });
    const size = el('button', 'link', '글자: 보통');
    size.type = 'button'; size.dataset.textSize = '';
    $('[data-worker]').after(size);
    const extra = el('button', 'link text-extra', '보조 칸');
    extra.type = 'button'; extra.dataset.textExtra = ''; extra.setAttribute('aria-expanded', 'false');
    size.after(extra);
    new ResizeObserver(() => {
      fitHeader();
      document.documentElement.style.setProperty('--hdr-h', `${Math.ceil($('.top').getBoundingClientRect().height)}px`);
    }).observe($('.top'));
    window.addEventListener('shellpush', (event) => window.KMHost.push(event.detail));
    window.addEventListener('kmshellerror', (event) => { ui.stopped = event.detail.message; paint(); });
    window.addEventListener('focus', focusChanged);
    window.addEventListener('blur', focusChanged);
    window.addEventListener('resize', layoutNotices);
    document.addEventListener('visibilitychange', focusChanged);
    for (const type of ['scanbuffer', 'scanpending', 'scanresult', 'scanevidence']) {
      document.addEventListener(type, (event) => {
        if (app.onScan) app.onScan(type, event.detail, ui);
        if (type === 'scanresult') {
          const reply = event.detail;
          const quiet = reply?.accepted === true && reply.sound === null && reply.result === null && reply.message === '';
          if (reply?.accepted === true && ['ok', 'saved'].includes(reply.result?.kind)) {
            recoveryDismissed = true; ui.recovery = false;
          }
          if (!quiet) answer(reply);
        }
      });
    }
    document.addEventListener('click', (event) => {
      if (partial() && !event.target.closest('[data-partial]')) return;
      const shown = shownSheet();
      if (shown && shown.modal && fromProcess() && !event.target.closest('[data-sheet]')) return;
      const target = event.target.closest('button');
      if (ui.admin_open && !event.target.closest('[data-admin], [data-admin-btn]')) open(null);
      if (!target) return;
      if (target.matches('[data-text-size]')) {
        textLevel = (textLevel + 1) % 4;
        try { localStorage.setItem(textKey, textNames[textLevel]); } catch { textLevel = 0; }
        document.documentElement.removeAttribute('data-text-expanded');
        extra.setAttribute('aria-expanded', 'false');
        paint();
        target.blur();
      }
      else if (target.matches('[data-text-extra]')) {
        extra.setAttribute('aria-expanded', document.documentElement.toggleAttribute('data-text-expanded'));
        fitHeader();
        target.blur();
      }
      else if (target.matches('[data-admin-btn]')) open(ui.admin_open ? null : 'admin');
      else if (target.matches('[data-pin-ok]')) submitPin(event);
      else if (target.matches('[data-form-ok]')) submitForm();
      else if (target.dataset.open) open(target.dataset.open);
      else if (target.dataset.go) go(target.dataset.go);
    });

    window.addEventListener('keydown', (event) => {
      if (partial() && !event.repeat && !event.isComposing && window.AppScan && !window.AppScan.editable(document.activeElement)) {
        const sink = window.AppScan.state();
        if (event.key === 'Tab' && !sink.buffer && !sink.queued) {
          const buttons = [...$('[data-partial]').querySelectorAll('button')];
          const index = buttons.indexOf(document.activeElement);
          buttons[index < 0 ? event.shiftKey ? buttons.length - 1 : 0 : (index + (event.shiftKey ? -1 : 1) + buttons.length) % buttons.length].focus();
          event.preventDefault(); event.stopImmediatePropagation(); return;
        }
        const enter = event.key === 'Enter' || event.code === 'NumpadEnter';
        if (event.key === 'Escape' || enter && !sink.buffer && !sink.queued && !window.AppScan.control()) {
          event.preventDefault(); event.stopImmediatePropagation();
          go(`command:${event.key === 'Escape' ? 'partial_continue' : 'partial_submit'}`);
          return;
        }
      }
      if (!app.keys || !app.keys.Enter || event.repeat || event.isComposing || (event.code !== 'Enter' && event.code !== 'NumpadEnter')) return;
      if (shownSheet() || ui.admin_open || !window.AppScan || window.AppScan.editable(document.activeElement) || window.AppScan.control()) return;
      const sink = window.AppScan.state();
      if (!sink.buffer && !sink.queued) go(app.keys.Enter);
    }, true);
    document.addEventListener('beforeinput', (event) => {
      const input = pinField(event);
      if (!input) return;
      const prev = input.value, end = input.selectionStart === prev.length && input.selectionEnd === prev.length;
      const paste = event.inputType === 'insertFromPaste';
      input.pinDiff = (value, time) => {
        const times = input.pinTimes || Array(prev.length).fill(null);
        if (!paste && end && value.length === prev.length + 1 && value.startsWith(prev)) return [...times, time];
        if (!paste && value.length < prev.length && prev.startsWith(value)) return times.slice(0, value.length);
        return Array(value.length).fill(null);
      };
      setTimeout(() => { input.pinDiff = null; }, 0);
    });
    document.addEventListener('input', (event) => {
      if (event.target.matches('[data-field]')) formEdits.set(event.target, {});
      const input = pinField(event);
      if (!input) return;
      input.pinTimes = input.pinDiff ? input.pinDiff(input.value, event.timeStamp) : Array(input.value.length).fill(null);
      input.pinDiff = null;
      if (input.pinUntil > performance.now() || (input.matches('[data-pin]') && input.value.length > 6) || pinIsScan(input)) pinClearScan(input);
      else input.pinUntil = 0;
    });
    for (const type of ['compositionstart', 'compositionend']) document.addEventListener(type, (event) => {
      const input = pinField(event);
      if (input) input.pinComposing = type === 'compositionstart';
    });
    document.addEventListener('keydown', (event) => {
      if (partial()) return;
      const sheet = shownSheet();
      if (sheet && sheet.kind === 'pin' && (event.key === 'Enter' || event.code === 'Enter' || event.code === 'NumpadEnter') &&
          (event.isComposing || event.keyCode === 229 || [...$('[data-sheet-body]').querySelectorAll('[data-pin], [data-pin-id]')].some(input => input.pinComposing))) {
        event.preventDefault();
        return;
      }
      if (event.key === 'Enter' && event.target.closest('[data-sheet-body] input')) {
        event.preventDefault();
        const shown = shownSheet();
        if (shown && shown.kind === 'form') submitForm(); else submitPin(event);
        return;
      }
      if (event.key === 'Escape' && ui.admin_open) { open(null); return; }
      if (event.key === 'Escape' && shownSheet()) { closeSheet(); return; }
      const name = event.ctrlKey && !event.altKey && /^Key[A-Z]$/.test(event.code) && event.code !== 'KeyV' ? `Ctrl+${event.code.slice(3)}` : event.key;
      const action = app.keys && FN_KEY.test(name) && app.keys[name];
      if (action && name.startsWith('Ctrl+')) event.preventDefault();
      if (action && !event.repeat && !shownSheet() && !event.target.closest('input, textarea')) go(action);
    });
    tick();
    setInterval(tick, 15000);
    focusChanged();
    const connect = () => refresh().catch(() => {
      if (!KMShell.status().timedOut) setTimeout(connect, 500);
    });
    connect();
  }

  return { keepTogether, workerLabel, textParts, icon, el, $, code, exchange, workers, startup, today, menu, band, drawResult, drawFrame, start, open, refresh, current: () => current, ui };
})();
