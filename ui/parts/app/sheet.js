// 현장 앱 판 그리기 — 계약과 구조 설명: docs/ui-app-sheet-notes.md.
'use strict';
window.AppSheet = (() => {
  const { el, $, drawResult } = window.AppFrame;
  const PIN_NOTE = 'PIN 은 저장되지 않습니다. 누른 뒤 창이 닫혀도 적용은 끝까지 진행됩니다';
  let shown = null;
  let blockedBand = null;
  let blockedNode = null;
  function clearBlocked() {
    if (blockedBand) blockedBand.remove();
    blockedBand = null;
  }
  function placeBlocked() {
    if (!blockedBand?.classList.contains('sheet-scan-blocked')) return;
    const sheet = $('[data-sheet]'), body = $('[data-sheet-body]');
    const rect = sheet.getBoundingClientRect(), padding = body.offsetLeft;
    Object.assign(blockedBand.style, {height:'', left:`${rect.left + padding}px`, width:`${sheet.clientWidth - 2 * padding}px`, top:`${rect.top + body.offsetTop}px`});
    const band = blockedBand.getBoundingClientRect(), bodyBottom = body.getBoundingClientRect().bottom;
    const coversButton = band.bottom > bodyBottom || [...sheet.querySelectorAll('button')].some(node => {
      const r = node.getBoundingClientRect();
      return r.width && r.height && r.left < band.right && r.right > band.left && r.top < band.bottom && r.bottom > band.top;
    });
    blockedBand.dataset.scanPosition = coversButton ? 'title' : 'body';
    if (coversButton) {
      const head = $('.sheet__head').getBoundingClientRect();
      Object.assign(blockedBand.style, {top:`${head.top + sheet.scrollTop}px`, height:`${Math.max(band.height, head.height)}px`});
    }
    const base = blockedBand.getBoundingClientRect();
    const across = r => r.left < base.right && r.right > base.left;
    let bottom = base.bottom, stop = bodyBottom;
    for (const node of body.querySelectorAll('label, input, button, .sheet__facts')) {
      const r = node.getBoundingClientRect();
      if (r.width && r.height && across(r) && r.top >= base.bottom) stop = Math.min(stop, r.top);
    }
    for (const node of body.querySelectorAll('p')) {
      const r = node.getBoundingClientRect();
      if (getComputedStyle(node).visibility !== 'hidden' && across(r) && r.top < bottom && r.bottom > bottom) bottom = r.bottom;
    }
    blockedBand.style.height = `${Math.max(base.height, Math.min(bottom, stop) - base.top)}px`;
  }
  function blocked() {
    if (blockedBand?.isConnected) { placeBlocked(); return; }
    const sheet = $('[data-sheet]');
    const inside = sheet && !sheet.hidden;
    const box = inside ? $('[data-sheet-body]') : $('.bands');
    if (!box) return;
    blockedBand = blockedNode || (blockedNode = el('div', 'band'));
    blockedBand.className = 'band';
    blockedBand.removeAttribute('style');
    delete blockedBand.dataset.scanPosition;
    blockedBand.replaceChildren();
    blockedBand.dataset.scanBlocked = '';
    blockedBand.setAttribute('role', 'status');
    if (inside) blockedBand.append(el('div', '', '열린 창에 먼저 답하세요'));
    blockedBand.append(el('div', '', '방금 스캔은 처리되지 않았습니다'));
    if (inside) {
      blockedBand.classList.add('sheet-scan-blocked');
      document.body.append(blockedBand);
      placeBlocked();
    } else box.prepend(blockedBand);
  }
  document.addEventListener('scanblocked', blocked);
  window.addEventListener('scroll', placeBlocked, true);
  window.addEventListener('resize', placeBlocked);
  window.addEventListener('keydown', clearBlocked, true);
  window.addEventListener('pointerdown', clearBlocked, true);
  const bands = $('.bands');
  if (bands) new MutationObserver(() => {
    const sheet = $('[data-sheet]');
    if (blockedBand && !blockedBand.isConnected && (!sheet || sheet.hidden)) bands.prepend(blockedBand);
  }).observe(bands, {childList:true});

  function render(sheet) {
    const key = sheet ? JSON.stringify(sheet) : null;
    if (key === shown) return;
    clearBlocked();
    shown = key;
    $('[data-sheet]').hidden = !sheet;
    $('[data-sheet-scrim]').hidden = !sheet;
    if (!sheet) return;
    $('[data-sheet-tag]').textContent = sheet.tag || '';
    $('[data-sheet-title]').textContent = sheet.title || '';
    const body = [];
    const lead = el('p', 'sheet__lead');
    if (Array.isArray(sheet.lead)) lead.append(sheet.lead[0], el('b', '', sheet.lead[1]));
    else lead.textContent = sheet.lead || '';
    body.push(lead);
    if (sheet.kind === 'result' && sheet.result) body.push(resultNode(sheet.result));
    if (sheet.facts) body.push(factList(sheet.facts));
    if (sheet.kind === 'pin') body.push(...pinBody(sheet));
    if (sheet.kind === 'form') body.push(...formBody(sheet));
    if (sheet.kind === 'list') {
      const ul = el('ul', 'sheet__list');
      for (const row of sheet.rows || []) {
        const li = el('li');
        li.append(el('b', '', row.title), el('p', '', row.text), buttons(row.actions || []));
        ul.append(li);
      }
      body.push(ul);
    }
    if (sheet.kind === 'confirm') {
      body.push(buttons([{ text: '그만두기', go: 'close', quiet: true }, { text: sheet.go, go: sheet.command ? `command:${sheet.command}` : 'close', danger: true }]));
      if (sheet.note) body.push(el('p', 'sheet__note', sheet.note));
    }
    if (sheet.actions) body.push(buttons(sheet.actions));
    const box = $('[data-sheet-body]');
    let footer = box.parentNode.querySelector('.sheet__footer');
    if (!footer) { footer = el('div', 'sheet__footer'); box.after(footer); }
    footer.replaceChildren(...body.filter(node => node.classList.contains('sheet__btns')));
    footer.hidden = !footer.childElementCount;
    box.replaceChildren(...body.filter(node => !node.classList.contains('sheet__btns')));
    const inputs = [...box.querySelectorAll('input:not(:disabled)')];
    (inputs.find((input) => !input.value) || inputs[0] || box.querySelector('button:not(:disabled)') || footer.querySelector('button:not(:disabled)') || $('[data-sheet] .link')).focus(); // 빈 첫 칸 → 첫 칸(처음 값을 고칠 때) → 첫 단추
  }
  function resultNode(result) {
    const node = el('div', 'result');
    drawResult(node, result);
    return node;
  }
  function factList(rows) {
    const dl = el('dl', 'sheet__facts');
    for (const [k, v] of rows) {
      const pair = el('div');
      pair.append(el('dt', '', k), el('dd', '', v));
      dl.append(pair);
    }
    return dl;
  }
  function buttons(list) {
    const row = el('div', 'sheet__btns');
    for (const b of list) {
      const button = el('button', `act${b.quiet ? ' act--quiet' : ''}${b.danger ? ' act--danger-solid' : ''}`, b.text);
      button.type = 'button';
      if (b.go) button.dataset.go = b.go;
      row.append(button);
    }
    return row;
  }
  function input(props, data, cls) {
    const node = el('input', cls);
    Object.assign(node, props);
    Object.assign(node.dataset, data);
    return node;
  }
  function label(text, child) {
    const node = el('label', '', text);
    node.append(child);
    return node;
  }
  function formBody(sheet) {
    const form = el('div', 'pinform');
    for (const f of sheet.fields || []) {
      const secret = f.type === 'password';
      const value = secret ? '' : sheet.values ? sheet.values[f.name] || '' : f.value || '';
      form.append(label(f.label, input({ type: secret ? 'password' : 'text', inputMode: f.type === 'number' ? 'numeric' : 'text', autocomplete: secret ? 'one-time-code' : 'off',
        placeholder: f.placeholder || '', disabled: !!sheet.sending, value }, { field: f.name, ...(f.scanInto ? {scanInto:''} : {}) })));
    }
    if (sheet.error) form.append(el('p', 'pinform__error', sheet.error));
    const ok = el('button', 'act', sheet.sending ? '보내는 중…' : sheet.go || '확인');
    ok.type = 'button';
    ok.disabled = !!sheet.sending;
    ok.dataset.formOk = '';
    const row = buttons([{ text: '그만두기', go: 'close', quiet: true }]);
    row.append(ok);
    return [form, row];
  }
  function pinBody(sheet) {
    if (sheet.pin === 'done' || sheet.pin === 'denied') {
      const done = sheet.pin === 'done';
      return [resultNode(sheet.result || (done ? { kind: 'ok', text: '했습니다' } : { kind: 'warn', text: 'PIN 확인이 거절됐습니다' })),
        buttons(done ? [{ text: '작업 화면으로', go: 'close' }] : [{ text: '작업 화면으로', go: 'close', quiet: true }, { text: 'PIN 다시 넣기', go: 'retry' }])];
    }
    const busy = sheet.pin === 'checking';
    const form = el('div', 'pinform');
    for (const f of sheet.fields || []) {
      const value = busy && sheet.values ? sheet.values[f.name] || '' : '';
      form.append(label(f.label, input({ type: 'text', autocomplete: 'off', placeholder: f.placeholder || '', disabled: busy, value }, { field: f.name, ...(f.scanInto ? {scanInto:''} : {}) })));
    }
    form.append(label('관리자 ID', input({ type: 'text', autocomplete: 'off', placeholder: '관리자 ID', disabled: busy, value: sheet.admin_id || '' }, { pinId: '' })));
    const row = el('div', 'pinform__row');
    const ok = el('button', 'act', busy ? '확인 중…' : sheet.go || '확인');
    ok.type = 'button';
    ok.disabled = busy;
    ok.dataset.pinOk = '';
    row.append(input({ type: 'password', inputMode: 'numeric', autocomplete: 'one-time-code', placeholder: busy ? '●●●●●●' : '······', disabled: busy },
      { pin: '' }, 'pinform__pin'));
    form.append(label('PIN 6자리', row));
    if (busy) form.append(el('p', 'pinform__wait', '회사 서버에서 PIN 을 확인하는 중 — 창이 닫혀도 적용은 끝까지 진행됩니다'));
    form.append(el('p', 'pinform__note', PIN_NOTE));
    const actions = buttons([{ text: '그만두기', go: 'close', quiet: true }]);
    actions.prepend(ok);
    return [form, actions];
  }
  document.addEventListener('mousedown', (event) => {
    const box = $('[data-sheet]');
    if (box && !box.hidden && box.querySelector('input[type=password], [data-pin]') && !event.target.closest('input, button, a, label, select, textarea')) event.preventDefault();
  }, true);

  return { render, blocked };
})();
