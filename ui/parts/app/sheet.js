// 현장 앱 판 그리기(주인 w9webnewui1014 — 10/13 이적 레인 apps-mock/lane-sheet.js 에서 올림, .ca- 뗌) — 관리자 ▾ 항목 · 되돌릴 수 없는 확인.
// 판 = {name, tag?, kind: pin|form|result|info|list|confirm, title, load?(열 때 부를 명령 — 답의 sheet 를 덮음), lead?(글 또는 [글, 굵은 글]), facts?:[[이름, 값]], result?:{kind, text, sub?},
//   fields?:[{name, label, placeholder?, type?, value?(form 처음 값)}](PIN 앞에 더 받을 칸 · form 의 칸), pin?: enter|checking|done|denied, go?(확인 단추 글), command?(confirm · form 이 부를 명령),
//   rows?:[{title, text, actions:[단추]}], actions?:[단추], note?, modal?(처리 층 state.sheet — 화면 판 위에 먼저), cancel?(처리 층 판을 닫을 때 보낼 명령)}. 단추 = {text, go?: close|retry|open:<판>|command:<이름>, quiet?, danger?}.
// 그리기만 한다 — 누름 · PIN 보내기는 frame.js(AppFrame.start), PIN 값은 처리 층이 받고 화면에 남기지 않는다(웹 PIN 상자와 같은 꼴 + 관리자 ID).
'use strict';
window.AppSheet = (() => {
  const { el, $, drawResult } = window.AppFrame;
  const PIN_NOTE = 'PIN 은 저장되지 않습니다. 누른 뒤 창이 닫혀도 적용은 끝까지 진행됩니다';
  let shown = null;

  function render(sheet) {
    const key = sheet ? JSON.stringify(sheet) : null;
    if (key === shown) return; // 같은 판이면 그대로 — 넣던 글 · 초점을 지우지 않는다
    shown = key;
    $('[data-sheet]').hidden = !sheet;
    $('[data-sheet-scrim]').hidden = !sheet;
    if (!sheet) return;
    $('[data-sheet-tag]').textContent = sheet.tag || '';
    $('[data-sheet-title]').textContent = sheet.title || '';
    const body = [];
    if (sheet.lead) {
      const lead = el('p', 'sheet__lead');
      if (Array.isArray(sheet.lead)) lead.append(sheet.lead[0], el('b', '', sheet.lead[1]));
      else lead.textContent = sheet.lead;
      body.push(lead);
    }
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
    box.replaceChildren(...body);
    const inputs = [...box.querySelectorAll('input:not(:disabled)')];
    (inputs.find((input) => !input.value) || inputs[0] || box.querySelector('button:not(:disabled)') || $('[data-sheet] .link')).focus(); // 빈 첫 칸 → 첫 칸(처음 값을 고칠 때) → 첫 단추
  }
  function resultNode(result) {
    const node = el('div', 'result');
    drawResult(node, result);
    return node;
  }
  function factList(rows) {
    const dl = el('dl', 'sheet__facts');
    for (const [k, v] of rows) dl.append(el('dt', '', k), el('dd', '', v));
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
  function formBody(sheet) { // PIN 없는 글 칸 판 — 칸 안 Enter · 확인 = command(sheet.command, {칸 이름: 값}) (frame.js submitForm)
    const form = el('div', 'pinform');
    for (const f of sheet.fields || []) { // type: text · number · password(로컬 관리자 코드 — 값은 판에 남기지 않음)
      const secret = f.type === 'password';
      const value = secret ? '' : sheet.values ? sheet.values[f.name] || '' : f.value || ''; // 처음 값은 처리 층이 준 것(password 칸은 늘 빈 칸)
      form.append(label(f.label, input({ type: secret ? 'password' : 'text', inputMode: f.type === 'number' ? 'numeric' : 'text', autocomplete: secret ? 'one-time-code' : 'off',
        placeholder: f.placeholder || '', disabled: !!sheet.sending, value }, { field: f.name })));
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
      form.append(label(f.label, input({ type: 'text', autocomplete: 'off', placeholder: f.placeholder || '', disabled: busy, value }, { field: f.name })));
    }
    form.append(label('관리자 ID', input({ type: 'text', autocomplete: 'off', placeholder: '관리자 ID', disabled: busy, value: sheet.admin_id || '' }, { pinId: '' })));
    const row = el('div', 'pinform__row');
    const ok = el('button', 'act', busy ? '확인 중…' : sheet.go || '확인');
    ok.type = 'button';
    ok.disabled = busy;
    ok.dataset.pinOk = '';
    row.append(input({ type: 'password', inputMode: 'numeric', maxLength: 6, autocomplete: 'one-time-code', placeholder: busy ? '●●●●●●' : '······', disabled: busy },
      { pin: '' }, 'pinform__pin'), ok);
    form.append(label('PIN 6자리', row));
    if (busy) form.append(el('p', 'pinform__wait', '회사 서버에서 PIN 을 확인하는 중 — 창이 닫혀도 적용은 끝까지 진행됩니다'));
    form.append(el('p', 'pinform__note', PIN_NOTE));
    return [form, buttons([{ text: '그만두기', go: 'close', quiet: true }])];
  }

  return { render };
})();
