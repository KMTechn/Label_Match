// 현장 앱 공통 화면 틀 — 입력·판·셸 이음. 스캔 조립은 scan.js.
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
  function el(tag, cls, text) {
    const node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text != null) node.textContent = text;
    return node;
  }
  const $ = (sel) => document.querySelector(sel);

  /** 띠 하나 — alert = {kind: warn|bad|quiet|strong, text, choices?:[{text, default?, go?}], tail?} */
  const BAND_ICON = { warn: 'alert', bad: 'alert', strong: 'alert', quiet: 'info' };
  function band(alert) {
    const kind = alert.kind || 'warn';
    const node = el('div', kind === 'warn' ? 'band' : `band band--${kind}`);
    node.setAttribute('role', kind === 'bad' || kind === 'strong' ? 'alert' : 'status');
    node.append(icon(BAND_ICON[kind] || 'alert', 20), el('span', 'band__text', alert.text));
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

  /** 방금 결과 — {kind: ok|bad|warn|saved|idle, text, sub?, sound?} (saved = 이 PC 에 저장 · 회사 서버 확인 전) */
  const RESULT_ICON = { ok: 'check', bad: 'alert', warn: 'alert', saved: 'now', idle: 'idle' };
  function drawResult(node, result) {
    node.classList.remove('result--ok', 'result--bad', 'result--warn', 'result--saved', 'result--idle'); // 자리 이름(area-result) 같은 다른 이름은 그대로
    node.classList.add('result', `result--${result.kind}`);
    const body = el('div');
    body.append(result.text);
    if (result.sub) body.append(el('small', '', result.sub));
    if (result.sound) {
      const sound = el('span', 'result__sound');
      sound.append(icon('sound', 18), `소리: ${result.sound}`);
      body.append(sound);
    }
    node.replaceChildren(icon(RESULT_ICON[result.kind] || 'idle', 26), body);
  }

  /** 위 줄(작업자 · 관리자 메뉴) · 띠 · 바닥 줄 — 앱 화면은 그 사이 작업 칸만 그린다. ui = 화면 잠깐 상태 */
  function drawFrame(s, ui) {
    $('[data-worker]').textContent = s.worker ? s.worker.label : s.app ? '작업자를 고르세요' : '';
    const admin = $('[data-admin]');
    if (admin) {
      admin.hidden = !ui.admin_open;
      $('[data-admin-btn]').setAttribute('aria-expanded', String(!!ui.admin_open));
    }
    const focusLost = ui.focus === 'off';
    const alerts = (focusLost ? [{ kind: 'strong', text: '창을 한 번 누르세요 — 지금은 스캔이 들어오지 않습니다' }] : [])
      .concat(ui.stopped ? [{ kind: 'bad', text: ui.stopped }] : [], (ui.notices || []).map((text, i) => ({ kind: 'warn', text, choices: [{ text: '확인', go: `dismiss:${i}` }] })), s.alerts || []);
    $('[data-bands]').replaceChildren(...alerts.map(band));

    // 서버 상태는 처리 층 conn.state 로 고른다.
    const conn = s.conn || {};
    const queue = s.queue || { pending: 0, failed: 0 };
    const SERVER = {
      review: ['dot dot--amber', `관리자 확인 ${queue.failed}건`],
      offline: ['dot dot--amber', `회사 서버 연결 안 됨 · 이 PC에 저장 중 · 보낼 것 ${queue.pending}건`],
      late: ['dot dot--amber', `회사 서버 연결됨 · 못 보낸 것 ${queue.pending}건`],
      first: ['dot dot--off', '회사 서버: 첫 작업 때 확인'],
      ok: ['dot', `회사 서버 연결됨 · 보낼 것 ${queue.pending}건`],
    };
    const [dot, text] = s.app ? SERVER[conn.state] || SERVER.ok : ['dot dot--off', '프로그램 준비 중'];
    $('[data-status]').className = conn.state === 'offline' ? 'status status--offline' : 'status'; // 바탕색은 끊김 때만
    $('[data-server]').replaceChildren(el('span', dot), conn.label || text);
    const update = $('[data-update]'); // 새 판은 작업 끝나고 PC 를 끄거나 켤 때 바뀐다(현장 업데이트 시점 결정)
    if (update) {
      const ready = !!(s.update && s.update.state === 'ready');
      update.hidden = !ready;
      update.textContent = ready ? `새 판 ${s.update.version} 준비됨 — PC 를 끄거나 켤 때 바뀝니다` : '';
    }
    $('[data-scan]').replaceChildren(el('span', focusLost ? 'dot dot--amber' : 'dot'), focusLost ? '스캔 멈춤 — 창을 누르세요' : '스캔 받는 중');
    $('[data-clock]').textContent = ui.clock || '';
  }

  // 앱은 render · sheets · keys · onPush · onScan 을 준다.
  const ui = { focus: 'on', admin_open: false, sheet: null, stopped: '', clock: '', notices: [] };
  const LOCAL_PUSH = { pedal: 1, focus_state: 1 }; // 화면 표시만 바뀌는 알림 — state() 를 다시 묻지 않는다(app.onPush 가 그림)
  const FN_KEY = /^(F([1-9]|1[01])|Escape|Delete|Ctrl\+[A-Z])$/;
  let app = null, current = null, shown = -Infinity, asked = 0, scanning = false, hostActive = true, readySent = false;

  function paint() {
    drawFrame(current || {}, ui);
    if (current) app.render(current, ui);
    if (window.AppSheet) window.AppSheet.render(shownSheet());
  }
  function shownSheet() { // 처리 층 판에 modal 이 있으면 그것이 먼저(화면 판 위 — 처리 층이 묻는 확인 · 값 넣기), 아니면 화면 판 → 처리 층 판
    const own = current && current.sheet;
    return own && own.modal ? own : ui.sheet || own || null;
  }
  const fromProcess = () => !!current && !!current.sheet && shownSheet() === current.sheet;
  function closeSheet() { // 닫기 · Esc · 그만두기: 처리 층 판은 cancel 명령을 보내고(다음 state 가 판을 걷음), 화면 판은 그냥 닫음
    const sheet = shownSheet();
    if (sheet && sheet.cancel) { go(`command:${sheet.cancel}`); return; } // 화면 판이든 처리 층 판이든 cancel 이 있으면 보냄(화면 판은 go 가 닫음)
    if (sheet && fromProcess()) return;
    open(null);
  }
  function take(s) {
    if (!s || typeof s !== 'object') return;
    const seq = s.last_request_seq ?? s.last_seq;
    if (typeof seq === 'number') {
      if (seq < shown) return; // 늦게 온 옛 state 는 버린다
      shown = seq;
    }
    current = s;
    if (ui.sheet && ui.sheet.pending && s.sheet && !s.sheet.modal) ui.sheet = null; // 받는 중 판(pending) 뒤 처리 층 결과 판이 오면 그 판이 자리를 잇는다
    if (!scanning && window.AppScan) { // 스캔 받이는 첫 state 뒤 한 번(번호는 처리 층 마지막 번호 다음부터)
      scanning = true;
      window.AppScan.start(s.profile || s.app, typeof seq === 'number' ? seq : 0, { recovery: s.recovery });
    }
    paint();
    if (!readySent) { // host 약속(HOST-USE §5·6): 첫 state → 그림 → AppScan.start 뒤 ready 한 번(되살린 문서마다) — 없으면 host 가 8 s 뒤 다시 읽고 push 를 붙잡아 둠
      readySent = true;
      KMShell.call('ready', [{ readyAt: performance.timeOrigin + performance.now(), transport: KMShell.status().mode, app: s.app }]).catch(() => {});
    }
  }
  function bridge(kind, name, args) { // host 답 {ok, result: 처리 층 값, state} → 처리 층 값 {ok, result?, sheet?} 을 펼쳐 state 를 붙임
    return KMShell.call(kind, [name, args]).then((reply) => {
      if (reply && reply.result && typeof reply.result === 'object' && 'ok' in reply.result) return { ...reply.result, state: reply.result.state || reply.state };
      if (reply && reply.ok === false && !reply.result) return { ok: false, result: { kind: 'warn', text: '처리하지 못했습니다 — 다시 해 보세요' }, state: reply.state };
      return reply;
    });
  }
  function refresh() {
    const n = ++asked;
    return KMShell.call('state', []).then((s) => { if (n === asked) take(s); });
  }
  function answer(reply) { // 처리 층 답에 state 가 있으면 그것, 없으면 state() 를 다시 묻는다
    if (reply && reply.state) take(reply.state);
    else refresh().catch(() => {});
    return reply;
  }
  function open(name, target) {
    const sheet = name && name !== 'admin' && app.sheets ? app.sheets[name] : null;
    ui.admin_open = name === 'admin';
    ui.sheet = sheet ? { ...sheet, name, target, pin: sheet.kind === 'pin' ? 'enter' : undefined } : null;
    paint();
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
      if (!fromProcess() && (ui.sheet || ui.admin_open)) open(null); // 처리 층 판의 단추는 밑의 화면 판을 건드리지 않음
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
  function pinHint(text) {
    const box = $('[data-sheet-body]');
    let hint = box.querySelector('.pinform__error');
    if (!hint) {
      hint = el('p', 'pinform__error');
      hint.setAttribute('role', 'alert');
      box.querySelector('.pinform').append(hint);
    }
    hint.textContent = text;
  }
  function pinClearScan(input) {
    if (!(input.pinUntil > performance.now())) pinHint(pinIsScan(input) ? '스캔은 PIN 판을 닫은 뒤 하세요 — 칸에 들어간 스캔은 지웠습니다' : 'PIN 칸에 스캔이 들어가 지웠습니다 — PIN 은 손으로 넣으세요');
    input.value = '';
    input.pinTimes = [];
    input.pinUntil = performance.now() + 300;
  }
  function submitPin(event) {
    const sheet = ui.sheet;
    if (sheet !== shownSheet()) return; // 처리 층 modal 판이 위에 있으면 밑의 PIN 판은 안 보냄
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
    if (held) { held.focus(); return; }
    if (!adminId && id && id.pinUntil) { id.focus(); return; }
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
    pin.value = ''; pin.pinTimes = []; // PIN 은 화면에 남기지 않는다
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
    }, () => done({ ok: false, result: { kind: 'warn', text: 'PIN 을 확인하지 못했습니다', sub: '잠시 뒤 다시 넣으세요 — 작업은 계속할 수 있습니다' } }));
  }
  function submitForm() { // PIN 없는 글 칸 판 → command(sheet.command || 판 이름, {칸: 값}) — 답 ok+sheet 면 그 판, ok 면 result 를 보이거나 닫힘,
    const sheet = shownSheet(); //   아니면 넣은 값 그대로 오류 한 줄(password 칸 값은 판에 남기지 않음)
    if (!sheet || sheet.kind !== 'form' || sheet.sending) return;
    const values = {}, kept = {};
    for (const input of $('[data-sheet-body]').querySelectorAll('[data-field]')) {
      values[input.dataset.field] = input.type === 'password' ? input.value : input.value.trim();
      if (input.type !== 'password') kept[input.dataset.field] = values[input.dataset.field];
      if (input.type === 'password') input.value = '';
    }
    if (fromProcess()) { // 처리 층 판: 판은 처리 층 것 — 보내고 다음 state 를 그림
      bridge('command', sheet.command || sheet.name, values).then(answer, () => refresh().catch(() => {}));
      return;
    }
    ui.sheet = { ...sheet, sending: true, values: kept, error: '' };
    paint();
    const settle = (reply) => {
      if (!ui.sheet || ui.sheet.name !== sheet.name) return;
      if (reply.ok && reply.sheet) ui.sheet = { ...reply.sheet, name: sheet.name };
      else if (reply.ok) ui.sheet = reply.result ? { ...sheet, kind: 'result', result: reply.result, actions: [{ text: '작업 화면으로', go: 'close' }] } : null;
      else ui.sheet = { ...sheet, values: kept, error: (reply.result && reply.result.text) || '보내지 못했습니다 — 다시 누르세요' };
      paint();
    };
    bridge('command', sheet.command || sheet.name, sheet.target === undefined ? values : { ...values, target: sheet.target }).then((reply) => {
      settle(reply || {});
      answer(reply);
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
      if (event.kind === 'alert' && event.message) { // 처리 층 알림 글(messagebox 자리) — 띠로, 「확인」 으로 닫음
        ui.notices = [...ui.notices, String(event.message)].slice(-3);
        paint();
      }
      if (event.state) take(event.state);
      else if (!LOCAL_PUSH[event.kind]) refresh().catch(() => {});
    },
    focus(state) { // host 가 창 활성 · 비활성을 알림 — 스캔 받이의 F12 되살림(물리 키 물음)도 이 길
      if (state === 'page_focus') { focusChanged(); return; }
      if (state !== 'win_activated' && state !== 'win_deactivated') return;
      hostActive = state === 'win_activated';
      if (window.AppScan) window.AppScan.focusEvent(hostActive ? 'focus' : 'blur');
      focusChanged();
    },
    transportStopped(message) { ui.stopped = String(message || '프로그램과 화면 연결이 끊겼습니다'); paint(); },
    control(op) { // host 가 화면 되살림 뒤 포커스 판정에 읽음(HOST-USE §5)
      if (op === 'get_state') return { active: hostActive && document.hasFocus() && document.visibilityState === 'visible' };
      throw new Error('unknown control operation');
    },
  };

  function start(definition) {
    app = definition;
    window.addEventListener('shellpush', (event) => window.KMHost.push(event.detail));
    window.addEventListener('kmshellerror', (event) => { ui.stopped = event.detail.message; paint(); });
    window.addEventListener('focus', focusChanged);
    window.addEventListener('blur', focusChanged);
    document.addEventListener('visibilitychange', focusChanged);
    for (const type of ['scanbuffer', 'scanpending', 'scanresult', 'scanevidence']) {
      document.addEventListener(type, (event) => {
        if (app.onScan) app.onScan(type, event.detail, ui);
        if (type === 'scanresult') answer(event.detail); // 처리 층 답 → 작업 칸을 state 로 다시 그림
      });
    }
    document.addEventListener('click', (event) => {
      const shown = shownSheet();
      if (shown && shown.modal && fromProcess() && !event.target.closest('[data-sheet]')) return; // 처리 층 modal 판 밑(관리자 ▾ · 위 줄 · 작업 단추)은 안 먹음
      const target = event.target.closest('button');
      if (ui.admin_open && !event.target.closest('[data-admin], [data-admin-btn]')) open(null); // 메뉴 밖을 누르면 닫힘
      if (!target) return;
      if (target.matches('[data-admin-btn]')) open(ui.admin_open ? null : 'admin');
      else if (target.matches('[data-pin-ok]')) submitPin(event);
      else if (target.matches('[data-form-ok]')) submitForm();
      else if (target.dataset.open) open(target.dataset.open);
      else if (target.dataset.go) go(target.dataset.go);
    });
    // 빈 Enter: window 잡기 단계는 받이(document 잡기 단계)보다 먼저 돈다 — 그때 받이 버퍼가 비어 있으면 스캔 끝이 아니라 「확인」.
    // scan.js 는 그대로(빈 Enter 는 지금처럼 버림), 판정은 처리 층(keys.Enter 의 명령).
    window.addEventListener('keydown', (event) => {
      if (!app.keys || !app.keys.Enter || event.repeat || event.isComposing || (event.code !== 'Enter' && event.code !== 'NumpadEnter')) return;
      if (shownSheet() || ui.admin_open || !window.AppScan || window.AppScan.editable(document.activeElement) || window.AppScan.control()) return; // 단추 등에 초점 = 그 요소가 눌림(M60)
      const sink = window.AppScan.state();
      if (!sink.buffer && !sink.queued) go(app.keys.Enter);
    }, true);
    document.addEventListener('beforeinput', (event) => {
      const input = pinField(event);
      if (!input) return;
      // prev lives only through this native input; retained scan metadata is counts/times.
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
      if (event.key === 'Escape' && ui.admin_open) { open(null); return; } // Esc 차례: 관리자 메뉴 → 판(처리 층 판은 cancel)
      if (event.key === 'Escape' && shownSheet()) { closeSheet(); return; }
      // Ctrl+글자는 scan.js 가 글로 안 먹는다(Ctrl+V 붙여넣기만 받이 몫) — 이름은 자판 자리로(Ctrl+N), 한글 입력 상태와 상관없이
      const name = event.ctrlKey && !event.altKey && /^Key[A-Z]$/.test(event.code) && event.code !== 'KeyV' ? `Ctrl+${event.code.slice(3)}` : event.key;
      const action = app.keys && FN_KEY.test(name) && app.keys[name];
      if (action && name.startsWith('Ctrl+')) event.preventDefault();
      if (action && !event.repeat && !shownSheet() && !event.target.closest('input, textarea')) go(action);
    });
    tick();
    setInterval(tick, 15000);
    focusChanged();
    const connect = () => refresh().catch(() => {
      if (!KMShell.status().timedOut) setTimeout(connect, 500); // 셸 다리 준비 전 — 15초 넘으면 transport.js 가 kmshellerror
    });
    connect();
  }

  return { icon, el, $, band, drawResult, drawFrame, start, open, refresh, current: () => current, ui };
})();
