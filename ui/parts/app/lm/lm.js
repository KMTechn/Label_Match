// 포장(Label_Match) 작업 화면 — 주인 w9uiphone1014(APP-SCREENS-1013), 시안 w9uiout1014 apps/mock/lm-work.js. 꼴 = contract.json(lm-state/1).
// 그리기 = render(state, ui) 하나: 처리 층 state() 가 진실이고 화면은 수 · 남은 수를 셈하지 않는다(scan_left · 건수는 받은 값 그대로).
// state 를 보이는 판(오늘 기록 · 진행 현황 · 설정 · 멈춘 작업 · 원문)도 render 가 같은 state 로 채운다 — 같은 state → 같은 화면.
// 부르기 · 알림 · 판 · 기능 키 · 스캔 받이는 ../frame.js(AppFrame.start) — 이 파일은 그리기와 승인 누름 판정만, 키 듣기 · 소리 0.
// 되돌릴 수 없는 일(완료 트레이 취소 · 끝내기 · 기록 삭제)의 승인은 마우스로만: 보이는 승인 단추에는 data-go 가 없어 click(키보드로도 남)으로는
// 아무것도 안 하고, 같은 단추 안에서 왼쪽 단추를 누르고 뗄 때만 숨은 단추로 command approve 를 보낸다(Label_Match.py:21437–21472 와 같은 규칙).
'use strict';
(() => {
  const { el, $, icon, code, drawResult } = window.AppFrame;
  const SOUND = { scan_master: '성공음', scan_1: '성공음', pass: '완료음', fail: '오류음', error: '오류음' };
  const SOURCE = { server: '웹에서 바꿈', last_saved: '마지막 받은 값 · 웹에서 바꿈', default: '기본값 · 웹에서 바꿈' };
  const ENDED = { full: '완료했습니다', partial: '부분 제출 완료', failed: '처리하지 못했습니다\n잠시 후 다시 시도하세요' };
  const MARK = { complete: 'check', current: 'now', swap: 'swap', error: 'alert' };
  const ACTION = { 'LABEL.RECHECK': '다시 확인', 'LABEL.SET_HOLD': '보류 후 계속', 'LABEL.F5_HOLD': '보류 후 계속', 'LABEL.SUPPORT_HANDOFF': '지원 인계' };
  const PROGRESS = { loading: '진행 현황을 받는 중입니다', offline: '회사 서버에 닿지 않아 받지 못했습니다\n작업은 그대로 하세요', none: '아직 받지 않았습니다' };
  const CANCEL_ERROR = {
    SCAN_CANCEL_BAD_INPUT: '기록이 바뀌었습니다\n목록을 다시 보고 선택하세요',
    SCAN_CANCEL_TARGET_CHANGED: '기록이 바뀌었습니다\n목록을 다시 보고 선택하세요',
    SCAN_CANCEL_IN_PROGRESS: '진행 중인 묶음은 「방금 스캔 되돌리기」 로 고치세요',
    SCAN_CANCEL_BUSY: '지금은 처리 중입니다\n잠시 후 다시 누르세요',
    SCAN_CANCEL_READ_ONLY: '지난 기록은 여기서 취소할 수 없습니다\n현장 관리자에게 알리세요',
    SCAN_CANCEL_CONFIRMATION_PENDING: '열린 확인을 먼저 끝내세요',
    SCAN_CANCEL_NOT_APPLIED: '취소되지 않았습니다\n현장 관리자에게 알리세요',
    SCAN_CANCEL_FAILED: '취소 중 오류가 났습니다\n현장 관리자에게 알리세요',
  };
  const CANCEL_STATE = { pending: '취소됨 · 회사 서버에 알리는 중', acknowledged: '취소됨', review: '취소됨\n확인 필요 — 현장 관리자에게 알리세요' };
  let cancelBusy = false, cancelSending = false, cancelTarget = null, cancelApprovalId = null, cancelApprovalBefore = null, cancelAwaitingApproval = false, cancelWaitTimer = null, cancelRequest = 0, cancelNotice = null;
  const cancellable = (s, row) => s.today.is_today && !s.today.loading && row && row.status === 'completed' && !row.cancellation && Number.isInteger(row.products) && row.products >= 0;
  const pin = (title, go, lead, fields) => ({ kind: 'pin', tag: 'PIN', title, go, lead, fields });

  const sheets = {
    history: { kind: 'list', title: '오늘 기록', rows: [] },
    progress: { kind: 'list', title: '진행 현황', load: 'refresh_progress', rows: [] },
    settings: { kind: 'list', title: '작업자 선택', rows: [], actions: [{ text: '새 이름 등록', go: 'open:register' }] },
    register: { kind: 'form', title: '새 이름 등록', lead: '명단에 없는 이름을 이 PC 에 등록합니다', fields: [{ name: 'name', label: '이름', placeholder: '이름을 넣으세요' }], go: '등록', command: 'register_worker' },
    cancel_completed: { kind: 'form', title: '완료 트레이 취소', lead: '취소할 트레이의 현품표를 스캔하거나 입력하세요', fields: [{ name: 'label', label: '현품표', placeholder: '현품표를 스캔하세요' }], go: '찾기', command: 'cancel_completed' },
    recovery: { kind: 'list', tag: '관리자', title: '멈춘 작업 확인', load: 'recovery_list', rows: [] },
    pending: { kind: 'list', tag: '관리자', title: '대기 현황', load: 'pending_status', rows: [] },
    raw: { kind: 'list', tag: '관리자', title: '바코드 원문', rows: [] },
    about: { kind: 'info', title: '정보', facts: [] },
    'LABEL.RECHECK': pin('같은 요청 다시 확인', '다시 확인'),
    'LABEL.SET_HOLD': pin('보류 후 계속', '보류'),
    'LABEL.F5_HOLD': pin('보류 후 계속', '보류'),
    'LABEL.SUPPORT_HANDOFF': pin('지원 인계', '인계'),
    'LABEL.PHS_LOOKUP': pin('실물 현품표 조회', '조회', '실물 현품표를 스캔하고 관리자 ID\nPIN 을 넣으세요',
      [{ name: 'physical_qr', label: '실물 현품표', placeholder: '현품표를 스캔하세요' }]),
  };

  const SHEET_PIN = {"admin": true, "history": false, "progress": false, "settings": false, "register": false, "cancel_completed": true, "recovery": false, "LABEL.RECHECK": true, "LABEL.SET_HOLD": false, "LABEL.F5_HOLD": false, "LABEL.SUPPORT_HANDOFF": false, "LABEL.PHS_LOOKUP": false, "pending": false, "raw": false, "about": false};
  for (const [name, sheet] of Object.entries(sheets)) {
    if (SHEET_PIN[name] === false) {
      sheet.tag = '작업';
      if (sheet.kind === 'pin') Object.assign(sheet, { kind: 'form', command: name });
    }
  }
  sheets['LABEL.PHS_LOOKUP'].lead = '실물 현품표를 스캔하세요';

  /* 판 내용 중 state 에서 오는 것 — 판이 열려 있으면 render 가 그 판을 같은 state 로 다시 채운다 */
  const LIVE = {
    history(s, sheet) {
      const t = s.today;
      const selected = t.rows.find((row) => row.row === sheet.target);
      return {
        lead: t.loading ? '기록을 불러오는 중입니다' : t.is_today ? `오늘 ${t.date}` : '오늘 기록을 다시 불러오세요',
        facts: undefined,
        rows: t.rows.map((row) => ({ title: `${row.no}. ${row.time} · ${row.status === 'cancelled' || row.cancellation ? CANCEL_STATE[row.cancellation?.state] || CANCEL_STATE.review : row.result}`,
          text: `${row.tag} ${row.item} · 스캔 ${row.scans}번` })),
        actions: cancellable(s, selected) && !s.approval ? [{ text: cancelBusy ? '처리 중…' : '이 기록 취소', danger: true }] : [],
      };
    },
    progress(s) {
      const p = s.progress;
      return {
        lead: p.state === 'ok' ? `${p.date} · 받은 시각 ${p.fetched_at || '—'}` : PROGRESS[p.state],
        rows: p.rows.map((row) => ({ title: row.item, text: `계획 ${row.plan} · 포장 ${row.packed} · 남은 ${row.left}${row.left_boxes == null ? '' : ` (${row.left_boxes}트레이)`}` })),
      };
    },
    register(s) { // 같은 이름 후보가 있으면 이 판이 후보 줄로 바뀜(처리 층 답 sheet) — 없으면 이름 칸
      const reg = s.settings.register;
      if (!reg) return { kind: 'form', lead: s.settings.note || sheets.register.lead, rows: undefined };
      return { kind: 'list', lead: `${reg.text} — 등록하려는 이름: ${reg.name}`,
        rows: reg.candidates.map((c) => ({ title: c.label, text: '같은 분이면 이 줄을 선택하세요', actions: [{ text: '같은 사람', go: `command:register_same:${c.id}` }] }))
          .concat({ title: '다른 분입니다', text: '구분할 이름으로 다시 등록합니다', actions: [{ text: '다른 사람', go: 'command:register_other', quiet: true }] }) };
    },
    settings(s) {
      const set = s.settings;
      return { lead: set.note || (set.required ? '작업자를 선택한 뒤 스캔하세요' : '트레이 시작 때 이름이 정해집니다'), rows: [] };
    },
    recovery(s) {
      const r = s.admin.recovery;
      const result = s.admin.result;
      return {
        lead: result ? result.text : r && r.loading ? '목록을 받는 중입니다' : '멈춘 작업을 선택하세요',
        rows: (r ? r.rows : []).map((row) => ({ title: `${row.tag} ${row.item}`, text: row.status,
          actions: row.actions.map((name) => ({ text: ACTION[name], go: `open:${name}:${row.set_id}` })) })),
      };
    },
    pending(s) {
      const rows = s.admin.pending ? s.admin.pending.rows : [];
      return { lead: rows.length ? '' : '대기 중인 것이 없습니다', rows: rows.map((row) => ({ title: `${row.text} · ${row.count}건`, text: row.kind })) };
    },
    raw(s) {
      return { lead: '스캔한 원문 그대로 — 복사는 이 PC 의 붙여넣기 칸으로',
        rows: s.today.rows.map((row) => ({ title: `${row.no}. ${row.time} · ${row.tag}`, text: row.raw.join('\n'), actions: [{ text: '복사', go: `command:copy_raw:${row.row}`, quiet: true }] })) };
    },
    about(s) {
      return { lead: '단축키 — F1 현재 트레이 취소 · F2 완료 트레이 취소 · F4 제품 교환 · Esc 알림 확인',
        facts: [['판', s.version], ['만든 날', s.about.built_at || '—'], ['화면 부품', s.about.parts || '—']] };
    },
  };
  for (const name of ['LABEL.RECHECK', 'LABEL.SET_HOLD', 'LABEL.F5_HOLD', 'LABEL.SUPPORT_HANDOFF']) {
    LIVE[name] = (s, sheet) => {
      const row = ((s.admin.recovery && s.admin.recovery.rows) || []).find((item) => item.set_id === sheet.target);
      return { lead: row ? `${row.tag} ${row.item} · ${row.status}` : '멈춘 작업 확인 목록에서 선택한 트레이' };
    };
  }

  function slotValue(slot, w) {
    if (slot.state === 'current') return '지금 스캔할 칸';
    if (slot.state === 'swap') return '교환 중';
    if (!slot.filled) return slot.state === 'error' ? '틀림' : '—';
    if (slot.role === 'master_label') return slot.value || (w.tag ? w.tag.code : '확인됨');
    return slot.value;
  }

  function drawTop(s, w) {
    const tag = $('[data-tag]');
    if (w.tag) {
      const parts = [w.tag.item, w.tag.spec, w.tag.phase, w.quantity].filter(Boolean);
      tag.replaceChildren(el('b', '', w.tag.code), ` ${parts.join(' · ')}`);
    } else tag.textContent = '현품표를 스캔하면 품목이 여기 나옵니다';
    const legacy = $('[data-legacy]');
    legacy.hidden = !w.legacy.enabled;
    legacy.textContent = w.legacy.enabled ? w.legacy.text || '옛 현품표 받기 켬' : '';
    const setting = w.top_scan_setting;
    $('[data-top-setting]').replaceChildren('상단 확인 수 ', el('b', '', String(setting.value)), ` · ${SOURCE[setting.source]}`);
  }

  function drawWork(w) {
    $('[data-next]').textContent = w.next;
    const sub = w.next_sub.map((part) => (part.mark === 'on' ? el('b', '', part.text) : part.mark === 'off' ? el('span', 'off', part.text) : part.text));
    if (w.held_scans) sub.push(el('span', 'lm-held', `저장 뒤 이어서 ${w.held_scans}건`));
    $('[data-next-sub]').replaceChildren(...sub);

    const card = $('[data-count-card]');
    const full = w.completion === 'full' || (!w.completion && w.scan_left === 0);
    card.classList.toggle('count-card--done', full);
    $('[data-badges]').replaceChildren(...w.badges.map((text) => el('span', 'lm-badge', text)));
    $('[data-count]').replaceChildren(String(w.scan_done), el('span', 'count-card__of', `/${w.scan_total}`));
    $('[data-count-left]').textContent = w.completion ? ENDED[w.completion] : w.scan_left ? `남은 스캔 ${w.scan_left}번` : ENDED.full;
    const nextSet = $('[data-next-set]');
    nextSet.hidden = w.next_set_top_scan_count == null;
    nextSet.textContent = nextSet.hidden ? '' : `다음 세트부터 상단 ${w.next_set_top_scan_count}개`;

    AppFrame.exchange($('[data-next]').parentElement, w.exchange && 'remove' in w.exchange && 'insert' in w.exchange ? { phase: ({ target: 'remove', replacement: 'insert' })[w.exchange.phase] || w.exchange.phase, remove: w.exchange.remove.code, insert: w.exchange.insert.code, error: w.result.kind === 'bad' } : null);
    drawResult($('[data-result]'), { ...w.result, sound: w.result.sound ? SOUND[w.result.sound] : null }, true);

    const steps = $('[data-steps]');
    steps.style.setProperty('--slots', String(w.slots.length));
    steps.setAttribute('aria-label', `이 트레이 스캔 ${w.slots.length}칸`);
    steps.replaceChildren(...w.slots.map((slot) => {
      const li = el('li', `lm-step lm-step--${slot.state}`);
      li.append(el('span', 'lm-step__name', slot.label));
      if (MARK[slot.state]) {
        const holder = el('span', 'lm-step__icon');
        holder.append(icon(MARK[slot.state], 22));
        li.append(holder);
      }
      li.append(slot.filled ? code(slotValue(slot, w), 'lm-step__val') : el('span', 'lm-step__val', slotValue(slot, w)));
      return li;
    }));

    for (const button of document.querySelectorAll('[data-act]')) {
      button.disabled = !w.allow[button.dataset.act];
      if (button.dataset.act === 'exchange') button.setAttribute('aria-pressed', String(w.exchange.phase != null));
    }
  }

  function drawStart(start) {
    $('[data-work]').classList.toggle('lm-starting', !!start);
    $('[data-start]').hidden = !start;
    if (!start) return;
    AppFrame.startup($('[data-start]'), { name: '포장', status: start.title, sub: start.text, busy: !start.retry, error: !!start.retry, retry: start.retry ? 'start_retry' : null });
  }

  let approvalId = null;
  function showHistory(target = AppFrame.ui.sheet?.target) {
    const rows = AppFrame.current().today.rows;
    AppFrame.open('history', rows.some((row) => row.row === target) ? target : undefined);
  }

  const commandResult = (reply) => reply?.result && typeof reply.result === 'object' && 'ok' in reply.result ? reply.result : reply;
  function stopCancelWait() {
    cancelAwaitingApproval = false;
    clearTimeout(cancelWaitTimer);
    cancelWaitTimer = null;
  }
  function cancelRefreshFailed() {
    if (!cancelAwaitingApproval) return;
    stopCancelWait();
    cancelBusy = false;
    cancelNotice = { kind: 'warn', text: CANCEL_ERROR.SCAN_CANCEL_FAILED };
    showHistory();
  }
  async function receiveState(reply, result) {
    const state = reply?.state || result?.state;
    if (state) KMHost.push({ state });
    else await AppFrame.refresh();
  }

  function drawApproval(a) {
    const previous = approvalId;
    approvalId = a ? a.id : null;
    $('[data-approval]').hidden = !a;
    $('[data-approval-scrim]').hidden = !a;
    if (!a) {
      if (previous && previous === cancelApprovalId) {
        const target = cancelTarget?.row;
        cancelBusy = false;
        cancelApprovalId = null;
        cancelTarget = null;
        queueMicrotask(() => showHistory(target));
      }
      return;
    }
    if (cancelAwaitingApproval && a.id !== cancelApprovalBefore && !cancelApprovalId) {
      cancelNotice = null;
      cancelApprovalId = a.id;
      cancelBusy = true;
      cancelSending = false;
      stopCancelWait();
    }
    $('[data-approval-title]').textContent = a.title;
    $('[data-approval-text]').textContent = a.message;
    const go = $('[data-approval-go]');
    go.textContent = a.confirm;
    const no = $('[data-approval-no]');
    no.textContent = a.cancel;
    no.dataset.go = `command:dismiss:${a.id}`;
    go.disabled = no.disabled = cancelSending;
    if (a.id === cancelApprovalId && cancelTarget) {
      $('[data-approval-title]').textContent = '이 기록을 취소할까요?';
      $('[data-approval-text]').textContent = `이 트레이 기록 전체가 취소되며 되돌릴 수 없습니다\n자세히: 현품표 ${cancelTarget.tag.slice(-12)} · 제품 ${cancelTarget.products}개 · ${cancelTarget.time}`;
      go.textContent = '취소 확인';
      no.textContent = '그만두기';
    }
    if (previous !== a.id) no.focus();
  }

  function drawHistory(s, ui, focus) {
    if (AppFrame.current() !== s || ui.sheet?.name !== 'history' || s.sheet?.modal) return;
    const box = $('[data-sheet-body]');
    box.classList.add('lm-history');
    const items = box.querySelectorAll('.sheet__list > li');
    s.today.rows.forEach((row, i) => {
      const li = items[i];
      if (!li) return;
      let button = li.querySelector('[data-history-row]');
      if (!button) {
        button = el('button', 'lm-history__row');
        button.type = 'button';
        button.append(...li.childNodes);
        li.replaceChildren(button);
      }
      button.dataset.historyRow = row.row;
      button.dataset.go = `open:history:${row.row}`;
      button.setAttribute('aria-pressed', String(row.row === ui.sheet.target));
      button.classList.toggle('lm-history__row--cancelled', row.status === 'cancelled' || !!row.cancellation);
      button.classList.toggle('lm-history__row--review', row.cancellation?.state === 'review');
      let detail = li.querySelector('[data-history-detail]');
      if (row.cancellation?.text) {
        if (!detail) {
          detail = el('small', 'sheet__note');
          detail.dataset.historyDetail = '';
          li.append(detail);
        }
        detail.textContent = row.cancellation.text;
      } else detail?.remove();
    });
    let cards = box.querySelector('.today-items');
    if (!cards) { cards = el('div'); box.prepend(cards); }
    AppFrame.today(cards, s.today.summary.map(r => ({ item: r.item, trays: r.count, products: r.products })));
    const list = box.querySelector('.sheet__list');
    let timeline = box.querySelector('.today-timeline');
    if (!timeline) {
      timeline = el('details', 'today-timeline'); list.before(timeline); timeline.append(el('summary', '', '시간순 목록'), list);
      timeline.addEventListener('toggle', () => { ui.historyExpanded = timeline.open; });
    }
    timeline.open = ui.sheet.target != null || !!ui.historyExpanded;
    const cancel = box.querySelector(':scope > .sheet__btns button');
    if (cancel) { cancel.dataset.historyCancel = ''; cancel.disabled = cancelBusy; }
    let notice = box.querySelector('[data-history-result]');
    if (cancelNotice?.text) {
      if (!notice) {
        notice = el('p');
        notice.dataset.historyResult = '';
        box.querySelector('.sheet__list').before(notice);
      }
      notice.className = cancelNotice.kind === 'warn' ? 'result result--warn' : 'sheet__lead';
      notice.setAttribute('role', cancelNotice.kind === 'warn' ? 'alert' : 'status');
      if (notice.textContent !== cancelNotice.text) notice.textContent = cancelNotice.text;
    } else notice?.remove();
    if (!box.querySelector('[data-history-note]')) {
      const note = el('p', 'sheet__note', '잘못 스캔한 기록은 줄을 눌러 취소합니다\n지난 기록은 회사 웹 리포트에서 봅니다');
      note.dataset.historyNote = '';
      box.append(note);
    }
    const chosen = [...box.querySelectorAll('[data-history-row]')].find((button) => button.dataset.historyRow === ui.sheet.target);
    if (s.approval) {
      if (!document.activeElement.closest('[data-approval]')) $('[data-approval-no]').focus();
      return;
    }
    if (focus === 'cancel' && cancel) cancel.focus();
    else if (chosen && (focus || document.activeElement === cancel || !box.contains(document.activeElement))) chosen.focus();
    else if (!chosen && !box.contains(document.activeElement)) {
      const title = $('[data-sheet-title]');
      title.tabIndex = -1;
      (timeline.open ? box.querySelector('[data-history-row]') || title : timeline.querySelector('summary')).focus();
    }
  }

  document.addEventListener('click', async (event) => {
    const dismiss = event.target.closest('[data-approval-no]');
    if ((event.target.closest('[data-approval-send]') || dismiss) && approvalId === cancelApprovalId && approvalId) {
      event.stopImmediatePropagation();
      if (cancelSending) return;
      const id = approvalId;
      cancelBusy = true;
      cancelSending = true;
      showHistory();
      try {
        const reply = await KMShell.call('command', [dismiss ? 'dismiss' : 'approve', { id }]);
        const result = commandResult(reply);
        if (result?.result) cancelNotice = result.result;
        else if (!result?.ok) cancelNotice = { kind: 'warn', text: CANCEL_ERROR.SCAN_CANCEL_FAILED };
        await receiveState(reply, result);
      } catch (_) {
        if (!cancelNotice) cancelNotice = { kind: 'warn', text: CANCEL_ERROR.SCAN_CANCEL_FAILED };
      } finally {
        cancelSending = false;
        cancelBusy = cancelAwaitingApproval || AppFrame.current().approval?.id === cancelApprovalId && !!cancelApprovalId;
        showHistory();
      }
      return;
    }
    if (event.target.closest('[data-history-row]')) cancelNotice = null;
    if (!event.target.closest('[data-history-cancel]')) return;
    event.stopImmediatePropagation();
    const s = AppFrame.current(), ui = AppFrame.ui;
    const row = s.today.rows.find((item) => item.row === ui.sheet?.target);
    if (cancelBusy || s.approval || !cancellable(s, row)) return;
    const request = ++cancelRequest;
    cancelApprovalBefore = s.approval?.id ?? null;
    cancelTarget = { ...row };
    cancelBusy = true;
    cancelSending = true;
    cancelNotice = null;
    stopCancelWait();
    cancelApprovalId = null;
    AppFrame.open('history', row.row);
    try {
      const reply = await KMShell.call('command', ['scan_cancel', { rows: [row.row] }]);
      if (request !== cancelRequest) return;
      cancelSending = false;
      const result = commandResult(reply);
      cancelAwaitingApproval = result?.code === 'SCAN_CANCEL_CONFIRMATION_REQUIRED' && result.ok;
      // refresh()가 겹치면 먼저 끝난 답은 틀이 버린다. 적용된 state만 대기를 끝낸다.
      if (cancelAwaitingApproval) cancelWaitTimer = setTimeout(cancelRefreshFailed, 15000);
      if (!result?.ok) cancelNotice = { kind: 'warn', text: CANCEL_ERROR[result?.code] || CANCEL_ERROR.SCAN_CANCEL_FAILED };
      else if (!cancelAwaitingApproval && result.result) cancelNotice = result.result;
      await receiveState(reply, result);
    } catch (_) {
      if (request === cancelRequest) {
        cancelRefreshFailed();
        if (!cancelApprovalId) {
          cancelSending = false;
          if (!cancelNotice) cancelNotice = { kind: 'warn', text: CANCEL_ERROR.SCAN_CANCEL_FAILED };
        }
      }
    } finally {
      if (request === cancelRequest) {
        cancelBusy = cancelAwaitingApproval || AppFrame.current().approval?.id === cancelApprovalId && !!cancelApprovalId;
        showHistory();
      }
    }
  }, true);

  $('[data-approval]').onkeydown = (event) => {
    if (!approvalId) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopImmediatePropagation();
      $('[data-approval-no]').click();
    } else if (event.key === 'Tab') {
      event.preventDefault();
      event.stopImmediatePropagation();
      const no = $('[data-approval-no]'), go = $('[data-approval-go]');
      (document.activeElement === no ? go : no).focus();
    }
  };

  function render(s, ui) {
    if (cancelAwaitingApproval && !s.approval && !s.today.rows.some((row) => row.row === cancelTarget?.row)) {
      stopCancelWait();
      cancelBusy = false;
      cancelTarget = null;
    }
    if (ui.sheet?.name !== 'history') ui.historyExpanded = false;
    const focused = document.activeElement;
    const historyFocus = focused?.hasAttribute('data-history-cancel') ? 'cancel' : focused?.dataset.historyRow;
    $('[data-sheet-body]').classList.remove('lm-history');
    const w = s.work;
    drawTop(s, w);
    AppFrame.menu($('[data-admin]'), SHEET_PIN);
    drawStart(s.start);
    drawWork(w);
    drawApproval(s.approval);
    if (s.settings.required && !ui.sheet) ui.sheet = { ...sheets.settings, name: 'settings' }; // 작업자를 골라야 스캔이 됨(:15334) — 판을 저절로 엶
    if (ui.sheet?.name === 'history' && !s.today.rows.some((row) => row.row === ui.sheet.target)) ui.sheet.target = undefined;
    if (ui.sheet && LIVE[ui.sheet.name]) ui.sheet = { ...ui.sheet, ...LIVE[ui.sheet.name](s, ui.sheet) };
    if (ui.sheet?.name === 'history') queueMicrotask(() => drawHistory(s, ui, historyFocus));
  }

  function afterPaint(s, ui) {
    if (ui.sheet?.name !== 'settings' || s.sheet?.modal || s.confirm?.kind === 'partial_submit') return;
    const body = $('[data-sheet-body]');
    let picker = body.querySelector('.worker-picker');
    if (!picker) { picker = el('div'); body.querySelector('.sheet__list')?.replaceWith(picker); }
    AppFrame.workers(picker, s.settings.workers.map(w => ({ id: w.name, label: w.name, recent: w.name === s.settings.selected, go: `command:select_worker:${w.name}` })));
  }

  /* 승인 — 같은 승인 단추 안에서 왼쪽 단추를 누르고 뗄 때만(뗄 때 단추 밖이면 없음). 보내기는 틀의 단추 길(숨은 단추의 data-go) */
  let pressed = null;
  document.addEventListener('pointerdown', (event) => {
    const button = event.target.closest('[data-approval-go]');
    pressed = event.button === 0 && approvalId && button && !button.disabled ? { id: approvalId, pointer: event.pointerId } : null;
  });
  document.addEventListener('pointerup', (event) => {
    const was = pressed;
    pressed = null;
    if (!was || event.button !== 0 || event.pointerId !== was.pointer || was.id !== approvalId) return;
    const box = $('[data-approval-go]').getBoundingClientRect();
    if (event.clientX < box.left || event.clientX >= box.right || event.clientY < box.top || event.clientY >= box.bottom) return;
    const send = $('[data-approval-send]');
    send.dataset.go = `command:approve:${was.id}`;
    send.click(); // 숨은 단추라 사람 손 · 키보드로는 못 누름
  });
  document.addEventListener('pointercancel', () => { pressed = null; });

  AppFrame.start({ render, sheets, afterPaint, keys: { F1: 'command:cancel_current', F2: 'open:cancel_completed', F4: 'command:exchange', Escape: 'command:escape', Enter: 'command:empty_enter' } });
})();
