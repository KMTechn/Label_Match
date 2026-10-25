'use strict';
window.AppNotice = (() => {
  const { el } = window.AppFrame;
  let state = {}, api, scope, detail = null, held = null, busy = false, queued = false, error = '', request = 0, popup;
  const owner = s => JSON.stringify([s.app, s.worker && (s.worker.id || s.worker.label)]);
  const notices = () => state.notices || { items: [], pending_ack: [] };
  const find = id => notices().items.find(item => item.id === id);
  const pending = () => (notices().pending_ack || []).map(find).find(item => item && item.weight === 'important');

  function sentAt(value) {
    const match = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(Z|[+-]\d{2}:\d{2})?$/.exec(value);
    if (!match) return '';
    if (!match[6]) return `${match[2]}/${match[3]} ${match[4]}:${match[5]} 보냄 · 관리자`;
    const date = new Date(Date.parse(value) + 9 * 60 * 60 * 1000);
    if (!Number.isFinite(date.getTime())) return '';
    const pad = n => String(n).padStart(2, '0');
    return `${pad(date.getUTCMonth() + 1)}/${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())} 보냄 · 관리자`;
  }
  function pointer(button, action, only = true) {
    button.type = 'button';
    button.addEventListener('pointerdown', event => event.preventDefault());
    button.addEventListener('click', event => { if (!only || event.detail > 0) action(); });
  }
  function makePopup() {
    popup = el('section', 'notice-popup');
    popup.dataset.noticePopup = '';
    popup.setAttribute('role', 'dialog');
    popup.setAttribute('aria-modal', 'false');
    popup.setAttribute('aria-labelledby', 'notice-title');
    const heading = el('p', 'notice-popup__heading'); heading.dataset.noticeHeading = '';
    const title = el('h2', 'notice-popup__title'); title.id = 'notice-title';
    const body = el('div', 'notice-popup__body'); body.dataset.noticeBody = '';
    const sent = el('p', 'notice-popup__sent'); sent.dataset.noticeSent = '';
    const note = el('p', 'notice-popup__error'); note.dataset.noticeError = ''; note.setAttribute('role', 'status');
    const actions = el('div', 'notice-popup__actions');
    const ack = el('button', 'act', '확인'); ack.dataset.noticeAck = '';
    const close = el('button', 'act', '닫기'); close.dataset.noticeClose = '';
    pointer(ack, acknowledge);
    pointer(close, () => { detail = null; draw(); });
    actions.append(ack, close);
    popup.append(heading, title, body, sent, note, actions);
    document.body.append(popup);
  }
  function draw() {
    const bar = document.querySelector('[data-notice-bar]');
    if (!bar) return;
    const items = notices().items;
    bar.hidden = !items.length;
    if (items.length) {
      const first = items[0];
      bar.classList.toggle('notice-bar--important', first.weight === 'important');
      bar.querySelector('[data-notice-weight]').textContent = first.weight === 'important' ? '중요' : '공지';
      bar.querySelector('[data-notice-title]').textContent = first.title;
      const more = bar.querySelector('[data-notice-more]');
      more.textContent = items.length > 1 ? `· 외 ${items.length - 1}건` : '';
      more.hidden = items.length < 2;
      const open = bar.querySelector('[data-notice-open]');
      if (!open.noticeBound) {
        open.noticeBound = true;
        pointer(open, () => {
          const item = notices().items[0];
          if (!item) return;
          detail = item.id; draw();
          api.send('open_notice', { id: item.id }).catch(() => {});
        }, false);
      }
    }
    const item = find(detail) || held || pending();
    if (!item && !popup) return;
    if (!popup) makePopup();
    popup.hidden = !item;
    if (!item) return;
    const full = !!find(detail);
    popup.querySelector('[data-notice-heading]').textContent = item.weight === 'important' ? '중요 공지' : '공지';
    popup.querySelector('#notice-title').textContent = item.title;
    const body = popup.querySelector('[data-notice-body]');
    if (body.textContent !== item.body) { body.textContent = item.body; body.scrollTop = 0; }
    popup.querySelector('[data-notice-sent]').textContent = sentAt(item.sent_at);
    const note = popup.querySelector('[data-notice-error]');
    note.textContent = full ? '' : error; note.hidden = !note.textContent;
    const ack = popup.querySelector('[data-notice-ack]');
    ack.hidden = full; ack.disabled = busy;
    ack.textContent = queued ? '저장 중' : '확인';
    popup.querySelector('[data-notice-close]').hidden = !full;
  }
  async function acknowledge() {
    const item = held || pending();
    if (!item || busy) return;
    held = item; busy = true; error = ''; draw();
    const serial = ++request, connection = api;
    const alive = () => serial === request;
    const failed = () => {
      if (!alive()) return;
      busy = queued = false; error = '확인을 저장하지 못했습니다\n「확인」을 눌러 다시 시도하세요'; draw();
    };
    try {
      const reply = await connection.send('ack_notice', { id: item.id });
      if (!alive()) return;
      queued = reply?.ok === true && reply.queued === true;
      if (reply && reply.state) connection.take(reply.state);
      else if (!queued && (!reply || reply.ok !== false)) await connection.refresh();
      if (!alive()) return;
      if (queued) { render(connection.current(), connection); return; }
      if (reply && reply.ok === false || (connection.current().notices?.pending_ack || []).includes(item.id)) { failed(); return; }
      held = null; busy = false; error = ''; draw();
    } catch {
      failed();
    }
  }
  function render(next, connection) {
    api = connection;
    const identity = owner(next);
    if (scope !== identity) { scope = identity; detail = null; held = null; busy = queued = false; error = ''; request++; }
    state = next;
    if (held && (!find(held.id) || queued && !(notices().pending_ack || []).includes(held.id))) { held = null; busy = queued = false; error = ''; request++; }
    if (detail && !find(detail)) detail = null;
    draw();
  }
  return { render, sentAt };
})();
