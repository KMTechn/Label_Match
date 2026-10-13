'use strict';
(() => {
  let connected = false, connecting = false, hostActive = true, active = true, renderedSeq = -1, renderedRequestSeq = -1, stoppedMessage = null;
  const pending = new Map();
  const $ = id => document.getElementById(id);
  const testControl = new URLSearchParams(location.search || location.hash.slice(1)).get('test') === '1';
  const milestones = {timeOrigin:performance.timeOrigin,domContentLoaded:null,transportReadyAt:null,transportMode:null,firstPaint:null,firstContentfulPaint:null,readyAt:null};
  document.addEventListener('DOMContentLoaded',() => { milestones.domContentLoaded = performance.timeOrigin + performance.now(); });
  function focus(state) {
    if (state === 'win_activated') hostActive = true;
    if (state === 'win_deactivated') hostActive = false;
    if (state === 'win_activated') KMScanSink.focusEvent('focus');
    if (state === 'win_deactivated') KMScanSink.focusEvent('blur');
    active = hostActive && document.hasFocus() && document.visibilityState === 'visible';
    $('focus-warning').hidden = active;
    $('focus-status').textContent = stoppedMessage || (connected ? (active ? '스캔 받을 준비 됨' : '창을 한 번 누르세요') :
      KMShell.status().timedOut ? '화면 연결 준비가 15초를 넘었습니다' : '화면 연결을 기다리고 있습니다');
    if (connected) KMShell.call('log_batch',[[{ev:'focus',state}]]).catch(() => {});
  }
  function pedal(state) {
    $('pedal').textContent = state.pressed ? 'F12 밟음' : 'F12 뗌';
    $('pedal').className = `km-chip ${state.pressed ? 'km-chip--danger' : 'km-chip--ok'}`;
  }
  function row(item) {
    const element = document.createElement('div');
    element.className = 'km-scanrow';
    const number = document.createElement('span'); number.textContent = String(item.seq);
    const text = document.createElement('span'); text.className = 'scan-text'; text.textContent = item.text;
    const verdict = document.createElement('span'); verdict.className = `km-chip ${item.verdict === 'ng' ? 'km-chip--danger' : 'km-chip--ok'}`;
    verdict.textContent = item.pending ? '처리 중' : item.error || item.accepted === false ? '다시 스캔하세요' : item.verdict === 'ng' ? '불량' : '양품';
    element.append(number,text,verdict); return element;
  }
  function render(state) {
    const cursor = state.last_request_seq ?? state.last_seq;
    if (state.last_seq < renderedSeq || cursor < renderedRequestSeq) return false;
    if (state.last_seq === renderedSeq && cursor === renderedRequestSeq) { pedal(state.pedal); return true; }
    renderedSeq = state.last_seq;
    renderedRequestSeq = cursor;
    $('count').textContent = `총 ${state.count}개 · 양품 ${state.ok}개 · 불량 ${state.ng}개`;
    $('recent').replaceChildren(...state.recent.slice().reverse().map(row));
    const last = state.recent.at(-1);
    if (last) { $('last-result').className = `km-chip ${last.verdict === 'ng' ? 'km-chip--danger' : 'km-chip--ok'}`; $('last-result').textContent = last.verdict === 'ng' ? '방금 불량' : '방금 양품'; }
    pedal(state.pedal);
    return true;
  }
  async function connect() {
    if (connected || connecting || stoppedMessage) return;
    connecting = true;
    try {
      const state = await KMShell.call('state',[]);
      if (stoppedMessage) return;
      connected = true;
      $('app-name').textContent = state.profile === 'iw' ? '검사' : '리워크';
      $('pedal').hidden = state.profile !== 'iw';
      $('scan-verdict').hidden = state.profile !== 'iw';
      $('test-controls').hidden = !testControl;
      render(state); KMScanSink.start(state.profile,state.last_request_seq ?? state.last_seq,{recovery:state.recovery});
      const paints = performance.getEntriesByType('paint');
      milestones.firstPaint = paints.find(p => p.name === 'first-paint')?.startTime ?? null;
      milestones.firstContentfulPaint = paints.find(p => p.name === 'first-contentful-paint')?.startTime ?? null;
      const transport = KMShell.status();
      milestones.transportReadyAt = transport.readyAt; milestones.transportMode = transport.mode;
      if (transport.events.length) await KMShell.call('log_batch',[transport.events]);
      milestones.readyAt = performance.timeOrigin + performance.now();
      await KMShell.call('ready',[milestones]);
      focus('page_focus');
      document.dispatchEvent(new CustomEvent('shellready'));
    } catch (error) {
      connected = false;
      $('focus-status').textContent = stoppedMessage || (KMShell.status().timedOut ? '화면 연결 준비가 15초를 넘었습니다' : '화면 연결을 기다리고 있습니다');
      if (!stoppedMessage && !KMShell.status().timedOut) setTimeout(connect,100);
    }
    finally { connecting = false; }
  }
  document.addEventListener('scanbuffer',event => { $('buffer').textContent = event.detail.text || '바코드를 스캔하세요'; });
  document.addEventListener('scanpending',event => {
    pending.set(event.detail.seq,event.detail); $('recent').prepend(row({...event.detail,pending:true}));
    while ($('recent').children.length > 8) $('recent').lastElementChild.remove();
  });
  async function result(value) {
    pending.delete(value.seq);
    // A push may have already supplied this request's state. Its API reply
    // must not fetch state again or rebuild the same eight recent rows.
    if (!value.state && value.seq <= renderedRequestSeq) return;
    const state = value.state || await KMShell.call('state',[]);
    if (!render(state)) return;
    for (const item of pending.values()) {
      if (item.seq > (state.last_request_seq ?? state.last_seq)) $('recent').prepend(row({...item,pending:true}));
    }
    while ($('recent').children.length > 8) $('recent').lastElementChild.remove();
    if (!value.accepted) $('last-result').textContent = value.message || '기록 확인 필요 — 다시 확인하세요';
  }
  document.addEventListener('scanresult',event => { result(event.detail); });
  document.addEventListener('scanevidence',event => {
    const evidence = event.detail;
    $('scan-verdict').textContent = !evidence ? '새 스캔을 기다립니다' : evidence.pedal_pressed ? '이번 스캔 = 불량으로 잡힘' : '이번 스캔 = 양품으로 잡힘';
    $('scan-verdict').className = `km-chip ${evidence && evidence.pedal_pressed ? 'km-chip--danger' : 'km-chip--ok'}`;
  });
  window.addEventListener('focus',() => focus('page_focus'));
  window.addEventListener('blur',() => focus('page_blur'));
  document.addEventListener('visibilitychange',() => focus(document.visibilityState === 'visible' ? 'page_focus' : 'page_blur'));
  document.addEventListener('click',event => {
    if (event.target.closest('button')) {
      document.activeElement.blur();
      $('button-result').textContent = '확인했습니다';
    }
  });
  function churnList({rows,times} = {}) {
    if (!testControl) throw new Error('test control not enabled');
    if (!Number.isInteger(rows) || !Number.isInteger(times) || rows < 1 || rows > 1000 || times < 1 || times > 100) throw new Error('invalid churn dimensions');
    const start = performance.now();
    for (let cycle=0;cycle<times;cycle++) {
      const panel = document.createElement('section'); panel.hidden = true; panel.className = 'km-panel';
      panel.dataset.testChurn = '1';
      try {
        const list = document.createElement('div'); list.className = 'km-scanlist';
        for (let i=0;i<rows;i++) list.append(row({seq:i+1,text:`CHURN-${i+1}`,verdict:'ok'}));
        panel.append(list); document.body.append(panel);
      } finally { panel.remove(); }
    }
    const walker = document.createTreeWalker(document,NodeFilter.SHOW_ALL);
    let domNodes = 1; while (walker.nextNode()) domNodes++;
    return {rows,times,ms:performance.now()-start,dom_nodes_after:domNodes};
  }
  window.KMHost = {
    focus, pedal,
    transportStopped: message => { stoppedMessage = message; connected = false; $('focus-status').textContent = message; },
    push: event => {
      if (event.kind === 'pedal') pedal(event);
      else if (event.kind === 'scan_result') result(event);
      else if (event.kind === 'focus_state') focus(event.state);
      else if (event.kind === 'alert') $('last-result').textContent = event.message;
    },
    control: (op, options) => {
      if (op === 'churn_list') return churnList(options);
      if (op === 'focus_input') { $('test-input').focus(); return true; }
      if (op === 'focus_sink') { document.activeElement.blur(); return true; }
      if (op === 'read_input') return $('test-input').value;
      if (op === 'click_button') { $('test-button').click(); return true; }
      if (op === 'rect') {
        const rect = id => { const r = $(id).getBoundingClientRect(); return {x:r.x,y:r.y,width:r.width,height:r.height}; };
        return {input:rect('test-input'),button:rect('test-button'),viewport:{width:innerWidth,height:innerHeight},devicePixelRatio};
      }
      if (op === 'get_state') return {sink:KMScanSink.state(),input:$('test-input').value,active,connected,button:$('button-result').textContent,
        diagnostics:{pending_scans:pending.size,rendered_request_seq:renderedRequestSeq},
        resource_urls:performance.getEntriesByType('resource').map(entry=>entry.name).concat([location.href])};
      throw new Error('unknown control operation');
    }
  };
  window.addEventListener('shellpush',event=>KMHost.push(event.detail));
  window.addEventListener('kmshellready',connect);
  window.addEventListener('kmshellerror',event => { $('focus-status').textContent = stoppedMessage || event.detail.message; });
  window.addEventListener('DOMContentLoaded',connect);
  if (document.readyState !== 'loading') {
    const navigation = performance.getEntriesByType('navigation')[0];
    milestones.domContentLoaded = navigation?.domContentLoadedEventEnd ? performance.timeOrigin + navigation.domContentLoadedEventEnd : performance.timeOrigin + performance.now();
    connect();
  }
})();
