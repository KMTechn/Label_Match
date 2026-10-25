'use strict';
// 셸 다리 KMShell.call(name, [args])(주인 w9appshell1012 셸 레인 — 10/13 넘김: shell/web/transport.js sha256 fdb207a3…, 내용 그대로).
// pywebview(js_api) · WebView2 직접(postMessage) 둘 다. 처리 → 화면 알림은 host 가 KMHost.push 를 부르거나 shellpush 사건(frame.js 가 받음).
// A native host must set the mode before this script. chrome.webview alone
// is also present in pywebview before its API exists and never selects native.
window.KMShell = (() => {
  const mode = window.__KMSHELL_TRANSPORT__ === 'native' ? 'native' : 'pywebview';
  const startedAt = performance.timeOrigin + performance.now();
  const queued = [], pending = new Map(), events = [];
  let serial = 0, readyAt = null, failure = null, timer;
  const apiAvailable = () => window.pywebview?.api && Object.values(window.pywebview.api).some(value => typeof value === 'function');
  function send(item) {
    try {
      if (mode === 'pywebview') {
        const api = window.pywebview?.api;
        if (!api || typeof api[item.name] !== 'function') throw new Error(`화면을 열지 못했습니다\n현장 관리자에게 알리세요\n코드: ${item.name}`);
        item.resolve(api[item.name](...item.args));
      } else {
        const id = ++serial;
        pending.set(id,item);
        try { window.chrome.webview.postMessage({id,name:item.name,args:item.args}); }
        catch (error) { pending.delete(id); throw error; }
      }
    } catch (error) { item.reject(error); }
  }
  function markReady() {
    if (readyAt !== null) return;
    readyAt = performance.timeOrigin + performance.now();
    failure = null; clearTimeout(timer);
    for (const item of queued.splice(0)) send(item);
    window.dispatchEvent(new CustomEvent('kmshellready',{detail:{mode,readyAt}}));
  }
  if (mode === 'native' && window.chrome?.webview) {
    window.chrome.webview.addEventListener('message', event => {
      const reply = event.data;
      if (!reply || typeof reply !== 'object') return;
      if (reply.kind && reply.id === undefined) { window.dispatchEvent(new CustomEvent('shellpush',{detail:reply})); return; }
      const waiter = pending.get(reply.id);
      if (!waiter) return;
      pending.delete(reply.id);
      if (reply.error) waiter.reject(new Error(reply.error));
      else waiter.resolve(reply.result);
    });
  }
  if (mode === 'pywebview') window.addEventListener('pywebviewready',markReady);
  timer = setTimeout(() => {
    failure = new Error('화면 연결을 준비하지 못했습니다\n현장 관리자에게 알리세요');
    const detail = {ev:'transport_timeout',mode,t:performance.timeOrigin + performance.now(),wait_ms:15000,message:failure.message};
    events.push(detail);
    for (const item of queued.splice(0)) item.reject(failure);
    window.dispatchEvent(new CustomEvent('kmshellerror',{detail}));
  },15000);
  if ((mode === 'pywebview' && apiAvailable()) || (mode === 'native' && window.chrome?.webview)) markReady();
  function call(name, args = []) {
    return new Promise((resolve,reject) => {
      if (typeof name !== 'string' || !Array.isArray(args)) { reject(new TypeError('invalid bridge call')); return; }
      if (readyAt === null && mode === 'pywebview' && typeof window.pywebview?.api?.[name] === 'function') markReady();
      const item = {name,args,resolve,reject};
      if (readyAt !== null) send(item);
      else if (failure) reject(failure);
      else queued.push(item);
    });
  }
  return {call,status:() => ({mode,startedAt,readyAt,queued:queued.length,timedOut:!!failure,events:events.slice()})};
})();
