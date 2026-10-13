'use strict';
// 현장 앱 스캔 받이(주인 w9appshell1012 셸 레인 — 10/13 넘김: shell/web/sink.js 셸 v3.10 sha256 bc572234…, 이름만 KMScanSink → AppScan).
// 10/13 틀 고침(셸 원본과 다름 — 셸 다음 판 몫): 키 기록에 key · code 값 0 · 비밀 판이 열리면 칸 밖 키 버림 · 빈 버퍼 Tab 은 초점 이동 그대로.
// 키 조립 · F12 판정 증거(v2.0.152 사건 순서, F12-V152-RULES.md)는 여기서만 — 앱 화면 JS 에 keydown 조립 0(APP-SCREENS 불변식 4).
// 판정 규칙 · meta 꼴은 처리 층(bridge.py · event_order.py)이 읽으므로 셸 레인과 함께만 고친다. 붙이기 = frame.js(AppFrame.start)가 첫 state 뒤 start 한 번.
// Framing correction: IW components_v2.py:665,765–833 @a211ea4;
// RW ui/scan_entry.py @347a50e3. Enter/Tab in both; no idle timer.
// F12 frame: ui/integration.py:7589–7684 @198add5 (V152 event order).
window.AppScan = (() => {
  const shifted = {Digit1:'!',Digit2:'@',Digit3:'#',Digit4:'$',Digit5:'%',Digit6:'^',Digit7:'&',Digit8:'*',Digit9:'(',Digit0:')',
    Minus:'_',Equal:'+',BracketLeft:'{',BracketRight:'}',Backslash:'|',Semicolon:':',Quote:'"',Comma:'<',Period:'>',Slash:'?',Backquote:'~'};
  const plain = {Minus:'-',Equal:'=',BracketLeft:'[',BracketRight:']',Backslash:'\\',Semicolon:';',Quote:"'",Comma:',',Period:'.',Slash:'/',Backquote:'`',Space:' '};
  const num = {NumpadDecimal:'.',NumpadAdd:'+',NumpadSubtract:'-',NumpadMultiply:'*',NumpadDivide:'/'};
  let buffer = '', seq = 0, first = null, nKeys = 0, profile = 'rw', started = false;
  let paths = {key:0, code:0, keypress:0, paste:0};
  let keys = [], events = [];
  let pressed = false, generation = 0, pressedAt = null, releasedAt = null, frame = null, focusRaise = null;
  let outside = false, waiting = null, focusSerial = 0, attachedAt = null, recovered = false, recoveryFirst = true;
  const stamp = event => performance.timeOrigin + event.timeStamp;
  const mod = (event, name) => typeof event.getModifierState === 'function' && !!event.getModifierState(name);
  function editable(element) {
    if (!element) return false;
    if (element.isContentEditable || element.tagName === 'TEXTAREA') return true;
    return element.tagName === 'INPUT' && !['button','submit','reset','checkbox','radio','range','color','file','hidden','image'].includes(element.type);
  }
  function physical(event) {
    const {code, shiftKey} = event;
    if (/^Key[A-Z]$/.test(code)) return shiftKey !== mod(event, 'CapsLock') ? code.slice(3) : code.slice(3).toLowerCase();
    if (/^Digit[0-9]$/.test(code)) return shiftKey ? shifted[code] : code.slice(5);
    if (code in plain) return shiftKey && code in shifted ? shifted[code] : plain[code];
    if (mod(event, 'NumLock')) {
      if (/^Numpad[0-9]$/.test(code)) return code.slice(6);
      if (code in num) return num[code];
    }
    return null;
  }
  function logKey(event) {
    // 칸 안팎 모두 글자 키의 key · code 값을 남기지 않는다(PIN 이 칸 밖에서 쳐져도 셸 기록에 0) — 순서 · 시각 · 표시만, 이름 키(Enter · Tab · Escape · Backspace · F12)만 이름.
    keys.push({type:event.type,t:stamp(event),editing:event.editing,...(['Enter','Escape','Tab','Backspace','F12'].includes(event.key) ? {key:event.key} : {})});
  }
  // 비밀 판(PIN · password 칸이 보이는 판)이 열려 있으면 칸 밖 키로 스캔을 조립하지 않는다 — 버림, scan() 0. 판이 닫히면 다시.
  const secret = () => [...document.querySelectorAll('input[type=password], [data-pin]')].some(node => !node.closest('[hidden]'));
  // M60: 초점이 조작 요소(단추 · 링크 · 입력 · 고르기)에 있고 버퍼가 비었으면 Enter · Space 는 그 요소 몫(눌림) — body 빈 Enter 는 frame 「확인」 길
  const control = () => !!document.activeElement && document.activeElement.matches('button, a[href], input, select, summary, [role=button]');
  const owns = event => !buffer && (event.key === 'Enter' || event.code === 'NumpadEnter' || event.key === ' ') && control();
  function eventLog(ev, fields = {}) { events.push({ev, ...fields}); }
  function flush() {
    if (keys.length) { events.push({ev:'keys',batch:keys}); keys = []; }
    if (!events.length) return;
    const batch = events; events = [];
    KMShell.call('log_batch',[batch]).catch(() => { events = batch.concat(events); });
  }
  function evidenceChanged() { document.dispatchEvent(new CustomEvent('scanevidence',{detail:frame ? {...frame} : null})); }
  function observe(kind, at) {
    if (kind === 'clear') { frame = null; evidenceChanged(); return null; }
    if (!frame) {
      frame = {source:'webview_event_order',pedal_pressed:pressed,press_generation:generation,
        first_character_at:kind === 'character' ? at : null,pressed_at:pressed ? pressedAt : null,released_at:null};
      if (pressed && focusRaise && focusRaise.generation === generation) frame.focus_return_raise = {...focusRaise};
    }
    if (kind === 'enter') { const result = {...frame,enter_at:at}; frame = null; evidenceChanged(); return result; }
    evidenceChanged(); return null;
  }
  function pedalDown(at) {
    if (!pressed) { generation++; pressedAt = at; }
    pressed = true;
    if (frame) Object.assign(frame,{pedal_pressed:true,press_generation:generation,pressed_at:pressedAt});
    evidenceChanged();
  }
  function pedalUp(at) {
    pressed = false; releasedAt = at;
    if (frame) frame.released_at = at;
    evidenceChanged();
  }
  function changed() { document.dispatchEvent(new CustomEvent('scanbuffer',{detail:{text:buffer}})); }
  function append(text, path, event) {
    if (!buffer) first = stamp(event);
    if (path === 'paste' || /[^\s]/.test(text)) observe('character',stamp(event));
    buffer += text; paths[path] += 1; changed();
  }
  function completePHS2(text) {
    // Match Python str.strip(), including NEL and excluding JavaScript's BOM.
    const strip = value => value.replace(/^[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+|[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/g,'');
    const order = ['PHS','SRC','ITG','CLC','LBL','HSH'];
    const parts = strip(text).split('|'), fields = {};
    if (parts.length !== order.length) return false;
    for (let i=0;i<parts.length;i++) {
      const pair = parts[i].split('=');
      if (pair.length !== 2 || pair[0] !== order[i] || !pair[1] || pair[1] !== strip(pair[1]) || /[\x00-\x1f]/.test(pair[1])) return false;
      fields[pair[0]] = pair[1];
    }
    return fields.PHS === '2' && fields.SRC === 'KMTECH_INPUT_TAG' &&
      !!strip(fields.ITG) && !!strip(fields.CLC) && !!strip(fields.LBL) && /^[0-9a-f]{16}$/i.test(fields.HSH);
  }
  function submit(event) {
    if (!buffer) return;
    if (profile === 'iw' && buffer.startsWith('PHS=2|SRC=KMTECH_INPUT_TAG|') && !completePHS2(buffer)) {
      eventLog('incomplete_phs2',{text:buffer}); flush(); return;
    }
    const text = buffer;
    const meta = {seq:++seq,at:stamp(event),source:'scanner',t_first:first,t_enter:stamp(event),n_keys:nKeys,paths:{...paths},
      evidence:observe('enter',stamp(event)),t_attach:attachedAt,recovered,recovery_first_scan:recoveryFirst};
    recoveryFirst = false;
    buffer = ''; first = null; nKeys = 0; paths = {key:0,code:0,keypress:0,paste:0};
    // Dispatch immediately, without awaiting any previous scan or logging call.
    const response = KMShell.call('scan',[text,meta]);
    document.dispatchEvent(new CustomEvent('scanpending',{detail:{seq:meta.seq,text}}));
    changed(); flush();
    response.then(result => document.dispatchEvent(new CustomEvent('scanresult',{detail:{...result,text}})))
      .catch(error => document.dispatchEvent(new CustomEvent('scanresult',{detail:{seq:meta.seq,text,accepted:false,error:String(error)}})));
  }
  function down(event) {
    if (event.code === 'F12' || event.key === 'F12') {
      event.preventDefault(); pedalDown(stamp(event)); eventLog('f12',{type:event.type,repeat:event.repeat}); return;
    }
    if (event.editing) return;
    if (event.ctrlKey || event.metaKey) {
      if (event.code !== 'KeyV') event.preventDefault(); return;
    }
    if (event.altKey && /^Numpad[0-9]$/.test(event.code)) { eventLog('alt_numpad'); return; }
    if (owns(event)) return;
    const tab = event.code === 'Tab' || event.key === 'Tab';
    if (event.code === 'Enter' || event.code === 'NumpadEnter' || event.key === 'Enter' || tab && buffer) {
      event.preventDefault(); submit(event); return;
    }
    if (tab) return; // 빈 버퍼 Tab · Shift+Tab 은 초점 이동 그대로(키보드만으로 메뉴 · 판 단추) — 끝 Tab 은 버퍼가 있을 때만 먹음
    if (event.key === 'Backspace' || event.code === 'Backspace') {
      event.preventDefault(); buffer = Array.from(buffer).slice(0,-1).join(''); nKeys++;
      if (!buffer) { first = null; observe('clear',stamp(event)); }
      changed(); return;
    }
    const fromCode = physical(event);
    const fromKey = event.key.length === 1 && /^[\x20-\x7e]$/.test(event.key) && !event.isComposing && event.keyCode !== 229 ? event.key : null;
    if (fromKey !== null && fromCode !== null && fromKey !== fromCode) eventLog('key_diag',{t:stamp(event)/1000});
    const character = fromKey !== null ? fromKey : fromCode;
    event.preventDefault();
    if (character !== null) { nKeys++; append(character,fromKey !== null ? 'key' : 'code',event); }
  }
  function press(event) {
    if (event.editing || owns(event)) return;
    if (event.ctrlKey || event.metaKey || event.altKey || event.key.length !== 1 || event.key.charCodeAt(0) < 0x20) { event.preventDefault(); return; }
    event.preventDefault(); nKeys++; append(event.key,'keypress',event); eventLog('keypress_path');
  }
  function up(event) {
    if (event.code === 'F12' || event.key === 'F12') {
      event.preventDefault(); pedalUp(stamp(event)); eventLog('f12',{type:event.type,repeat:event.repeat});
    }
  }
  function paste(event) {
    if (event.editing) return;
    event.preventDefault(); append(event.pasteText,'paste',event);
  }
  function route(event) {
    ({keydown:down,keypress:press,keyup:up,paste})[event.type](event);
  }
  function capture(event) {
    const editing = editable(document.activeElement);
    if (!editing && event.code !== 'F12' && event.key !== 'F12' && secret()) { logKey({type:event.type,timeStamp:event.timeStamp,editing}); return; }
    const caps = mod(event,'CapsLock'), numlock = mod(event,'NumLock');
    const copy = {type:event.type,key:event.key || '',code:event.code || '',keyCode:event.keyCode,shiftKey:event.shiftKey,
      altKey:event.altKey,ctrlKey:event.ctrlKey,metaKey:event.metaKey,isComposing:event.isComposing,repeat:event.repeat,
      timeStamp:event.timeStamp,editing,getModifierState:name=>name === 'CapsLock' ? caps : name === 'NumLock' ? numlock : false,
      preventDefault:()=>event.preventDefault(),pasteText:event.type === 'paste' && !editing ? event.clipboardData.getData('text') : ''};
    if (event.type !== 'paste') logKey(copy);
    if (waiting) {
      if (event.code === 'F12' || event.key === 'F12') event.preventDefault();
      // Keep native editable input and Ctrl+V/Alt-number delivery available.
      if (!editing && !(event.type === 'keydown' && ((event.ctrlKey || event.metaKey) && event.code === 'KeyV' || event.altKey && /^Numpad[0-9]$/.test(event.code)))) event.preventDefault();
      waiting.queue.push(copy); return;
    }
    route(copy);
  }
  function finishFocus(token, result) {
    if (!waiting || waiting.token !== token) return;
    const pending = waiting; waiting = null; clearTimeout(pending.timer);
    const ms = performance.now() - pending.started;
    if (ms >= 200) result = null;
    if (result === true) {
      const at = performance.timeOrigin + performance.now(); pedalDown(at);
      focusRaise = {generation,at};
      if (frame) frame.focus_return_raise = {...focusRaise};
    }
    eventLog('f12_physical',{result,ms,phase:'focus_return'});
    eventLog('key_queue',{held:pending.queue.length,released_after_ms:ms});
    for (const event of pending.queue) route(event);
    flush();
  }
  function focusEvent(kind) {
    if (kind === 'blur') {
      if (waiting) finishFocus(waiting.token,null);
      outside = true; pressed = false; releasedAt = performance.timeOrigin + performance.now();
      return;
    }
    if (!outside || waiting) return;
    outside = false;
    const token = ++focusSerial;
    waiting = {token,started:performance.now(),queue:[],timer:setTimeout(()=>finishFocus(token,null),200)};
    KMShell.call('f12_physical',[]).then(reply=>finishFocus(token,reply && typeof reply === 'object' ? reply.result : reply))
      .catch(()=>finishFocus(token,null));
  }
  function start(selected, lastSeq, options = {}) {
    profile = selected; seq = lastSeq;
    if (started) return;
    started = true;
    attachedAt = performance.timeOrigin + performance.now(); recovered = !!options.recovery;
    for (const type of ['keydown','keypress','keyup','paste']) document.addEventListener(type,capture,true);
    window.addEventListener('blur',()=>focusEvent('blur')); window.addEventListener('focus',()=>focusEvent('focus'));
    setInterval(flush,250);
  }
  return {start,flush,editable,completePHS2,focusEvent,secret,control,state:() => ({buffer,seq,profile,paths:{...paths},evidence:frame ? {...frame} : null,
    pressed,generation,t_attach:attachedAt,recovered,queued:waiting ? waiting.queue.length : 0,log_keys:keys.length,log_events:events.length})};
})();
