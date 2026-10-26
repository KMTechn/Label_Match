// Shared scan sink; contract and pinned source rules: docs/spec/app-editing-scan.md.


'use strict';



window.AppScan = (() => {
  const shifted = {Digit1:'!',Digit2:'@',Digit3:'#',Digit4:'$',Digit5:'%',Digit6:'^',Digit7:'&',Digit8:'*',Digit9:'(',Digit0:')',
    Minus:'_',Equal:'+',BracketLeft:'{',BracketRight:'}',Backslash:'|',Semicolon:':',Quote:'"',Comma:'<',Period:'>',Slash:'?',Backquote:'~'};
  const plain = {Minus:'-',Equal:'=',BracketLeft:'[',BracketRight:']',Backslash:'\\',Semicolon:';',Quote:"'",Comma:',',Period:'.',Slash:'/',Backquote:'`',Space:' '};
  const num = {NumpadDecimal:'.',NumpadAdd:'+',NumpadSubtract:'-',NumpadMultiply:'*',NumpadDivide:'/'};
  let buffer = '', seq = 0, first = null, nKeys = 0, profile = 'rw', started = false;
  let paths = {key:0, code:0, keypress:0, paste:0};
  let incompleteTimer = null;
  let continuation = null;
  const phs2Head = 'PHS=2|SRC=KMTECH_INPUT_TAG|';
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



  let editingRun = null, editingSuffix = null;
  function editingSnapshot(node) {
    if ('value' in node) {
      const value = node.value, start = node.selectionStart, end = node.selectionEnd, direction = node.selectionDirection;
      const pinTimes = node.pinTimes, pinUntil = node.pinUntil;
      return () => {
        node.value = value;
        if (start !== null && start !== undefined) node.setSelectionRange(start, end, direction);
        if (pinTimes === undefined) delete node.pinTimes; else node.pinTimes = pinTimes;
        if (pinUntil === undefined) delete node.pinUntil; else node.pinUntil = pinUntil;
      };
    }
    const contents = [...node.childNodes].map(child => child.cloneNode(true));
    const selection = window.getSelection();
    const path = child => {
      const steps = [];
      while (child && child !== node) { steps.unshift([...child.parentNode.childNodes].indexOf(child)); child = child.parentNode; }
      return child === node ? steps : null;
    };
    const anchor = selection && path(selection.anchorNode), focus = selection && path(selection.focusNode);
    const a = selection && selection.anchorOffset, f = selection && selection.focusOffset;
    return () => {
      node.replaceChildren(...contents);
      const at = steps => steps.reduce((child, index) => child.childNodes[index], node);
      if (anchor && focus) window.getSelection().setBaseAndExtent(at(anchor), a, at(focus), f);
    };
  }
  function editingCapture(event) {
    const suffix = event.key === 'Enter' || event.key === 'Tab' || event.code === 'NumpadEnter';
    if (event.type !== 'keydown') {
      if (editingSuffix && suffix && editingSuffix === event.key) {
        event.preventDefault(); event.stopImmediatePropagation();
        if (event.type === 'keyup') editingSuffix = null;
      }
      return;
    }
    editingSuffix = null;
    if (event.key === 'F12' || event.code === 'F12') return;
    if (event.key === 'Shift') return;
    const node = document.activeElement, at = event.timeStamp;
    const into = node && node.closest('[data-scan-into]');
    if (!editable(node) || event.ctrlKey || event.metaKey || event.altKey || event.isComposing || event.keyCode === 229 || event.repeat) {
      editingRun = null; return;
    }
    if (suffix) {
      const run = editingRun; editingRun = null;
      if (!run || run.node !== node || run.count < 6 || at - run.last > 40 || at < run.last) return;
      if (run.into) {
        if (run.replace) {
          node.value = run.part();
          node.setSelectionRange(node.value.length,node.value.length);
        }
        node.scanFromScanner = true;
        return;
      }
      run.restore();
      event.preventDefault(); event.stopImmediatePropagation(); editingSuffix = event.key;
      document.dispatchEvent(new CustomEvent('scanblocked', {detail:{n_keys:run.count,reason:'editing'}}));
      try {
        const sheet = node.closest('[data-sheet]');
        Promise.resolve(KMShell.call('scan_blocked', [{reason:sheet && !sheet.hidden ? 'sheet' : 'field',n_keys:run.count}])).catch(()=>{});
      } catch { /* Optional shell sound: no retry or additional notice. */ }
      return;
    }
    const printable = event.key && event.key.length === 1 && event.key.charCodeAt(0) >= 0x20 || physical(event) !== null;
    if (!printable) { editingRun = null; return; }
    if (!editingRun || editingRun.node !== node || at - editingRun.last > 40 || at < editingRun.last) {
      const start = node.selectionStart, suffixLength = node.value?.length - node.selectionEnd;
      editingRun = {node, restore:editingSnapshot(node), count:0, last:at,into:!!into,replace:!!node.scanFromScanner,
        part:()=>node.value.slice(start,node.value.length - suffixLength)};
    }
    if (into) node.scanFromScanner = false;
    editingRun.count++; editingRun.last = at;
  }
  // ISO 15434 controls belong only to the scan sink, never editable fields.
  function scanControl(event) {
    if (event.metaKey) return null;
    if (/^[\x04\x1c-\x1f]$/.test(event.key)) return event.key;
    if (!event.ctrlKey || event.altKey) return null;
    if (event.code === 'BracketRight' || event.key === ']') return '\x1d';
    if (event.key === '^' || event.code === 'Digit6' && event.shiftKey) return '\x1e';
    if (event.code === 'Backslash' || event.key === '\\') return '\x1c';
    if (event.key === '_' || event.code === 'Minus' && event.shiftKey) return '\x1f';
    return null;
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
    keys.push({type:event.type,t:stamp(event),editing:event.editing,...(['Enter','Escape','Tab','Backspace','F12'].includes(event.key) ? {key:event.key} : {})});
  }
  const secret = () => [...document.querySelectorAll('input[type=password], [data-pin]')].some(node => !node.closest('[hidden]'));
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
  function newFrame(at) {
    const value = {source:'webview_event_order',pedal_pressed:pressed,press_generation:generation,
      first_character_at:at,pressed_at:pressed ? pressedAt : null,released_at:null};
    if (pressed && focusRaise && focusRaise.generation === generation) value.focus_return_raise = {...focusRaise};
    return value;
  }
  function observe(kind, at) {
    if (kind === 'clear') { frame = null; evidenceChanged(); return null; }
    if (!frame) {
      frame = newFrame(kind === 'character' ? at : null);
    }
    if (kind === 'enter') { const result = {...frame,enter_at:at}; frame = null; evidenceChanged(); return result; }
    evidenceChanged(); return null;
  }
  function pedalDown(at) {
    if (!pressed) { generation++; pressedAt = at; }
    pressed = true;
    if (frame) Object.assign(frame,{pedal_pressed:true,press_generation:generation,pressed_at:pressedAt});
    if (continuation) for (const part of continuation) Object.assign(part.frame,{pedal_pressed:true,press_generation:generation,pressed_at:pressedAt});
    evidenceChanged();
  }
  function pedalUp(at) {
    pressed = false; releasedAt = at;
    if (frame) frame.released_at = at;
    if (continuation) for (const part of continuation) part.frame.released_at = at;
    evidenceChanged();
  }
  function changed() { document.dispatchEvent(new CustomEvent('scanbuffer',{detail:{text:buffer}})); }
  function clearBuffer() {
    buffer = ''; first = null; nKeys = 0; paths = {key:0,code:0,keypress:0,paste:0};
    continuation = null;
  }
  function cancelIncomplete() {
    if (incompleteTimer !== null) clearTimeout(incompleteTimer);
    incompleteTimer = null;
  }
  function armIncomplete(at = performance.now()) {
    cancelIncomplete();
    incompleteTimer = setTimeout(() => {
      incompleteTimer = null;
      if (!buffer || profile !== 'iw') return;
      const count = nKeys;
      const length = buffer.length;
      clearBuffer(); observe('clear',performance.timeOrigin + performance.now()); changed();
      eventLog('incomplete_phs2_dropped',{length}); flush();

      if (window.AppSheet) {
        window.AppSheet.blocked();
        const band = document.querySelector('[data-scan-blocked]');
        if (band) {
          if (!band.classList.contains('sheet-scan-blocked')) band.style.display = 'block';
          band.replaceChildren(window.AppFrame.el('div','','방금 읽힘이 끊겼습니다'),window.AppFrame.el('div','','현품표를 다시 찍으세요'));
          window.AppSheet.blocked();
        }
      }
      try { Promise.resolve(KMShell.call('scan_blocked',[{reason:'field',n_keys:count}])).catch(()=>{}); }
      catch { /* Optional shell sound follows the existing blocked-input rule. */ }
    },Math.max(0,300 - (performance.now() - at)));
  }
  function append(text, path, event) {
    if (continuation) continuation.push({start:buffer.length,end:buffer.length + text.length,at:stamp(event),
      keys:nKeys - (path === 'paste' ? 0 : 1),paths:{...paths},frame:newFrame(stamp(event))});
    if (!buffer) first = stamp(event);
    if (path === 'paste' || /[^\s]/.test(text)) observe('character',stamp(event));
    buffer += text; paths[path] += 1; changed();
    if (continuation) {
      const next = buffer.lastIndexOf(phs2Head);
      if (next > 0) {
        const part = continuation.find(part => part.start <= next && part.end > next);
        if (part) {
          buffer = buffer.slice(next); first = part.at; nKeys -= part.keys;
          for (const name of Object.keys(paths)) paths[name] -= part.paths[name];
          frame = part.frame; continuation = [];
          eventLog('incomplete_phs2_dropped',{length:next}); flush(); evidenceChanged(); changed();
        }
      }
      continuation = continuation.filter(part => part.end > buffer.length - phs2Head.length);
    }
    if (incompleteTimer !== null) armIncomplete(event.timeStamp);
  }
  function completePHS2(text) {

    const strip = value => value.replace(/^[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+|[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/g,'');
    const order = ['PHS','SRC','ITG','CLC','LBL','HSH'];
    const parts = strip(text).split('|'), fields = {};
    if (parts.length === order.length + 1) order.push('QT');
    if (parts.length !== order.length) return false;
    for (let i=0;i<parts.length;i++) {
      const pair = parts[i].split('=');
      if (pair.length !== 2 || pair[0] !== order[i] || !pair[1] || pair[1] !== strip(pair[1]) || /[\x00-\x1f]/.test(pair[1])) return false;
      fields[pair[0]] = pair[1];
    }
    if (fields.QT !== undefined && !/^[1-9][0-9]{0,3}$/.test(fields.QT)) return false;
    return fields.PHS === '2' && fields.SRC === 'KMTECH_INPUT_TAG' &&
      !!strip(fields.ITG) && !!strip(fields.CLC) && !!strip(fields.LBL) && /^[0-9a-f]{16}$/i.test(fields.HSH);
  }
  function submit(event) {
    if (!buffer) return;
    if (profile === 'iw' && buffer.startsWith(phs2Head) && !completePHS2(buffer)) {
      continuation ||= [];
      eventLog('incomplete_phs2',{length:buffer.length}); flush(); armIncomplete(event.timeStamp); return;
    }
    cancelIncomplete();
    const text = buffer;
    const meta = {seq:++seq,at:stamp(event),source:'scanner',t_first:first,t_enter:stamp(event),n_keys:nKeys,paths:{...paths},
      evidence:observe('enter',stamp(event)),t_attach:attachedAt,recovered,recovery_first_scan:recoveryFirst};
    recoveryFirst = false;
    clearBuffer();

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
    const control = scanControl(event);
    if (control !== null) { event.preventDefault(); nKeys++; append(control,'key',event); return; }
    if (event.ctrlKey || event.metaKey) {
      if (event.code !== 'KeyV') event.preventDefault(); return;
    }
    if (event.altKey && /^Numpad[0-9]$/.test(event.code)) { eventLog('alt_numpad'); return; }
    if (event.altKey) { event.preventDefault(); return; }
    if (owns(event)) return;
    const tab = event.code === 'Tab' || event.key === 'Tab';
    if (event.code === 'Enter' || event.code === 'NumpadEnter' || event.key === 'Enter' || tab && buffer) {
      event.preventDefault(); submit(event); return;
    }
    if (tab) return;
    if (event.key === 'Backspace' || event.code === 'Backspace') {
      event.preventDefault(); buffer = Array.from(buffer).slice(0,-1).join(''); nKeys++;
      if (continuation) continuation = continuation.filter(part=>part.start < buffer.length).map(part=>({...part,end:Math.min(part.end,buffer.length)}));
      if (!buffer) { clearBuffer(); cancelIncomplete(); observe('clear',stamp(event)); }
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
    const control = scanControl(event);
    if (control !== null) { event.preventDefault(); nKeys++; append(control,'keypress',event); return; }
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
    if (incompleteTimer !== null && !editing && event.type === 'keydown') armIncomplete(event.timeStamp);
    if (waiting) {
      if (event.code === 'F12' || event.key === 'F12') event.preventDefault();

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
      if (continuation) for (const part of continuation) part.frame.focus_return_raise = {...focusRaise};
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
    if (profile !== 'iw') { cancelIncomplete(); continuation = null; }
    if (started) return;
    started = true;
    attachedAt = performance.timeOrigin + performance.now(); recovered = !!options.recovery;


    for (const type of ['keydown','keypress','keyup']) window.addEventListener(type,editingCapture,true);
    for (const type of ['paste','compositionstart','focusout','pointerdown']) window.addEventListener(type,()=>{ editingRun = null; },true);
    window.addEventListener('input',event=>{ if (event.target.closest('[data-scan-into]')) event.target.scanFromScanner = false; },true);
    for (const type of ['keydown','keypress','keyup','paste']) document.addEventListener(type,capture,true);
    window.addEventListener('blur',()=>focusEvent('blur')); window.addEventListener('focus',()=>focusEvent('focus'));
    setInterval(flush,250);
  }
  return {start,flush,editable,completePHS2,focusEvent,secret,control,state:() => ({buffer,seq,profile,paths:{...paths},evidence:frame ? {...frame} : null,
    pressed,generation,t_attach:attachedAt,recovered,queued:waiting ? waiting.queue.length : 0,log_keys:keys.length,log_events:events.length})};
})();
