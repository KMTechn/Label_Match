// Imported from WorkerAnalysisGUI-web 5748d155d3984e36a5d15101cf6ce79d49cc1a24; only the scan.js path is supplied by the app test.
// No browser, credentials or field state. Only boolean observations leave this VM.
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source = readFileSync(process.argv[2], 'utf8');
const checks = {};
function run({gap=10, suffix='Enter', count=8, into=false, waiting=false, type='password', paste=false, boundary=false, shift=false, interrupt=false, legacy=false, sheet=false, soundReply='ok'}={}) {
  const listeners = {}, calls = [], details = [];
  const field = {tagName:type === 'textarea' ? 'TEXTAREA' : 'INPUT', type, value:'', selectionStart:0, selectionEnd:0,
    selectionDirection:'forward', matches:()=>false, closest:selector=>selector === '[data-scan-into]' && into ? field : selector === '[data-sheet]' && sheet ? {hidden:false} : null,
    setSelectionRange(a,b,d){this.selectionStart=a;this.selectionEnd=b;this.selectionDirection=d;}};
  // Private sample and selection never enter stdout or assertion messages.
  field.value = String.fromCharCode(97,98,99,100); field.selectionStart=1; field.selectionEnd=3;
  const original=field.value;
  const document = {activeElement:field, querySelectorAll:()=>[], addEventListener:(name,fn)=>{(listeners['d'+name] ||= []).push(fn);},
    dispatchEvent:event=>details.push(event.detail)};
  const window = {addEventListener:(name,fn)=>{(listeners['w'+name] ||= []).push(fn);}};
  const context = {window,document,performance:{timeOrigin:0,now:()=>0},CustomEvent:class {constructor(type,props){this.type=type;this.detail=props.detail;}},
    KMShell:{call:(name,args)=>{
      calls.push({name,args});
      if(name==='scan_blocked'){
        if(soundReply==='throw')throw new Error('unavailable');
        if(soundReply==='reject')return Promise.reject(new Error('unknown command'));
        if(soundReply==='pending')return new Promise(()=>{});
        return Promise.resolve({ok:soundReply==='ok'});
      }
      return name === 'f12_physical' ? new Promise(()=>{}) : Promise.resolve({});
    }},
    setInterval:()=>0,setTimeout:()=>0,clearTimeout:()=>{}};
  vm.runInNewContext(source,context); window.AppScan.start('iw',0);
  if(waiting){window.AppScan.focusEvent('blur');window.AppScan.focusEvent('focus');}
  let handler=0,clock=100,blocked=false;
  function emit(type,key,code=key){
    const event={type,key,code,timeStamp:clock,target:field,preventDefault(){this.defaultPrevented=true;},
      stopImmediatePropagation(){this.stopped=true;},getModifierState:()=>false};
    for(const fn of listeners['w'+type] || []){fn(event);if(event.stopped)break;}
    if(!event.stopped)for(const fn of listeners['d'+type] || []){fn(event);if(event.stopped)break;}
    if(type==='keydown' && !event.stopped && key===suffix)handler++;
    if(type==='keydown' && key.length===1 && !event.defaultPrevented){
      field.value=field.value.slice(0,field.selectionStart)+key+field.value.slice(field.selectionEnd);
      field.selectionStart++;field.selectionEnd=field.selectionStart;
      if(legacy&&field.value.length>6){field.pinTimes=[];field.pinUntil=300;}
    }
    return event;
  }
  if(paste){emit('paste','');field.value+='manual';}
  for(let i=0;i<count;i++){
    if(shift)emit('keydown','Shift','ShiftLeft');
    if(interrupt&&i===4)emit('keydown','ArrowLeft');
    emit('keydown','7','Digit7');emit('keypress','7','Digit7');emit('keyup','7','Digit7');clock+=gap;
  }
  if(boundary)clock+=41;
  const end=emit('keydown',suffix);blocked=!!end.defaultPrevented && !!end.stopped;
  const press=emit('keypress',suffix),up=emit('keyup',suffix);
  window.AppScan.flush();
  return {restored:field.value===original,selection:field.selectionStart===1&&field.selectionEnd===3,
    blocked,handler,scan:calls.filter(c=>c.name==='scan').length,details,
    privateSafe:!JSON.stringify({calls,details,state:window.AppScan.state()}).includes(original),
    suffixStopped:!!press.stopped&&!!up.stopped,pinRestored:!('pinTimes' in field)&&!('pinUntil' in field),
    soundCalls:calls.filter(c=>c.name==='scan_blocked').map(c=>c.args)};
}
for(const suffix of ['Enter','Tab'])for(const waiting of [false,true])for(const type of ['text','password','textarea']){
  const r=run({suffix,waiting,type}); const name=`${suffix}_${waiting?'waiting':'ready'}_${type}`;
  checks[name]=r.restored&&r.selection&&r.blocked&&r.handler===0&&r.scan===0&&r.privateSafe&&r.suffixStopped&&
    JSON.stringify(r.details)==='[{"n_keys":8,"reason":"editing"}]';
}
for(const options of [{gap:120},{into:true},{count:5},{boundary:true},{paste:true,gap:120}]){
  const r=run(options);checks[`native_${Object.keys(options).join('_')}`]=!r.blocked&&r.handler===1&&r.scan===0&&r.details.length===0&&r.soundCalls.length===0;
}
checks.exact_boundary=run({gap:40,count:6}).blocked;
checks.above_boundary=!run({gap:41,count:6}).blocked;
checks.shifted_scanner=run({shift:true}).restored;
checks.interrupted_native=!run({interrupt:true}).blocked;
checks.legacy_pin_state_restored=run({legacy:true}).pinRestored;
const unhandled=[];
process.on('unhandledRejection',()=>unhandled.push(true));
for(const sheet of [false,true]){
  const r=run({sheet});checks[`blocked_call_${sheet?'sheet':'field'}`]=r.privateSafe&&
    JSON.stringify(r.soundCalls)===JSON.stringify([[{reason:sheet?'sheet':'field',n_keys:8}]]);
}
for(const soundReply of ['reject','throw','not_ok','pending']){
  const r=run({soundReply});checks[`blocked_call_${soundReply}_quiet`]=r.restored&&r.blocked&&r.handler===0&&r.scan===0&&r.privateSafe&&r.details.length===1&&r.soundCalls.length===1;
}
await new Promise(setImmediate);
checks.blocked_call_no_unhandled_rejection=unhandled.length===0;
console.log(JSON.stringify(checks));
process.exitCode=Object.values(checks).every(Boolean)?0:1;
