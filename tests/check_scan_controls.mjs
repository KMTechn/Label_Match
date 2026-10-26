// Scan bytes stay in this VM; stdout contains case names and booleans only.
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(process.argv[2] || new URL('../../../static/ui/app/scan.js', import.meta.url), 'utf8');
const checks = {};
function harness(fieldType = null) {
  const listeners = {}, calls = [], events = [];
  const field = fieldType && {tagName:fieldType === 'textarea' ? 'TEXTAREA' : 'INPUT', type:fieldType,
    value:'', selectionStart:0, selectionEnd:0, closest:()=>null, matches:()=>false};
  const document = {activeElement:field, querySelectorAll:()=>[],
    addEventListener:(type,fn)=>(listeners['d'+type] ||= []).push(fn),
    dispatchEvent:event=>events.push(event)};
  const window = {addEventListener:(type,fn)=>(listeners['w'+type] ||= []).push(fn)};
  vm.runInNewContext(source, {window, document, performance:{timeOrigin:1000, now:()=>0},
    setInterval:()=>0, setTimeout:()=>0, clearTimeout:()=>{},
    CustomEvent:class {constructor(type,options){this.type=type;this.detail=options.detail;}},
    KMShell:{call:(name,args)=>{calls.push({name,args});return Promise.resolve({accepted:true});}}});
  window.AppScan.start('rw',0);
  let time = 0;
  function emit(type, options) {
    const event = {type, key:'', code:'', timeStamp:++time, target:field,
      getModifierState:()=>false, preventDefault(){this.defaultPrevented=true;},
      stopImmediatePropagation(){this.stopped=true;}, ...options};
    for (const target of ['w','d']) for (const fn of listeners[target+type] || []) {
      if (!event.stopped) fn(event);
    }
    return event;
  }
  function key(options) {
    const down = emit('keydown',options);
    // The browser suppresses keypress when keydown's default was prevented.
    if (!down.defaultPrevented && options.key.length === 1) emit('keypress',options);
    emit('keyup',options);
    return down;
  }
  return {window, calls, events, field, emit, key};
}

const chords = [
  ['gs_code', {key:'Unidentified',code:'BracketRight',ctrlKey:true}, '\x1d'],
  ['gs_key', {key:']',ctrlKey:true}, '\x1d'],
  ['rs_code', {key:'Unidentified',code:'Digit6',ctrlKey:true,shiftKey:true}, '\x1e'],
  ['rs_key', {key:'^',ctrlKey:true}, '\x1e'],
  ['fs_code', {key:'Unidentified',code:'Backslash',ctrlKey:true}, '\x1c'],
  ['fs_key', {key:'\\',ctrlKey:true}, '\x1c'],
  ['us_code', {key:'Unidentified',code:'Minus',ctrlKey:true,shiftKey:true}, '\x1f'],
  ['us_key', {key:'_',ctrlKey:true}, '\x1f'],
];
const direct = [4,28,29,30,31].map(value=>[String(value),String.fromCharCode(value)]);
for (const route of ['ctrl','alt029']) {
  const h=harness(), fields=['[)>','SYNTHETIC-A','SYNTHETIC-B','SYNTHETIC-C'];
  for (let i=0;i<fields.length;i++) {
    if(i) h.key(route==='ctrl' ? {key:']',code:'BracketRight',ctrlKey:true} : {key:'\x1d',altKey:true});
    for(const key of fields[i])h.key({key});
  }
  h.key({key:'Enter'});
  checks[`packing_gs_only_${route}`]=h.calls.find(call=>call.name==='scan')?.args[0]===fields.join('\x1d');
}
for (const type of ['keydown','keypress']) {
  for (const [name,options,expected] of chords) {
    const h=harness(); h.key({key:'A',code:'KeyA'}); h.emit(type,options); h.key({key:'B',code:'KeyB'}); h.key({key:'Enter',code:'Enter'});
    checks[`${type}_${name}`]=h.calls.filter(call=>call.name==='scan').length===1 &&
      h.calls.find(call=>call.name==='scan').args[0]==='A'+expected+'B' && h.window.AppScan.state().buffer==='';
  }
  for (const [name,key] of direct) for (const altKey of [false,true]) {
    const h=harness(); h.key({key:'A'}); h.emit(type,{key,altKey}); h.key({key:'B'}); h.key({key:'Enter'});
    checks[`${type}_direct_${name}_${altKey}`]=h.calls.find(call=>call.name==='scan')?.args[0]==='A'+key+'B';
  }
}
{
  const h=harness(); h.key({key:'['}); h.key({key:')'}); h.key({key:'>'});
  for (const [,options] of chords.filter(([name])=>name.endsWith('_code'))) h.key(options);
  for (const [,key] of direct) h.key({key});
  h.key({key:'Enter'});
  const scan=h.calls.find(call=>call.name==='scan');
  checks.mixed_iso15434_bytes=scan?.args[0]==='[)>\x1d\x1e\x1c\x1f\x04\x1c\x1d\x1e\x1f';
  checks.no_duplicate_browser_keypress=scan?.args[1].n_keys===12;
  checks.original_bytes_in_pending=h.events.find(event=>event.type==='scanpending')?.detail.text===scan?.args[0];
}
for (const options of [
  {key:'z',code:'KeyZ',ctrlKey:true}, {key:'^',code:'Digit6',metaKey:true},
  {key:']',code:'BracketRight',ctrlKey:true,altKey:true}, {key:'6',code:'Digit6',ctrlKey:true},
  {key:'-',code:'Minus',ctrlKey:true}, {key:'x',code:'KeyX',altKey:true},
  {key:'0',code:'Numpad0',altKey:true}, {key:'Escape',code:'Escape'},
  {key:'F1',code:'F1'}, {key:'F2',code:'F2'},
]) {
  const h=harness();h.key(options);
  checks[`ignored_${options.code}_${options.ctrlKey?'ctrl':options.metaKey?'meta':options.altKey?'alt':'plain'}`]=
    h.window.AppScan.state().buffer==='' && h.calls.every(call=>call.name!=='scan');
}
for (const type of ['text','password','textarea']) {
  const h=harness(type);
  const results=[...chords.map(([,options])=>options),...direct.map(([,key])=>({key}))].map(options=>h.key(options));
  checks[`native_${type}`]=results.every(event=>!event.defaultPrevented && !event.stopped) &&
    h.window.AppScan.state().buffer==='' && h.calls.every(call=>call.name!=='scan') && h.field.value==='';
}
{
  const h=harness();h.key({key:'F12',code:'F12'});
  checks.pedal_unchanged=h.window.AppScan.state().generation===1 && !h.window.AppScan.state().pressed;
  h.key({key:'^',code:'Digit6',shiftKey:true});h.key({key:'_',code:'Minus',shiftKey:true});h.key({key:']',code:'BracketRight'});h.key({key:'\\',code:'Backslash'});
  checks.unmodified_punctuation=h.window.AppScan.state().buffer==='^_]\\';
}
await Promise.resolve();
if (process.argv[3] === '--decoder-samples') {
  const samples = [];
  for (const chord of [chords[0][1], chords[2][1], {key:'\x1d',altKey:true}]) {
    const h = harness();
    for (const key of '[)>06') h.key({key});
    h.key(chord);
    for (const key of 'SYNTHETIC') h.key({key});
    h.key(chord);
    for (const key of '6D20261026') h.key({key});
    h.key({key:'Enter'});
    samples.push(h.calls.find(call=>call.name==='scan')?.args[0]);
  }
  console.log(JSON.stringify(samples));
} else console.log(JSON.stringify(checks));
process.exitCode=Object.values(checks).every(Boolean)?0:1;
