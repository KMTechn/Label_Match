"""Test-PC-only Tk comparison.
IW ui/integration.py:7564–7684 and ui/components_v2.py:640–830 @198add5:
bind_all F12/Focus events -> character/enter/clear frame evidence.
RW framing remains ui/scan_entry.py @347a50e3. No idle completion.
"""
import argparse
import os
import sys
import time
from pathlib import Path

SHELL = Path(__file__).resolve().parent
if (SHELL.parent / 'site-packages').is_dir():
    sys.path.insert(0, str(SHELL.parent / 'site-packages'))
sys.path.insert(1 if (SHELL.parent / 'site-packages').is_dir() else 0, str(SHELL))

from bridge import Bridge
from event_order import EventOrder
from scan_rules import can_submit


class TkScanController:
    """Native event callbacks separated so mocks exercise the actual Tk path."""
    def __init__(self, entry, output, bridge, profile):
        self.entry, self.output, self.bridge, self.profile = entry, output, bridge, profile
        self.order = EventOrder()
        self.first, self.keys = None, 0
        self.seq = bridge.state()['last_request_seq']

    def down(self, event):
        char = str(getattr(event, 'char', '') or '')
        keysym = str(getattr(event, 'keysym', '') or '').lower()
        state = int(getattr(event, 'state', 0) or 0)
        printable = bool(char and char.isprintable() and not char.isspace())
        paste = keysym == 'v' and bool(state & 4)
        at = time.time() * 1000
        if printable or paste:
            if self.first is None: self.first = at
            self.order.observe('character', at)
        self.keys += 1
        self.bridge.events.record('tk', 'keys', batch=[{'type':'keydown','key':event.keysym,
            'code':event.keycode,'keyCode':event.keycode,'shift':bool(state & 1),'caps':bool(state & 2),
            'ctrl':bool(state & 4),'alt':bool(state & 0x20000),'numlock':None,
            'isComposing':None,'repeat':None,'t':at}])

    def paste(self, event):
        at = time.time() * 1000
        if self.first is None: self.first = at
        self.order.observe('character', at)

    def press_f12(self, event):
        self.order.press()
        self.bridge.events.record('tk','f12',type='keydown',repeat=None)

    def release_f12(self, event):
        self.order.release()
        self.bridge.events.record('tk','f12',type='keyup',repeat=False)

    def submit(self, event):
        text = self.entry.get()
        if self.profile == 'rw': text = text.strip()  # v2 RW baseline retained.
        if not can_submit(self.profile, text): return 'break'
        at = time.time() * 1000
        evidence = self.order.observe('enter', at)
        self.seq += 1
        result = self.bridge.scan(text, {'seq':self.seq,'at':at,'source':'scanner','t_first':self.first,
            't_enter':at,'n_keys':self.keys,'paths':{'key':self.keys,'code':0,'keypress':0,'paste':0},
            'evidence':evidence})
        if result['accepted']:
            self.entry.delete(0,'end')
            self.order.observe('clear')
            self.first, self.keys = None, 0
            self.output.configure(text=('불량' if result['verdict']=='ng' else '양품')+' · '+text)
        else:
            self.output.configure(text=result.get('message') or '기록 확인 필요 — 글을 그대로 두세요')
        return 'break'


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile',choices=('rw','iw'),required=True)
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--pedal',action='store_true')
    parser.add_argument('--decoy',action='store_true')
    args = parser.parse_args(argv)
    import tkinter as tk
    from tkinter import ttk
    bridge = Bridge(args.data.resolve(),args.profile,src='tk')
    bridge.events.record('tk','py_start',pid=os.getpid(),argv=sys.argv,profile=args.profile)
    root = tk.Tk()
    root.title('KMTech Tk 비교 — '+('리워크' if args.profile=='rw' else '검사'))
    root.geometry('1366x768')
    ttk.Label(root,text='바코드를 스캔하세요').pack(padx=24,pady=24)
    entry = ttk.Entry(root,font=('Malgun Gothic',24)); entry.pack(fill='x',padx=24)
    output = ttk.Label(root,text='스캔을 기다리고 있습니다'); output.pack(padx=24,pady=24)
    controller = TkScanController(entry,output,bridge,args.profile)
    entry.bind('<KeyPress>',controller.down)
    entry.bind('<<Paste>>',controller.paste,add='+')
    for sequence in ('<Return>','<KP_Enter>','<Tab>'): entry.bind(sequence,controller.submit)

    def belongs(event):
        try: return event.widget.winfo_toplevel() is root
        except (AttributeError,tk.TclError): return False
    def press(event):
        if belongs(event): controller.press_f12(event)
    def release(event):
        if belongs(event): controller.release_f12(event)
    def outside():
        try:
            widget = root.focus_displayof()
            # The trial decoy simulates another application; normal widgets and
            # the kit's IME probe belong to this application's root.
            return widget is None or widget.winfo_toplevel() is not root
        except tk.TclError: return False
    def focus_out(event):
        if outside(): controller.order.blur()
        def reset():
            if outside(): controller.order.blur()
        root.after(0,reset)
        bridge.events.record('tk','focus',state='win_deactivated')
    def focus_in(event):
        if not belongs(event) or not controller.order.outside: return
        reply = bridge.f12_physical()
        controller.order.focus(reply['result'])
        bridge.events.record('tk','focus',state='win_activated')
    for sequence, callback in (('<KeyPress-F12>',press),('<KeyRelease-F12>',release),
                                ('<FocusOut>',focus_out),('<FocusIn>',focus_in)):
        root.bind_all(sequence,callback,add='+')
    if args.decoy:
        decoy = tk.Toplevel(root); decoy.title('KMTech 다른 프로그램'); decoy.geometry('500x180')
        ttk.Label(decoy,text='다른 프로그램 — 여기에 들어온 글을 기록합니다').pack(padx=16,pady=16)
        value = tk.StringVar(); ttk.Entry(decoy,textvariable=value).pack(fill='x',padx=16)
        value.trace_add('write',lambda *_:bridge.events.record('tk','decoy',text=value.get()))
    code = 0
    try:
        if args.pedal: bridge.pedal.start()
        root.update_idletasks()
        bridge.events.record('tk','window_created',hwnd=root.winfo_id(),title=root.title())
        bridge.events.record('tk','loaded')
        bridge.ready({'timeOrigin':None,'domContentLoaded':None,'firstPaint':None,'firstContentfulPaint':None,
                      'readyAt':time.time()*1000})
        entry.focus_set()
        root.mainloop()
    except Exception:
        code = 1
        raise
    finally:
        bridge.close()
        bridge.events.record('tk','exit',code=code)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
