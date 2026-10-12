"""Display hook from core/business_logic.py @a211ea4; physical read from
ui/integration.py:7634–7641 @198add5. V3 grading uses UI event-order evidence.

One hold = one generation. Snapshot at arrival is diagnostic only.
Hooks only publish state to a queue; log/JS callbacks run on a separate thread.
"""
import queue
import threading


def read_physical_f12():
    import os
    if os.name != 'nt': return None
    try:
        import ctypes
        return bool(ctypes.windll.user32.GetAsyncKeyState(0x7B) & 0x8000)
    except (AttributeError, OSError):
        return None


class Pedal:
    def __init__(self, on_change=None):
        self._press_state = (False, 0)
        self._lock = threading.Lock()
        self._keyboard = None
        self._hooks = []
        self.unhook_errors = []
        self._listening = False
        self._on_change = on_change
        self._queue = queue.Queue()
        self._worker = threading.Thread(target=self._publish, name='pedal-publisher', daemon=True)
        self._worker.start()

    def snapshot(self):
        return self._press_state

    def set_callback(self, callback):
        self._on_change = callback

    def press(self, event=None):
        if not self._listening:
            return
        with self._lock:
            pressed, generation = self._press_state
            self._press_state = (True, generation + int(not pressed))
            self._queue.put_nowait(self._press_state)

    def release(self, event=None):
        with self._lock:
            self._press_state = (False, self._press_state[1])
            self._queue.put_nowait(self._press_state)

    def start(self, keyboard_module=None):
        if self._listening:
            return
        if keyboard_module is None:
            import keyboard as keyboard_module
        self._keyboard = keyboard_module
        self._listening = True
        try:
            if hasattr(keyboard_module, 'hook_key'):
                # keyboard 0.13.5 keys its remover table by key name: two hooks on
                # 'F12' (press + release) make the second removal raise KeyError.
                self._hooks.append(keyboard_module.hook_key('F12', self._on_key))
            else:
                self._hooks.append(keyboard_module.on_press_key('F12', self.press))
                self._hooks.append(keyboard_module.on_release_key('F12', self.release))
        except Exception:
            self.stop()
            raise

    def _publish(self):
        while True:
            state = self._queue.get()
            try:
                if state is None:
                    return
                if self._on_change:
                    self._on_change(*state)
            finally:
                self._queue.task_done()

    def _on_key(self, event):
        (self.release if getattr(event, 'event_type', None) == 'up' else self.press)(event)

    def stop(self):
        self._listening = False
        for hook in list(self._hooks):
            try:
                if callable(hook):
                    hook()
                else:
                    self._keyboard.unhook(hook)
            except Exception as error:  # keep closing; the caller must still reach its exit record
                self.unhook_errors.append(type(error).__name__)
            self._hooks.remove(hook)
        self.release()

    def close(self):
        self.stop()
        self._queue.put(None)
        self._worker.join(2)
