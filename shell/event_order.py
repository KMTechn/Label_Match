"""Tk event-order port: ui/integration.py:7589–7684 @198add5.
No mode-transition suppression in this shell. Epoch ms is diagnostic; processing
order alone controls grading.
"""
import time


class EventOrder:
    def __init__(self, source='tk_event_order', clock=None):
        self.source = source
        self.clock = clock or (lambda: time.time() * 1000)
        self.pressed = False
        self.generation = 0
        self.pressed_at = self.released_at = None
        self.focus_raise = self.frame = None
        self.outside = False

    def press(self, at=None):
        at = self.clock() if at is None else at
        if not self.pressed:
            self.generation += 1; self.pressed_at = at
        self.pressed = True
        if self.frame is not None:
            self.frame.update(pedal_pressed=True, press_generation=self.generation, pressed_at=self.pressed_at)

    def release(self, at=None):
        self.pressed = False
        self.released_at = self.clock() if at is None else at
        if self.frame is not None: self.frame['released_at'] = self.released_at

    def blur(self, at=None):
        self.outside = True
        self.pressed = False
        self.released_at = self.clock() if at is None else at

    def focus(self, physical, at=None):
        if not self.outside: return
        self.outside = False
        if physical is not True: return
        at = self.clock() if at is None else at
        self.press(at)
        self.focus_raise = {'generation': self.generation, 'at': at}
        if self.frame is not None: self.frame['focus_return_raise'] = dict(self.focus_raise)

    def observe(self, kind, at=None):
        at = self.clock() if at is None else at
        if kind == 'clear': self.frame = None; return None
        if self.frame is None:
            self.frame = {'source': self.source, 'pedal_pressed': self.pressed, 'press_generation': self.generation,
                          'first_character_at': at if kind == 'character' else None,
                          'pressed_at': self.pressed_at if self.pressed else None, 'released_at': None}
            if self.pressed and self.focus_raise and self.focus_raise['generation'] == self.generation:
                self.frame['focus_return_raise'] = dict(self.focus_raise)
        if kind == 'enter':
            self.frame['enter_at'] = at
            result = dict(self.frame); self.frame = None
            return result
        return None
