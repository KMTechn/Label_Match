"""Two local WAV cues; PlaySound always uses SND_ASYNC."""
import math
import struct
import wave
from pathlib import Path


class Sound:
    def __init__(self, data, player=None):
        self.player = player
        self.paths = {}
        root = Path(data) / 'sounds'
        root.mkdir(parents=True, exist_ok=True)
        for kind, frequency in (('ok', 1400), ('error', 600)):
            path = root / (kind + '.wav')
            samples = b''.join(struct.pack('<h', int(6500 * math.sin(2 * math.pi * frequency * i / 22050))) for i in range(1764))
            with wave.open(str(path), 'wb') as stream:
                stream.setnchannels(1); stream.setsampwidth(2); stream.setframerate(22050); stream.writeframes(samples)
            self.paths[kind] = path

    def play(self, kind):
        import winsound
        flags = winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_NODEFAULT
        (self.player or winsound.PlaySound)(str(self.paths['ok' if kind == 'ok' else 'error']), flags)
