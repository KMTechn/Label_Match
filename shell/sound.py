"""Two local WAV cues; PlaySound always uses SND_ASYNC."""
import math
import struct
from pathlib import Path

import shell_fileio


class Sound:
    def __init__(self, data, player=None, fileio=None):
        self.fileio = fileio or shell_fileio.FileIO()
        self.player = player
        self.paths = {}
        root = Path(data) / 'sounds'
        try: self.fileio.make_dirs(root, 'sound_cache')
        except Exception: pass  # play() then fails and records sound ok=False; the scan flow continues
        for kind, frequency in (('ok', 1400), ('error', 600)):
            path = root / (kind + '.wav')
            samples = b''.join(struct.pack('<h', int(6500 * math.sin(2 * math.pi * frequency * i / 22050))) for i in range(1764))
            # 16-bit mono 22050 Hz PCM WAV built in memory (same bytes as the wave module wrote before v3.14);
            # no file-like open here, the one write goes through shell_fileio.
            header = (b'RIFF' + struct.pack('<I', 36 + len(samples)) + b'WAVEfmt ' + struct.pack('<IHHIIHH', 16, 1, 1, 22050, 44100, 2, 16)
                      + b'data' + struct.pack('<I', len(samples)))
            try: self.fileio.replace_atomic(path, header + samples, 'sound_cache')
            except Exception: pass  # same: a missing cue is recorded at play time
            self.paths[kind] = path

    def play(self, kind):
        import winsound
        flags = winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_NODEFAULT
        (self.player or winsound.PlaySound)(str(self.paths['ok' if kind == 'ok' else 'error']), flags)
