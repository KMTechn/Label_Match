"""Presentation state for app doubles, using the shipped app color settings."""

import json
from pathlib import Path
from types import SimpleNamespace


def prepare_label_presentation(app):
    # Tk installs tk before app initialization; missing attributes must not
    # recurse into an absent interpreter on an object.__new__ test double.
    app.tk = SimpleNamespace()
    app.initialized_successfully = False
    settings = Path(__file__).resolve().parents[1] / 'config' / 'app_settings.json'
    app.colors = json.loads(settings.read_text(encoding='utf-8'))['colors']
    app.display_options = {}
    app.big_display_label = SimpleNamespace(config=lambda **options: app.display_options.update(options))
