"""Isolate launch state and replace only the screen owner for startup-contract tests."""
import pytest


_INITIAL_SCREEN_HOST = None


@pytest.fixture(autouse=True)
def fresh_screen_launch(monkeypatch):
    import Label_Match
    global _INITIAL_SCREEN_HOST
    if _INITIAL_SCREEN_HOST is None:
        _INITIAL_SCREEN_HOST = dict(Label_Match.LABEL_MATCH_SCREEN_HOST)
    # A production launch gets module initialization in its own process.
    # main() spies in these tests must not carry their selection into the next test.
    host = dict(_INITIAL_SCREEN_HOST)
    monkeypatch.setattr(Label_Match, "LABEL_MATCH_SCREEN_HOST", host)
    return host


def install_screen_owner_double(monkeypatch, module):
    """Keep the real guarded startup body; replace the GUI adapter boundary."""
    import label_match_screen
    routes = []

    def screen_runner(actual_module):
        assert actual_module is module

        def run():
            host = module.LABEL_MATCH_SCREEN_HOST
            assert host["requested"] is True and host["active"] is False
            routes.append("new")

            def run_owner(app):
                try:
                    app.mainloop()
                finally:
                    app._finish_after_mainloop()
                return 0

            host.update(active=True, run=run_owner)
            try:
                report = host.get("relay_reuse_report")
                return module._run_label_match_application(relay_reuse_report=report)
            finally:
                host.update(active=False, run=None)

        return run

    monkeypatch.setattr(label_match_screen, "screen_runner", screen_runner)
    return routes
