"""PyInstaller entry point for the canonical active-work probe."""

from kmtech_factory_contracts.active_work_probe.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
