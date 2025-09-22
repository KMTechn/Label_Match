# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Label_Match is a Korean barcode verification system application built with Python/Tkinter. It validates barcode sets through 5-step scanning processes and maintains audit logs. The application features auto-update functionality from GitHub releases and comprehensive UI for tracking scan history and results.

## Key Architecture

### Main Application Structure
- **Label_Match.py**: Single-file application (~2000+ lines) containing the main `Label_Match` class that inherits from `tk.Tk`
- **DataManager class**: Handles logging and state persistence using JSON files
- **CalendarWindow class**: Date picker dialog for historical data viewing

### Configuration System
- **config/app_settings.json**: UI settings, colors, fonts, sound files, and worker configuration
- **assets/Item.csv**: Product catalog with item codes, names, specs, and tray images
- **README.txt**: Comprehensive documentation for barcode validation rules (Korean)

### Data Flow
1. Barcode scanning through 5 sequential steps (ScanPosition 1-5)
2. Validation against configurable rules from CSV files
3. Real-time audio feedback for each scan step
4. Automatic logging to daily JSON files
5. UI updates for current status and historical summaries

## Development Commands

### Running the Application
```bash
python Label_Match.py
```

### Installing Dependencies
```bash
pip install -r requirements.txt
```

### Dependencies
- **Pillow**: Image processing
- **pygame**: Audio playback for scan feedback
- **requests**: Auto-update functionality and HTTP requests
- **tkcalendar**: Date picker widget

## Auto-Update System

The application includes GitHub-based auto-update functionality:
- **Repository**: KMTechn/Label_Match
- **Current Version**: v2.0.4 (defined in APP_VERSION variable at Label_Match.py:28)
- **Update Process**: Downloads latest release ZIP, creates batch updater script, applies updates automatically

## Audio System

Audio feedback system using pygame with WAV files in assets/:
- **one.wav, two.wav, three.wav, four.wav**: Scan step indicators
- **pass.wav**: Success completion sound
- **fail.wav**: Error/failure sound

## Configuration Files

### app_settings.json Structure
- **ui_settings**: Font sizes, scaling factors, display preferences
- **colors**: Theme colors for UI elements
- **sound_files**: Audio file mappings
- **ui_persistence**: Window dimensions, column widths, scale factors
- **worker_name**: Application version identifier

### Barcode Validation Rules
The system uses configurable CSV-based validation rules (referenced in README.txt):
- Rules defined by RuleName groups with 5 ScanPosition entries each
- Length validation (MinLength/MaxLength) for each scan position
- String slicing (SliceStart/SliceEnd) for code extraction
- First scan determines which rule set applies based on barcode length

## Key Classes and Methods

### Label_Match Main Class (Label_Match.py:227)
- **process_input()**: Core barcode processing logic (Label_Match.py:905)
- **_finalize_set()**: Completes validation set and logs results (Label_Match.py:1071)
- **_load_history_and_rebuild_summary()**: Loads historical data (Label_Match.py:526)
- **_run_auto_test_simulation()**: Automated testing functionality (Label_Match.py:658)

### DataManager Class (Label_Match.py:171)
- **log_event()**: Records events to daily JSON logs (Label_Match.py:202)
- **save_current_state()**: Persists current scan state (Label_Match.py:205)
- **load_current_state()**: Restores scan state on startup (Label_Match.py:214)

## Testing Features

The application includes built-in testing capabilities:
- Automated test simulation with configurable master codes and set counts
- Manual testing through demonstration mode
- Unit test compatibility (imports unittest module)

## Important Notes

- All UI text and documentation is in Korean
- Application supports both executable and script modes (sys.frozen detection)
- Uses Windows-specific features (batch files for updates, Windows audio)
- Extensive error handling with modal dialogs and audio feedback
- State persistence across application restarts
- Real-time clock display and status updates