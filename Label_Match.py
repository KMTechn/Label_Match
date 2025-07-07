import tkinter as tk
from tkinter import ttk, messagebox, TclError, filedialog
from collections import defaultdict
import re
import csv
from datetime import datetime, timedelta
import threading
import time
import sys
import os
import json
import tkinter.font as tkFont
import queue
import pygame
import uuid
import socket

# 새로 추가된 import
import requests
import zipfile
import subprocess

# ####################################################################
# # 자동 업데이트 기능 (Auto-Updater Functionality) - 앱 내부에 통합
# ####################################################################

# 이 상수는 BarcodeScannerApp 클래스 내부에 정의되어 앱 설정을 따르도록 합니다.
# REPO_OWNER 및 REPO_NAME은 Label_Match 저장소에 맞게 설정되었습니다.

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS  # type: ignore
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

class DataManager:
    def __init__(self, save_dir, process_name, computer_name):
        self.save_directory = save_dir
        self.process_name = process_name
        self.computer_name = computer_name
        self.log_queue = queue.Queue()
        self.log_thread = threading.Thread(target=self._log_writer_thread, daemon=True)
        self.log_thread.start()

    def _get_log_filepath(self):
        filename = f"{self.process_name}작업이벤트로그_{self.computer_name}_{datetime.now().strftime('%Y%m%d')}.csv"
        return os.path.join(self.save_directory, filename)

    def _log_writer_thread(self):
        while True:
            try:
                log_item = self.log_queue.get()
                if log_item is None: break
                filepath = self._get_log_filepath()
                file_exists = os.path.exists(filepath)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    if not file_exists or os.stat(filepath).st_size == 0:
                        writer.writerow(["timestamp", "worker_name", "event", "details"])
                    writer.writerow(log_item)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"로그 쓰기 스레드 오류: {e}")

    def log_event(self, event_type, details, worker_name):
        log_item = [datetime.now().isoformat(), worker_name, event_type, json.dumps(details, ensure_ascii=False, cls=DateTimeEncoder)]
        self.log_queue.put(log_item)


class BarcodeValidator:
    def __init__(self, rules):
        self.rules = rules if rules else {}

    def find_matching_rule_name(self, barcode):
        for name, rule_set in self.rules.items():
            if not rule_set: continue
            first_scan_rule = rule_set[0]
            if len(barcode) >= int(first_scan_rule['MinLength']):
                return name
        return None

    def validate(self, barcode, scan_pos, active_rule_set):
        if not (0 < scan_pos <= len(active_rule_set)):
            return None, f"규칙 정의 오류: {scan_pos}번째 스캔 규칙 없음"
        
        rule = active_rule_set[scan_pos - 1]
        min_len, max_len = int(rule['MinLength']), int(rule['MaxLength'])

        if len(barcode) < min_len:
            return None, f"길이 오류 (최소 {min_len}자 필요, 실제 {len(barcode)}자)"

        if len(barcode) > max_len:
            barcode = barcode[:max_len]

        start, end = int(rule['SliceStart']), int(rule['SliceEnd'])
        
        if start > len(barcode) or end > len(barcode):
             return None, f"슬라이싱 오류 (코드 길이: {len(barcode)}, 슬라이스: {start}~{end})"
        
        return barcode[start:end], None

class BarcodeScannerApp(tk.Tk):
    class FILES:
        SETTINGS = "app_settings.json"
        RULES = "validation_rules.csv"
        ITEMS = "Item.csv"

    class Events:
        APP_START = "APP_START"
        APP_CLOSE = "APP_CLOSE"
        SCAN_OK = "SCAN_OK"
        TRAY_COMPLETE = "TRAY_COMPLETE"
        SET_CANCELLED = "SET_CANCELLED"
        SET_DELETED = "SET_DELETED"
        UI_ERROR = "UI_ERROR"

    class Results:
        PASS = "통과"
        FAIL_MISMATCH = "불일치"
        FAIL_INPUT_ERROR = "입력오류"
        IN_PROGRESS = "진행중..."

    class Worker:
        PACKAGING = "포장실"

    # [업데이트 기능 추가] 앱 버전 및 GitHub 저장소 정보
    REPO_OWNER = "replay121678"
    REPO_NAME = "Label_Match" # "Container_Audit" -> "Label_Match" 로 변경
    # 현재 프로그램의 버전 (업데이트 릴리스 시 이 값을 올려야 함)
    CURRENT_APP_VERSION = "v1.0.1" # 이 값을 새로운 버전으로 업데이트해야 합니다.
            
    def _get_computer_name(self):
        try:
            return socket.gethostname()
        except Exception as e:
            print(f"컴퓨터 이름을 가져오는 데 실패했습니다: {e}")
            return "UNKNOWN_PC"

    def __init__(self):
        super().__init__()
        self.initialized_successfully = False

        # [업데이트 기능 추가] 앱 시작 시 업데이트 확인 스레드 시작
        # Tkinter 창이 초기화되기 전에 메시지 박스를 띄울 수 있도록 Tk() 인스턴스를 직접 생성하여 사용
        self._check_for_updates_at_startup()

        try:
            pygame.mixer.init()
        except pygame.error as e:
            messagebox.showerror("오디오 초기화 오류", f"Pygame 오디오 시스템을 시작할 수 없습니다.\n오류: {e}")

        self.computer_name = self._get_computer_name()
        self._setup_paths()

        self.app_settings = self._load_app_settings()
        
        # [수정된 부분] 저장 경로를 'C:\Sync'로 변경
        self.save_directory = r'C:\Sync' 
        
        os.makedirs(self.save_directory, exist_ok=True)
        print(f"*** 데이터는 다음 경로에 저장됩니다: {self.save_directory} ***")

        self.ui_cfg = self.app_settings.get("ui_settings", {})
        self.base_font_size = self.ui_cfg.get("base_font_size", 11)
        self.colors = self.app_settings.get("colors", {})
        self.sounds = self.app_settings.get("sound_files", {})

        self.sound_objects = {}
        self.validation_rules = {}
        self.items_data = {}
        self.validator = None
        
        self.worker_name = self.app_settings.get("worker_name", self.computer_name)
        
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.computer_name)

        self.current_set_info = {
            'id': None, 'parsed': [], 'raw': [],
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False
        }
        self.is_blinking = False
        self.active_rule_name = None
        self.scan_count = defaultdict(int)
        self.global_scanned_set = set()
        self.set_details_map = {}

        self.title("바코드 세트 검증기 (v9.11) - 로딩 중...")
        self.state('zoomed')
        self.configure(bg=self.colors.get("background", "#ECEFF1"))

        self.scale_factor = 1.0
        self.tree_font_size = 11
        self.summary_col_widths = {}
        self.history_col_widths = {}
        self.sash_position = None
        self._load_ui_persistence_settings()

        self.hist_proportions = {"Set": 4, "Input1": 14, "Input2": 14, "Input3": 14, "Input4": 14, "Input5": 14, "Result": 8, "Timestamp": 18}
        self.summary_proportions = {"Code": 80, "Count": 20}
        self.default_font_name = self.ui_cfg.get("default_font", "Malgun Gothic")

        self.style = ttk.Style(self)
        self._configure_base_styles()
        self._create_widgets()
        self._configure_treeview_styles()

        self.show_loading_overlay()

        self.initial_load_queue = queue.Queue()
        threading.Thread(target=self._async_initial_load, daemon=True).start()
        self.after(100, self._process_initial_load_queue)

        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.bind_all("<Control-MouseWheel>", self.on_ctrl_wheel)

    # ####################################################################
    # # 자동 업데이트 기능 관련 메서드 (클래스 내부로 이동 및 개선)
    # ####################################################################

    def _check_for_updates_at_startup(self):
        """
        앱 시작 시점에 업데이트를 확인하고, GUI 스레드와 별개로 처리하여
        Tkinter 메인 루프 시작 전에 메시지 박스를 띄울 수 있도록 함.
        """
        download_url, new_version = self._check_for_updates()
        if download_url:
            # Tkinter의 root 윈도우가 아직 생성되지 않았을 수 있으므로,
            # 별도의 Tk() 인스턴스를 생성하여 메시지 박스를 띄웁니다.
            # 이 인스턴스는 메시지 박스가 닫히면 바로 파괴됩니다.
            root_alert = tk.Tk()
            root_alert.withdraw() # 메인 윈도우를 숨김
            should_update = messagebox.askyesno(
                "업데이트 발견",
                f"새로운 버전({new_version})이 발견되었습니다.\n지금 업데이트하시겠습니까? (현재: {self.CURRENT_APP_VERSION})",
                parent=root_alert # parent 지정하여 Z-order 문제 방지
            )
            root_alert.destroy() # 메시지 박스 닫힌 후 임시 root 파괴

            if should_update:
                threading.Thread(target=self._download_and_apply_update, args=(download_url,), daemon=True).start()
                # 업데이트가 진행되므로 앱은 종료될 예정
                sys.exit(0) # 앱 즉시 종료
            else:
                print("사용자가 업데이트를 거부했습니다.")

    def _check_for_updates(self):
        """GitHub에서 최신 릴리스 정보를 확인하고, 업데이트가 필요하면 .zip 파일의 다운로드 URL을 반환합니다."""
        try:
            api_url = f"https://api.github.com/repos/{self.REPO_OWNER}/{self.REPO_NAME}/releases/latest"
            print(f"업데이트 확인 URL: {api_url}")
            response = requests.get(api_url, timeout=5)
            response.raise_for_status()
            latest_release_data = response.json()
            latest_version = latest_release_data['tag_name']
            print(f"현재 버전: {self.CURRENT_APP_VERSION}, 최신 버전: {latest_version}")

            if latest_version.strip().lower() != self.CURRENT_APP_VERSION.strip().lower():
                print("새로운 버전이 있습니다.")
                for asset in latest_release_data['assets']:
                    # 릴리스 에셋 중 .zip 파일 찾기
                    if asset['name'].endswith('.zip'):
                        return asset['browser_download_url'], latest_version
                print("릴리스에 .zip 파일이 없습니다.")
                return None, None
            else:
                print("프로그램이 최신 버전입니다.")
                return None, None
        except requests.exceptions.RequestException as e:
            print(f"업데이트 확인 중 오류 발생 (네트워크 문제일 수 있음): {e}")
            return None, None

    def _download_and_apply_update(self, url):
        """업데이트 .zip 파일을 다운로드하고, 압축 해제 후 적용 스크립트를 실행합니다."""
        try:
            # TEMP 환경 변수 사용, 없을 경우 C:\Temp 대신 현재 실행 폴더에 Temp 폴더 생성
            temp_dir = os.environ.get("TEMP", os.path.join(os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__), "Temp"))
            os.makedirs(temp_dir, exist_ok=True)
            zip_path = os.path.join(temp_dir, "update.zip")
            print(f"'{url}' 에서 새 버전을 다운로드 중...")
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("다운로드 완료.")

            temp_update_folder = os.path.join(temp_dir, "temp_update")
            if os.path.exists(temp_update_folder):
                import shutil
                shutil.rmtree(temp_update_folder)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_update_folder)
            print(f"'{temp_update_folder}'에 압축 해제 완료.")
            os.remove(zip_path)

            if getattr(sys, 'frozen', False): # PyInstaller로 빌드된 경우
                application_path = os.path.dirname(sys.executable)
            else: # 스크립트로 실행되는 경우
                application_path = os.path.dirname(os.path.abspath(__file__))
            
            updater_script_name = "updater.bat"
            updater_script_path = os.path.join(application_path, updater_script_name)
            
            extracted_content = os.listdir(temp_update_folder)
            if len(extracted_content) == 1 and os.path.isdir(os.path.join(temp_update_folder, extracted_content[0])):
                # 압축 해제 시 최상위 폴더가 하나 더 있는 경우 (예: myapp-1.0.0.zip -> myapp-1.0.0/ 안에 파일들)
                new_program_folder_path = os.path.join(temp_update_folder, extracted_content[0])
            else:
                new_program_folder_path = temp_update_folder
                
            with open(updater_script_path, "w", encoding='utf-8') as bat_file:
                bat_file.write(f"""@echo off
chcp 65001 > nul
echo.
echo ==========================================================
echo  프로그램을 업데이트합니다. 이 창을 닫지 마세요.
echo ==========================================================
echo.
echo 잠시 후 프로그램이 자동으로 종료됩니다...
timeout /t 3 /nobreak > nul

taskkill /F /IM "{os.path.basename(sys.executable)}" > nul

echo.
echo 기존 파일을 백업하고 새 파일로 교체합니다...
xcopy "{new_program_folder_path}" "{application_path}" /E /H /C /I /Y > nul

echo.
echo 임시 업데이트 파일을 삭제합니다...
rmdir /s /q "{temp_update_folder}"

echo.
echo ========================================
echo  업데이트 완료!
echo ========================================
echo.
echo 3초 후에 프로그램을 다시 시작합니다.

timeout /t 3 /nobreak > nul
start "" "{os.path.join(application_path, os.path.basename(sys.executable))}"
del "{updater_script_name}"
""")
            
            print("업데이트 적용을 위해 프로그램을 종료하고 업데이트 스크립트를 실행합니다.")
            subprocess.Popen(updater_script_path, creationflags=subprocess.CREATE_NEW_CONSOLE)
            sys.exit(0) # 현재 앱 프로세스 종료

        except Exception as e:
            print(f"업데이트 적용 중 오류 발생: {e}")
            # Tkinter의 메인 루프가 아직 시작되지 않았을 수 있으므로,
            # 메시지 박스를 띄우는 방법을 _check_for_updates_at_startup과 동일하게 처리
            root_alert = tk.Tk()
            root_alert.withdraw()
            messagebox.showerror("업데이트 실패", f"업데이트 적용 중 오류가 발생했습니다.\n\n{e}\n\n프로그램을 다시 시작해주세요.", parent=root_alert)
            root_alert.destroy()
            sys.exit(1) # 오류 발생 시에도 앱 종료

    # ####################################################################
    # # 기존 BarcodeScannerApp 코드 (일부 수정)
    # ####################################################################

    def _async_initial_load(self):
        try:
            validation_rules = self._load_validation_rules_from_csv()
            if validation_rules is None:
                raise FileNotFoundError("규칙 파일(validation_rules.csv) 로드에 실패했습니다.")

            items_data = self._load_items_data()

            loaded_data = {
                "rules": validation_rules,
                "items": items_data,
            }
            self.initial_load_queue.put(loaded_data)
        except Exception as e:
            self.initial_load_queue.put({"error": str(e)})

    def _process_initial_load_queue(self):
        try:
            result = self.initial_load_queue.get_nowait()

            if "error" in result:
                error_msg = result['error']
                self.hide_loading_overlay()
                messagebox.showerror("초기화 오류", f"프로그램 시작에 필요한 파일을 불러올 수 없습니다.\n\n오류: {error_msg}")
                self.destroy()
                return

            self.validation_rules = result['rules']
            self.items_data = result.get('items', {})
            self.validator = BarcodeValidator(self.validation_rules)

            self.sound_objects = self._preload_sounds()

            self.hide_loading_overlay()
            self.entry.config(state='normal')
            self.entry.focus_set()
            self._reset_current_set()

            # 앱 타이틀에 버전 정보 포함
            self.title(f"바코드 세트 검증기 (v9.11 - 현재 버전: {self.CURRENT_APP_VERSION})")

            self.data_manager.log_event(self.Events.APP_START, {"message": "Application initialized.", "version": self.CURRENT_APP_VERSION}, self.worker_name)
            self.initialized_successfully = True

            self.history_queue = queue.Queue()
            self._load_history_and_rebuild_summary()
            self.after(100, self._process_history_queue) 
            
            self.after(200, self._update_ui_scaling)
            self._update_clock()

        except queue.Empty:
            self.after(100, self._process_initial_load_queue)
        except Exception as e:
            self.hide_loading_overlay()
            messagebox.showerror("초기화 오류", f"초기화 마무리 중 오류가 발생했습니다: {e}")
            self.destroy()

    def show_loading_overlay(self):
        self.loading_overlay.grid(row=0, column=0, rowspan=3, sticky='nsew')
        self.loading_overlay.tkraise()
        self.loading_progressbar.start(10)
        self.update_idletasks()

    def hide_loading_overlay(self):
        self.loading_progressbar.stop()
        self.loading_overlay.grid_forget()

    def _preload_sounds(self):
        sound_objects = {}
        for key, filename in self.sounds.items():
            sound_path = resource_path(os.path.join("assets", filename))
            if os.path.exists(sound_path):
                try:
                    sound_objects[key] = pygame.mixer.Sound(sound_path)
                except pygame.error as e:
                    print(f"사운드 로드 오류 ({filename}): {e}")
            else:
                print(f"사운드 파일 없음: {sound_path}")
        return sound_objects

    def _setup_paths(self):
        self.base_path = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
        self.config_directory = resource_path("config")
        os.makedirs(self.config_directory, exist_ok=True)
        self.app_settings_path = os.path.join(self.config_directory, self.FILES.SETTINGS)
        self.rules_path = resource_path(os.path.join("assets", self.FILES.RULES))

    def _load_app_settings(self):
        if not os.path.exists(self.app_settings_path):
            return {}
        try:
            with open(self.app_settings_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            messagebox.showwarning("설정 불러오기 오류", 
                                  f"설정 파일({self.FILES.SETTINGS})을 불러오는 데 실패했습니다.\n"
                                  f"기본 설정으로 시작합니다.\n\n오류: {e}")
            return {}

    def _save_app_settings(self):
        if not self.initialized_successfully: return
        
        persistence_data = {
            "scale_factor": self.scale_factor,
            "tree_font_size": self.tree_font_size
        }
        
        try:
            persistence_data["sash_position"] = self.content_pane.sashpos(0)
            persistence_data["summary_col_widths"] = {col: self.summary_tree.column(col, 'width') for col in self.summary_tree['columns']}
            persistence_data["history_col_widths"] = {col: self.history_tree.column(col, 'width') for col in self.history_tree['columns']}
        except TclError as e:
            print(f"UI 상태(sash/column)를 가져오는 중 오류 발생 (무시 가능): {e}")
            if "ui_persistence" in self.app_settings:
                persistence_data.setdefault("sash_position", self.app_settings["ui_persistence"].get("sash_position"))
                persistence_data.setdefault("summary_col_widths", self.app_settings["ui_persistence"].get("summary_col_widths"))
                persistence_data.setdefault("history_col_widths", self.app_settings["ui_persistence"].get("history_col_widths"))

        self.app_settings['worker_name'] = self.worker_name
        self.app_settings["ui_persistence"] = persistence_data
        
        try:
            with open(self.app_settings_path, 'w', encoding='utf-8') as f:
                json.dump(self.app_settings, f, indent=4, ensure_ascii=False)
        except OSError as e:
            messagebox.showerror("설정 저장 오류",
                                 f"설정 파일({self.FILES.SETTINGS})을 저장할 수 없습니다.\n"
                                 f"파일 권한을 확인해주세요.\n\n오류: {e}")

    def _load_ui_persistence_settings(self):
        persistence_settings = self.app_settings.get("ui_persistence", {})

        self.scale_factor = persistence_settings.get("scale_factor", 1.0)
        if not (0.5 <= self.scale_factor <= 3.0): self.scale_factor = 1.0

        self.tree_font_size = persistence_settings.get("tree_font_size", 11)
        if not (6 <= self.tree_font_size <= 20): self.tree_font_size = 11

        self.summary_col_widths = persistence_settings.get("summary_col_widths", {})
        self.history_col_widths = persistence_settings.get("history_col_widths", {})
        self.sash_position = persistence_settings.get("sash_position", None)

    def _load_items_data(self):
        items_path = resource_path(os.path.join("assets", self.FILES.ITEMS))
        try:
            with open(items_path, 'r', encoding='utf-8-sig') as f:
                return {row['Item Code']: row for row in csv.DictReader(f)}
        except FileNotFoundError:
            print(f"경고: 품목 정보 파일({self.FILES.ITEMS})이 없어 품목명을 기록할 수 없습니다.")
            return {}
        except Exception as e:
            print(f"품목 정보 로드 오류: {e}")
            return {}

    def on_closing(self):
        if not self.initialized_successfully:
            self.destroy()
            return

        self._save_app_settings()

        self.is_blinking = False
        self.data_manager.log_event(self.Events.APP_CLOSE, {"message": "Application closed."}, self.worker_name)
        
        self.data_manager.log_queue.put(None)
        self.data_manager.log_thread.join()

        self.destroy()

    def _load_validation_rules_from_csv(self):
        rules = defaultdict(list)
        def parse_csv(csv_reader):
            for row in csv_reader:
                if not any(row.values()): continue
                required_keys = ['RuleName', 'ScanPosition', 'MinLength', 'MaxLength', 'SliceStart', 'SliceEnd']
                if not all(key in row and row[key] for key in required_keys):
                    print(f"경고: '{self.FILES.RULES}'의 일부 행에 필수 열이 누락되었거나 비어 있습니다: {row}")
                    continue
                rules[row['RuleName']].append(row)
        try:
            with open(self.rules_path, 'r', encoding='utf-8-sig') as f: parse_csv(csv.DictReader(f))
        except UnicodeDecodeError:
            print("UTF-8 디코딩 실패. cp949(Excel 저장 방식)로 다시 시도합니다.")
            try:
                with open(self.rules_path, 'r', encoding='cp949') as f: parse_csv(csv.DictReader(f))
            except Exception as e:
                print(f"cp949 인코딩으로도 파일을 읽을 수 없습니다. 오류: {e}")
                return None
        except FileNotFoundError:
            print(f"필수 파일 '{self.FILES.RULES}'를 찾을 수 없습니다. 경로: {self.rules_path}")
            return None
        except Exception as e:
            print(f"규칙 파일을 읽는 중 알 수 없는 오류가 발생했습니다: {e}")
            return None

        if not rules:
            print("규칙 파일에서 유효한 규칙을 하나도 찾지 못했습니다.")
            return None

        for rule_name in rules: rules[rule_name].sort(key=lambda x: int(x['ScanPosition']))
        return rules

    def _load_history_and_rebuild_summary(self):
        print("과거 기록 비동기 로드 시작...")
        self.scan_count.clear()
        self.history_tree.delete(*self.history_tree.get_children())
        self.summary_tree.delete(*self.summary_tree.get_children())
        self.global_scanned_set.clear()
        self.set_details_map.clear()
        self.history_tree.insert("", "end", iid="loading", values=("", "기록을 불러오는 중입니다...", "", "", "", "", "", ""), tags=("in_progress",))
        loader_thread = threading.Thread(target=self._async_load_history_task, args=(self.history_queue,), daemon=True)
        loader_thread.start()

    def _async_load_history_task(self, result_queue):
        try:
            completed_sets = {}
            voided_set_ids = set()
            
            today_str = datetime.now().strftime('%Y%m%d')
            # [수정] 파일명에 컴퓨터 이름을 사용
            log_filename_today = f"{self.Worker.PACKAGING}작업이벤트로그_{self.computer_name}_{today_str}.csv"
            filepath = os.path.join(self.save_directory, log_filename_today)
            
            # [진단용 로그 추가] 어떤 파일을 읽으려고 시도하는지 명확히 출력
            print(f"--- [데이터 이어쓰기] 오늘 로그 파일을 확인합니다: {filepath} ---")

            if not os.path.exists(filepath):
                print(f"   -> 오늘 로그 파일 없음. 새로 시작합니다.")
                result_queue.put({'sorted_sets': [], 'scan_count': defaultdict(int), 'global_scanned_set': set(), 'set_details_map': {}})
                return
            
            print(f"   -> 파일 발견, 읽기 시도...")
            rows_found = 0
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows_found += 1
                        event = row.get('event')
                        details_str = row.get('details', '{}')
                        if not details_str: continue
                        try:
                            details = json.loads(details_str)
                        except json.JSONDecodeError:
                            print(f"경고: JSON 파싱 오류. 건너뜁니다: {details_str}")
                            continue
                        
                        set_id = details.get('master_label_code') if event == self.Events.TRAY_COMPLETE else details.get('set_id')
                        if set_id is None: continue

                        if event == self.Events.TRAY_COMPLETE:
                            parsed_scans_for_display = details.get('parsed_product_barcodes', [])
                            first_scan = parsed_scans_for_display[0] if parsed_scans_for_display else "N/A"
                            other_scans = parsed_scans_for_display[1:5]
                            timestamp_str = datetime.fromisoformat(row.get('timestamp', '')).strftime('%H:%M:%S')
                            result_display = self.Results.PASS if not details.get('has_error_or_reset') else self.Results.FAIL_MISMATCH
                            values_to_display = (set_id, first_scan, *other_scans + [""]*(4-len(other_scans)), result_display, timestamp_str)
                            completed_sets[set_id] = {'values': values_to_display, 'tags': ("success" if result_display == self.Results.PASS else "error",), 'details': details}
                        
                        elif event == self.Events.SET_DELETED and details.get('set_id'):
                            voided_set_ids.add(details['set_id'])
            except Exception as e:
                print(f"기록 파일 로드 오류 ({filepath}): {e}")

            print(f"   -> {rows_found}개의 행을 읽었습니다.")
            final_sets = {sid: data for sid, data in completed_sets.items() if sid not in voided_set_ids}
            sorted_final_sets = sorted(final_sets.items(), key=lambda item: item[1]['details'].get('end_time'))
            
            temp_scan_count = defaultdict(int)
            temp_global_scanned_set = set()
            temp_set_details_map = {sid: data['details'] for sid, data in final_sets.items()}
            
            for set_id, data in sorted_final_sets:
                details = data['details']
                if not details.get('has_error_or_reset'):
                    passed_code = details.get('item_code')
                    if passed_code: temp_scan_count[passed_code] += 1
                
                raw_scans = details.get('scanned_product_barcodes', [])
                if raw_scans and len(raw_scans) > 1:
                    temp_global_scanned_set.update(raw_scans[1:])
            
            result_queue.put({'sorted_sets': sorted_final_sets, 'scan_count': temp_scan_count, 'global_scanned_set': temp_global_scanned_set, 'set_details_map': temp_set_details_map})

        except Exception as e:
            print(f"백그라운드 기록 로딩 중 치명적 오류: {e}")
            result_queue.put({'error': str(e)})

    def _process_history_queue(self, *args, **kwargs):
        try:
            result = self.history_queue.get_nowait()
            if self.history_tree.exists("loading"): self.history_tree.delete("loading")
            if 'error' in result:
                messagebox.showerror("기록 로딩 오류", f"과거 기록을 불러오는 중 오류가 발생했습니다:\n{result['error']}")
                return

            self.scan_count = result['scan_count']
            self.global_scanned_set = result['global_scanned_set']
            self.set_details_map = result['set_details_map']
            sorted_final_sets = result['sorted_sets']
            
            for index, (set_id, data) in enumerate(sorted_final_sets, 1):
                values = list(data['values'])
                values[0] = index
                self.history_tree.insert("", "end", iid=str(set_id), values=tuple(values), tags=data['tags'])

            self._update_summary_tree()
            print("비동기 기록 로드 및 UI 적용 완료.")
        except queue.Empty:
            self.after(100, self._process_history_queue)
        except Exception as e:
            print(f"UI 업데이트 중 오류 발생: {e}")
            if self.history_tree.exists("loading"): self.history_tree.delete("loading")
            messagebox.showerror("UI 업데이트 오류", f"결과를 화면에 표시하는 중 오류가 발생했습니다:\n{e}")

    def process_input(self, event=None):
        if self.is_blinking or not self.initialized_successfully: return

        if not self.validator:
            print("경고: Validator가 아직 초기화되지 않았습니다.")
            return

        raw_input = self.entry.get()
        self.entry.delete(0, tk.END)
        if not raw_input: return

        clean_input = re.sub(r'[^\x20-\x7E]', '', raw_input)
        scan_pos = len(self.current_set_info['raw']) + 1

        if not self.active_rule_name:
            self.active_rule_name = self.validator.find_matching_rule_name(clean_input)
            if not self.active_rule_name:
                self._handle_input_error(raw_input, "지원하지 않는 바코드 형식입니다.")
                return

        if scan_pos > 1 and clean_input in self.global_scanned_set:
            self._handle_input_error(raw_input, "전체 기록에서 중복된 바코드입니다.")
            return
            
        active_rule_set = self.validator.rules[self.active_rule_name]
        edited_code, error = self.validator.validate(clean_input, scan_pos, active_rule_set)

        if error:
            self._handle_input_error(raw_input, error)
            return

        if scan_pos > 1 and edited_code != self.current_set_info['parsed'][0]:
            self._handle_mismatch(raw_input, edited_code, self.current_set_info['parsed'][0])
            return

        if scan_pos > 1 and clean_input in self.current_set_info['raw']:
            self._handle_input_error(raw_input, "현재 세트 내 중복 바코드입니다.")
            return

        self._update_on_success_scan(clean_input, edited_code)

    def _update_on_success_scan(self, raw, parsed):
        self.update_big_display(parsed, "green")

        if len(self.current_set_info['raw']) == 0:
            self.current_set_info['id'] = str(time.time_ns())
            self.current_set_info['start_time'] = datetime.now()

        self.current_set_info['raw'].append(raw)
        self.current_set_info['parsed'].append(parsed)
        num_scans = len(self.current_set_info['parsed'])
        self._play_sound(f"scan_{num_scans}")
        self.progress_bar['value'] = num_scans
        self._update_status_label()
        self._update_history_tree_in_progress()
        self.data_manager.log_event(self.Events.SCAN_OK, {"raw": raw, "parsed": parsed, "set_id": self.current_set_info['id']}, self.worker_name)
        
        if num_scans == 5:
            self._finalize_set(self.Results.PASS)

    def _finalize_set(self, result, error_details=""):
        if result == self.Results.PASS:
            self._play_sound("pass")

        raw_scans_to_log = self.current_set_info['raw'].copy()
        parsed_scans_to_log = self.current_set_info['parsed'].copy()
        item_code = parsed_scans_to_log[0] if parsed_scans_to_log else "N/A"
        item_info = self.items_data.get(item_code, {})
        item_name = item_info.get("Item Name", "알 수 없음")
        item_spec = item_info.get("Spec", "")
        start_time = self.current_set_info.get('start_time')
        work_time_sec = (datetime.now() - start_time).total_seconds() if start_time else 0.0
        
        if result == self.Results.PASS:
            if item_code != "N/A":
                self.scan_count[item_code] += 1
                self.global_scanned_set.update(raw_scans_to_log[1:])
        
        details = {
            'master_label_code': item_code, 'item_code': item_code, 'item_name': item_name,
            'spec': item_spec, 'scan_count': len(raw_scans_to_log),
            'scanned_product_barcodes': raw_scans_to_log,
            'parsed_product_barcodes': parsed_scans_to_log,
            'work_time_sec': work_time_sec,
            'error_count': self.current_set_info.get('error_count', 0),
            'has_error_or_reset': self.current_set_info.get('has_error_or_reset', False) or (result != self.Results.PASS),
            'is_partial_submission': False, 'start_time': start_time,
            'end_time': datetime.now()
        }
        self.data_manager.log_event(self.Events.TRAY_COMPLETE, details, self.worker_name)
        self.set_details_map[item_code] = details
        set_id_str = str(self.current_set_info['id'])

        if self.history_tree.exists(set_id_str):
            current_values = list(self.history_tree.item(set_id_str, 'values'))
            display_id = current_values[0]
            final_timestamp = datetime.now().strftime('%H:%M:%S')
            
            other_scans_for_display = parsed_scans_to_log[1:5]
            padded_scans = other_scans_for_display + [""] * (4 - len(other_scans_for_display))
            values_to_update = (display_id, item_code, *padded_scans, result, final_timestamp)
            
            self.history_tree.item(set_id_str, values=values_to_update, tags=("success" if result == self.Results.PASS else "error",))

        self.save_status_label.config(text=f"✓ 기록됨 ({datetime.now().strftime('%H:%M:%S')})")
        self.after(3000, lambda: self.save_status_label.config(text=""))
        self._update_summary_tree()
        self._reset_current_set(from_finalize=True)

    def _handle_input_error(self, raw, reason):
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        self.update_big_display(str(raw), "red")
        self.status_label.config(text=f"❌ 입력 오류: {reason}", style="Error.TLabel")
        self._trigger_modal_error(f"입력값이 올바르지 않습니다.\n({reason})", self.Results.FAIL_INPUT_ERROR, raw)

    def _handle_mismatch(self, raw, edited, master):
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        error_message = f"불일치: {edited}\n(기준: {master})"
        self.update_big_display(raw, "red")
        self.status_label.config(text=f"❌ {error_message.replace(chr(10), ' ')}", style="Error.TLabel")
        self._trigger_modal_error(error_message, self.Results.FAIL_MISMATCH, edited)

    def _delete_selected_row(self):
        selected_iids = self.history_tree.selection()
        if not selected_iids:
            messagebox.showwarning("경고", "삭제할 행을 선택하세요.")
            return
        if not messagebox.askyesno("삭제 확인", f"선택된 {len(selected_iids)}개 기록을 삭제(무효화)합니다.\n이 작업은 되돌릴 수 없습니다. 계속하시겠습니까?"):
            return
        for iid in selected_iids:
            if iid == 'loading': continue
            values = self.history_tree.item(iid, 'values')
            item_code_from_row = values[1]
            details = {'set_id': iid, 'deleted_values': values}
            self.data_manager.log_event(self.Events.SET_DELETED, details, self.worker_name)
            
            result, passed_code = values[6], item_code_from_row
            if result == self.Results.PASS and passed_code in self.scan_count:
                self.scan_count[passed_code] -= 1
                if self.scan_count[passed_code] == 0:
                    del self.scan_count[passed_code]
            
            if item_code_from_row in self.set_details_map:
                deleted_details = self.set_details_map.get(item_code_from_row, {})
                raw_scans_to_remove = deleted_details.get('scanned_product_barcodes', [])
                if len(raw_scans_to_remove) > 1:
                    for barcode in raw_scans_to_remove[1:]:
                        self.global_scanned_set.discard(barcode)
            
            self.history_tree.delete(iid)
        self._update_summary_tree()
        self._on_history_tree_select()
        messagebox.showinfo("완료", f"{len(selected_iids)}개 기록이 삭제 처리되었습니다. 통계가 업데이트 되었습니다.")

    def _reset_current_set(self, full_reset=False, from_finalize=False):
        if self.is_blinking: return
        
        if full_reset and self.current_set_info.get('id'):
            self.data_manager.log_event(self.Events.SET_CANCELLED, {"set_id": self.current_set_info['id'], "cancelled_set": self.current_set_info}, self.worker_name)
            if self.history_tree.exists(str(self.current_set_info['id'])):
                self.history_tree.delete(str(self.current_set_info['id']))
            self.current_set_info['has_error_or_reset'] = True
        
        self.current_set_info = {
            'id': None, 'parsed': [], 'raw': [],
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False
        }
        self.active_rule_name = None
        self.progress_bar['value'] = 0
        if self.initialized_successfully:
            self.status_label.config(text="첫 번째 바코드를 스캔하세요...", style="Status.TLabel")
            self.update_big_display("바코드를 스캔하세요.", "")
            self.entry.focus_set()

    def _close_popup(self, popup, result, error_details):
        if popup.winfo_exists():
            popup.grab_release()
            popup.destroy()
        self.is_blinking = False
        self.entry.focus_set()
        if not self.current_set_info.get('id'):
            self.current_set_info['id'] = str(time.time_ns())
        self.after(10, lambda: self._finalize_set(result, error_details))

    def _play_error_siren_loop(self):
        sound = self.sound_objects.get("fail")
        if not sound:
            self.after_idle(lambda: messagebox.showwarning("사운드 설정 오류", "오류 사운드('fail')가 로드되지 않았습니다."))
            return

        try:
            sound.play(loops=-1)
            while self.is_blinking:
                time.sleep(0.1)
            sound.stop()
        except Exception as e:
            self.after_idle(lambda: messagebox.showerror("사운드 재생 오류", f"사운드 재생 중 오류가 발생했습니다.\n{e}"))

    def _trigger_modal_error(self, message, result, error_details):
        if self.is_blinking: return
        self.is_blinking = True
        threading.Thread(target=self._play_error_siren_loop, daemon=True).start()
        self.after(0, self._blink_background_loop)
        try:
            popup = tk.Toplevel(self)
            popup.title("⚠️ 시스템 경고")
            popup.attributes('-fullscreen', True)
            popup.attributes('-topmost', True)
            popup_frame = tk.Frame(popup, bg=self.colors.get("danger", "#E74C3C"))
            popup_frame.pack(expand=True, fill='both')
            label = tk.Label(popup_frame, text=f"⚠️\n\n{message}", font=("Impact", 80, "bold"), fg='white', bg=self.colors.get("danger", "#E74C3C"), anchor='center', justify='center', wraplength=self.winfo_screenwidth() - 100)
            label.pack(pady=40, expand=True, fill='both')
            btn_frame = ttk.Frame(popup_frame, style='Borderless.TFrame', background=self.colors.get("danger", "#E74C3C")) # 추가: 배경색 적용
            btn_frame.pack(pady=40)
            btn = tk.Button(btn_frame, text="확인", command=lambda: self._close_popup(popup, result, error_details), font=("Impact", 36, "bold"), bg="yellow", fg="black", relief="raised", borderwidth=5)
            btn.pack(ipady=20, ipadx=50)
            popup.focus_force()
            btn.focus_set()
            popup.protocol("WM_DELETE_WINDOW", lambda: self._close_popup(popup, result, error_details))
            self.update_idletasks()
            popup.transient(self)
            popup.grab_set()
        except Exception as e:
            log_details = {"context": "modal_popup_creation", "error": str(e), "original_message": message}
            self.data_manager.log_event(self.Events.UI_ERROR, log_details, self.worker_name)
            print(f"CRITICAL: 경고 팝업 생성 실패! 원인: {e}")
            self.is_blinking = False
            fail_sound = self.sound_objects.get("fail")
            if fail_sound: fail_sound.stop()
            messagebox.showerror("시스템 오류", f"치명적인 오류가 발생하여 경고창을 표시할 수 없습니다.\n\n[기존 오류 메시지]\n{message}")
            self._reset_current_set(full_reset=True)

    def open_settings_window(self):
        if self.current_set_info.get('id'):
            messagebox.showwarning("작업 중 경고", "현재 스캔 작업이 진행 중입니다.\n설정 변경은 다음 작업부터 적용됩니다.")
        settings_window = tk.Toplevel(self)
        settings_window.title("설정")
        settings_window.geometry("600x200")
        settings_window.resizable(False, False)
        settings_window.transient(self)
        settings_window.grab_set()
        settings_window.configure(bg=self.colors.get("background", "#ECEFF1"))

        main_frame = ttk.Frame(settings_window, padding=20, style="TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(main_frame, text="현재 작업자 이름:", font=(self.default_font_name, 11)).grid(row=0, column=0, sticky='w', pady=(20,5), padx=(0, 10))
        self.worker_name_var = tk.StringVar(value=self.worker_name)
        worker_entry = ttk.Entry(main_frame, textvariable=self.worker_name_var, font=(self.default_font_name, 10))
        worker_entry.grid(row=1, column=0, columnspan=3, sticky='ew')

        button_frame = ttk.Frame(main_frame, padding=(0, 20, 0, 0), style="TFrame")
        button_frame.grid(row=2, column=0, columnspan=3, sticky='e', pady=(20,0))

        save_button = ttk.Button(button_frame, text="저장", command=lambda: self._save_settings_and_close(settings_window, self.worker_name_var.get()))
        save_button.pack(side=tk.LEFT, padx=5)
        cancel_button = ttk.Button(button_frame, text="취소", command=settings_window.destroy)
        cancel_button.pack(side=tk.LEFT)

    def _save_settings_and_close(self, window: tk.Toplevel, new_worker_name: str):
        if not new_worker_name.strip():
            messagebox.showerror("오류", "작업자 이름은 비워둘 수 없습니다.", parent=window)
            return

        self.worker_name = new_worker_name.strip()
        self._save_app_settings()
        
        messagebox.showinfo("저장 완료", f"설정이 변경되었습니다. 작업자 이름이 '{self.worker_name}'(으)로 업데이트 되었습니다.", parent=window)
        window.destroy()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        self.top_card = ttk.Frame(main_frame, style="Card.TFrame", padding=25)
        self.top_card.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        self.top_card.grid_columnconfigure(0, weight=1)

        self.big_display_label = ttk.Label(self.top_card, text="바코드를 스캔하세요.", anchor="center", wraplength=1200)
        self.big_display_label.grid(row=0, column=0, sticky="ew", pady=(20, 30), ipady=10)

        settings_button = ttk.Button(self.top_card, text="⚙️", command=self.open_settings_window, style='Control.TButton')
        settings_button.place(relx=1.0, rely=0.0, x=-20, y=20, anchor='ne')

        input_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        input_frame.grid(row=1, column=0, sticky="ew")
        input_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(input_frame, text="바코드 입력:", style="TLabel", background=self.colors.get("card_background", "#FFFFFF")).grid(row=0, column=0, padx=(0, 10), sticky='w')

        self.entry = ttk.Entry(input_frame, style="TEntry", state='disabled')
        self.entry.grid(row=0, column=1, sticky="ew")
        self.entry.bind("<Return>", self.process_input)

        progress_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        progress_frame.grid(row=2, column=0, sticky="ew", pady=(20, 0))
        progress_frame.grid_columnconfigure(0, weight=1)

        self.status_label = ttk.Label(progress_frame, text="첫 번째 바코드를 스캔하세요...", style="Status.TLabel", background=self.colors.get("card_background", "#FFFFFF"))
        self.status_label.grid(row=0, column=0, sticky="w", padx=10)

        self.progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', length=100, mode='determinate', maximum=5, style="green.Horizontal.TProgressbar")
        self.progress_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))

        self.content_pane = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        self.content_pane.grid(row=1, column=0, sticky="nsew", pady=(10, 0))

        history_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=25)
        self.content_pane.add(history_card, weight=3)
        history_card.grid_rowconfigure(1, weight=1)
        history_card.grid_columnconfigure(0, weight=1)

        hist_header_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        hist_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hist_header_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(hist_header_frame, text="스캔 기록", style="Header.TLabel", background=self.colors.get("card_background", "#FFFFFF")).grid(row=0, column=0, sticky="w")
        font_control_frame = ttk.Frame(hist_header_frame, style="Borderless.TFrame")
        font_control_frame.grid(row=0, column=2, sticky="e")
        decrease_font_btn = ttk.Button(font_control_frame, text="-", style="Control.TButton", command=self._decrease_tree_font)
        decrease_font_btn.pack(side=tk.LEFT, padx=(0, 0))
        increase_font_btn = ttk.Button(font_control_frame, text="+", style="Control.TButton", command=self._increase_tree_font)
        increase_font_btn.pack(side=tk.LEFT)

        tree_frame_hist = ttk.Frame(history_card, style="Card.TFrame")
        tree_frame_hist.grid(row=1, column=0, sticky='nsew')
        tree_frame_hist.grid_rowconfigure(0, weight=1)
        tree_frame_hist.grid_columnconfigure(0, weight=1)
        hist_cols = list(self.hist_proportions.keys())
        v_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.VERTICAL)
        self.history_tree = ttk.Treeview(tree_frame_hist, columns=hist_cols, show="headings", yscrollcommand=v_scroll_hist.set)
        v_scroll_hist.config(command=self.history_tree.yview)
        col_map = {"Set": "#", "Input1": "현품표", "Input2": "입력 2", "Input3": "입력 3", "Input4": "입력 4", "Input5": "라벨지", "Result": "결과", "Timestamp": "시간"}
        for col, name in col_map.items():
            self.history_tree.heading(col, text=name, anchor="w", command=lambda c=col: self._treeview_sort_column(self.history_tree, c, False))
            self.history_tree.column(col, anchor="w")
        v_scroll_hist.pack(side=tk.RIGHT, fill=tk.Y)
        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.history_tree.bind("<Configure>", self._resize_all_columns)
        self.history_tree.bind("<ButtonRelease-1>", self._on_history_tree_resize_release)

        summary_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=25)
        self.content_pane.add(summary_card, weight=1)
        summary_card.grid_rowconfigure(1, weight=1)
        summary_card.grid_columnconfigure(0, weight=1)
        ttk.Label(summary_card, text="누적 통과 코드", style="Header.TLabel").grid(row=0, column=0, sticky='w', pady=(0, 10))
        tree_frame_sum = ttk.Frame(summary_card, style="Card.TFrame")
        tree_frame_sum.grid(row=1, column=0, sticky='nsew')
        tree_frame_sum.grid_rowconfigure(0, weight=1)
        tree_frame_sum.grid_columnconfigure(0, weight=1)
        summary_cols = list(self.summary_proportions.keys())
        v_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient=tk.VERTICAL)
        self.summary_tree = ttk.Treeview(tree_frame_sum, columns=summary_cols, show="headings", yscrollcommand=v_scroll_sum.set)
        v_scroll_sum.config(command=self.summary_tree.yview)
        self.summary_tree.heading("Code", text="코드", anchor="w", command=lambda: self._treeview_sort_column(self.summary_tree, "Code", False))
        self.summary_tree.heading("Count", text="횟수", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Count", False))
        v_scroll_sum.pack(side=tk.RIGHT, fill=tk.Y)
        self.summary_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.summary_tree.bind("<Configure>", self._resize_all_columns)
        self.summary_tree.bind("<ButtonRelease-1>", self._on_summary_tree_resize_release)

        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.grid(row=2, column=0, sticky="ew", pady=(20, 0))
        bottom_frame.grid_columnconfigure(2, weight=1)
        reset_button = ttk.Button(bottom_frame, text="현재 세트 취소 (F1)", command=lambda: self._reset_current_set(full_reset=True))
        reset_button.grid(row=0, column=0, sticky="w")
        self.bind("<F1>", lambda e: self._reset_current_set(full_reset=True))
        self.del_row_btn = ttk.Button(bottom_frame, text="선택 행 삭제 (Delete)", command=self._delete_selected_row)
        self.del_row_btn.grid(row=0, column=1, sticky="w", padx=(15, 0))
        self.bind("<Delete>", lambda e: self._delete_selected_row())
        self.del_row_btn.state(['disabled'])
        self.history_tree.bind("<<TreeviewSelect>>", self._on_history_tree_select)
        self.save_status_label = ttk.Label(bottom_frame, text="", style="Save.Success.TLabel", background=self.colors.get("background", "#ECEFF1"))
        self.save_status_label.grid(row=0, column=2, sticky="w", padx=20)
        self.clock_label = ttk.Label(bottom_frame, text="", style="TLabel", background=self.colors.get("background", "#ECEFF1"))
        self.clock_label.grid(row=0, column=3, sticky="e", padx=20)

        self.loading_overlay = ttk.Frame(main_frame, style="Overlay.TFrame")
        loading_content_frame = ttk.Frame(self.loading_overlay, style="Overlay.TFrame")
        loading_content_frame.pack(expand=True)
        loading_label = ttk.Label(loading_content_frame, text="데이터를 불러오는 중입니다...", style="Loading.TLabel")
        loading_label.pack(pady=(0, 10))
        self.loading_progressbar = ttk.Progressbar(loading_content_frame, mode='indeterminate', length=300)
        self.loading_progressbar.pack(pady=10)

    def _configure_base_styles(self):
        self.style.theme_use('clam')
        self.style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        self.style.configure("TFrame", background=self.colors.get("background", "#ECEFF1"))
        self.style.configure("Card.TFrame", background=self.colors.get("card_background", "#FFFFFF"), borderwidth=1, relief='solid', bordercolor=self.colors.get("border", "#BDBDBD"))
        self.style.configure("Borderless.TFrame", background=self.colors.get("card_background", "#FFFFFF"), borderwidth=0)
        self.style.configure("ErrorCard.TFrame", background=self.colors.get("danger", "#E74C3C"), borderwidth=1, relief='solid', bordercolor=self.colors.get("danger", "#E74C3C"))
        self.style.configure("TLabel", background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("text", "#212121"))
        self.style.configure("Header.TLabel", background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("text", "#212121"))
        self.style.configure("TButton", padding=8, relief="flat", borderwidth=0, background=self.colors.get("primary", "#455A64"), foreground='white')
        self.style.map("TButton", background=[('active', self.colors.get("primary_active", "#263238")), ('disabled', self.colors.get("border", "#BDBDBD"))], foreground=[('disabled', self.colors.get("text_subtle", "#616161"))])
        self.style.configure("Control.TButton", padding=(1, 1), font=(self.default_font_name, 10), background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("text", "#212121"), relief="groove", borderwidth=1, bordercolor=self.colors.get("border", "#BDBDBD"))
        self.style.map("Control.TButton", background=[('active', self.colors.get("background", "#ECEFF1"))])
        self.style.configure("Status.TLabel", background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("text_subtle", "#616161"))
        self.style.configure("Success.TLabel", background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("success", "#00875A"))
        self.style.configure("Error.TLabel", background=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("danger", "#E57373"))
        self.style.configure("Save.Success.TLabel", background=self.colors.get("background", "#ECEFF1"), foreground=self.colors.get("success", "#00875A"))
        self.style.configure("green.Horizontal.TProgressbar", background=self.colors.get("success", "#80CBC4"), troughcolor=self.colors.get("border", "#BDBDBD"), borderwidth=0)
        self.style.configure("TEntry", bordercolor=self.colors.get("border", "#BDBDBD"), fieldbackground=self.colors.get("card_background", "#FFFFFF"))
        self.style.configure("TScrollbar", gripcount=0, troughcolor=self.colors.get("background", "#ECEFF1"), bordercolor=self.colors.get("background", "#ECEFF1"), lightcolor=self.colors.get("background", "#ECEFF1"), darkcolor=self.colors.get("background", "#ECEFF1"), arrowcolor=self.colors.get("text_subtle", "#616161"), background=self.colors.get("border", "#BDBDBD"))
        self.style.map("TScrollbar", background=[('active', self.colors.get("text_subtle", "#616161"))])

        overlay_bg = self.colors.get("background", "#ECEFF1")
        self.style.configure("Overlay.TFrame", background=overlay_bg)
        self.style.configure("Loading.TLabel", background=overlay_bg, foreground=self.colors.get("text", "#212121"), font=(self.default_font_name, 20, "bold"))

    def _configure_treeview_styles(self):
        self.style.configure("Treeview", background=self.colors.get("card_background", "#FFFFFF"), fieldbackground=self.colors.get("card_background", "#FFFFFF"), foreground=self.colors.get("text", "#212121"), borderwidth=0, relief='flat')
        self.style.map("Treeview", background=[('selected', self.colors.get("primary", "#455A64"))], foreground=[('selected', 'white')])
        self.style.configure("Treeview.Heading", background=self.colors.get("heading_background", "#FFFFFF"), foreground=self.colors.get("text_subtle", "#616161"), relief="flat", borderwidth=0)
        self.style.map("Treeview.Heading", background=[('active', self.colors.get("background", "#ECEFF1"))])
        self.history_tree.tag_configure("success", background=self.colors.get("success_light", "#E0F2F1"), foreground=self.colors.get("text_strong", "#000000"))
        self.history_tree.tag_configure("error", background=self.colors.get("danger_light", "#FFCDD2"), foreground=self.colors.get("text_strong", "#000000"))
        self.history_tree.tag_configure("in_progress", foreground=self.colors.get("text_subtle", "#616161"), background=self.colors.get("card_background", "#FFFFFF"))

    def _increase_tree_font(self):
        if not self.initialized_successfully: return
        self.tree_font_size = min(20, self.tree_font_size + 1)
        self._apply_tree_font_style()

    def _decrease_tree_font(self):
        if not self.initialized_successfully: return
        self.tree_font_size = max(6, self.tree_font_size - 1)
        self._apply_tree_font_style()

    def _apply_tree_font_style(self):
        try:
            tree_font = (self.default_font_name, self.tree_font_size)
            row_height_scale = self.ui_cfg.get("treeview_row_height_scale", 3.0)
            row_height = int(self.tree_font_size * row_height_scale * 0.8)
            self.style.configure("Treeview", font=tree_font, rowheight=row_height)
        except Exception as e:
            print(f"테이블 폰트 적용 오류: {e}")

    def on_ctrl_wheel(self, event):
        if not self.initialized_successfully: return
        if event.delta > 0: self._zoom_in()
        else: self._zoom_out()
        return "break"

    def _zoom_in(self):
        self.scale_factor = min(3.0, self.scale_factor + 0.1)
        self._update_ui_scaling()

    def _zoom_out(self):
        self.scale_factor = max(0.5, self.scale_factor - 0.1)
        self._update_ui_scaling()

    def _update_ui_scaling(self):
        if not self.initialized_successfully: return

        font_size = int(self.base_font_size * self.scale_factor)
        header_scale = self.ui_cfg.get("header_font_scale", 1.4)
        status_scale = self.ui_cfg.get("status_font_scale", 1.1)
        big_display_scale = self.ui_cfg.get("big_display_font_scale", 4.0)
        default_font = (self.default_font_name, font_size)
        bold_font = (self.default_font_name, font_size, "bold")
        header_font = (self.default_font_name, int(font_size * header_scale), "bold")
        status_font = (self.default_font_name, int(font_size * status_scale))
        status_bold_font = (self.default_font_name, int(font_size * status_scale), "bold")
        save_status_font = (self.default_font_name, int(font_size * 0.9), "bold")
        tree_heading_font = (self.default_font_name, int(font_size * 1.0), "bold")
        big_display_font = (self.default_font_name, min(int(font_size * big_display_scale), 70), "bold")
        clock_font = ("Consolas", int(font_size * 0.95))
        self.style.configure("TLabel", font=default_font)
        self.style.configure("Header.TLabel", font=header_font)
        self.entry.configure(font=default_font)
        self.style.configure("Treeview.Heading", font=tree_heading_font)
        self.style.configure("TButton", font=bold_font)
        self.style.configure("Status.TLabel", font=status_font)
        self.style.configure("Success.TLabel", font=status_bold_font)
        self.style.configure("Error.TLabel", font=status_bold_font)
        self.style.configure("Save.Success.TLabel", font=save_status_font)
        self.style.configure("Control.TButton", font=(self.default_font_name, int(font_size * 0.8), "bold"))
        self.big_display_label.config(font=big_display_font)
        self.clock_label.config(font=clock_font)
        self._apply_tree_font_style()
        self._resize_all_columns()
        
        if self.sash_position is not None:
            try:
                self.after(50, lambda: self.content_pane.sashpos(0, self.sash_position))
            except TclError as e:
                print(f"Sash 위치 적용 중 오류 발생 (무시 가능): {e}")


    def _resize_all_columns(self, event=None):
        if not self.initialized_successfully: return
        padding = self.ui_cfg.get("column_padding", 20)
        try:
            hist_width = self.history_tree.winfo_width() - padding
            if hist_width > 1:
                if self.history_col_widths:
                    current_total_width = sum(self.history_col_widths.values())
                    scale = hist_width / current_total_width if current_total_width > 0 else 0
                    for col in self.history_col_widths:
                        if col in self.history_tree['columns']:
                            self.history_tree.column(col, width=int(self.history_col_widths[col] * scale))
                else:
                    total_prop = sum(self.hist_proportions.values())
                    for col, prop in self.hist_proportions.items():
                        if col in self.history_tree['columns']:
                            self.history_tree.column(col, width=int(hist_width * (prop / total_prop)))

            summary_width = self.summary_tree.winfo_width() - padding
            if summary_width > 1:
                if self.summary_col_widths:
                    current_total_width = sum(self.summary_col_widths.values())
                    scale = summary_width / current_total_width if current_total_width > 0 else 0
                    for col in self.summary_col_widths:
                        if col in self.summary_tree['columns']:
                            self.summary_tree.column(col, width=int(self.summary_col_widths[col] * scale))
                else:
                    total_prop = sum(self.summary_proportions.values())
                    for col, prop in self.summary_proportions.items():
                           if col in self.summary_tree['columns']:
                                self.summary_tree.column(col, width=int(summary_width * (prop / total_prop)))
        except (TclError, KeyError):
            pass

    def _on_summary_tree_resize_release(self, event):
        if not self.initialized_successfully: return
        for col in self.summary_tree['columns']:
            self.summary_col_widths[col] = self.summary_tree.column(col, 'width')

    def _on_history_tree_resize_release(self, event):
        if not self.initialized_successfully: return
        for col in self.history_tree['columns']:
            self.history_col_widths[col] = self.history_tree.column(col, 'width')

    def _treeview_sort_column(self, tv, col, reverse):
        if not self.initialized_successfully: return
        try:
            items = [item for item in tv.get_children('') if item != 'loading']
            if col == 'Set' or col == 'Count':
                l = sorted([(int(tv.set(k, col)), k) for k in items], reverse=reverse)
            else:
                l = sorted([(tv.set(k, col), k) for k in items], reverse=reverse)
            
            for index, (val, k) in enumerate(l): tv.move(k, '', index)
            tv.heading(col, command=lambda: self._treeview_sort_column(tv, col, not reverse))
        except (ValueError, TclError) as e:
            print(f"정렬 오류: {e}")
            pass

    def _on_history_tree_select(self, event=None):
        if not self.initialized_successfully: return
        self.del_row_btn.state(['!disabled'] if self.history_tree.selection() else ['disabled'])

    def _update_clock(self):
        if self.initialized_successfully:
            self.clock_label.config(text=time.strftime('%Y-%m-%d %H:%M:%S'))
        self.after(1000, self._update_clock)

    def update_big_display(self, text, color=""):
        fg_color = self.colors.get("text_strong", "#000000")
        if color == "red": fg_color = self.colors.get("danger", "#E57370")
        elif color == "green": fg_color = self.colors.get("success", "#00875A")
        self.big_display_label.config(text=text or "", foreground=fg_color)

    def _play_sound(self, sound_key, block=False):
        if not self.initialized_successfully: return
        sound = self.sound_objects.get(sound_key)
        if sound:
            try:
                sound.play()
            except Exception as e:
                print(f"pygame 사운드 재생 오류: {e}")
        else:
            if sound_key in self.sounds:
                print(f"경고: 사운드 키 '{sound_key}'가 존재하지만, 로드되지 않았습니다. 파일 경로를 확인하세요.")

    def _update_summary_tree(self):
        if not self.initialized_successfully: return
        self.summary_tree.delete(*self.summary_tree.get_children())
        filtered_items = {code: count for code, count in self.scan_count.items() if count > 0}
        sorted_items = sorted(filtered_items.items(), key=lambda item: item[1], reverse=True)
        for code, count in sorted_items:
            self.summary_tree.insert("", "end", values=(code, count))

    def _update_status_label(self):
        if not self.initialized_successfully: return
        num_scans = len(self.current_set_info['parsed'])
        rule_name = self.active_rule_name or '규칙 탐색중'
        if self.current_set_info.get('has_error_or_reset'):
            rule_name += " (오류 발생)"
        status_text = f"{num_scans}/5 스캔됨 | {rule_name}"
        self.status_label.config(text=status_text, style="Status.TLabel")

    def _update_history_tree_in_progress(self):
        if not self.initialized_successfully: return
        num_scans = len(self.current_set_info['parsed'])
        if num_scans == 0: return

        set_id = str(self.current_set_info['id'])
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        parsed_scans = self.current_set_info['parsed']
        other_scans_for_display = parsed_scans[1:]

        values = (
            "...",
            parsed_scans[0],
            *other_scans_for_display[:4] + [""] * (4 - len(other_scans_for_display)),
            self.Results.IN_PROGRESS,
            timestamp
        )

        if self.history_tree.exists(set_id):
            try:
                current_display_id = self.history_tree.item(set_id, 'values')[0]
                values = (current_display_id, *values[1:])
            except IndexError:
                valid_rows = [item for item in self.history_tree.get_children() if item != 'loading']
                values = (len(valid_rows) + 1, *values[1:])

            self.history_tree.item(set_id, values=values, tags=("in_progress",))
        else:
            valid_rows = [item for item in self.history_tree.get_children() if item != 'loading']
            display_id = len(valid_rows) + 1
            values = (display_id, *values[1:])
            self.history_tree.insert("", 0, values=values, iid=set_id, tags=("in_progress",))

        self.history_tree.yview_moveto(0)

    def _blink_background_loop(self):
        if not hasattr(self, 'top_card') or not self.top_card.winfo_exists(): return
        original_style = "Card.TFrame"
        error_style = "ErrorCard.TFrame"
        def blink():
            if not self.is_blinking:
                if self.top_card.winfo_exists(): self.top_card.config(style=original_style)
                return
            try:
                current_style = self.top_card.cget("style")
                next_style = error_style if current_style == original_style else original_style
                if self.top_card.winfo_exists():
                    self.top_card.config(style=next_style)
                    self.after(400, blink)
            except TclError:
                pass
        self.after(0, blink)

if __name__ == "__main__":
    app = BarcodeScannerApp()
    app.mainloop()