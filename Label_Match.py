import tkinter as tk
from tkinter import ttk, messagebox, TclError, simpledialog
from collections import defaultdict
import csv
from datetime import datetime
import threading
import time
import sys
import os
import json
import tkinter.font as tkFont
import queue
import pygame
import socket
import requests
import zipfile
import subprocess

# #####################################################################
# 자동 업데이트 설정 (Auto-Updater Configuration)
# #####################################################################
REPO_OWNER = "KMTechn"
REPO_NAME = "Label_Match"
APP_VERSION = "v1.0.5"

def check_for_updates():
    """GitHub에서 최신 릴리스 정보를 확인하고, 업데이트가 필요하면 .zip 파일의 다운로드 URL을 반환합니다."""
    try:
        api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        latest_release_data = response.json()
        latest_version = latest_release_data['tag_name']
        if latest_version.strip().lower() != APP_VERSION.strip().lower():
            for asset in latest_release_data['assets']:
                if asset['name'].endswith('.zip'):
                    return asset['browser_download_url'], latest_version
            return None, None
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"업데이트 확인 중 오류 발생 (네트워크 문제일 수 있음): {e}")
        return None, None

def download_and_apply_update(url):
    """업데이트 .zip 파일을 다운로드하고, 압축 해제 후 적용 스크립트를 실행합니다."""
    try:
        temp_dir = os.environ.get("TEMP", "C:\\Temp")
        os.makedirs(temp_dir, exist_ok=True)
        zip_path = os.path.join(temp_dir, "update.zip")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        temp_update_folder = os.path.join(temp_dir, "temp_update")
        if os.path.exists(temp_update_folder):
            import shutil
            shutil.rmtree(temp_update_folder)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_update_folder)
        os.remove(zip_path)
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))
        updater_script_path = os.path.join(application_path, "updater.bat")
        extracted_content = os.listdir(temp_update_folder)
        if len(extracted_content) == 1 and os.path.isdir(os.path.join(temp_update_folder, extracted_content[0])):
            new_program_folder_path = os.path.join(temp_update_folder, extracted_content[0])
        else:
            new_program_folder_path = temp_update_folder
        with open(updater_script_path, "w", encoding='utf-8') as bat_file:
            bat_file.write(f"""@echo off
chcp 65001 > nul
echo.
echo ==========================================================
echo    프로그램을 업데이트합니다. 이 창을 닫지 마세요.
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
echo    업데이트 완료!
echo ========================================
echo.
echo 3초 후에 프로그램을 다시 시작합니다.
timeout /t 3 /nobreak > nul
start "" "{os.path.join(application_path, os.path.basename(sys.executable))}"
del "%~f0"
            """)
        subprocess.Popen(updater_script_path, creationflags=subprocess.CREATE_NEW_CONSOLE)
        sys.exit(0)
    except Exception as e:
        root_alert = tk.Tk()
        root_alert.withdraw()
        messagebox.showerror("업데이트 실패", f"업데이트 적용 중 오류가 발생했습니다.\n\n{e}\n\n프로그램을 다시 시작해주세요.", parent=root_alert)
        root_alert.destroy()
        sys.exit(1)

def threaded_update_check():
    """백그라운드에서 업데이트를 확인하고 필요한 경우 UI에 프롬프트를 표시합니다."""
    print("백그라운드 업데이트 확인 시작...")
    download_url, new_version = check_for_updates()
    if download_url:
        root_alert = tk.Tk()
        root_alert.withdraw()
        if messagebox.askyesno("업데이트 발견", f"새로운 버전({new_version})이 발견되었습니다.\n지금 업데이트하시겠습니까? (현재: {APP_VERSION})", parent=root_alert):
            root_alert.destroy()
            download_and_apply_update(download_url)
        else:
            print("사용자가 업데이트를 거부했습니다.")
            root_alert.destroy()
    else:
        print("업데이트 확인 완료. 새 버전이 없거나 오류 발생.")

# #####################################################################
# 애플리케이션 코드 시작
# #####################################################################
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)
def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)
class DataManager:
    def __init__(self, save_dir, process_name, worker_name, unique_id):
        self.save_directory = save_dir
        self.process_name = process_name
        self.worker_name = worker_name
        self.unique_id = unique_id
        self.log_queue = queue.Queue()
        self.log_thread = threading.Thread(target=self._log_writer_thread, daemon=True)
        self.log_thread.start()
    def _get_log_filepath(self):
        # ### 수정된 부분 ###
        # 파일명에 worker_name 대신 unique_id(컴퓨터 이름)를 사용합니다.
        filename = f"{self.process_name}작업이벤트로그_{self.unique_id}_{datetime.now().strftime('%Y%m%d')}.csv"
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
    def log_event(self, event_type, details):
        log_item = [datetime.now().isoformat(), self.worker_name, event_type, json.dumps(details, ensure_ascii=False, cls=DateTimeEncoder)]
        self.log_queue.put(log_item)
    def save_current_state(self, state_data):
        state_path = os.path.join(self.save_directory, BarcodeScannerApp.FILES.CURRENT_STATE)
        try:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            state_data_with_worker = {'worker_name': self.worker_name, **state_data}
            with open(state_path, 'w', encoding='utf-8') as f:
                json.dump(state_data_with_worker, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
        except Exception as e:
            print(f"임시 상태 저장 실패: {e}")
    def load_current_state(self):
        state_path = os.path.join(self.save_directory, BarcodeScannerApp.FILES.CURRENT_STATE)
        if not os.path.exists(state_path): return None
        try:
            with open(state_path, 'r', encoding='utf-8') as f: return json.load(f)
        except Exception as e:
            print(f"임시 상태 로드 실패: {e}"); return None
    def delete_current_state(self):
        state_path = os.path.join(self.save_directory, BarcodeScannerApp.FILES.CURRENT_STATE)
        if os.path.exists(state_path):
            try: os.remove(state_path)
            except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")
class BarcodeScannerApp(tk.Tk):
    class FILES:
        CURRENT_STATE = "_current_set_state_packaging.json"
        SETTINGS = "app_settings.json"
        ITEMS = "Item.csv"
    class Events:
        APP_START = "APP_START"
        APP_CLOSE = "APP_CLOSE"
        SCAN_OK = "SCAN_OK"
        TRAY_COMPLETE = "TRAY_COMPLETE"
        SET_CANCELLED = "SET_CANCELLED"
        SET_DELETED = "SET_DELETED"
        SET_RESTORED = "SET_RESTORED"
        UI_ERROR = "UI_ERROR"
        ERROR_INPUT = "ERROR_INPUT"
        ERROR_MISMATCH = "ERROR_MISMATCH"
        SCAN_ATTEMPT = "SCAN_ATTEMPT"
        TRAY_COMPLETION_CANCELLED = "TRAY_COMPLETION_CANCELLED"
    class Results:
        PASS = "통과"
        FAIL_MISMATCH = "불일치"
        FAIL_INPUT_ERROR = "입력오류"
        IN_PROGRESS = "진행중..."
    class Worker:
        PACKAGING = "포장실"
    def __init__(self):
        super().__init__()
        self.initialized_successfully = False
        try:
            pygame.mixer.init()
        except pygame.error as e:
            messagebox.showerror("오디오 초기화 오류", f"Pygame 오디오 시스템을 시작할 수 없습니다.\n오류: {e}")
        self._setup_paths()
        self.app_settings = self._load_app_settings()
        self.custom_save_path = "C:\\Sync"
        self._update_save_directory()
        self.ui_cfg = self.app_settings.get("ui_settings", {})
        self.base_font_size = self.ui_cfg.get("base_font_size", 14)
        self.colors = {
            "background": "#F9FAFB", "card_background": "#FFFFFF", "text": "#111827",
            "text_subtle": "#6B7280", "text_strong": "#000000", "primary": "#3B82F6",
            "primary_active": "#2563EB", "success": "#10B981", "success_light": "#D1FAE5",
            "danger": "#EF4444", "danger_light": "#FEE2E2", "border": "#D1D5DB",
            "heading_background": "#FFFFFF"
        }
        self.sounds = self.app_settings.get("sound_files", {})
        self.sound_objects = {}
        self.items_data = {}
        self.unique_id = socket.gethostname()
        self.worker_name = self.app_settings.get("worker_name", self.Worker.PACKAGING)
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.worker_name, self.unique_id)
        self.current_set_info = {
            'id': None, 'parsed': [], 'raw': [],
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False
        }
        self.is_blinking = False
        self.scan_count = defaultdict(lambda: defaultdict(int))
        self.global_scanned_set = set()
        self.set_details_map = {}
        self.title(f"바코드 세트 검증기 ({APP_VERSION}) - 로딩 중...")
        self.state('zoomed')
        self.configure(bg=self.colors.get("background", "#ECEFF1"))
        self.scale_factor = 1.2
        self.tree_font_size = 13
        self.summary_col_widths = {}
        self.history_col_widths = {}
        self.sash_position = None
        self._load_ui_persistence_settings()
        self.hist_proportions = {"Set": 4, "Input1": 14, "Input2": 14, "Input3": 14, "Input4": 14, "Input5": 14, "Result": 8, "Timestamp": 18}
        self.summary_proportions = {"Date": 20, "Code": 60, "Count": 20}
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
        self.bind("<Button-1>", self._on_root_click)
        
    def _on_root_click(self, event):
        if event.widget not in [self.history_tree, self.summary_tree]:
            self.history_tree.selection_remove(self.history_tree.selection())
            self.summary_tree.selection_remove(self.summary_tree.selection())
        self.entry.focus_set()

    def _async_initial_load(self):
        try:
            items_data = self._load_items_data()
            loaded_data = {"items": items_data}
            self.initial_load_queue.put(loaded_data)
        except Exception as e:
            self.initial_load_queue.put({"error": str(e)})

    def _process_initial_load_queue(self):
        try:
            result = self.initial_load_queue.get_nowait()
            if "error" in result:
                self.hide_loading_overlay()
                messagebox.showerror("초기화 오류", f"프로그램 시작에 필요한 파일을 불러올 수 없습니다.\n\n오류: {result['error']}")
                self.destroy()
                return
            self.items_data = result.get('items', {})
            self.sound_objects = self._preload_sounds()
            self.hide_loading_overlay()
            self.entry.config(state='normal')
            self.entry.focus_set()
            self._reset_current_set()
            self.title(f"바코드 세트 검증기 ({APP_VERSION}) - {self.worker_name} ({self.unique_id})")
            self.data_manager.log_event(self.Events.APP_START, {"message": "Application initialized."})
            self.initialized_successfully = True
            self.history_queue = queue.Queue()
            self._load_history_and_rebuild_summary()
            self._process_history_queue()
            self._load_current_set_state()
            self.after(200, self._update_ui_scaling)
            self._update_clock()
            threading.Thread(target=threaded_update_check, daemon=True).start()
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

    def _update_save_directory(self):
        self.save_directory = self.custom_save_path
        os.makedirs(self.save_directory, exist_ok=True)

    def _load_app_settings(self):
        try:
            with open(self.app_settings_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_app_settings(self):
        try:
            if not self.initialized_successfully: return
            self.app_settings['worker_name'] = self.worker_name
            if "ui_persistence" not in self.app_settings:
                self.app_settings["ui_persistence"] = {}
            self.app_settings["ui_persistence"]["scale_factor"] = self.scale_factor
            self.app_settings["ui_persistence"]["tree_font_size"] = self.tree_font_size
            self.app_settings["ui_persistence"]["sash_position"] = self.content_pane.sashpos(0)
            self.app_settings["ui_persistence"]["summary_col_widths"] = {col: self.summary_tree.column(col, 'width') for col in self.summary_tree['columns']}
            self.app_settings["ui_persistence"]["history_col_widths"] = {col: self.history_tree.column(col, 'width') for col in self.history_tree['columns']}
            with open(self.app_settings_path, 'w', encoding='utf-8') as f:
                json.dump(self.app_settings, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"앱 설정 저장 오류: {e}")

    def _load_ui_persistence_settings(self):
        persistence_settings = self.app_settings.get("ui_persistence", {})
        self.scale_factor = persistence_settings.get("scale_factor", 1.2)
        if not (0.5 <= self.scale_factor <= 3.0): self.scale_factor = 1.2
        self.tree_font_size = persistence_settings.get("tree_font_size", 13)
        if not (6 <= self.tree_font_size <= 20): self.tree_font_size = 13
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
        self.is_blinking = False
        self.data_manager.log_event(self.Events.APP_CLOSE, {"message": "Application closed."})
        self.data_manager.log_queue.put(None)
        self._save_app_settings()
        self.destroy()

    def _save_current_set_state(self):
        if not self.initialized_successfully or not self.current_set_info['raw']: return
        state_data = {'current_set_info': self.current_set_info, 'timestamp': datetime.now().isoformat()}
        self.data_manager.save_current_state(state_data)

    def _load_current_set_state(self):
        state_data = self.data_manager.load_current_state()
        if not state_data: return
        try:
            saved_timestamp_str = state_data.get('timestamp')
            if saved_timestamp_str:
                saved_dt = datetime.fromisoformat(saved_timestamp_str)
                if saved_dt.date() != datetime.now().date():
                    messagebox.showinfo("이전 작업 만료", "어제 완료되지 않은 작업 데이터는 자동으로 삭제됩니다.")
                    self.data_manager.delete_current_state()
                    return
        except (ValueError, TypeError) as e:
            print(f"저장된 타임스탬프 파싱 오류: {e}. 이전 작업을 무시합니다.")
            self.data_manager.delete_current_state()
            return
        msg = f"이전에 완료되지 않은 스캔 세트가 있습니다.\n(스캔 수: {len(state_data.get('current_set_info', {}).get('raw', []))})\n\n이어서 진행하시겠습니까?"
        if messagebox.askyesno("작업 복구", msg):
            saved_worker_name = state_data.get('worker_name')
            if saved_worker_name and saved_worker_name != self.worker_name:
                response = messagebox.askyesnocancel("작업자 불일치",
                                                     f"이 저장된 세트는 '{saved_worker_name}' 작업자의 것입니다.\n"
                                                     f"현재 '{self.worker_name}' 작업자가 이어서 하시겠습니까?",
                                                     icon='warning')
                if response is None: return
                elif response is False:
                    self.data_manager.delete_current_state()
                    messagebox.showinfo("작업 삭제", "이전 작업이 삭제되었습니다.")
                    return
            self.current_set_info = state_data['current_set_info']
            if self.current_set_info.get('start_time') and isinstance(self.current_set_info['start_time'], str):
                self.current_set_info['start_time'] = datetime.fromisoformat(self.current_set_info['start_time'])
            self.data_manager.log_event(self.Events.SET_RESTORED, {"restored_set": self.current_set_info, "continued_by": self.worker_name})
            self.progress_bar['value'] = len(self.current_set_info['raw'])
            self.update_big_display(self.current_set_info['parsed'][-1] if self.current_set_info['parsed'] else "", "green")
            self._update_status_label()
            self._update_history_tree_in_progress()
        else:
            self.data_manager.delete_current_state()

    def _delete_current_set_state(self):
        self.data_manager.delete_current_state()

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
            cancelled_set_ids = set()
            # ### 수정된 부분 ###
            # 로그 파일을 읽을 때 worker_name 대신 unique_id(컴퓨터 이름)를 사용합니다.
            log_filename = f"{self.Worker.PACKAGING}작업이벤트로그_{self.unique_id}_{datetime.now().strftime('%Y%m%d')}.csv"
            filepath = os.path.join(self.save_directory, log_filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            event = row.get('event')
                            details_str = row.get('details', '{}')
                            if not details_str: continue
                            try:
                                details = json.loads(details_str)
                            except json.JSONDecodeError:
                                print(f"경고: JSON 파싱 오류. 건너뜁니다: {details_str}")
                                continue

                            set_id = details.get('set_id')
                            if event == self.Events.SET_DELETED and details.get('set_id'):
                                voided_set_ids.add(details['set_id'])
                                continue
                            if event == self.Events.TRAY_COMPLETION_CANCELLED and details.get('cancelled_set_id'):
                                cancelled_set_ids.add(details.get('cancelled_set_id'))
                                continue
                            
                            if set_id is None: continue

                            if event == self.Events.TRAY_COMPLETE:
                                displays = details.get('parsed_product_barcodes', [])
                                first_scan = displays[0] if displays else "N/A"
                                other_scans = displays[1:5]
                                
                                timestamp_str = datetime.fromisoformat(row.get('timestamp', '')).strftime('%H:%M:%S')
                                result_display = self.Results.PASS if not details.get('has_error_or_reset') else self.Results.FAIL_MISMATCH
                                values_to_display = (set_id, first_scan, *other_scans + [""]*(4-len(other_scans)), result_display, timestamp_str)
                                
                                completed_sets[set_id] = {'values': values_to_display, 'tags': ("success" if result_display == self.Results.PASS else "error",), 'details': details}

                except Exception as e:
                    print(f"기록 파일 로드 오류 ({filepath}): {e}")
            
            final_sets = {sid: data for sid, data in completed_sets.items() if sid not in voided_set_ids and sid not in cancelled_set_ids}
            sorted_final_sets = sorted(final_sets.items(), key=lambda item: item[1]['details'].get('end_time'))
            temp_scan_count = defaultdict(lambda: defaultdict(int))
            temp_global_scanned_set = set()
            temp_set_details_map = {sid: data['details'] for sid, data in final_sets.items()}
            for set_id, data in sorted_final_sets:
                details = data['details']
                if not details.get('has_error_or_reset'):
                    passed_code = details.get('item_code')
                    production_date = details.get('production_date')
                    if passed_code and production_date:
                        temp_scan_count[production_date][passed_code] += 1
                raw_scans = details.get('scanned_product_barcodes', [])
                if raw_scans and len(raw_scans) > 1:
                    temp_global_scanned_set.update(raw_scans[1:])
            result_queue.put({'sorted_sets': sorted_final_sets, 'scan_count': temp_scan_count, 'global_scanned_set': temp_global_scanned_set, 'set_details_map': temp_set_details_map})
        except Exception as e:
            print(f"백그라운드 기록 로딩 오류: {e}")
            result_queue.put({'error': str(e)})

    def _process_history_queue(self):
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
        raw_input = self.entry.get().strip()
        self.entry.delete(0, tk.END)
        if not raw_input: return
        
        self.data_manager.log_event(self.Events.SCAN_ATTEMPT, {"raw_input": raw_input, "scan_pos": len(self.current_set_info['raw']) + 1})
        scan_pos = len(self.current_set_info['raw']) + 1
        MASTER_LABEL_LENGTH = 13
        
        if scan_pos == 1:
            if len(raw_input) != MASTER_LABEL_LENGTH:
                self._handle_input_error(raw_input, f"현품표는 {MASTER_LABEL_LENGTH}자리여야 합니다.")
                return
            if raw_input not in self.items_data:
                self._handle_input_error(raw_input, "미등록 현품표입니다. (Item.csv 확인)")
                return
            self._update_on_success_scan(raw_input, raw_input)
        elif 2 <= scan_pos <= 5:
            master_code = self.current_set_info['parsed'][0]
            
            if scan_pos < 5 and len(raw_input) <= MASTER_LABEL_LENGTH:
                self._handle_input_error(raw_input, f"제품/라벨 바코드는 {MASTER_LABEL_LENGTH}자리보다 길어야 합니다.")
                return
            
            if scan_pos == 5 and len(raw_input) < 31:
                self._handle_input_error(raw_input, "마지막 라벨지는 31자리 이상이어야 합니다.")
                return

            if master_code not in raw_input:
                self._handle_mismatch(raw_input, raw_input, master_code)
                return
            if raw_input in self.current_set_info['raw']:
                self._handle_input_error(raw_input, "현재 세트 내 중복된 바코드입니다.")
                return
            if raw_input in self.global_scanned_set:
                self._handle_input_error(raw_input, "전체 기록에서 이미 스캔된 바코드입니다.")
                return
            
            production_date = None
            if scan_pos == 5:
                production_date = self._extract_production_date(raw_input)
                if not production_date:
                    self._handle_input_error(raw_input, "생산 날짜(6D 필드)를 추출할 수 없습니다.")
                    return
                self.current_set_info['production_date'] = production_date
            
            self._update_on_success_scan(raw_input, master_code)

    def _extract_production_date(self, raw_input):
        try:
            fields = raw_input.split('\x1D')
            for field in fields:
                if field.startswith('6D'):
                    date_str = field[2:]
                    if len(date_str) == 8 and date_str.isdigit():
                        return f"{int(date_str[:4]):04d}-{int(date_str[4:6]):02d}-{int(date_str[6:8]):02d}"
            return None
        except Exception as e:
            print(f"생산 날짜 추출 오류: {e}")
            return None

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
        self.data_manager.log_event(self.Events.SCAN_OK, {"raw": raw, "parsed": parsed, "set_id": self.current_set_info['id']})
        self._save_current_set_state()
        if num_scans == 5:
            self._finalize_set(self.Results.PASS)

    def _finalize_set(self, result, error_details=""):
        if result == self.Results.PASS:
            self._play_sound("pass")
        
        raw_scans_to_log = self.current_set_info['raw'].copy()
        parsed_scans_to_log = self.current_set_info['parsed'].copy()
        item_code = parsed_scans_to_log[0] if parsed_scans_to_log else "N/A"
        item_info = self.items_data.get(item_code, {})
        start_time = self.current_set_info.get('start_time')
        work_time_sec = (datetime.now() - start_time).total_seconds() if start_time else 0.0
        production_date = self.current_set_info.get('production_date')
        
        set_id_for_log = str(self.current_set_info['id'])

        if result == self.Results.PASS:
            if item_code != "N/A" and production_date:
                self.scan_count[production_date][item_code] += 1
                self.global_scanned_set.update(raw_scans_to_log[1:])

        details = {
            'master_label_code': item_code, 'item_code': item_code,
            'item_name': item_info.get("Item Name", "알 수 없음"),
            'spec': item_info.get("Spec", ""),
            'scan_count': len(raw_scans_to_log),
            'scanned_product_barcodes': raw_scans_to_log,
            'parsed_product_barcodes': parsed_scans_to_log,
            'work_time_sec': work_time_sec,
            'error_count': self.current_set_info.get('error_count', 0),
            'has_error_or_reset': self.current_set_info.get('has_error_or_reset', False) or (result != self.Results.PASS),
            'is_partial_submission': False, 'start_time': start_time,
            'end_time': datetime.now(),
            'production_date': production_date,
            'set_id': set_id_for_log 
        }
        self.data_manager.log_event(self.Events.TRAY_COMPLETE, details)
        
        if result == self.Results.PASS:
            self.set_details_map[set_id_for_log] = details

        if self.history_tree.exists(set_id_for_log):
            current_values = list(self.history_tree.item(set_id_for_log, 'values'))
            display_id = current_values[0]
            final_timestamp = datetime.now().strftime('%H:%M:%S')
            
            first_scan_display = parsed_scans_to_log[0] if parsed_scans_to_log else ""
            other_scans_display = parsed_scans_to_log[1:5]
            values_to_update = (display_id, first_scan_display, *other_scans_display + [""]*(4-len(other_scans_display)), result, final_timestamp)
            
            self.history_tree.item(set_id_for_log, values=values_to_update, tags=("success" if result == self.Results.PASS else "error",))
            
        self.save_status_label.config(text=f"✓ 기록됨 ({datetime.now().strftime('%H:%M:%S')})")
        self.after(3000, lambda: self.save_status_label.config(text=""))
        self._update_summary_tree()
        self._reset_current_set(from_finalize=True)

    def _handle_input_error(self, raw, reason):
        self.data_manager.log_event(self.Events.ERROR_INPUT, {"raw": raw, "reason": reason})
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        self.update_big_display(str(raw), "red")
        self.status_label.config(text=f"❌ 입력 오류: {reason}", style="Error.TLabel")
        self._trigger_modal_error(f"입력값이 올바르지 않습니다.\n({reason})", self.Results.FAIL_INPUT_ERROR, raw)
        
    def _handle_mismatch(self, raw, edited, master):
        self.data_manager.log_event(self.Events.ERROR_MISMATCH, {"raw": raw, "edited": edited, "master": master})
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
            if iid == 'loading': 
                continue
            
            deleted_details = self.set_details_map.get(iid)
            values = self.history_tree.item(iid, 'values')
            log_details = {'set_id': iid, 'deleted_values': values, 'original_details': deleted_details}
            self.data_manager.log_event(self.Events.SET_DELETED, log_details)
            
            if deleted_details:
                result = values[6]
                if result == self.Results.PASS:
                    production_date = deleted_details.get('production_date')
                    passed_code = deleted_details.get('item_code')
                    if production_date and passed_code and production_date in self.scan_count and passed_code in self.scan_count[production_date]:
                        self.scan_count[production_date][passed_code] -= 1
                        if self.scan_count[production_date][passed_code] == 0:
                            del self.scan_count[production_date][passed_code]
                        if not self.scan_count[production_date]:
                            del self.scan_count[production_date]
                    raw_scans_to_remove = deleted_details.get('scanned_product_barcodes', [])
                    if len(raw_scans_to_remove) > 1:
                        for barcode in raw_scans_to_remove[1:]:
                            self.global_scanned_set.discard(barcode)

            self.history_tree.delete(iid)

            if iid in self.set_details_map:
                del self.set_details_map[iid]

        self._update_summary_tree()
        messagebox.showinfo("완료", f"{len(selected_iids)}개 기록이 삭제 처리되었습니다. 통계가 업데이트 되었습니다.")

    def _reset_current_set(self, full_reset=False, from_finalize=False):
        if self.is_blinking: return
        if full_reset and self.current_set_info.get('id'):
            self.data_manager.log_event(self.Events.SET_CANCELLED, {"set_id": self.current_set_info['id'], "cancelled_set": self.current_set_info})
            if self.history_tree.exists(str(self.current_set_info['id'])):
                self.history_tree.delete(str(self.current_set_info['id']))
            self.current_set_info['has_error_or_reset'] = True
        if from_finalize or full_reset:
            self._delete_current_set_state()
        self.current_set_info = {
            'id': None, 'parsed': [], 'raw': [],
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False
        }
        self.progress_bar['value'] = 0
        if self.initialized_successfully:
            self._update_status_label()
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
            btn_frame = tk.Frame(popup_frame, bg=self.colors.get("danger", "#E74C3C"))
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
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "modal_popup_creation", "error": str(e), "original_message": message})
            self.is_blinking = False
            fail_sound = self.sound_objects.get("fail")
            if fail_sound: fail_sound.stop()
            messagebox.showerror("시스템 오류", f"치명적인 오류가 발생하여 경고창을 표시할 수 없습니다.\n\n[기존 오류 메시지]\n{message}")
            self._reset_current_set(full_reset=True)

    def _prompt_and_cancel_completed_tray(self):
        if not self.initialized_successfully: return
        master_label = simpledialog.askstring("완료된 트레이 취소",
                                              "취소할 트레이의 현품표(13자리)를 스캔하거나 입력하세요:",
                                              parent=self)
        if not master_label: return
        master_label = master_label.strip()
        if not master_label:
            messagebox.showwarning("입력 오류", "현품표가 입력되지 않았습니다.", parent=self)
            return
        self._cancel_completed_tray_by_master_label(master_label)
        
    def _cancel_completed_tray_by_master_label(self, master_label_to_cancel):
        found_sets = []
        for set_id, details in self.set_details_map.items():
            if details.get('master_label_code') == master_label_to_cancel and not details.get('has_error_or_reset'):
                end_time_str = details.get('end_time')
                if end_time_str and isinstance(end_time_str, str):
                    try:
                        end_time_dt = datetime.fromisoformat(end_time_str)
                        found_sets.append({'set_id': set_id, 'details': details, 'end_time': end_time_dt})
                    except ValueError:
                        continue
        if not found_sets:
            messagebox.showerror("찾기 실패", f"현품표 '{master_label_to_cancel}'에 해당하는 '통과'된 완료 기록을 찾을 수 없습니다.", parent=self)
            return
        found_sets.sort(key=lambda x: x['end_time'], reverse=True)
        latest_set = found_sets[0]
        target_set_id = latest_set['set_id']
        target_details = latest_set['details']
        end_time_display = datetime.fromisoformat(target_details.get('end_time')).strftime('%H:%M:%S')
        item_name = target_details.get('item_name', '알 수 없음')
        confirm_msg = (f"다음 기록을 '포장 대기' 상태로 되돌리시겠습니까?\n\n"
                       f"현품표: {master_label_to_cancel}\n"
                       f"품명: {item_name}\n"
                       f"완료 시간: {end_time_display}\n\n"
                       f"이 작업은 기록에 남으며, 통계에서 제외됩니다.")
        if not messagebox.askyesno("취소 확인", confirm_msg, icon='warning', parent=self): return
        
        try:
            self.data_manager.log_event(self.Events.TRAY_COMPLETION_CANCELLED, {
                'cancelled_set_id': target_set_id,
                'master_label_code': master_label_to_cancel,
                'details': target_details
            })
            production_date = target_details.get('production_date')
            item_code = target_details.get('item_code')
            if production_date and item_code and production_date in self.scan_count and item_code in self.scan_count[production_date]:
                self.scan_count[production_date][item_code] -= 1
                if self.scan_count[production_date][item_code] == 0: del self.scan_count[production_date][item_code]
                if not self.scan_count[production_date]: del self.scan_count[production_date]
            raw_scans_to_remove = target_details.get('scanned_product_barcodes', [])
            if len(raw_scans_to_remove) > 1:
                for barcode in raw_scans_to_remove[1:]: self.global_scanned_set.discard(barcode)
            if target_set_id in self.set_details_map: del self.set_details_map[target_set_id]
            if self.history_tree.exists(target_set_id): self.history_tree.delete(target_set_id)
            self._update_summary_tree()
            messagebox.showinfo("처리 완료", f"현품표 '{master_label_to_cancel}' 트레이가 '포장 대기'로 처리되었습니다.", parent=self)
        except Exception as e:
            messagebox.showerror("처리 오류", f"취소 작업 중 오류가 발생했습니다: {e}", parent=self)
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "tray_cancellation", "error": str(e)})

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
        self._update_save_directory()
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.worker_name, self.unique_id)
        self.title(f"바코드 세트 검증기 ({APP_VERSION}) - {self.worker_name} ({self.unique_id})")
        messagebox.showinfo("저장 완료", f"설정이 변경되었습니다.\n\n- 저장 경로: {self.save_directory} (고정)\n- 작업자: {self.worker_name}\n\n과거 기록을 새로 불러오려면 앱을 다시 시작해주세요.", parent=window)
        window.destroy()

    def _configure_base_styles(self):
        self.style.theme_use('clam')
        self.style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        self.style.configure("TFrame", background=self.colors["background"])
        self.style.configure("Card.TFrame", background=self.colors["card_background"], borderwidth=2, relief='solid', bordercolor=self.colors["border"])
        self.style.configure("Borderless.TFrame", background=self.colors["card_background"], borderwidth=0)
        self.style.configure("ErrorCard.TFrame", background=self.colors["danger"], borderwidth=2, relief='solid', bordercolor=self.colors["danger"])
        self.style.configure("TLabel", background=self.colors["card_background"], foreground=self.colors["text"], font=(self.default_font_name, 14))
        self.style.configure("Header.TLabel", background=self.colors["card_background"], foreground=self.colors["text"], font=(self.default_font_name, 18, "bold"))
        self.style.configure("TButton", padding=12, relief="flat", borderwidth=0, background=self.colors["primary"], foreground=self.colors["text_strong"], font=(self.default_font_name, 14, "bold"))
        self.style.map("TButton", background=[('active', self.colors["primary_active"]), ('disabled', self.colors["border"])], foreground=[('disabled', self.colors["text_subtle"])])
        self.style.configure("Control.TButton", padding=(4, 4), font=(self.default_font_name, 12), background=self.colors["card_background"], foreground=self.colors["text"], relief="groove", borderwidth=2, bordercolor=self.colors["border"])
        self.style.map("Control.TButton", background=[('active', self.colors["background"])])
        self.style.configure("Status.TLabel", background=self.colors["card_background"], foreground=self.colors["text_subtle"], font=(self.default_font_name, 14))
        self.style.configure("Success.TLabel", background=self.colors["card_background"], foreground=self.colors["success"], font=(self.default_font_name, 14, "bold"))
        self.style.configure("Error.TLabel", background=self.colors["card_background"], foreground=self.colors["danger"], font=(self.default_font_name, 14, "bold"))
        self.style.configure("Save.Success.TLabel", background=self.colors["background"], foreground=self.colors["success"], font=(self.default_font_name, 12, "bold"))
        self.style.configure("green.Horizontal.TProgressbar", background=self.colors["success"], troughcolor=self.colors["border"], borderwidth=0)
        self.style.configure("TEntry", bordercolor=self.colors["border"], fieldbackground=self.colors["card_background"])
        self.style.configure("TScrollbar", gripcount=0, troughcolor=self.colors["background"], bordercolor=self.colors["background"], lightcolor=self.colors["background"], darkcolor=self.colors["background"], arrowcolor=self.colors["text_subtle"], background=self.colors["border"])
        self.style.map("TScrollbar", background=[('active', self.colors["text_subtle"])])
        overlay_bg = self.colors["background"]
        self.style.configure("Overlay.TFrame", background=overlay_bg)
        self.style.configure("Loading.TLabel", background=overlay_bg, foreground=self.colors["text"], font=(self.default_font_name, 24, "bold"))
        
        # 버튼 크기를 키우기 위해 폰트와 패딩 값을 조정합니다.
        self.style.configure("Action.TButton", font=(self.default_font_name, 15, "bold"), padding=15)
        self.style.map("Action.TButton",
                       foreground=[('disabled', self.colors["text_subtle"]), ('active', 'white'), ('!disabled', 'white')],
                       background=[('disabled', '#E5E7EB'), ('active', self.colors["primary_active"]), ('!disabled', self.colors["primary"])])

    def _configure_treeview_styles(self):
        self.style.configure("Treeview", background=self.colors["card_background"], fieldbackground=self.colors["card_background"], foreground=self.colors["text"], borderwidth=0, relief='flat', rowheight=40)
        self.style.map("Treeview", background=[('selected', self.colors["primary"])], foreground=[('selected', 'white')])
        self.style.configure("Treeview.Heading", background=self.colors["heading_background"], foreground=self.colors["text_subtle"], relief="flat", borderwidth=0, font=(self.default_font_name, 14, "bold"))
        self.style.map("Treeview.Heading", background=[('active', self.colors["background"])])
        self.history_tree.tag_configure("success", background=self.colors["success_light"], foreground=self.colors["text_strong"])
        self.history_tree.tag_configure("error", background=self.colors["danger_light"], foreground=self.colors["text_strong"])
        self.history_tree.tag_configure("in_progress", foreground=self.colors["text_subtle"], background=self.colors["card_background"])

    def _show_history_context_menu(self, event):
        """오른쪽 클릭 시 컨텍스트 메뉴를 표시합니다."""
        iid = self.history_tree.identify_row(event.y)
        if iid:
            # 클릭된 행이 현재 선택 영역에 포함되지 않은 경우, 현재 선택을 지우고 클릭된 행만 선택합니다.
            if iid not in self.history_tree.selection():
                self.history_tree.selection_set(iid)
            self.history_context_menu.post(event.x_root, event.y_root)

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="30")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        self.top_card = ttk.Frame(main_frame, style="Card.TFrame", padding=30)
        self.top_card.grid(row=0, column=0, sticky="ew", pady=(0, 30))
        self.top_card.grid_columnconfigure(0, weight=1)
        self.big_display_label = ttk.Label(self.top_card, text="바코드를 스캔하세요.", anchor="center", wraplength=1400, font=(self.default_font_name, 50, "bold"))
        self.big_display_label.grid(row=0, column=0, sticky="ew", pady=(30, 40), ipady=15)
        settings_button = ttk.Button(self.top_card, text="⚙️", command=self.open_settings_window, style='Control.TButton')
        settings_button.place(relx=1.0, rely=0.0, x=-30, y=30, anchor='ne')
        input_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        input_frame.grid(row=1, column=0, sticky="ew")
        input_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(input_frame, text="바코드 입력:", style="TLabel", background=self.colors["card_background"]).grid(row=0, column=0, padx=(0, 15), sticky='w')
        self.entry = ttk.Entry(input_frame, style="TEntry", state='disabled', font=(self.default_font_name, 18))
        self.entry.grid(row=0, column=1, sticky="ew")
        self.entry.bind("<Return>", self.process_input)
        progress_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        progress_frame.grid(row=2, column=0, sticky="ew", pady=(30, 0))
        progress_frame.grid_columnconfigure(0, weight=1)
        self.status_label = ttk.Label(progress_frame, text="첫 번째 바코드를 스캔하세요...", style="Status.TLabel", background=self.colors["card_background"])
        self.status_label.grid(row=0, column=0, sticky="w", padx=15)
        self.progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', length=200, mode='determinate', maximum=5, style="green.Horizontal.TProgressbar")
        self.progress_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        self.content_pane = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        self.content_pane.grid(row=1, column=0, sticky="nsew", pady=(15, 0))
        history_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=30)
        self.content_pane.add(history_card, weight=3)
        history_card.grid_rowconfigure(1, weight=1)
        history_card.grid_columnconfigure(0, weight=1)
        hist_header_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        hist_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 15))
        hist_header_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(hist_header_frame, text="스캔 기록", style="Header.TLabel", background=self.colors["card_background"]).grid(row=0, column=0, sticky="w")
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
        self.history_tree = ttk.Treeview(tree_frame_hist, columns=hist_cols, show="headings", yscrollcommand=v_scroll_hist.set, selectmode="extended")
        v_scroll_hist.config(command=self.history_tree.yview)
        col_map = {"Set": "#", "Input1": "현품표", "Input2": "입력 2", "Input3": "입력 3", "Input4": "입력 4", "Input5": "라벨지", "Result": "결과", "Timestamp": "시간"}
        for col, name in col_map.items():
            self.history_tree.heading(col, text=name, anchor="center", command=lambda c=col: self._treeview_sort_column(self.history_tree, c, False))
            self.history_tree.column(col, anchor="center")
        v_scroll_hist.pack(side=tk.RIGHT, fill=tk.Y)
        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.history_tree.bind("<Configure>", self._resize_all_columns)
        self.history_tree.bind("<ButtonRelease-1>", self._on_history_tree_resize_release)
        
        # 오른쪽 클릭 메뉴 생성 및 바인딩
        self.history_context_menu = tk.Menu(self, tearoff=0, font=(self.default_font_name, 14))
        self.history_context_menu.add_command(label="선택 항목 삭제", command=self._delete_selected_row)
        self.history_tree.bind("<Button-3>", self._show_history_context_menu)

        summary_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=30)
        self.content_pane.add(summary_card, weight=1)
        summary_card.grid_rowconfigure(1, weight=1)
        summary_card.grid_columnconfigure(0, weight=1)
        ttk.Label(summary_card, text="누적 통과 코드", style="Header.TLabel").grid(row=0, column=0, sticky='w', pady=(0, 15))
        tree_frame_sum = ttk.Frame(summary_card, style="Card.TFrame")
        tree_frame_sum.grid(row=1, column=0, sticky='nsew')
        tree_frame_sum.grid_rowconfigure(0, weight=1)
        tree_frame_sum.grid_columnconfigure(0, weight=1)
        summary_cols = list(self.summary_proportions.keys())
        v_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient=tk.VERTICAL)
        self.summary_tree = ttk.Treeview(tree_frame_sum, columns=summary_cols, show="headings", yscrollcommand=v_scroll_sum.set)
        v_scroll_sum.config(command=self.summary_tree.yview)
        self.summary_tree.heading("Date", text="날짜", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Date", False))
        self.summary_tree.heading("Code", text="코드", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Code", False))
        self.summary_tree.heading("Count", text="No", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Count", False))
        v_scroll_sum.pack(side=tk.RIGHT, fill=tk.Y)
        self.summary_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.summary_tree.column("Date", anchor="center")
        self.summary_tree.column("Code", anchor="center")
        self.summary_tree.column("Count", anchor="center")
        self.summary_tree.bind("<Configure>", self._resize_all_columns)
        self.summary_tree.bind("<ButtonRelease-1>", self._on_summary_tree_resize_release)
        
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.grid(row=2, column=0, sticky="ew", pady=(30, 0))
        bottom_frame.grid_columnconfigure(2, weight=1)
        
        reset_button = ttk.Button(bottom_frame, text="현재 세트 취소 (F1)", command=lambda: self._reset_current_set(full_reset=True), style="Action.TButton", takefocus=0)
        reset_button.grid(row=0, column=0, sticky="w")
        self.bind("<F1>", lambda e: self._reset_current_set(full_reset=True))
        
        cancel_tray_button = ttk.Button(bottom_frame, text="완료된 트레이 취소 (F2)", command=self._prompt_and_cancel_completed_tray, style="Action.TButton", takefocus=0)
        cancel_tray_button.grid(row=0, column=1, sticky="w", padx=(20, 0))
        self.bind("<F2>", lambda e: self._prompt_and_cancel_completed_tray())
        self.bind("<Delete>", lambda e: self._delete_selected_row())
        
        self.save_status_label = ttk.Label(bottom_frame, text="", style="Save.Success.TLabel", background=self.colors["background"])
        self.save_status_label.grid(row=0, column=2, sticky="w", padx=30)
        self.clock_label = ttk.Label(bottom_frame, text="", style="TLabel", background=self.colors["background"])
        self.clock_label.grid(row=0, column=3, sticky="e", padx=30)
        self.loading_overlay = ttk.Frame(main_frame, style="Overlay.TFrame")
        loading_content_frame = ttk.Frame(self.loading_overlay, style="Overlay.TFrame")
        loading_content_frame.pack(expand=True)
        loading_label = ttk.Label(loading_content_frame, text="데이터를 불러오는 중입니다...", style="Loading.TLabel")
        loading_label.pack(pady=(0, 15))
        self.loading_progressbar = ttk.Progressbar(loading_content_frame, mode='indeterminate', length=400)
        self.loading_progressbar.pack(pady=15)
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
        header_scale = self.ui_cfg.get("header_font_scale", 1.5)
        status_scale = self.ui_cfg.get("status_font_scale", 1.2)
        big_display_scale = self.ui_cfg.get("big_display_font_scale", 4.5)
        default_font = (self.default_font_name, font_size)
        bold_font = (self.default_font_name, font_size, "bold")
        header_font = (self.default_font_name, int(font_size * header_scale), "bold")
        status_font = (self.default_font_name, int(font_size * status_scale))
        status_bold_font = (self.default_font_name, int(font_size * status_scale), "bold")
        save_status_font = (self.default_font_name, int(font_size * 1.0), "bold")
        tree_heading_font = (self.default_font_name, int(font_size * 1.2), "bold")
        big_display_font = (self.default_font_name, min(int(font_size * big_display_scale), 80), "bold")
        clock_font = ("Consolas", int(font_size * 1.0))
        self.style.configure("TLabel", font=default_font)
        self.style.configure("Header.TLabel", font=header_font)
        self.entry.configure(font=default_font)
        self.style.configure("Treeview.Heading", font=tree_heading_font)
        self.style.configure("TButton", font=bold_font)
        self.style.configure("Status.TLabel", font=status_font)
        self.style.configure("Success.TLabel", font=status_bold_font)
        self.style.configure("Error.TLabel", font=status_bold_font)
        self.style.configure("Save.Success.TLabel", font=save_status_font)
        self.style.configure("Control.TButton", font=(self.default_font_name, int(font_size * 0.9), "bold"))
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
                total_prop = sum(self.hist_proportions.values())
                for col, prop in self.hist_proportions.items():
                    width = int(hist_width * (prop / total_prop))
                    self.history_tree.column(col, width=width)
            
            summary_width = self.summary_tree.winfo_width() - padding
            if summary_width > 1:
                total_prop = sum(self.summary_proportions.values())
                for col, prop in self.summary_proportions.items():
                    width = int(summary_width * (prop / total_prop))
                    self.summary_tree.column(col, width=width)
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
                l = sorted([(int(tv.set(k, col)), k) for k in items if tv.set(k,col)], reverse=reverse)
            elif col == 'Date':
                l = sorted([(tv.set(k, col), k) for k in items if tv.set(k,col)], reverse=reverse, key=lambda x: datetime.strptime(x[0], '%m/%d'))
            else:
                l = sorted([(tv.set(k, col), k) for k in items], reverse=reverse)
            for index, (val, k) in enumerate(l): tv.move(k, '', index)
            tv.heading(col, command=lambda: self._treeview_sort_column(tv, col, not reverse))
        except (ValueError, TclError) as e:
            print(f"정렬 오류: {e}")
            pass
            
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
        for date in sorted(self.scan_count.keys(), reverse=True):
            month_day = datetime.strptime(date, '%Y-%m-%d').strftime('%m/%d')
            for code, count in sorted(self.scan_count[date].items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    self.summary_tree.insert("", "end", values=(month_day, code, count))
    def _update_status_label(self):
        if not self.initialized_successfully: return
        num_scans = len(self.current_set_info['parsed'])
        status_text = ""
        if num_scans == 0:
            status_text = "1/5: 현품표를 스캔하세요."
        elif num_scans < 4:
            status_text = f"{num_scans + 1}/5: 다음 제품을 스캔하세요."
        elif num_scans == 4:
            status_text = "5/5: 마지막 라벨지를 스캔하세요."
        if self.current_set_info.get('has_error_or_reset'):
            status_text += " (오류 발생)"
        self.status_label.config(text=status_text, style="Status.TLabel")
    def _update_history_tree_in_progress(self):
        if not self.initialized_successfully: return
        num_scans = len(self.current_set_info['parsed'])
        if num_scans == 0: return
        set_id = str(self.current_set_info['id'])
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        display_scans = self.current_set_info['parsed']
        first_scan_display = display_scans[0] if display_scans else ""
        other_scans_display = display_scans[1:]
        values = (
            "...",
            first_scan_display,
            *other_scans_display[:4] + [""] * (4 - len(other_scans_display)),
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