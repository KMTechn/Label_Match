import tkinter as tk
from tkinter import ttk, messagebox, TclError, simpledialog
from collections import defaultdict
import csv
from datetime import datetime, date
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
from tkcalendar import Calendar

# #####################################################################
# 자동 업데이트 설정 (Auto-Updater Configuration)
# #####################################################################
REPO_OWNER = "KMTechn"
REPO_NAME = "Label_Match"
APP_VERSION = "v2.0.1" # [수정] 버그 픽스 버전 업데이트

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
        messagebox.showerror("업데이트 실패", f"업데이트 파일을 적용하는 중 예상치 못한 오류가 발생했습니다.\n프로그램을 다시 시작하여 업데이트를 재시도해주세요.\n\n[오류 상세 정보]\n{e}", parent=root_alert)
        root_alert.destroy()
        sys.exit(1)

def threaded_update_check():
    """백그라운드에서 업데이트를 확인하고 필요한 경우 UI에 프롬프트를 표시합니다."""
    print("백그라운드 업데이트 확인 시작...")
    download_url, new_version = check_for_updates()
    if download_url:
        root_alert = tk.Tk()
        root_alert.withdraw()
        if messagebox.askyesno("업데이트 발견", f"새로운 버전({new_version})이 있습니다.\n지금 업데이트하시겠습니까? (현재 버전: {APP_VERSION})", parent=root_alert):
            root_alert.destroy()
            download_and_apply_update(download_url)
        else:
            print("사용자가 업데이트를 거부했습니다.")
            root_alert.destroy()
    else:
        print("업데이트 확인 완료. 최신 버전이거나 확인 중 오류가 발생했습니다.")

# #####################################################################
# 애플리케이션 코드 시작
# #####################################################################
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)
def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

class CalendarWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("날짜 선택")
        self.transient(parent)
        self.grab_set()
        self.result = None

        self.cal = Calendar(self, selectmode='day', year=datetime.now().year, month=datetime.now().month, day=datetime.now().day,
                            locale='ko_KR', background="white", foreground="black", headersbackground="#EAEAEA")
        self.cal.pack(pady=20, padx=20, fill="both", expand=True)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=(0, 10))

        select_btn = ttk.Button(btn_frame, text="선택", command=self.on_select)
        select_btn.pack(side="left", padx=5)
        cancel_btn = ttk.Button(btn_frame, text="취소", command=self.destroy)
        cancel_btn.pack(side="left", padx=5)

        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window(self)

    def on_select(self):
        self.result = self.cal.selection_get()
        self.destroy()

class DataManager:
    def __init__(self, save_dir, process_name, worker_name, unique_id):
        self.save_directory = save_dir
        self.process_name = process_name
        self.worker_name = worker_name
        self.unique_id = unique_id
        self.log_queue = queue.Queue()
        self.log_thread = threading.Thread(target=self._log_writer_thread, daemon=True)
        self.log_thread.start()
    def _get_log_filepath(self, target_date=None):
        if target_date is None:
            target_date = datetime.now()
        filename = f"{self.process_name}작업이벤트로그_{self.unique_id}_{target_date.strftime('%Y%m%d')}.csv"
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
            messagebox.showerror("오디오 초기화 오류", f"프로그램 효과음을 재생하는 데 필요한 오디오 장치를 시작할 수 없습니다.\n스피커 또는 사운드 드라이버에 문제가 없는지 확인해주세요.\n\n(효과음 없이 프로그램은 계속 실행됩니다.)\n\n[상세 오류]\n{e}")
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
        self.current_set_info = {} # Reset in _reset_current_set
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
        self.summary_proportions = {"Date": 18, "Code": 52, "Phase": 10, "Count": 20}
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
                messagebox.showerror("초기화 오류", f"프로그램 시작에 필요한 중요 파일을 불러올 수 없습니다.\n프로그램이 설치된 폴더가 손상되었거나 파일이 없을 수 있습니다.\n\n[오류 원인]\n{result['error']}\n\n프로그램을 종료합니다.")
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
            messagebox.showerror("초기화 오류", f"프로그램을 시작하는 마지막 단계에서 오류가 발생했습니다.\n일시적인 문제일 수 있으니 프로그램을 다시 시작해보세요.\n\n[상세 오류]\n{e}\n\n프로그램을 종료합니다.")
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
            messagebox.showwarning("기준 정보 파일 없음", f"품목 정보 파일({self.FILES.ITEMS})이 없어 품목명을 표시할 수 없습니다.\n프로그램 폴더 내 'assets' 폴더를 확인해주세요.")
            return {}
        except Exception as e:
            messagebox.showerror("기준 정보 로드 오류", f"품목 정보를 불러오는 중 오류가 발생했습니다.\n\n[상세 오류]\n{e}")
            return {}

    def on_closing(self):
        if not self.initialized_successfully:
            self.destroy()
            return
        if messagebox.askokcancel("종료 확인", "프로그램을 종료하시겠습니까?"):
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
            saved_set_info = state_data.get('current_set_info', {})
            self.current_set_info.update(saved_set_info)

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

    def _load_history_and_rebuild_summary(self, target_date=None):
        print(f"과거 기록 비동기 로드 시작... (대상 날짜: {target_date or '오늘'})")
        self.scan_count.clear()
        self.history_tree.delete(*self.history_tree.get_children())
        self.summary_tree.delete(*self.summary_tree.get_children())
        self.global_scanned_set.clear()
        self.set_details_map.clear()

        if target_date:
            date_str = target_date.strftime('%Y-%m-%d')
            self.hist_header_label.config(text=f"스캔 기록 ({date_str})")
        else:
            self.hist_header_label.config(text="스캔 기록 (오늘)")

        self.history_tree.insert("", "end", iid="loading", values=("", "기록을 불러오는 중입니다...", "", "", "", "", "", ""), tags=("in_progress",))
        loader_thread = threading.Thread(target=self._async_load_history_task, args=(self.history_queue, target_date), daemon=True)
        loader_thread.start()

    def _async_load_history_task(self, result_queue, target_date=None):
        try:
            completed_sets = {}
            voided_set_ids = set()
            cancelled_set_ids = set()

            log_filepath = self.data_manager._get_log_filepath(target_date)

            if os.path.exists(log_filepath):
                try:
                    with open(log_filepath, 'r', encoding='utf-8-sig') as f:
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
                    print(f"기록 파일 로드 오류 ({log_filepath}): {e}")

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
                    phase = details.get('phase', '-')
                    if passed_code and production_date:
                        temp_scan_count[production_date][(passed_code, phase)] += 1
                raw_scans = details.get('scanned_product_barcodes', [])
                if raw_scans:
                    if len(raw_scans) > 1:
                        temp_global_scanned_set.update(raw_scans[1:])
                    first_scan = raw_scans[0]
                    if '|' in first_scan and '=' in first_scan:
                        temp_global_scanned_set.add(first_scan)

            result_queue.put({'sorted_sets': sorted_final_sets, 'scan_count': temp_scan_count, 'global_scanned_set': temp_global_scanned_set, 'set_details_map': temp_set_details_map})
        except Exception as e:
            print(f"백그라운드 기록 로딩 오류: {e}")
            result_queue.put({'error': str(e)})

    def _process_history_queue(self):
        try:
            result = self.history_queue.get_nowait()
            if self.history_tree.exists("loading"): self.history_tree.delete("loading")
            if 'error' in result:
                messagebox.showerror("기록 로딩 오류", f"작업 기록을 불러오는 중 오류가 발생했습니다.\n로그 파일이 손상되었을 수 있습니다.\n\n[오류 원인]\n{result['error']}")
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
            messagebox.showerror("UI 업데이트 오류", f"기록을 화면에 표시하는 과정에서 예상치 못한 오류가 발생했습니다.\n프로그램을 다시 시작해주세요.\n\n[상세 오류]\n{e}")

    def _parse_new_format_label(self, raw_input):
        if '|' not in raw_input or '=' not in raw_input:
            return None
        try:
            parsed_data = {
                item.split('=', 1)[0].strip().upper(): item.split('=', 1)[1].strip()
                for item in raw_input.split('|') if '=' in item
            }
            if all(key in parsed_data for key in ['CLC', 'SPC', 'PHS']):
                return parsed_data
            else:
                return None
        except Exception as e:
            print(f"신규 라벨 형식 파싱 오류: {e}")
            return None

    def process_input(self, event=None):
        if self.is_blinking or not self.initialized_successfully: return
        raw_input = self.entry.get().strip()
        self.entry.delete(0, tk.END)
        if not raw_input: return

        self.data_manager.log_event(self.Events.SCAN_ATTEMPT, {"raw_input": raw_input, "scan_pos": len(self.current_set_info['raw']) + 1})
        scan_pos = len(self.current_set_info['raw']) + 1

        if scan_pos == 1:
            # 첫번째 스캔 (현품표)
            new_label_data = self._parse_new_format_label(raw_input)
            if new_label_data:
                if raw_input in self.global_scanned_set:
                    self._handle_input_error(
                        raw_input,
                        title="[현품표 중복 스캔]",
                        reason=f"이미 처리된 현품표입니다.\n\n- 중복 스캔: {self._truncate_string(raw_input)}\n\n→ 새 현품표로 다시 시작하세요."
                    )
                    return

                client_code = new_label_data.get('CLC')
                supplier_code = new_label_data.get('SPC')
                phase = new_label_data.get('PHS')

                self.current_set_info['phase'] = phase
                self.current_set_info['item_name_override'] = supplier_code
                self._update_on_success_scan(raw_input, client_code)
            else:
                MASTER_LABEL_LENGTH = 13
                if len(raw_input) != MASTER_LABEL_LENGTH:
                    self._handle_input_error(
                        raw_input,
                        title="[현품표 형식 오류]",
                        reason=f"잘못된 현품표 형식입니다 (13자리 아님).\n\n- 입력 값: {self._truncate_string(raw_input)}\n\n→ 올바른 현품표를 스캔하세요."
                    )
                    return
                if raw_input not in self.items_data:
                    self._handle_input_error(
                        raw_input,
                        title="[미등록 현품표]",
                        reason=f"미등록 현품표입니다.\n\n- 미등록 코드: {self._truncate_string(raw_input)}\n\n→ Item.csv를 확인하세요."
                    )
                    return
                self._update_on_success_scan(raw_input, raw_input)

        elif 2 <= scan_pos <= 5:
            # #####################################################################
            # [이동 및 수정] 테스트 로그 생성 기능
            # #####################################################################
            if scan_pos == 2 and raw_input.upper().startswith("TEST_LOG_"):
                parts = raw_input.split('_')
                if len(parts) == 3 and parts[2].isdigit():
                    num_sets = int(parts[2])
                    master_code = self.current_set_info['parsed'][0]
                    
                    confirm_msg = (f"현재 현품표 기준으로 {num_sets}개의 테스트 기록을 생성하시겠습니까?\n\n"
                                   f"▶ 현품표 코드: {master_code}\n\n"
                                   "(이 작업은 현재 진행중인 세트를 취소하고 시작됩니다.)")
                    
                    if messagebox.askyesno("테스트 데이터 생성", confirm_msg):
                        # 현재 진행중인 세트를 완전히 초기화
                        self._reset_current_set(full_reset=True)
                        # 시뮬레이션 시작
                        self.run_test_log_simulation(master_code, num_sets)
                    return # 테스트 모드 실행 후 함수 종료
                else:
                    messagebox.showwarning("입력 형식 오류", "테스트 코드 형식이 올바르지 않습니다.\n(예: TEST_LOG_100)")
                    return
            # #####################################################################
            # [수정 완료]
            # #####################################################################

            # 일반 스캔 로직
            master_code = self.current_set_info['parsed'][0]
            if scan_pos < 5 and len(raw_input) <= len(master_code):
               self._handle_input_error(
                      raw_input,
                      title="[바코드 종류 오류]",
                      reason=f"잘못된 바코드 종류입니다.\n\n- 스캔 값: {self._truncate_string(raw_input)}\n\n→ 제품 바코드를 스캔하세요."
               )
               return
            if scan_pos == 5 and len(raw_input) < 31:
                self._handle_input_error(
                    raw_input,
                    title="[라벨 형식 오류]",
                    reason=f"포장 라벨 길이가 너무 짧습니다.\n(입력: {len(raw_input)} / 최소: 31)\n\n→ 올바른 라벨을 스캔하세요."
                )
                return

            if master_code not in raw_input:
                self._handle_mismatch(raw_input, master_code)
                return
            if raw_input in self.current_set_info['raw']:
                self._handle_input_error(
                    raw_input,
                    title="[세트 내 중복 스캔]",
                    reason=f"세트 내 중복 스캔입니다.\n\n- 중복 제품: {self._truncate_string(raw_input)}\n\n→ 다른 제품을 스캔하세요."
                )
                return
            if raw_input in self.global_scanned_set:
                self._handle_input_error(
                    raw_input,
                    title="[전체 작업 내 중복 스캔]",
                    reason=f"이미 다른 세트에서 처리된 제품입니다.\n\n- 중복 제품: {self._truncate_string(raw_input)}\n\n→ 새 제품으로 교체하세요."
                )
                return

            production_date = None
            if scan_pos == 5:
                production_date = self._extract_production_date(raw_input)
                if not production_date:
                    self._handle_input_error(
                        raw_input,
                        title="[생산일자 누락]",
                        reason=f"라벨에서 생산일자(6D...)를 찾을 수 없습니다.\n\n- 스캔한 라벨: {self._truncate_string(raw_input)}\n\n→ 올바른 라벨을 사용하세요."
                    )
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

        item_name_override = self.current_set_info.get('item_name_override')
        if item_name_override:
            item_info = {"Item Name": item_name_override, "Spec": ""}
        else:
            item_info = self.items_data.get(item_code, {})

        start_time = self.current_set_info.get('start_time')
        work_time_sec = (datetime.now() - start_time).total_seconds() if start_time else 0.0
        production_date = self.current_set_info.get('production_date')
        phase = self.current_set_info.get('phase', '-')

        set_id_for_log = str(self.current_set_info['id'])

        if result == self.Results.PASS:
            if item_code != "N/A" and production_date:
                self.scan_count[production_date][(item_code, phase)] += 1

                self.global_scanned_set.update(raw_scans_to_log[1:])
                if item_name_override and raw_scans_to_log:
                    self.global_scanned_set.add(raw_scans_to_log[0])

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
            'set_id': set_id_for_log,
            'phase': phase
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

    def _handle_input_error(self, raw, title="[입력 오류]", reason="알 수 없는 입력 오류가 발생했습니다."):
        self.data_manager.log_event(self.Events.ERROR_INPUT, {"raw": raw, "reason": reason})
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        
        self.update_big_display(self._truncate_string(str(raw)), "red")
        
        self.status_label.config(text=f"❌ {title}: {reason.split(chr(10))[0]}", style="Error.TLabel")
        self._trigger_modal_error(title, reason, self.Results.FAIL_INPUT_ERROR, raw)

    def _handle_mismatch(self, raw, master):
        self.data_manager.log_event(self.Events.ERROR_MISMATCH, {"raw": raw, "master": master})
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        title = "[제품 불일치]"

        truncated_raw = self._truncate_string(raw)
        truncated_master = self._truncate_string(master)

        error_message = f"현품표와 제품이 불일치합니다.\n\n- 현품표: {truncated_master}\n- 스캔 제품: {truncated_raw}\n\n→ 올바른 제품을 스캔하세요."

        self.update_big_display(truncated_raw, "red")
        self.status_label.config(text=f"❌ 불일치: 현품표({truncated_master}) 없음", style="Error.TLabel")
        self._trigger_modal_error(title, error_message, self.Results.FAIL_MISMATCH, raw)

    def _delete_selected_row(self):
        selected_iids = self.history_tree.selection()
        if not selected_iids:
            messagebox.showwarning("선택 필요", "삭제할 기록을 목록에서 선택하세요.")
            return

        if not messagebox.askyesno("삭제 확인", f"선택된 {len(selected_iids)}개의 기록을 정말 삭제(무효화)하시겠습니까?\n이 작업은 되돌릴 수 없습니다.", icon="warning"):
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
                    phase = deleted_details.get('phase', '-')
                    if production_date and passed_code:
                        key = (passed_code, phase)
                        if production_date in self.scan_count and key in self.scan_count[production_date]:
                            self.scan_count[production_date][key] -= 1
                            if self.scan_count[production_date][key] == 0:
                                del self.scan_count[production_date][key]
                            if not self.scan_count[production_date]:
                                del self.scan_count[production_date]

                    raw_scans_to_remove = deleted_details.get('scanned_product_barcodes', [])
                    for barcode in raw_scans_to_remove:
                        self.global_scanned_set.discard(barcode)

            self.history_tree.delete(iid)

            if iid in self.set_details_map:
                del self.set_details_map[iid]

        self._update_summary_tree()
        messagebox.showinfo("삭제 완료", f"{len(selected_iids)}개의 기록이 삭제 처리되었습니다.")

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
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False,
            'phase': None, 'item_name_override': None, 'production_date': None
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
            self.after_idle(lambda: messagebox.showwarning("사운드 설정 오류", "경고음 파일을 찾을 수 없습니다.\n(assets 폴더의 fail.wav 파일 확인 필요)\n\n오류 발생 시 경고음이 울리지 않습니다."))
            return
        try:
            sound.play(loops=-1)
            while self.is_blinking:
                time.sleep(0.1)
            sound.stop()
        except Exception as e:
            self.after_idle(lambda: messagebox.showerror("사운드 재생 오류", f"경고음을 재생하는 중 오류가 발생했습니다.\n스피커 또는 사운드 드라이버를 확인해주세요.\n\n[상세 오류]\n{e}"))

    def _trigger_modal_error(self, title, message, result, error_details):
        if self.is_blinking: return
        self.is_blinking = True
        threading.Thread(target=self._play_error_siren_loop, daemon=True).start()
        self.after(0, self._blink_background_loop)
        try:
            popup = tk.Toplevel(self)
            popup.title(f"⚠️ {title}")
            popup.attributes('-fullscreen', True)
            popup.attributes('-topmost', True)

            popup_frame = tk.Frame(popup, bg=self.colors.get("danger", "#E74C3C"))
            popup_frame.pack(expand=True, fill='both')

            btn_frame = tk.Frame(popup_frame, bg=self.colors.get("danger", "#E74C3C"))
            btn_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(20, 60))

            btn = tk.Button(btn_frame, text="확인 (Enter / ESC)",
                            command=lambda: self._close_popup(popup, result, error_details),
                            font=("Impact", 36, "bold"), bg="yellow", fg="black",
                            relief="raised", borderwidth=5)
            btn.pack(ipady=20, ipadx=50)

            label = tk.Label(popup_frame, text=f"⚠️\n\n{message}",
                            font=("Impact", 60, "bold"), fg='white',
                            bg=self.colors.get("danger", "#E74C3C"),
                            anchor='center', justify='center',
                            wraplength=self.winfo_screenwidth() - 150)
            label.pack(pady=40, expand=True, fill='both')

            popup.focus_force()
            btn.focus_set()

            popup.bind("<Escape>", lambda e: self._close_popup(popup, result, error_details))
            btn.bind("<Return>", lambda e: self._close_popup(popup, result, error_details))
            popup.protocol("WM_DELETE_WINDOW", lambda: self._close_popup(popup, result, error_details))
            self.update_idletasks()
            popup.transient(self)
            popup.grab_set()

        except Exception as e:
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "modal_popup_creation", "error": str(e), "original_message": message})
            self.is_blinking = False
            fail_sound = self.sound_objects.get("fail")
            if fail_sound: fail_sound.stop()
            messagebox.showerror("시스템 오류", f"오류 경고창을 표시하는 데 실패했습니다.\n프로그램을 재시작해야 할 수 있습니다.\n\n[기존 오류 메시지]\n{message}")
            self._reset_current_set(full_reset=True)

    def _prompt_and_cancel_completed_tray(self):
        if not self.initialized_successfully: return

        master_label = simpledialog.askstring("완료된 트레이 취소",
                                            "취소할 트레이의 현품표를 스캔하거나 입력하세요:",
                                            parent=self)
        if not master_label: return
        master_label = master_label.strip()

        if not master_label:
            messagebox.showwarning("입력 오류", "현품표가 입력되지 않았습니다.", parent=self)
            return

        self._cancel_completed_tray_by_label(master_label)

    def _cancel_completed_tray_by_label(self, label_to_cancel):
        found_sets = []
        for set_id, details in self.set_details_map.items():
            raw_scans = details.get('scanned_product_barcodes', [])
            first_raw_scan = raw_scans[0] if raw_scans else None

            is_match = (details.get('master_label_code') == label_to_cancel or
                        first_raw_scan == label_to_cancel)

            if is_match and not details.get('has_error_or_reset'):
                try:
                    end_time_dt = datetime.fromisoformat(details.get('end_time'))
                    found_sets.append({'set_id': set_id, 'details': details, 'end_time': end_time_dt})
                except (ValueError, TypeError):
                    continue

        if not found_sets:
            messagebox.showerror("찾기 실패", f"입력하신 현품표 '{label_to_cancel}'에 해당하는 '통과' 기록을 현재 조회된 내역에서 찾을 수 없습니다.", parent=self)
            return

        found_sets.sort(key=lambda x: x['end_time'], reverse=True)
        latest_set = found_sets[0]
        target_set_id = latest_set['set_id']
        target_details = latest_set['details']

        end_time_display = latest_set['end_time'].strftime('%H:%M:%S')
        item_name = target_details.get('item_name', '알 수 없음')

        confirm_msg = (f"다음 기록을 취소하시겠습니까?\n\n"
                       f"현품표: {target_details.get('master_label_code')}\n"
                       f"품명: {item_name}\n"
                       f"완료 시간: {end_time_display}\n\n"
                       f"취소 시 통계와 기록이 모두 변경됩니다.")

        if not messagebox.askyesno("취소 확인", confirm_msg, icon='warning', parent=self):
            return

        try:
            self.data_manager.log_event(self.Events.TRAY_COMPLETION_CANCELLED, {
                'cancelled_set_id': target_set_id,
                'cancelled_by_label': label_to_cancel,
                'details': target_details
            })

            production_date = target_details.get('production_date')
            item_code = target_details.get('item_code')
            phase = target_details.get('phase', '-')
            if production_date and item_code:
                key = (item_code, phase)
                if production_date in self.scan_count and key in self.scan_count[production_date]:
                    self.scan_count[production_date][key] -= 1
                    if self.scan_count[production_date][key] <= 0:
                        del self.scan_count[production_date][key]
                    if not self.scan_count[production_date]:
                        del self.scan_count[production_date]

            raw_scans_to_remove = target_details.get('scanned_product_barcodes', [])
            for barcode in raw_scans_to_remove:
                self.global_scanned_set.discard(barcode)

            if target_set_id in self.set_details_map: del self.set_details_map[target_set_id]
            if self.history_tree.exists(target_set_id): self.history_tree.delete(target_set_id)

            self._update_summary_tree()

            messagebox.showinfo("처리 완료", f"해당 작업이 정상적으로 취소되었습니다.", parent=self)

        except Exception as e:
            messagebox.showerror("처리 오류", f"취소 작업을 처리하는 중 오류가 발생했습니다.\n프로그램을 다시 시작하여 확인해주세요.\n\n[상세 오류]\n{e}", parent=self)
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "tray_cancellation_by_label", "error": str(e)})

    def run_test_log_simulation(self, master_code_to_test, num_sets):
        """테스트 시뮬레이션을 위한 스레드를 시작하고 UI를 비활성화합니다."""
        self.entry.config(state='disabled')
        self.update_big_display(f"테스트 데이터 생성 시작...", "primary")
        self.progress_bar['value'] = 0
        
        sim_thread = threading.Thread(target=self._execute_test_simulation, args=(master_code_to_test, num_sets,), daemon=True)
        sim_thread.start()

    def _execute_test_simulation(self, master_code, num_sets):
        """(스레드에서 실행) 지정된 수량만큼의 '통과' 세트를 시뮬레이션합니다."""
        item_info = self.items_data.get(master_code, {"Item Name": "테스트 품목", "Spec": "T-SPEC"})
        
        for i in range(num_sets):
            progress_text = f"테스트 진행 중... ({i + 1}/{num_sets})"
            self.after(0, self.update_big_display, progress_text, "primary")

            set_id = f"TEST_{time.time_ns()}"
            start_time = datetime.now()
            time.sleep(0.01)
            end_time = datetime.now()
            production_date = datetime.now().strftime('%Y-%m-%d')
            phase = str((i % 3) + 1)

            scanned_barcodes = [
                f"CLC={master_code}|SPC={item_info['Item Name']}|PHS={phase}",
                f"PRODUCT_TEST_{master_code}_{set_id}_1",
                f"PRODUCT_TEST_{master_code}_{set_id}_2",
                f"PRODUCT_TEST_{master_code}_{set_id}_3",
                f"FINAL_LABEL_{master_code}_{set_id}\x1D6D{production_date.replace('-', '')}"
            ]
            parsed_scans = [master_code] * 5

            details = {
                'set_id': set_id,
                'master_label_code': master_code, 'item_code': master_code,
                'item_name': item_info.get("Item Name"), 'spec': item_info.get("Spec"),
                'scan_count': 5,
                'scanned_product_barcodes': scanned_barcodes,
                'parsed_product_barcodes': parsed_scans,
                'work_time_sec': (end_time - start_time).total_seconds(),
                'error_count': 0, 'has_error_or_reset': False,
                'is_partial_submission': False, 'start_time': start_time,
                'end_time': end_time,
                'production_date': production_date, 'phase': phase
            }

            self.data_manager.log_event(self.Events.TRAY_COMPLETE, details)
            self.scan_count[production_date][(master_code, phase)] += 1
            self.global_scanned_set.update(scanned_barcodes)
            self.set_details_map[set_id] = details
            self.after(0, self._add_test_set_to_history_ui, set_id, details, i + 1)

        self.after(0, self._finalize_test_simulation, num_sets)

    def _add_test_set_to_history_ui(self, set_id, details, display_index):
        """(UI 스레드에서 실행) 시뮬레이션된 한 개의 세트를 히스토리 트리에 추가합니다."""
        if not self.history_tree.winfo_exists(): return

        parsed_scans = details['parsed_product_barcodes']
        first_scan = parsed_scans[0] if parsed_scans else ""
        other_scans = parsed_scans[1:5]
        
        values_to_display = (
            len(self.history_tree.get_children()) + 1,
            first_scan,
            *other_scans,
            self.Results.PASS,
            details['end_time'].strftime('%H:%M:%S')
        )
        self.history_tree.insert("", "end", iid=set_id, values=values_to_display, tags=("success",))
        self.history_tree.yview_moveto(1.0)

    def _finalize_test_simulation(self, num_sets):
        """(UI 스레드에서 실행) 시뮬레이션 완료 후 UI를 정리합니다."""
        if not self.winfo_exists(): return
        
        self._play_sound("pass")
        self._update_summary_tree()
        self.update_big_display(f"테스트 완료: {num_sets}개 생성", "success")
        messagebox.showinfo("테스트 완료", f"{num_sets}개의 테스트 '통과' 기록 생성이 완료되었습니다.")
        
        self.entry.config(state='normal')
        self.entry.focus_set()
        self._reset_current_set()

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
            messagebox.showerror("입력 오류", "작업자 이름은 비워둘 수 없습니다.", parent=window)
            return
        self.worker_name = new_worker_name.strip()
        self._save_app_settings()
        self._update_save_directory()
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.worker_name, self.unique_id)
        self.title(f"바코드 세트 검증기 ({APP_VERSION}) - {self.worker_name} ({self.unique_id})")
        messagebox.showinfo("저장 완료", f"설정이 변경되었습니다.\n- 작업자: {self.worker_name}", parent=self)
        window.destroy()

    def _show_about_window(self):
        about_win = tk.Toplevel(self)
        about_win.title("정보")
        about_win.geometry("500x350")
        about_win.resizable(False, False)
        about_win.transient(self)
        about_win.grab_set()
        about_win.configure(bg=self.colors["background"])

        header_font = (self.default_font_name, 18, "bold")
        title_font = (self.default_font_name, 11, "bold")
        text_font = (self.default_font_name, 11)

        main_frame = ttk.Frame(about_win, padding=25)
        main_frame.pack(expand=True, fill=tk.BOTH)

        ttk.Label(main_frame, text="바코드 세트 검증기", font=header_font).pack(pady=(0, 5))
        ttk.Label(main_frame, text=f"Version {APP_VERSION}", font=(self.default_font_name, 10, "italic")).pack(pady=(0, 20))

        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=5)
        ttk.Label(info_frame, text="제작:", font=title_font, width=12).grid(row=0, column=0, sticky='w')
        ttk.Label(info_frame, text="KMTechn", font=text_font).grid(row=0, column=1, sticky='w')
        ttk.Label(info_frame, text="Copyright:", font=title_font, width=12).grid(row=1, column=0, sticky='w')
        ttk.Label(info_frame, text="© 2024 KMTechn. All rights reserved.", font=text_font).grid(row=1, column=1, sticky='w')

        ttk.Separator(main_frame, orient='horizontal').pack(fill='x', pady=15)

        keys_frame = ttk.Frame(main_frame)
        keys_frame.pack(fill=tk.X, pady=5)
        ttk.Label(keys_frame, text="주요 단축키", font=title_font).pack(anchor='w', pady=(0, 5))

        key_map = {
            "현재 세트 취소": "F1",
            "완료된 트레이 취소": "F2",
            "선택 항목 삭제": "Delete",
            "UI 확대/축소": "Ctrl + 마우스 휠"
        }
        for i, (desc, key) in enumerate(key_map.items()):
            ttk.Label(keys_frame, text=f"• {desc}", font=text_font).grid(row=i, column=0, sticky='w', padx=10)
            ttk.Label(keys_frame, text=key, font=(self.default_font_name, 11, "bold")).grid(row=i, column=1, sticky='e', padx=10)
        keys_frame.grid_columnconfigure(1, weight=1)

        close_button = ttk.Button(main_frame, text="닫기", command=about_win.destroy, style="TButton")
        close_button.pack(side=tk.BOTTOM, pady=(20, 0))


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
        iid = self.history_tree.identify_row(event.y)
        if iid:
            if iid not in self.history_tree.selection():
                self.history_tree.selection_set(iid)
            self.history_context_menu.post(event.x_root, event.y_root)

    def _reload_today_history(self):
        self._load_history_and_rebuild_summary(None)
        self._process_history_queue()

    def _truncate_string(self, text: str, max_len: int = 35) -> str:
        """문자열이 최대 길이를 초과하면 줄이고 "..."을 추가합니다."""
        if len(text) > max_len:
            return text[:max_len] + "..."
        return text

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

        top_right_frame = ttk.Frame(self.top_card, style="Borderless.TFrame")
        top_right_frame.place(relx=1.0, rely=0.0, x=-30, y=30, anchor='ne')

        about_button = ttk.Button(top_right_frame, text="❓", command=self._show_about_window, style='Control.TButton')
        about_button.pack(side=tk.RIGHT, padx=(5, 0))
        settings_button = ttk.Button(top_right_frame, text="⚙️", command=self.open_settings_window, style='Control.TButton')
        settings_button.pack(side=tk.RIGHT)

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

        self.hist_header_label = ttk.Label(hist_header_frame, text="스캔 기록", style="Header.TLabel", background=self.colors["card_background"])
        self.hist_header_label.grid(row=0, column=0, sticky="w")

        hist_control_frame = ttk.Frame(hist_header_frame, style="Borderless.TFrame")
        hist_control_frame.grid(row=0, column=2, sticky="e")

        today_btn = ttk.Button(hist_control_frame, text="오늘", style="Control.TButton", command=self._reload_today_history)
        today_btn.pack(side=tk.LEFT, padx=(0, 5))
        date_search_btn = ttk.Button(hist_control_frame, text="📅 날짜 조회", style="Control.TButton", command=self._prompt_for_date_and_reload)
        date_search_btn.pack(side=tk.LEFT, padx=(0, 15))

        decrease_font_btn = ttk.Button(hist_control_frame, text="-", style="Control.TButton", command=self._decrease_tree_font)
        decrease_font_btn.pack(side=tk.LEFT, padx=(0, 0))
        increase_font_btn = ttk.Button(hist_control_frame, text="+", style="Control.TButton", command=self._increase_tree_font)
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
        self.summary_tree.heading("Phase", text="차수", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Phase", False))
        self.summary_tree.heading("Count", text="No", anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Count", False))
        v_scroll_sum.pack(side=tk.RIGHT, fill=tk.Y)
        self.summary_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.summary_tree.column("Date", anchor="center")
        self.summary_tree.column("Code", anchor="center")
        self.summary_tree.column("Phase", anchor="center")
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

    def _prompt_for_date_and_reload(self):
        if not self.initialized_successfully: return

        cal_win = CalendarWindow(self)
        selected_date = cal_win.result

        if selected_date:
            try:
                target_datetime = datetime.combine(selected_date, datetime.min.time())
                self._load_history_and_rebuild_summary(target_datetime)
                self._process_history_queue()
            except Exception as e:
                messagebox.showerror("조회 오류", f"기록을 조회하는 중 오류가 발생했습니다.\n\n[상세 오류]\n{e}", parent=self)

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
            if col == 'Set' or col == 'Count' or col == 'Phase':
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
        elif color == "primary": fg_color = self.colors.get("primary", "#3B82F6")
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
        for date_str in sorted(self.scan_count.keys(), reverse=True):
            try:
                month_day = datetime.strptime(date_str, '%Y-%m-%d').strftime('%m/%d')
                sorted_items = sorted(self.scan_count[date_str].items(), key=lambda item: item[1], reverse=True)
                for (code, phase), count in sorted_items:
                    if count > 0:
                        self.summary_tree.insert("", "end", values=(month_day, code, phase, count))
            except (ValueError, TypeError) as e:
                print(f"요약 트리 업데이트 중 날짜 형식 오류: {date_str}, 오류: {e}")


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