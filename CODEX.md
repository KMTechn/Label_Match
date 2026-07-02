# CODEX 작업 안내서

작성 기준: 2026-06-11

이 문서는 Codex가 이 저장소를 빠르게 파악하기 위한 내부 작업 메모다. 현재 코드는 `README.txt`의 일부 설명과 다르므로 실제 `Label_Match.py` 기준으로 판단한다.

## 프로젝트 목적

`Label_Match`는 포장실 바코드 세트 검증용 Windows Tkinter 앱이다. 현재 코드 기준 일반 작업은 현품표 1회, 제품 3회, 최종 라벨지 1회를 총 5단계로 스캔하고, 품목 불일치, 형식 오류, 중복 스캔을 감지해 작업 로그를 남긴다. 최신 기본 저장소는 `%ProgramData%\KMTech\Label_Match\data`이며, `LABEL_MATCH_SAVE_DIR`로 override할 수 있다.

## 주요 기능

- 현품표, 제품 3개, 최종 라벨지를 순서대로 스캔해 하나의 세트를 완성한다.
- `assets/Item.csv`를 기준으로 품목명/규격 정보를 조회한다.
- 신규 `CLC|SPC|PHS` 형식과 생산일자 `6D` 필드 중심의 검증 로직을 포함한다.
- 사운드 피드백, 히스토리/집계 UI, 완료 트레이 취소를 제공한다.
- 현재 세트 상태를 저장해 비정상 종료 후 복구할 수 있다.
- 데모/테스트 시뮬레이션과 GitHub Release 자동 업데이트 코드를 포함한다.

## 기술 스택

- Python
- Tkinter/ttk
- pygame, Pillow, requests, tkcalendar
- CSV/JSON 파일 저장
- Windows/PyInstaller 배포 전제

## 실행 및 검증

```powershell
cd C:\company\program\Label_Match
pip install -r requirements.txt
python Label_Match.py
python -m py_compile Label_Match.py
```

배포 후보는 GitHub Actions workflow 기준으로 PyInstaller `--onedir --windowed` 빌드다.

## 주요 파일

- `Label_Match.py`: 단일 대형 메인 앱. `Label_Match(tk.Tk)`, `DataManager`, 업데이트 코드가 들어 있다.
- `assets/Item.csv`: 품목 기준 데이터.
- `assets/one.wav`, `two.wav`, `three.wav`, `four.wav`, `pass.wav`, `fail.wav`: 스캔 단계/결과 사운드.
- `assets/logo.ico`: 앱 아이콘.
- `config/app_settings.json`: 앱 설정.
- `README.txt`, `CLAUDE.md`: 기존 문서. 현재 코드와 차이가 있을 수 있다.

## 데이터와 설정 위치

- 앱 설정: `config/app_settings.json`
- 기본 저장 루트: `%ProgramData%\KMTech\Label_Match\data`
- override: `LABEL_MATCH_SAVE_DIR`
- 현재 세트 상태: `%ProgramData%\KMTech\Label_Match\data\_current_set_state_packaging.json`
- 작업 이벤트 로그: `%ProgramData%\KMTech\Label_Match\data\포장실작업이벤트로그_[unique_id]_[YYYYMMDD].csv`
- 품목 DB: `assets/Item.csv`

## 작업 시 주의점

- direct-sync 장기 보관/취합 관련 수정 전 `DIRECT_SYNC_DATA_PLATFORM_NOTES.md`를 먼저 확인한다.
- `README.txt`는 `validation_rules.csv` 기반 규칙을 설명하지만 현재 폴더에는 해당 파일이 없고 실제 코드는 `assets/Item.csv` 중심이다.
- GUI 실행은 사운드 장치, `%ProgramData%\KMTech\Label_Match\data` 쓰기 권한, GitHub 업데이트 네트워크 접근의 영향을 받는다.
- 단일 대형 파일이므로 기능 수정 전 관련 메서드와 상태 변수 흐름을 먼저 찾아야 한다.
- 스캔 순서와 상태 저장 로직은 운영 중단 복구와 연결되어 있어 단순 UI 변경도 데이터 흐름을 확인한다.
- 저장소 상태 메모는 시간 민감 정보다. 작업 전 `git status -sb`와 remote 상태를 새로 확인한다.
