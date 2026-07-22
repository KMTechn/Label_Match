# CODEX 작업 안내서

작성 기준: 2026-07-22

이 문서는 Codex가 이 저장소를 빠르게 파악하기 위한 내부 작업 메모다. 현재 코드는 `README.txt`의 일부 설명과 다르므로 실제 `Label_Match.py` 기준으로 판단한다.

## 프로젝트 목적

`Label_Match`는 포장실 바코드 검증 및 중앙 포장 완료 처리용 Windows Tkinter 앱이다. v2.0.39의 중앙 표준 작업은 **물리 PHS2 현품표 1회 스캔 → 필요 시 F4로 동일 품목 제품 1~2개 원자 교체 → F3 포장 완료 → 중앙 ACK 확인**이다. 원본 PHS2가 이적 멤버십 전체를 대표하므로 제품 3개, 최종 라벨, 전량 제품을 추가로 스캔하지 않는다. 과거의 `현품표 + 제품 3개 + 최종 라벨` 5단계는 레거시 입력 호환 경로일 뿐 중앙 PHS2 운영 절차가 아니다. 최신 기본 저장소는 `%ProgramData%\KMTech\Label_Match\data`이며, `LABEL_MATCH_SAVE_DIR`로 override할 수 있다.

## 주요 기능

- 엄격한 compact `PHS=2` 현품표를 한 번 스캔해 중앙 이적 멤버십을 조회하고 포장 준비 상태로 만든다.
- 필요할 때만 F4에서 교체 대상과 같은 품목의 양품을 차례로 스캔해 1~2개를 서버에서 원자 교체한다. 원본 PHS2는 유지하며 재봉인 QR은 화면에서 확인하는 전자 증거다.
- F3으로 랩핑 완료를 확정하고 durable outbox에 중앙 명령을 기록한 뒤, 중앙 ACK를 받아야 로컬 완료로 확정한다. pending/conflict 상태에서는 실물 이동이나 다음 작업을 진행하지 않는다.
- 중앙에서 분류되지 않은 입력은 fail-closed로 거부한다. 과거 5단계 입력은 명시적인 레거시 호환 경로에서만 처리한다.
- `assets/Item.csv`를 기준으로 품목명/규격 정보를 조회한다.
- 신규 `CLC|SPC|PHS` 형식과 생산일자 `6D` 필드 중심의 레거시 검증 로직도 호환용으로 포함한다.
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
- 중앙 표준 경로에서 첫 입력은 compact PHS2만 허용한다. sealed transfer QR을 첫 물리 입력으로 쓰거나 제품 3개·최종 라벨·F4 전량 재스캔을 요구하는 변경은 현재 계약의 역행이다.
- F4 교체, F3 제출, outbox 저장, 중앙 ACK, 충돌 격리는 하나의 완료 계약이다. 로컬 완료 표시를 ACK보다 먼저 만들거나 저장 실패를 무시하면 안 된다.
- PHS2 작업과 outbox는 날짜가 바뀌어도 보존·복구해야 한다. 여러 PC가 같은 PHS2를 동시에 제출하면 중앙 CAS/idempotency 판정에 따라 한 작업만 완료되고 나머지는 충돌로 격리되어야 한다.
- 스캔 순서와 상태 저장 로직은 운영 중단 복구와 연결되어 있어 단순 UI 변경도 데이터 흐름을 확인한다.
- 저장소 상태 메모는 시간 민감 정보다. 작업 전 `git status -sb`와 remote 상태를 새로 확인한다.
