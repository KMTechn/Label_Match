# R4 포장 회귀 입력 — 합성 XML

모든 XML은 **합성**이다. 실제 guest XML·호스트 task·실제 사용자/암호/WinCred 입력은 없다.
Author는 `SYNTHETIC_AUTHOR`, 작업 사용자는 고정 synthetic SID다.
`official-identities.json`은 `gh release list --limit 150`로 직접 확인한 공개 release 53개(Prerelease10)의
commit/source 파일:줄/함수 SHA를 기록한다. 로컬 태그63 중 release 없는10은 허용 묶음에서 제외한다.
42태그가 relay task creator를 포함하며 동적/고정 이름 각각의 같은 사용자 Password,
같은 사용자 InteractiveToken, SYSTEM ServiceAccount 총6개다. 나머지11태그는 생성 코드 없음으로 명시한다.
v2.0.6-11은 함수명이 없는 inline schtasks 생성이며 이것도 포함한다.
원 release 목록은 release-index.json이며 조사 시점 이후 publication은 이 입력의 범위 밖이다.

v2.0.38 근거:

- `Label_Match.py:164-171`: PC 이름과 합성 machine identity의 SHA256 앞 12자리로 source_host_id 생성.
- `Label_Match.py:516-530`: ProgramData 하위 source_host_id 및 `direct-sync-relay-{source_host_id}` 이름.
- `tools/direct_sync_relay_install_pack.py:43-52`: 안전한 파일명과 bin/run_<task>.vbs 경로.
- 같은 파일 `:109-135`: wscript wrapper 및 `wscript.exe //B //NoLogo <vbs>` action.
- 같은 파일 `:155-197`: 정식 사용자/password 분기 및 interactive 분기.
- 같은 파일 `:200-222`: `/SC MINUTE /MO 1` 생성. `/RL` 없음.
- `v2.0.66:tools/direct_sync_relay_install_pack.py:159-211`: 명시 SYSTEM 분기 추가.
각 다른 태그의 실제 줄 번호는 manifest의 해당 tag source 배열이 정본이다.

1차 `msg_a759ca80b8a3`은 v2.0.38의 같은 설치 작업 사용자 Password/InteractiveToken 분기를 승인했다.
다른 사용자는 거절한다. SYSTEM은 그 정의가 있는 공개 태그 항목에만 속한다.
XML의 `LeastPrivilege`는 schema가 유효한 합성 선택이며 source 기본값/guest 사실로 주장하지 않는다.
manifest의 run_level_evidence는 **UNPROVEN**이다. 실행 경로는 SCHEMA v1에 따라 synthetic system_directory에 바인딩한다.
sub-276 실제 사본이 도착하면 검증된 값과 출처를 같은 입력 묶음에 추가해야 한다.

`expected-outcomes.json`은 태그마다 허용 family와 근거 참조, 13개 정체성 필드 각각의 유효한 변형,
기록만 하는 필드, 복수 작업, 실행중, disabled, G 허용/거절 rollback, 같은 사용자 불일치,
묶음 어느 것에도 없는 정의, 조회 실패 및 제거 경로를 둔다. ComHandler/CalendarTrigger는 유효한
다른 task 형식이므로 type과 그 형식에 종속된 command/repetition 필드가 함께 달라진다.
부모/자식 phase 및 rollback driver는 제품 함수를 AST 이름으로 추출하며 catch/finally와 G는 실제 본문이다.
설치 identity audit는 canonical의 실제 $audit.legacy_relay_task_preimage 대입문으로 읽으며,
transition이 보존한 XML SHA와 metadata의 enabled/전체 지문도 독립 입력에 대조한다.

성공·제거·G 거절 rollback은 작업 정의를 남기고 disabled로 둔다. G 허용 rollback은 preimage의 enabled
상태 그대로 복원한다(enabled였으면 enabled, disabled였으면 disabled); 재등록·암호 조회·삭제는 금지한다.
이 차이는 계약 §5/§5-1을 함께 따른다. 설치 성공 뒤 old task와 새 relay가 동시에 enabled인 상태는 0이다.

53 공개 release code 전수의 SetValueEx/Set-ItemProperty/New-ItemProperty/reg.exe 및 지정 Python 함수 조사에서
옛 HKCU Run relay writer는 발견되지 않았다. 없는 writer의 fixture를 만들지 않았다.
기존 제품 파일 변경 없이 새 seam의 부재는 RED로 남는다. 이름/인수/반환은 인계 폴더 SEAMS-NEEDED.md 참고.

공유 schema: 8cf7eccc54d7ba58d7813b1d21c944edc1c2d75503ef3fc42d068621e0e55be1.
공유 relay-identity/Label_Match의 태그별 항목은 sub-274/sub-276 소유다. 시험은 그 사본을
제품 Get-LabelOfficialLegacyTaskExpectations 상수와 독립 바인딩·지문으로 대조하며, 없으면 RED로 표시한다.
공유 source 항목83개는 relay-identity/Label_Match 아래 바이트·SHA를 대조한 사본으로 함께 커밋한다.
R4_OFFICIAL_IDENTITY_CATALOG로 실제 공유 폴더를 지정할 수 있다. 정식 guest 항목은 아직 없다.
