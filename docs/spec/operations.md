# Label_Match 운영·복구·검증

<a id="module-boundaries-lm3"></a>
## LM-3 · F3 완료와 화면 구성 경계

`label_completion.py`는 F3 current-state snapshot·포장 준비/lease 검증·내구 commit·lane 결과 snapshot의 기존 본문을 소유한다. 앱의 다섯 메서드는 같은 signature로 위임하고 전역 dependency를 호출 시 읽는 callback을 전달한다. `gui_package_enqueue` writer admission은 기존 façade decorator에 남고 DataManager·outbox의 저장/transaction API는 유지한다.

`label_workbench_scan.py`는 중앙 scan pane의 rail·notice·입력·QA/F4 상세 위젯을 구성한다. `_create_widgets`가 호출 시점 Tk/ttk와 scan 상수를 전달하며 두 notice Enter binding은 원래 생성 순서의 owner에 남긴다. widget 생성 순서·owner callback·기존 notice/view-state renderer를 유지한다.

`label_workbench_activity.py`는 오른쪽 session/deferred activity pane을 구성한다. 기존 deferred 상태 그룹·경보 기준과 owner의 줄 높이 계산을 전달받으며 불변 worker snapshot의 조회·적용·notice 갱신은 기존 경계를 유지한다.

`label_workbench_history.py`는 같은 오른쪽 pane의 이력·통과 요약·작업 버튼을 구성한다. 완료 트레이 취소의 danger 버튼 생성자는 owner가 callback으로 전달해 원 생성 시점에 실행한다. 날짜 조회·원문/복사·정렬·취소·F1–F5/Enter/Escape/Delete의 기존 owner callback과 disabled 초기 상태를 유지하며 이력 worker·generation/input gate는 이동하지 않는다.

`label_workbench_context.py`는 공통 frame/header·왼쪽 작업 context와 footer/로딩 표시를 구성한다. profile·main frame·pane metrics를 기존 순서로 전달하고 `_create_widgets`는 pane 순서·notice binding·준비 flag·step rail·기존 renderer/responsive 적용을 소유한다. 버튼/문구/절차·레이아웃 계산·실행 thread는 바꾸지 않는다.

F3 진입/action gate·성공음/이력·중앙 worker dispatch·재시도/자정 복구는 기존 owner를 사용한다. [LM-3 검증 기록](D:/KMTech/program-improvement-20260912/work/Label_Match/w6lm3/RESULT.md)은 AST·동일 입력/실패 벡터·기존 headless pack 범위이며 실제 GUI·프린터·모니터·설치 수용은 별도다.

<a id="module-boundaries-lm2"></a>
## LM-2 · 포장 물류 모듈 경계

`package_command_draft.py`는 불변 draft·멤버십/명령 증거 계산을, `package_errors.py`는 공유 오류 타입·retry 상한을 소유한다. 기존 `package_logistics` import는 같은 객체를 재노출하며 command key·membership·expected_versions·오류 계약을 유지한다. GUI·relay·취소 caller는 기존 진입점을 사용한다.

`package_outbox.py`는 기존 생성 outbox의 transaction·내구 marker·due retry·review 저장을 소유한다. façade 생성자는 호출 시점의 clock·schema·review helper를 전달하고 취소 outbox와 공유 schema는 기존 파일에 둔다. SQL·commit/rollback·CSV 투영 순서와 저장 bytes는 유지한다.

`package_outbox_processor.py`는 claim→저장 명령/receipt 조회→전송→receipt 검증→ACK/retry/conflict의 기존 순서를 소유한다. `package_logistics.PackageOutboxProcessor`가 원 writer admission과 call-time client/receipt 검증 경계를 유지하며 취소 processor는 기존 파일에 남는다.

`package_validation.py`는 client의 static source projection·work-group·transfer seal 검증만 소유한다. 세 메서드는 client 상태·HTTP·저장에 접근하지 않으며 기존 signature의 façade가 위임한다. work-group의 seal 검증은 기존 class 메서드를 호출 시 전달한다. 요청 순서·transport·API 오류 변환·command 조립은 client에 남겨 LM-3에서 별도 판단한다.

검증은 [LM-2 실행 기록](D:/KMTech/program-improvement-20260912/work/Label_Match/w6lm2/RESULT.md)의 AST·동일 입력 전후 벡터·기존 focused 시험에 한정한다. GUI·설치·프린터·실물 모니터·relay 실서버는 별도 검증 대상이다.

<a id="module-boundaries-lm1"></a>
## LM-1 · 저장과 이력 모듈 경계

`label_data_manager.py`는 기존 queue writer·원자 current-state 저장을 소유하고 `Label_Match.DataManager`가 동일 생성자로 live callback을 전달한다. 새 모듈은 앱을 역으로 import하지 않는다. 경로 resolver·복구·durable 완료와 성공 표시·중앙 ACK의 순서는 기존 앱에 유지한다.

`label_history.py`로 이동한 것은 read worker와 summary 날짜·행 계산뿐이다. `_async_load_history_task`, `_summary_items`, `_label_match_summary_date`는 기존 앱 진입점으로 계속 노출된다. 로더 시작·queue 처리·generation/활성 입력 gate와 `_history_display_steps`/`_apply_history_display_chunk`는 앱에 유지하며 carrier parsing·error 순서는 재구성하지 않는다.

검증은 [LM-1 실행 기록](D:/KMTech/program-improvement-20260912/work/Label_Match/w6lm1/RESULT.md)의 AST 대조·동일 입력 전후 벡터·기존 focused 시험에 한정한다. GUI·설치·프린터·실물 모니터 검증은 별도다.

<a id="carrier-identity-w5b2"></a>
## W5-B2 · carrier·legacy·Item Code 경계

`carrier_identity_port.py`는 순수 명시 인자만 받고 기존 app/workflow/물류 caller의 진입 이름과 오류를 유지한다. [규칙·호출 표](carrier_identity.md)와 [고정 기준·동등성 시험](../../tests/test_carrier_identity_port.py)을 따른다. 설정·CSV·snapshot I/O, 표시용 느슨한 parser, source/receipt 검증, F3 내구 완료·성공·ACK와 F4/재시도/자정 복구는 기존 caller 소유다.

기준 `a6253d1`의 고정 벡터·생성 입력 47 PASS를 `cb7ef7f`에 기록했다. 기존 관련 6개 suite는 변경 전 C: 격리에서 530 PASS다. 기존 UI 안내 문구 assertion 실패는 범위 밖으로 보존하며, ProgramData 기본 경로 시험은 전체 core 시험이 만든 synthetic onboarding state의 영향을 받으므로 독립 실행(1 PASS)한다. 기존 업무 테스트와 기대값은 수정하지 않는다.

기존 writer fixture는 현재 파일 목록으로 과거 `57f52e1`을 archive하여 신규 포트 파일 부재로 14 setup ERROR가 발생했다(동일 run 25 PASS). X13-B의 historical fixture만 실제 `57f52e1`→수용 `a6253d1` bytes에 고정하고 이전/최종 트리에 같은 archive 직렬화를 적용한다. 현재 소스의 writer 전환은 기존 `portable_pair`가 계속 검증한다. positive/negative assertion과 제품 installer 허용 조건은 그대로이며, 최초 실패 로그·fixture 입력을 D:에 보존했다.

마지막 fixture 변경 뒤 두 writer suite 전체는 C:의 짧은 `wf` 경로에서 **39 PASS**다. 긴 `final-writer-corrected` 경로에서는 Copy-Item이 주입 지점 전에 실패한 rollback 2건(37 PASS/2 FAIL)을 보존했다. 제품/기대값 변경 없이 경로를 줄인 재실행에서 두 건 모두 실제 `RESTORED_PREIMAGE`를 확인했다. 최종 EOF 공백 정리 후 parser/catalog 51 PASS와 inventory `--check`도 유지했다.

독립 검토의 D: writer 실행은 **37 PASS/2 timeout**, 같은 rollback 두 건의 직렬 재실행도 **2 timeout**이었다. [W5-B2 후속 진단](D:/KMTech/program-improvement-20260912/work/Label_Match/w5b2fix/RESULT.md)은 같은 두 노드·180초 자식 제한·73자 입력 루트로 최종 `384c04b`의 D: **1 PASS/1 timeout**(452.88초), 기준 `a6253d1`의 D: **2 PASS**(286.84초), 기준/최종 C: **각 2 PASS**(248.51/261.70초)를 확인했다. C: 네 사례 모두 `FAILED_ROLLED_BACK`, `RESTORED_PREIMAGE`, 코드·runtime 복원을 검증했다. D: 지연·낮은 자식 CPU 사용과 별도 flush 측정은 환경성 timeout 판정을 뒷받침하며, 기준 D:에서는 timeout이 재현되지 않았다. 제품·포트·시험·기대값·timeout 변경은 없다. 원 실패는 FAIL로 보존하며 이번 두 노드 결과를 전체 writer suite·실제 설치 수용으로 확대하지 않는다.

호출자 연결과 최종 inventory pin 이후 scoped pack은 **581 PASS**(기존 530 + 동등성 49 + catalog snapshot 2), ProgramData 독립 시험은 **1 PASS**다. 동등성은 35 기록 carrier 입력과 2,640 생성 carrier 입력, 409 날짜 입력, 800 sample/exact 조합 및 dictionary 객체 보존을 대조한다. 기존 생성기를 작업 로그 경로만 옮겨 재사용한 `--check`는 같은 44 writer identity/guard와 양쪽 pin `f7fc92fa017a3b073b8dc49154ca37b4765cf99bce2c6626f053b26e56f1fc7e`를 확인한다. source AST 비교상 변경은 표의 parser/lookup/QA·exact 검사 호출에 한정되며 portable의 기존 루트 Python 수집은 새 모듈을 포함한다.

기준 D: 실행은 I/O 지연과 실패를 보존한 채 중단했고, 공통 지침의 예외에 따라 전후 scoped 입력·TEMP·basetemp를 `C:/KMTech/Label_Match-w5b2-tests-20260913`의 run별 하위 경로로 격리했다. 로그·JUnit·실패 원본과 [최종 보고](D:/KMTech/program-improvement-20260912/work/Label_Match/w5b2/RESULT.md)는 D: 작업 루트에 보존한다. host GUI·프린터·VM·서버·전체 제품 수용·새 정책 활성화는 NOT VERIFIED다.

<a id="shared-powershell-x13b"></a>
## X13-B · 설치기 PowerShell leaf

57f52e1(shared 0.2.0, leaf 없음) → X13-B 교체는 설치된 파일 집합 + 선언된 leaf 한 파일을 예상 집합으로 사용한다. shared 버전·manifest·lock·vendor provenance 및 placement/bootstrap helper 여섯 파일의 전/후 고정 SHA256 쌍이 모두 일치해야만 이 전환을 인정한다(Git LF 또는 CRLF checkout 표현). 그 외 파일·writer pin/멤버십·runtime·계약 동일성은 기존 정책을 유지하며, 새→새 트리에서 leaf를 빼는 것은 전환이 아니다. `X13B_PINNED_SHARED_LEAF_ADOPTION` receipt로 이 경로를 구별한다. 실제 57f52e1 Git bytes의 합성 설치본으로 PS5.1/PS7 preflight·교체 후 전체 integrity와 거부 벡터를 검사하며 [R1 결과](D:/KMTech/program-improvement-20260912/work/Label_Match/x13bfix/RESULT.md)에 실행 근거를 남긴다. 실제 lifecycle은 NOT VERIFIED다.

**0.3.1 재채택:** reviewed 정본 `1555dea`의 5파일을 전부 재복사하고 독립 manifest pin·lock·bootstrap/전환 선언·vendor provenance를 갱신했다. 0.3.0의 PS5 `[manifest]`·PS7 `[[manifest]]` 두 FAIL 원본은 보존한다. 0.3.1은 singleton·2/3중 배열·빈/중첩빈 배열의 원본 수용·거부를 양 엔진에서 검사하며 PS7의 기존 singleton 수용은 유지한다. [R1/R2 결과·실패 이력](D:/KMTech/program-improvement-20260912/work/Label_Match/x13bfix/RESULT.md)을 따른다.

`Sha`/`Full`, bootstrap strict/relative/aggregate와 Manifest의 required/reparse·parse·signature 블록만 고정 0.3.1 leaf에 위임한다. 기존 wrapper의 mandatory/type/default와 LM Manifest schema/hash 순서는 유지하며 LM inventory framing·critical 3파일·ordinal 정렬·task migration·receipt·복원 함수는 변경하지 않는다. `INSTALL_THIS_PC.ps1`의 변경은 기존 bootstrap 로드에 `-SharedCodeRoot $PSScriptRoot`를 전달하는 한 줄뿐이다.

신뢰된 installer/helper의 로컬 bootstrap은 root/상위 경로/reparse → consumer lock의 고정 manifest pin → manifest bytes → leaf hash 순서로 검증한다. source/frozen은 `kmtech_shared/powershell/portable.ps1`, portable은 `app/kmtech_shared/powershell/portable.ps1`을 선택한다. UAC/복원 probe는 복사한 manifest/lock/leaf를 기존 helper ACL에 함께 결속하며, caller가 pinned in-memory integrity helper에 frozen root를 명시한다. 복원 probe의 짧은 child launcher는 Windows 명령줄 길이 제한 안에서 frozen helper bytes를 다시 hash 확인한 뒤 평가한다. 초기 `Get-BootstrapFileSha256`은 module auto-loading 없이 쓸 수 있도록 기존 native 구현을 유지한다.

portable/frozen builder는 Python 준비 뒤 로컬 checker를 실행하고 실제 leaf data/배포 목록을 포함한다. staged verifier도 leaf/manifest/lock을 필수 closure로 검사한다. 기존 writer 생성기는 44개 identity/guard를 유지하며 변경된 placement source 한 행에 따라 양쪽 pin을 `d7eb573ed3792ce03cef8dd2e8472e19c007407c346208955acc33ebfc864a48`로 재생성한다. factory lock·runtime facade/계산 bytes는 불변이다.

동일 입력 원본/위임 leaf와 unsigned 거부·서명 spy 순서는 PS5.1/PS7 양쪽에서 검증한다. canonical/placement·code/current-user 복원·task/receipt·ordinal 회귀는 합성 트리와 기존 native/transport double 범위이며 [실행 결과와 실패 이력](D:/KMTech/program-improvement-20260912/work/Label_Match/x13b/RESULT.md)에 기록한다. 실제 설치·제거·복원, GUI/VM/서버, 실제 서명된 CPython 신뢰, 전체 portable/PyInstaller binary build는 NOT VERIFIED다.

<a id="shared-core-x04b"></a>
## X04-B · 고정 shared core

정본 `kmtech_shared` HEAD `1555dea`의 package 5개(`__init__`, catalog, raster, runtime, `powershell/portable.ps1`)를 byte 그대로 포함한다. code pin은 `1be471f04b40bc2feef6d5a9f1478ddbb2336050`, version은 `0.3.1`, manifest SHA256은 `feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117`다. 별도 manifest/lock을 portable 포함 목록과 실제 PyInstaller spec 생성 진입점 `tools/build_frozen_release_candidate.ps1`에 결속한다. `.spec` 로컬 파일은 기존처럼 ignored 생성물이다. runtime은 sibling import나 온라인 pin 검사를 하지 않는다.

`python -B qualification/check_kmtech_shared.py --check`는 앱의 별도 lock에 고정된 hash/version으로 manifest와 정확한 package 5파일을 읽기 전용 검사한다. `--root <앱>`으로 설치본/portable 복사본도 검사하며 형제 저장소가 필요 없다. 검사 함수 4개는 정본 `1555dea`의 `manifest/sync_shared.py`와 동일하다. QA 도구이며 제품 배포 입력에는 추가하지 않는다. `tests/test_kmtech_shared.py`는 누락·추가·내용/manifest 변조, 잘못된 lock 거부·형제 없는 복사본의 실제 시험·portable import identity를 확인한다. 기존 zero-PE native 의존성 검사는 shared package까지 포함하며 provenance의 facade hash를 대조한다. writer inventory의 44개 identity/guard는 유지하며 Python/PowerShell pin은 X13-B 설치기 source 변경에 맞춰 재생성한다.

정본 checkout을 함께 가진 개발자는 `python -B -m pytest -q -p no:cacheprovider tests/integration/check_kmtech_shared_canonical.py`를 명시 실행한다. 이 노드는 검사 함수 source·상수·manifest bytes를 대조하고 정본 checker에 앱 lock의 고정 hash를 전달한다. 파일명으로 기본 수집에서 제외하며, 명시 실행에서 형제가 없으면 skip 없이 실패한다.

[단독 checkout 후속 검증](D:/KMTech/program-improvement-20260912/work/Label_Match/x04bstandalone/RESULT.md)은 형제 없는 동일 5개 시험의 **수정 전 5 FAIL → 수정 후 5 PASS**, 기존 718개와 새 회귀 5개를 포함한 **723 PASS / 179.79초**, 명시 정본 대조 PASS다. QA 진입점·테스트·문서만 변경하며 제품 코드·shared pin·writer pin은 유지한다.

catalog 8개 leaf만 정본에 위임한다. 나머지 46개 함수/class는 baseline과 AST 동일하며 profile/credential·URL 승인·빈 port 거부·넓은 redaction/context·시작 정책·인증 I/O/복구·snapshot은 LM에 남는다. 기존 catalog56개 무수정과 새 shared9개, 합계65 PASS다. baseline에서 고정한 v2 sidecar를 그대로 읽고 동일 bytes로 쓰며 token rotation 거부·last-good 복구·verified snapshot을 검증했다. 독립 baseline 대조는 CSV20/URL10/authenticated payload330과 URL 상호 판정을 통과했다.

최종 Windows Python3.12.10 회귀는 기존 W1–3 637개를 모두 포함한 **718 PASS / 179.13초**, 실패·오류·skip·잔류 thread0이다. 합성 입력/TEMP/basetemp는 앞선 D 병목 근거에 따라 이 과제의 C 테스트 root에 격리하고 로그·JUnit·원 실패·RESULT는 D에 보존한다. 제품 및 기존 catalog 시험·factory lock은 지정 위임 외 변경이 없으며 기존 zero-PE 시험의 수정은 facade hash와 검사할 shared package 추가뿐이다.

PROVEN: 기존 baseline83 PASS, renderer/packaging/writer49 PASS, 정본 50개 RGB/RGBA filter 조합·resize/contain·오류·메모리 GDI golden 및 2개 QR payload/실제 PHS label renderer의 baseline/facade 픽셀·PNG bytes·QR 해독 일치. [근거](D:/KMTech/program-improvement-20260912/work/Label_Match/x04b/RESULT.md). GUI·실물 인쇄·VM·서버·전체 ZIP/실제 PyInstaller build는 NOT VERIFIED다.

<a id="shared-runtime-x05b"></a>
## X05-B · producer runtime facade

0.3.1 pin의 변경 없는 runtime 25함수에 JSON/HMAC framing·시각·retry·scope·redaction, metadata/grant/receipt/liveness binding, schema/state 및 caller-transaction SQL을 위임한다. `RuntimePreparation`과 앱 adapter를 유지하며 W5-S0의 ensure/prepare 변경은 scope 호출의 인자 이름에 한정한다. identity 생성·CNG 검증 callback은 현재 앱 함수를 전달하며 transport/TLS·connection·writer admission과 LM `reopen_reviewed_runtime_in_transaction` 정책을 유지한다. 정본 대응 함수/남긴 adapter 전체는 [결과](D:/KMTech/program-improvement-20260912/work/Label_Match/x05b/RESULT.md)에 기록한다.

Windows Python3.12.10 headless: 기존 `tests/test_producer_runtime_client.py` **45 PASS, bytes 무수정**, 새 facade 회귀 **7 PASS**. 실제 앱 ACK transaction에서 core rotation 후 외부 reader에는 이전 상태만 보이고, 강제 중단 시 ACK/token 모두 rollback하며 같은 예약으로 재개한다. issue 재시도·만료 교체·terminal fallback에서 현재 앱 identity/JWK callback을 사용한다. 기존 lost-ACK·두 worker 한 token·scope/owner·stale/expiry·reviewed 복구 성공/거부·audit 실패 회귀를 유지한다.

최종 W1–4/X04-B 선택은 기존 723개와 새 회귀 7개를 포함한 **730 PASS / 174.80초**, 실패·오류·skip0이다. source 함수/서명·정본 byte 대조와 writer `--check`, 실제 frozen 호출 인자 검증을 통과했다. 격리 C 합성 입력·D 로그/JUnit과 실행 노드는 위 결과에 보존하며 전체 suite/실제 업무 수용으로 확대하지 않는다.

portable의 기존 package 포함 목록은 runtime 파일을 포함하고 실제 frozen builder는 `kmtech_shared.runtime` hidden import를 전달한다. factory lock·catalog/raster bytes와 writer 44개 identity/guard는 유지한다. GUI·VM·서버·실제 설치·전체 ZIP/실제 PyInstaller build와 성능은 NOT VERIFIED다.

<a id="named-context-w5s0"></a>
## W5-S0 · 기존 context의 named 전달

runtime scope facade·ensure·prepare의 3호출과 profile/legacy 공통 `PackageLogisticsClient(config=config, ...)` 1호출만 명명했다. profile→config는 이미 named이며 signature·positional 호환·기본값·검증 순서와 X05 정본 bytes는 유지한다. 기존 writer 생성기의 44개 identity/위치/guard는 같고 package 소스 변경에 따른 Python/PowerShell pin만 `93d7055bc94ca3bb21e3d1a817d070c298dd13dc8ba4d52b519529b22ae34574`로 재생성했다.

최종 제품/pin·격리 입력 변경 뒤 Windows headless 기존 runtime45·facade7·profile69·package156·writer4·F3/자정3은 **284 PASS**, 실패/오류/skip0이다. 고정 clock·nonce 시퀀스·JWK의 전후 **10항목 동일**: raw request/metadata JSON·auth/HMAC, receipt·저장 상태, lost ACK, reviewed 만료 복구/복구 lease ACK 유실, scope와 profile/legacy/absent 결과다. 전체 두 모듈 AST는 네 호출 이름을 되돌리면 기준과 같고 기존 generator의 inventory `--check`도 PASS다. [RESULT·정확한 노드/로그·초기 fixture 실패](D:/KMTech/program-improvement-20260912/work/Label_Match/w5s0/RESULT.md)를 따른다.

시험 입력·TEMP/TMP·데이터와 로그/JUnit은 D 작업 루트에 격리했다. 명시 reviewed 복구·F3 durable→성공→ACK·자정 기존 set, queued 결속·carrier port는 유지한다. GUI·VM·서버·실물 출력·설치·성능은 NOT VERIFIED이며, 새 관리 필드·migration·다중 scope 기능으로 확대하지 않는다.

<a id="committed-stale-runtime-recovery-20260912"></a>
## 2026-09-12 · Committed stale-runtime의 지원 복구 교정

Main/Web은 원590b의 APP_CLOSE75·accepted/committed/RAW_LEGITIMATE/inserted1/errors0/quarantine0와 기존 observe mode의 STALE_RUNTIME_FENCE, lease6 expired·active0/maxfence6을 독립 확인했다. 정상 회전 거부는 유지하되 검토 후 authority를 재개하는 지원 consumer가 없던 공백을 Main의 `MAIN-COMMITTED-STALE-RECOVERY-FIX.md`에 따라 교정한다. 서버 변경·재등록은 없다.

기존 CLI `tools/direct_sync_relay_operator.py ack-reviewed`에 `--recover-expired-runtime`, `--expected-runtime-instance-id`, `--expected-runtime-fence`, `--expected-runtime-lease-id`, `--expected-runtime-expires-at`를 추가했다. 기존 exact receipt 입력과 필수 audit 경로도 함께 사용한다. 일치하는 원 수신·spool과 단일 만료 authority만 ACK/EXPIRED로 원자 변경하고 이전 grant를 기존 audit에 fsync한다. 실제 grant는 후속 정상 relay의 기존 인증 acquisition이 맡는다. 단순 `ack-reviewed`의 기존 동작은 같으며 미확정 request/다른 runtime-bound row가 있으면 명시 복구를 거부한다.

**PROVEN — host Python3.12.10:** 새 focused19 PASS는 실제 client SQLite·CLI·기존 signed-HTTP session double로 committed observed_rejected→review→정확한 명시 복구→정상 새 fence 취득과 다음 원 batch 전송을 확인한다. 원 source POST는1회, 원 receipt/metadata/attempt·spool과 다른 행의 초기 bytes/identity는 보존한다. 재호출 거부, lease 요청 timeout 후 같은 issue body 재사용, 다른 scope/runtime/fence/lease·live grant·in-flight 상태·다른 bound row·잘못된 receipt/digest·없는 audit·audit 쓰기 실패 rollback을 포함한다. 기존 선택13 PASS는 lost-ACK exact metadata, stale terminal scrub, 정상 unassigned renewal, raw lifecycle fence4와 기존 review/retry-dead guard다. 초기 focused15 PASS 뒤 추가4 refusal와 timeout 중 재호출을 검증해19가 됐으며 수를 합산하지 않는다. 로그는 D task root의 `pytest-runtime-recovery-01/02.log`, `pytest-runtime-existing-guards.log`다.

**PREP ONLY / NOT EXECUTED:** 최종 후보·지원 command를 Main 검토에 넘긴 뒤 하나의 guest disposition/recovery/settlement 배정이 필요하다. 원76a3은 Saved/0이며 원21ACKED/1REVIEW/3RETRY_WAIT·marker297d1465와 모든 이전 실패를 보존했다. 명시 recovery와 새 grant의 실제 서버 실행, producer settlement와 Today30/30은 아직 미입증이다. 원8d native UI 수용은 변경 없는 UI 범위로 재사용하며 observer/수치 방법은 유지하고 어떠한 sample 전에 최종 candidate만 새로 고정한다.

<a id="package-waiting-display-20260912"></a>
## 2026-09-12 · 실제 M06 관측의 두 표시 결함 교정

**SOURCE:** blackdwarfian70573d48/PID27660/gpt-6-astra-xhigh/FastOFF를 Main이 확인했다. 원017은 durable capture callback이 disabled Entry에서 `delete`를 호출해 Tk가 무시한 문제다. 수락 시에만 위젯 상태를 잠시 normal로 바꾸어 삭제하고 원 gate를 복구한다. 원034의 대기0/종결-미완료2는 deferred SUPERSEDED만 보고 실제036 package PENDING을 제외한 문제다. 동일 query-only transaction에서 실제 package 상태를 합산하고 같은 set·정확한 ref의 handoff만 중복 제외한다. 원문·명령·audit·상태는 수정하지 않는다.

**PROVEN — host:** source/readback·UI lane·workbench의 headless **238 PASS / Tk2 제외**, 짧은 D 경로에서 원 경로 오류6개 **6 PASS**다. 새 회귀는 capture 실패/성공과 비활성 gate 복구, package PENDING/SENDING/CONFLICT/ACKED·marker 유무, 독립 package와 연결 없는 SUPERSEDED, 실제 readback→대기 tree/footer·focus/업무 상태 보존을 다룬다. 초기 broad 선택이 이름에 native가 없는 기존 Tk2개를 잘못 포함한 사실, 원19 FAIL/392 PASS, 긴 D basetemp 경로 실패와 fixture 교정은 D 근거에 보존한다. 해당 Tk2개는 이전 숨김 상세를 전제로 한 오래된 layout 기대 실패이며 native 수용으로 사용하지 않는다. 원19 중 변경 없는 문구 literal 기대1개도 보존하고 이 수정 범위로 고치지 않았다. 종료 후 남은 own pytest37536은 정확히 확인해 정지했다.

후속 consumer 검토에서 기존 `list_conflicts`/`dismiss_recoverable_prewrite_conflict`의 종결 의미를 보존했다. 이미 dismiss된 prewrite CONFLICT는 raw 상태를 유지한 채 operator view에서 종결-미완료로 분류하고 관리자 확인·최장 대기를 다시 열지 않는다. 실제051은 ACKED2이며 원 업무에는 이런 행을 만들지 않았다. 이 경계와 handoff·coherent snapshot·실제 renderer의 명시적 headless node **12 PASS / native exit0**를 확인했으며 첫 bf8fe59 packet은 원본대로 보존하고 최종 후보를 새로 결속한다.

**PROVEN — 원 VM 격리 native:** Main의 제한 배정에서 최종 source `8d168868053be58c5fae9c7130a83b9521ec2b50`/archive `b2553067e277b8385f3bd0be24daaeee27a004bf6eb3e2aafd2626b0644d2b3a`의123파일을 별도 경로에 대조·stage했다. 원 kmadmin/runtime에서 실제1920×1009 client의8장면, 정상 close·native0·stderr0을 확인했다. 실제 disabled Entry의 durable 수락 뒤 빈 입력, 상세 닫힘·수량48, 격리 package PENDING의 전송 대기1/종결-미완료0, SENDING/CONFLICT/이미 dismiss된 CONFLICT/ACKED 표시를 검증했다. focus·현재 작업과 원문 상세는 유지했다. Main은 실제 두 PNG와 결과를 직접 수용했다. 이 장면은 격리 local fixture이며 업무 재전송·성능 관측이 아니다.

**PROVEN — 제한 producer disposition:** Main의 `MAIN-PRODUCER-REVIEW-DECISION.md`에 따른 기존 지원 API `ack_reviewed_relay_batch`를 원 relay6118에 한 번 실행했다. 원 수신·spool·hash와 다른 행/authority를 보존하고 local REVIEW1→ACKED1, 감사1을 기록해 총21ACKED/3RETRY_WAIT가 됐다. 서버 quarantine5·원 event47의 `RAW_EVIDENCE_ONLY/NOT_PROJECTED/NO_STAGE1_REDUCER`, `UNKNOWN/NO_EVENT_OUTCOME` 판정은 유지하며 projection 복구로 간주하지 않는다. query-only 진단의 실제 원 owner는 relay590b/fence6/sequence7이고 나머지 retry2는 runtime metadata가 없다. normal relay는 재개하지 않았다.

**보존·수탁:** 최초 native01은 runtime closure 밖의 계측 import 누락으로 실패했다. utility를 원 bytes대로 계측 경로에 두고 native02를 수행했으며 제품 source는 같다. 잘못된 PowerShell ETS 직렬화로 생긴15,999,904B 원005는 보존하고 제한된 scalar reader로 교정했다. 첫 export의 ZipArchiveMode assembly 실패도 보존한다. 최종 공개17파일 archive SHA `44832a9e03eb066b5363cebd02fdc40fda8026c7692f6aac5320e2c9d6f24927`를 전부 대조한 뒤 자체 task2만 제거, 기존354개 보존·Python0을 확인했다. 원76a3은09:35:13Z Saved/assigned0이며 Main이 직접 수용했다. 정상 설치1ac·원 stop marker·queue와 모든 기존 근거는 보존한다.

**FAILED — 후속 정상 producer 복구:** 실제 Web writer 반납 뒤 Main의09:39:47Z 배정으로 원 marker를 정확히 release하고 정상 relay7944를 시작했다. 원590b를 같은 metadata/hash로 한 번 재시도해 attempt2가 됐으며09:43:13Z `STALE_RUNTIME_FENCE`/OPERATOR_REVIEW로 종결됐다. 저장 request40d6a173 receipt는 accepted/committed·inserted1/errors0/quarantine0인 APP_CLOSE raw 근거이나 runtime 회전은 거부돼 정상 ACK가 아니다. 제품 자체가 authority를 OPERATOR_REVIEW로 옮기고 assignment를 비우며 terminal token을 audit digest로 치환했다. 수동 queue/authority 수정은 없다. 원 retry2는 유지되고 기존 CSV의 정상 발견73c36a가 추가돼 최종21ACKED/1REVIEW/3RETRY_WAIT다. 지원 stop helper0/relay exit1·Python0, 자체 launcher1 제거·기존354 task 보존 뒤09:47:29Z 원 VM Saved/0을 기록했다. 새 stop marker297d1465/SHAac2dcd60과 이전 marker·원 근거를 함께 보존한다.

**BLOCKED / UNPROVEN:** package preflight9개0·active saved state 없음과 실제09-12 CSV10행/11380B는 관측했으나 producer는 미종결이다. Today 입력 freeze·case·limits·새 samples는 만들지 않았다. 기존 `ack-reviewed`는 검증된 한 receipt만 local ACK하고 authority review를 풀지 않으며 `retry-dead`는 review를 거부한다. 두 authority의 exact-clone 전용 복구는 이번 실제 한 authority에 적용되지 않는다. [실제 source 경로·기존 회귀·한정 수정 제안](D:/KMTech/optimization-implementation-20260909/Label_Match/M06-Today-continuation-20260912/display-fixes-ctx3683d7f00e96/RUNTIME-RECOVERY-DIAGNOSIS.md)을 Main/Web의 독립 판정에 넘겼고 추가 guest 복구·제품 수정·재시험은 없다. 최종8d prospective packet의 observer/reader 함수·분석 방법은 기존과 같으며 원23 ACK+게시 실패와 수용 M06 single effect를 유지한다. [진행 근거](D:/KMTech/optimization-implementation-20260909/Label_Match/M06-Today-continuation-20260912/display-fixes-ctx3683d7f00e96/RESULT.md).

<a id="m06-account-handoff-20260912"></a>
## 2026-09-12 · M06 업무 복구·계정 전환 경계

**PROVEN:** Main의 실제 CA handoff와 원76a3 배정 뒤 제품1ac의123파일을 확인하고 원 kmadmin/READY identity·profile·runtime을 유지했다. 실제 PHS2를 한 번 수락한 저장 snapshot에서만 한 멤버 fault를 결속했다. F3 한 번 뒤 set1789200370748249000/key `label-package-cmd-12a5e0bab5cdf51c4e421ed1`의 로컬 marker1/PENDING과 중앙 `receipt_48ceef1afdf749ecab63e0c95892191f` COMMITTED/package `PACKAGE-WORK-9BFF9D4E70D69D6E4B16FA9E` AVAILABLE/v1/member1·SHIPPING-WAIT를 독립 확인했다. 정상 재시작 후08:15:52Z ACKED/attempt7이며 command SHAe75776ef·local completion은 같고 원 package/lease 행도 정확히 보존됐다. 실제 현재 중앙 현품표 조회는 원 보호144B PHS2와 일치하며 새 QR을 만들지 않았다.

**CHECKPOINT:** 최신 Main 지시msg25e1ac8a에 따라 account347/gpt-6-astra/xhigh에서 종료한다. Main이08:13Z에 FastOFF로 변경·확인했고 작업자는 토글하지 않았다. 최종 GUI 정상 종료0, relay 정상 정지 관측/exit1, Python0·자체 task0 뒤 원76a3은08:27:36Z Saved/assigned0이다. relay stop marker와 guest task root는 후속 작업자를 위해 보존했고 공개 근거178파일은 D archive의 모든 bytes/SHA를 대조했다. [ACCOUNT-HANDOFF](D:/KMTech/optimization-implementation-20260909/Label_Match/M06-Today-continuation-20260912/ACCOUNT-HANDOFF.md), [RESULT](D:/KMTech/optimization-implementation-20260909/Label_Match/M06-Today-continuation-20260912/RESULT.md)를 따른다.

**UNFINISHED:** package readiness9개는0이나 producer는20ACKED·원 REVIEW1·RETRY_WAIT3이다. 원 SET_RESTORED 수신1fc08b49는 accepted/committed·quarantined1·UNKNOWN/NO_EVENT_OUTCOME이며 Main이 독립 서버 판정을 소유한다. review/authority를 지우거나 재전송하지 않았다. source-ready의 긴 원문 입력과 SUPERSEDED deferred 항목만 집계하는 대기 표시의 package PENDING 누락을 보존했고 제품 수정은 없다. Today 공통 입력·새 case·수치 limits·관측은 없고 원23 ACK+게시 실패는 그대로다. 이 계정 전환 checkpoint는 원 continuation·전체 Goal 완료가 아니다.

<a id="m06-today-preparation-20260912"></a>
## 2026-09-12 · M06 lost-ACK·새 Today 비교 준비

**PREPARED — host only:** active `task_f95347b741be/ctx_f7cfcb72507d`는 Main이 수용한 clock6b348ea/제품1ac를 유지한다. [호스트 packet과 정확한 후속 절차](D:/KMTech/optimization-implementation-20260909/Label_Match/M06-Today-continuation-20260912/HOST-PREPARATION.md)는 실제 수용1ac archive에서123파일을 골라 전부 대조했다. 새 candidate archive는 `cffa8655637edd346d98347bdc335760f2c45671effc30bac081c3d4ecba9633`, manifest는 `060a6f64a5e6c28283e76a337b68b02466424d759a2d5d0191e9092fefc30e29`다. 옛8df export와52파일 byte hash가 다르지만51개는 BOM/newline 표현만 다르고 정규화 비교의 유일한 코드 변화는 제품 main이다. archive/main과 checkout의 실제 hash를 별도로 보존하며 source bytes를 재정규화하지 않았다.

새 observer는 after ref만1ac로, reader와 한 멤버 fault는 새 task 경로로 결속했다. 함수/class AST는 기존 최종본과 동일하고 두 소스의 관측 widget8개 구성을 확인했다. 숨긴 작업 상세를 observer 때문에 열지 않는다. Python compile·기존 launcher parse와 원본 hash 대조는 PASS이며 기존 host27·clock32+38·native UI는 각 변경 없는 원래 범위로 재사용하고 재실행하지 않았다. 첫 ZipInfo readback 오류와 다음 byte-equality 가정 실패를 보존하고 실제 archive/표현 차이로 진단했다.

**WAITING / UNPROVEN:** CA가 원76a3의 실제 정상 중앙 seal과 Label handoff를 완료하고 Main이 배정하기 전 guest/VM/profile/업무 동작은 없다. 실제 pre-F3 state와 순수 draft/key로만 fault binding을 만들고 기존 한 command의 central COMMITTED·package1/receipt1·membership1, client pending·local marker1·same-key 정상 복구를 독립 대조해야 한다. M06 조정 후 실제 당일 history와9개 preflight count0이 확보된 경우에만 새 `baseline a7e / candidate1ac` 입력·환경·방법을 sampling 전에 결속한다. 원 case03은23 ACK+게시 실패/30·30 미달로 보존하고 새 numerical limits는 실제 baseline30에서만 정한다. 실제 F3/shipping·성능·전체 Goal 완료는 아직 아니다.

<a id="clock-recovery-tests-20260912"></a>
## 2026-09-12 · clock 회귀의 현재 발급 시점 교정

**진단:** clean HEAD `f97c0cff8dbe46ea8b6d0af9ecf05a20bb0aa7fd` / 제품 `1ac57294ea278b699936b6606d36e166a2e99608`에서 원 clock 파일은 **14 FAIL / 13 PASS**다. 초기 검증은 이미 C-04대로 `label-package-source/READ_ONLY`만 실행하므로 과거 first-scan lease assertion이 실제 검증 경로에 도달하지 않았다. 이 결과는 UI 단위에서 보존한 같은14실패와 일치한다. 제품 결함으로 판단해 초기 lease 발급을 되살리지 않았다.

**PROVEN — host Python3.12.10, 격리 SQLite·합성 서명:** `tests/test_deferred_lease_clock_retry.py`의 **32 PASS**는 읽기 전용 admission/정상 merged·split source, F3 발급의 signed issuance 1초 전 거부·정확한 시작 경계 허용·정확한 expiry 거부, 재개·service 실패 후 같은 요청/key, signature/binding/snapshot/artifact fence 차단, outbox marker0·중복 enqueue 방지, 기존13개 absent-destination version guard를 포함한다. UI acceptance만 대체하고 source validator·F3 queue·서명·durable store는 실제 코드를 사용한다. 별도5개는 기존 legacy-plan fixture로 보존 2단계 plan만 seed해 read 실패·1/2회 definite service 실패 뒤 원 key/hash, unknown reconcile·API clock 문구 거부 및 저장 backoff 전/정시의 scheduler 경계를 확인한다. 이5개는32개 안에 포함된다.

**PROVEN — 기존 계약38 PASS:** lease/keyring/issue/receipt/원자 marker 회귀 모듈과 선택된 read-only admission·F4 guard·offline F3·legacy freeze/unknown·materializer durable 완료 사례다. 전체 suite·VM·공유 backend·live profile·실제 입력은 실행하지 않았다. F3 clock 회귀는 enqueue/marker0까지이며 실제 F3 성공·전송 receipt 근거가 아니다.

**FAILED, 보존:** 새 legacy fixture의 첫 실행은 저장 backoff보다 이른 재시도로2실패였다. 다음5실패는 과거 fixture가 production `utc_now()`의 `Z` 대신 `+00:00` 문자열을 반환해 exact-due SQL 비교를 바꾼 탓이다. 현재 fixture는 실제 formatter와 저장 `next_attempt_at`을 사용한다. 실제 시계·보안 gate·retry 정책은 변경하지 않았다. 원 실패와 중간 실행·최종 소스/환경 결속은 [RESULT](D:/KMTech/optimization-implementation-20260909/Label_Match/clock-recovery-20260912/RESULT.md) 및 그 evidence에 보존한다.

**범위:** test와 명세만 변경했다. 따라서 [기존 격리 native UI 근거](#routine-details-20260912)는 동일 제품 범위로 재사용하며 새 native 실행으로 표시하지 않는다. Web/CA/hub/API 계약 변경은 없고 Main에 회귀 정합 종결만 인계한다. Label 성능·real F3/shipping·lost-ACK·전체 Goal은 기존 미입증/배정 범위를 유지한다.

<a id="routine-details-20260912"></a>
## 2026-09-12 · 일상 포장 화면 단순화

표준 PHS2에서 반복되던 단계표·일반 안내와 스캔 원문 상시 노출을 줄였다. 원문·세트·접수/진단 코드·절차는 `작업 상세 보기`의 읽기 전용 스크롤 영역에서 확인한다. 품목·제품 수량과 다음 행동은 기본 화면에 남으며, 대기·실패·충돌·교체 복구 안내는 접히지 않는다. 기존 세 pane과 이력 위젯은 유지한다. 이전 left `operator_set_id_label`/`operator_left_hint_label`은 `operator_task_detail_text`로 합쳤다. Today observer의 제품/레이아웃 pin은 이전 후보의 증거이며 이번 UI나 성능 증거로 상속하지 않는다.

**PROVEN — 격리 host, Python 3.12.10:** workbench/presenter/adapter/action-gate와 closed-port 접수 UI를 실제 실행해 **151 PASS / 2 native 검사 제외**다. 상세 왕복의 입력·원문·상태·action gate 보존, source48개·seal12개 표시, 수량 미확인, 복구 경고 및 ID의 상세 이동을 확인했다. 기존 layout fake가 `Style.lookup`을 구현하지 않아 중간에 빠지던 실패를 원 HEAD에서도 확인하고 fixture를 보완했다. **FAILED, 기존 소스에서도 동일:** 별도 clock 회귀의14개는 UI 도달 전 `VALIDATED`/`ORDERED_LABEL_VALIDATION_VALID`와 과거 lease 기대값이 달라 실패했고 baseline에서도 재현했다. 원 로그를 보존하며 이번 UI PASS에 합치지 않는다.

Main의 `52bddfd` 검토에서 falsey source를 과거 seal로 대체하거나 `int()`가 실수를 자를 수 있는 UI 수량 공백을 확인했다. 후속은 기존 `package_logistics._strict_int`를 사용하고 명시적 invalid snapshot·잘못된 seal·양쪽 수량 불일치를 수량 확인 상태로 표시한다. snapshot 없음/`None`일 때만 seal fallback을 허용한다. 수량 관련13사례와 기존 workbench/action-gate를 합친 **88 PASS / native2 제외**이며 정상48/12와 raw/state·기존 action gate는 유지한다. backend·멤버십·lease 검증은 변경하지 않았다.

대기 현황 외의 탭에서도 footer에 미완료 수를 유지하고 관리자 확인이 있으면 해당 건수를 경고 색으로 함께 표시한다. 기존 대기 상세의 두 레이아웃 검사는 **2 PASS**이며 관리자 확인2건/미완료13건을 확인했다. 자동 처리 대기와 별도 조치 필요를 구분하며 상태 전이는 변경하지 않는다.

**PROVEN — 배정 VM의 격리 native UI:** 제품 `1ac57294ea278b699936b6606d36e166a2e99608`과 기존 Python3.12.10 runtime으로 실제13개 widget 상태를 확인하고14개 PNG를 모두 열어 검토했다. 최대화 client1920×1009에서 대기/48개 준비·수량 미확인·상세·F4 복구·선행조건 대기·중앙 충돌·성공·이력/관리자 확인·정보 창, client1366×768/앱 글자1.4배에서 펼친 상세와 접힌 경고가 읽히며 필요한 입력/버튼/스크롤 영역이 남는다. Space로 상세를 열고 닫을 때 입력문과 focus를 보존했다. 수량 미확인은 표시 상태이며 기존 F3/F4 admission은 바꾸지 않았다.

**FAILED 이력 보존:** native01은 helper의 선택적 `win32gui` 부재, native02는 layout까지 유지하지 않은 adapter의 `win32con` 부재로 종료1이었다. native03은 기존 Win32 `GetAncestor`/`IsWindowVisible`을 ctypes로 실제 호출하는 한정 adapter로 확인했으며 허용 결과를 반환하는 fake나 dependency 설치는 없다. native03 자체는 정상 종료0/closed이며 한 PowerShell reader가 UTF8을 지정하지 않아 실패한 `09-native03-result.json`도 보존했다. 명시 UTF8 reader로 실제 성공·PID11032 소멸을 읽었으며 앱을 재실행하지 않았다.

**PROVEN — 자원 반환:** Main 배정 원76a3/kmadmin session1에서 own task3개와 process를 정상 정리했다. guest root 전체462파일/34,040,916B를 D에 보존하고 모든 SHA를 대조한 뒤 literal 임시 root만 제거했다. manifest `98DD2A7E073E7F22260A67DE23BE54FBEEE8408A4572FF11B19389EF04733ADC`, 기존 runtime 해시 불변, own session 종료와06:33:15Z Saved/0 반환을 [이번 보고서](D:/KMTech/optimization-implementation-20260909/UI-IMPROVEMENT-20260912-1318/Label_Match/RESULT.md)에 결속했다. 원901–904/M06/F4/F3/shipping과 live DB는 재실행하지 않았다. 실물 스캐너·업무 receipt·설치·Today 성능은 **NOT TESTED**이며 대표 UI 검사로 대체하지 않는다.

<a id="original-f4-f3-continuation-20260911"></a>
## 2026-09-11 · 원래 F4 같은 요청 복구와 F3 완료

**PROVEN — 원 VM·정상 제품 경로:** [이번 실행과 한계](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/BUSINESS-CONTINUATION-RESULT.md)는 runtime `8dfcee754b0cf32138462c49fb88c5fd393ca8fe`, Main이 수용한 Web78 전환·정상 grant에 결속된다. 원 GUI1240을 정상 종료0, relay10504를 정상 STOPPED/실제 종료1로 정지하고 업무 표·원 command·identity를 보존했다. own marker만 제품 API로 해제0한 뒤 같은 kmadmin/session1의 GUI3164·relay4416으로 복귀했다. relay의 기존 FAIL/종료1과 timeout-zero UNKNOWN은 보존한다.

저장된901→904 목록에서 같은 요청 재시도를 한 번 확인했다. 원 intent `3890d945…`, key와2953B command/SHA `315bda91343a641365f89ba91c72d2575143fa4a78187661a1c6da735a44cd20`은 그대로이며 ACKED/attempt3가 됐다. Web 독립 readback과 Main의 한정 수용은 같은 COMMITTED receipt1·멤버902/903/904·seal2·QT3, 원901의 damage hold와 donor 소비·원 receiving/ownership 보존을 확인한다. 화면의 새 QR을 실제 이미지에서 해독해 저장된316B/hash와 대조하고 정상 guest scanner 입력칸·Enter로 확인하여 VERIFIED/APPLIED를 기록했다. 실물 스캐너 장비 수용은 아니다. 원 물리 PHS2·raw scan1을 유지했고 원901–904를 재스캔하지 않았다.

일반 F3 확인 한 번 뒤 원 set `1789011023139836600`의 TRAY_COMPLETE/통과1건과 local_completion_committed1, outbox ACKED/attempt1, fresh lease issue1·lease1/ACKED를 확인했다. key `label-package-cmd-0ca6f7fb9dd8b4ab71528f0d`, package `PACKAGE-WORK-01BB376E8ABD4E9D57360DF8`의 저장 receipt는 COMMITTED이며902/903/904·SHIPPING-WAIT·exact_rescan_count0이다. 화면은 다음 PHS2 대기로 복귀했고 저장 active-state 파일은 정상 완료로 없어졌다. 이벤트의 packaging_set_count는1, packaging_piece_qty는null이며 중앙 receipt의 member_count3과 구분한다. [실제 outbox/event](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/030-read-f3-result.json), [lease·identity·관측 큐](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/031-completion-preservation.json)를 따른다. Web의 [독립 중앙 결과](D:/KMTech/optimization-implementation-20260909/WorkerAnalysisGUI-web/continuation-20260911/f3-central-after01/RESULT.md)는 같은 package/receipt의 효과1건·AVAILABLE 멤버3개·lease 소비1건을 확인했다. 실제 로컬 receipt_json은 기본 JSON 공백을 포함한8896B/SHA e647d1e0…이며 compact canonical 표현8562B/SHA 60b575c6…는 Web 중앙 hash와 정확히 같다. [원문 표현 readback](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/032-receipt-representation.json)과 Web의 독립 재계산으로 차이를 해소했으며 재실행은 없다.

기존 post-F4 pure reader의26번 실패는 현재 제품이 교체 후 `package_source_snapshot=None`으로 무효화하는데 reader가 즉시 snapshot을 가정한 제어 한계다. 제품은 F3에서 fresh PACKAGE_SOURCE/lease를 얻었으며 수동 상태 편집은 없다. 별도 relay14 acked·기존 producer_projection_incomplete review1/lastcycleFAIL은 그대로다. 후속 고유 업무·f3-ack-fault·최종 full-GUI Today30/30은 **UNPROVEN/미완료**이며 이번 일반 F3로 장애 복구나 성능 수용을 대체하지 않는다. 제품 소스 변경은 없다.

**Today case03 — UNPROVEN:** Main의 다음 독점 자원 창에서 정상 baseline GUI8140·warm Today1회와 개별 관측을 진행했다. 정상 응답23회 뒤24번째는 `PermissionError:PRESERVATION_READBACK`으로 중단했다. 원 결과의24행/current=null은 보존 확인 뒤 상태 게시 단계의 실패임을 보여 주지만 원 WinError·파일 경로는 기존 catch가 기록하지 않았다. 재시도·current 관측·기준 완화는 없다. 정상 종료 때 Input1 열 너비253→191과 허용된 worker clock 변경을 확인했고, 열 너비 guard 때문에 observer 종료1을 보존했다. [원 실패·호스트 제어 수정 후보·인계 근거](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/CASE03-FAILURE-AND-HANDOFF.md)를 따른다. 당시 남은 열 너비 guard의 후속 호스트 교정은 [아래](#today-host-repair-20260911)와 구분하며 guest 적용·재측정은 없다.

<a id="today-host-repair-20260911"></a>
### 2026-09-12 · Today 호스트 observer 교정·문서 종결

baseline `a7e57b7f7c3d815ad5aff945e302b916c5e68f4e`와 current `8dfcee754b0cf32138462c49fb88c5fd393ca8fe`는 저장 Input1 너비를 렌더링에 사용하지 않는다. 두 소스의 `_resize_all_columns`는 실제 Treeview 폭·프로필·글꼴로 열을 정하고, `_save_app_settings`는 실제 열 폭을 저장한다. 기존253→191은 실제 출력 변화이며 이전 저장 폭 불변을 요구한 harness 가정이 잘못됐다. 앱·사용자 설정은 수정하지 않았다.

[최종 호스트 종결](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/host-closure-20260912/RESULT.md)은 observer `f92a86713db2f98fe62f5769f6f3f01561c8570714ae970389e4f94a6ae4995c`와 reader `8b140708198a0f734f10c8bb82083b6b248e5354e4f22039f5ba765233ae9111`에 결속된다. 같은 실제 외곽1920×1032·Tk1904×993·정상 설정을 공통 입력으로 유지한다. Input1만 렌더링 출력으로 분리해 원 settings hash/값, 실제 widget·열 폭·표시 열·가로 view와 선택적인 legacy sash 상태를 기록하고 각 arm 내 변화를 거부한다. 종료 저장 Input1은 관측된 실제 폭과 같아야 하며 before/rendered/after와 변경 경로를 남긴다. 다른 모든 설정은 기존 active worker 종료 시각 예외 외에는 동일해야 하고, 각 arm 중 raw settings는 byte-exact다. 버전별 렌더링 결과 차이는 숨기지 않으며 기존 analyzer의 full-view 값·환경 일치 요구도 유지한다.

**PROVEN — 호스트 한정:** [최종27개 호스트 검사/종료0](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/HOST-LAYOUT-GUARD-V2.json)는 실제 `read_layout`을 호출하여 legacy pane 부재·지원·미지원, 세 operator pane과 열의 drift 거부, 기존 설정·identity·시각 guard를 확인했다. 두 고정 소스는 `operator_left_pane`·`operator_center_pane`·`operator_right_pane`을 포함한 필수 widget8개를 `_create_widgets`에서 구성하며 `content_pane`은 구성하지 않는다. [1317B delta](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/PANE-CORRECTION.patch)는 이전 a4 후보의 `read_layout`만 바꿔 실제 세 pane 사각형을 기록하고, 제품의 저장 경로와 같이 `app.__dict__.get("content_pane")`로 legacy 존재·sash 지원·선택적 위치를 읽는다. legacy 부재 시 위치는 null이며 저장1472를 현대 화면의 실제 sash로 간주하지 않는다. Main의 직접 소스 대조(msg_569421fc45ce)와 [이번 정적 대조](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/host-closure-20260912/SOURCE-ASSESSMENT.json)는 이 작은 변경과 unchanged reader를 확인했다. 기존 구문 검사와 고유 진행 파일 게시·열린 reader/오류 진단 probe의 종료0 근거를 재사용하며 새 host 실행은 없다. 가상 widget 좌표는 **합성 호스트 fixture**로 native geometry·성능 증거가 아니다. timing callback·sample driver·수치 기준·analyzer·두123-file source export·실제9월11일11-event/완료 세트1건 snapshot과 보호 설정 원본은 유지한다.

**FAILED 이력 보존:** 이전 `a4b557acb0ee5ae637fb97b7254ed6369d3f4289731b582146da6e304e0cc455`는 없는 `content_pane`을 필수로 읽어 [호스트 AttributeError](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/HOST-PANE-FAILURE-PROOF.json)를 재현했다. 앞선21개 검사는 `read_layout`을 fixture로 대체해 이 결속 오류를 놓쳤으므로 실제 widget binding 수용 근거가 아니다. [pane 교정 설명](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/PANE-CORRECTION.md), [중단 checkpoint](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/CHECKPOINT-SEQUENTIAL-STOP.md), [최종 PINS](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/PINS.json)와 preserved/의 옛 observer·probe·pins, 앞선 RESULT/REVIEW-PACKET, 원 게시·설정·probe 실패를 그대로 보존한다. 이 종결은 이전 성공 lead를 대체하되 실패를 삭제하거나 PASS로 바꾸지 않는다. Claude 검토는 **USER-ENDED/NOT COMPLETED — 완료 verdict 없음**이며 [종료 범위](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/REVIEW-STATUS.md)를 따른다. 대체 검토·사용량 대기 없이 Main이 중앙 수용을 소유한다.

**UNPROVEN/미실행:** guest 적용·새 baseline/current30/30·native geometry·성능 수용과 M06/lost-ACK 조정은 별도다. 2026-09-12 현재 원 VM76a3과 모든 live M06/공유 원장은 Inspection `task_0262b2a90516` / `ctx_c1dda568ef4b`의 독점 범위다. 이번 Label 단위는 호스트 문서 종결만 수행했고 guest 호출·relay/marker 소비·서버 변경·업무·측정은 없다. 기존 [지원 launcher·실행 절차](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/STAGING-AND-EXECUTION.md)는 과거 custody·dead PID·Sep11 입력을 그대로 재사용하는 명령이 아니며, [최소 후속 handoff](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/host-closure-20260912/HANDOFF.md)의 현재 소유권·최종 observer·날짜 조건을 먼저 따른다. 출력/TEMP/TMP는 지정 D 작업 루트에 두고 E는 active VM과 필수 VM 실행 파일만 사용한다.

**다음 실제 의존성:** 기존 **M06 Inspection receiving → CA → Label 일반 F3/lost-ACK reconciliation → quiescent 실제 당일 history → Main이 배정한 Today 성능 block** 순서다. 원901–904/F4/F3/shipping 업무는 이미 완료되어 재실행하지 않는다. 현재 새 Label Today 입력·case·동결 limits는 없다. [실제 당일 입력 절차](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/repair-ctx65f638/CURRENT-DAY-SUCCESSOR.md)에 따라 정상 승인 업무가 만든 eligible history와 durable/중앙 조정·아홉 대기 count0을 확인한 뒤에만 새 공통 case를 결속한다. 실제 날짜·행/세트 수·event 구성 차이를 기록하고 동일 snapshot으로 두 arm을 측정하며 수치 기준식은 유지하고 limits는 성공한 새 baseline30에서만 고정한다. 원 case03은 **23 acknowledged + 저장된 게시 실패1행/current0**, 수용30/30·동결 limits 없음으로 보존한다. 만료된9월11일11행 입력을 당일 입력으로 바꾸지 않는다. 이 유한한 호스트 종결은 전체 Label native·성능·Goal 완료가 아니다.

원 relay4416은 지원 stop 요청/native0 뒤14:08:52Z에 자연 종료1/STOPPED가 됐다. 전후 snapshot 동일, 원 업무 DB·보안9개·HKCU·123 소스·기존 CSV prefix·기존 review1을 보존했다. 31개 완료 helper 등록 해제와 Python0/소유 task0을14:11:57Z에 확인해 Main/Defect에 원 VM을 인계했다. 원 stop marker는 Label 소유로 남으며 Defect가 소비하지 않는다. 이후 Label의 guest 실행은 없다.

<a id="restored-exchange-guidance-20260911"></a>
## 2026-09-11 · 복원된 제품 교체 요청과 주 화면 안내

**실제 관측 — 0c73e13:** 원 VM의 정상 시작·복구 뒤 1920×1080 최대화 주 화면은 저장된 F4 거부 요청 때문에 F3를 막으면서도 `포장 준비`와 `랩핑 후 F3 포장 완료`를 표시했다. [주 화면 원본004](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/004-acknowledge-recovery.png)와 [저장 목록006](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/006-saved-f4-readback.png)을 실제 열어 확인했다. F4 목록의 901→904 한 쌍·거부 안내·읽기 전용 상태·재시도/닫기는 잘림 없이 보였다. 원 PHS2·멤버901/902/903·seal1, OPERATOR_REVIEW/attempt2와 저장 command는 그대로이며 새 Apply·재시도·F3는 실행하지 않았다.

**PROVEN — 당시 소스·격리 mock/SQLite:** 당시에는 current exchange attempt를 workbench render당 한 번 읽어 안내와 버튼 gate에 함께 사용했다. 현재는 [W2의 worker snapshot](#responsiveness-w2)을 사용하며 행동 직전 정본 guard는 유지한다. 저장 command가 있는 review는 F4 목록 확인, ACKED/새 seal 확인 대기는 F4 봉인 확인으로 안내하고 그동안 포장 준비·F3 완료를 지시하지 않는다. 기존 notice와 초기화·이력 조회·오류·완료·busy·종료 gate, 원 command와 실제 retry 조건을 유지한다. 대기 패널은 별도 `deferred_intents` 집계이므로 F4 표의 상태를 그 패널의 오분류로 간주하지 않는다.

Inspection 성능 측정 보류가 해제된 뒤 기존 presenter/adapter, 복원 안내·정상 복귀·우선 notice와 실제 public F4 saved-list 경로의 격리 회귀는 [91 PASS/18.66s/native0](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/restored-ui-tests01/NATIVE.json)다. 기존 writer 정적 네 사례도 [4 PASS/5.21s/native0](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/restored-ui-writer02/NATIVE.json)이며, 동일44개 identity/path/qualified-name/guard와 새 pin `fc2a6d47c1b04eb4e42ff4921e80dd4570468bf36ce3bff3feabaf4b310a5e88`을 확인했다. [실제 Claude 검토](D:/KMTech/optimization-implementation-20260909/COORDINATOR-RECOVERY-20260911-1144/label-review/REVIEW.md)는 정확한 `8dfcee754b0cf32138462c49fb88c5fd393ca8fe`의 소스 범위 PASS이며 필수 수정은 없다. 검토자는 기존 근거를 읽었으며 테스트나 VM 동작을 실행하지 않았다.

**PROVEN — 정상 전환·같은 실제 화면:** 원 VM에서 일반 확인으로 GUI8724 종료0, relay11084 정상 STOPPED 뒤 실제 종료1을 기록했다. relay의 기존 FAIL 마지막 cycle에 따른 종료1과 최초 timeout-zero UNKNOWN은 보존하며 성공0으로 바꾸어 기록하지 않는다. 다섯 변경 파일만 원본 백업 후 복사했고, 전체123 파일·44 writer·정지 시점 업무/identity와 own marker release0을 확인했다. 새 GUI1240/relay10504의 정상 시작 및 기존 reader는 native0이다. 원 물리 PHS2·901/902/903·seal1·교체 attempt2와 command를 보존했다. 일반 종료 저장으로 상태 파일 SHA는 바뀌었으나 기존 안전 업무 속성이 같음을 확인했으며, 전체 필드가 timestamp만 달랐다는 주장은 하지 않는다. [정확한 전환·실패·보존 범위](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/NATIVE-8DF-UI-RESULT.md)를 따른다.

실제 열어 본 같은1920×1080 [주 화면011](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/011-acknowledge-candidate.png)은 `작업 확인 필요`·`작업 보류`, 주황색 제품 교체 확인 안내와 F3 비활성/F4 활성을 함께 표시한다. [저장 F4 창013](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/013-candidate-f4-readback.png)은 원901→904 한 쌍·거절 사유·잠긴 목록과 같은 요청 재시도/닫기를 잘림 없이 표시했다. 010–013은 모두 native0이며 실제 재시도·새 스캔·F3를 실행하지 않았다. Claude의 비차단 관찰처럼 초기 제목 녹색과 일반 PHS2→F4→F3 절차 힌트는 남고,011에서 힌트 마지막 줄은 잘린다. 현재 행동 안내는 포장 보류를 명시하므로 이 관찰을 추가 소스 gate로 만들지 않는다.

[사전 기준](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/RESTORED-UI-CRITERIA.md)과 [당시 checkpoint](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/CHECKPOINT.md)를 따른다. 이 UI 전환 기록 시점에는 별도 relay terminal review와 Main/Web의 backend·정상 grant, F4 동일 요청 재시도/새 seal·F3·연결 업무·Today30/30이 남았다. 이후 원 F4/일반 F3의 실제 진행과 여전히 남은 장애·성능 범위는 [후속 실행](#original-f4-f3-continuation-20260911)에서 구분한다.

<a id="f4-explicit-retry-20260910"></a>
## 2026-09-10 · 저장 F4 거부 목록의 명시적 같은 요청 재시도

실제 cad3b68 GUI의 단일 F4 적용12:25:48Z 뒤 원 intent는 attempt2, exact instruction-conflict를 감싼 `SEALED_TRANSFER_EXCHANGE_RECOVERY_REJECTED`/OPERATOR_REVIEW이며 receipt·새 seal은 없다. 자동 drain은 이 결과를 재전송하지 않으므로 backend 교정만으로 진행되지 않는다. [원 실제 실패](E:/KMTech/optimization-implementation-20260909/Label_Match/business/ui-relay-cad3b68-01/F4-FAILURE-TRACE.md)와 901→904 한 쌍·원 물리 PHS2·멤버901/902/903·seal1을 보존한다.

Main GO 뒤 원 C의 기존 F4 창과 coordinator에 [C-03](contracts.md#c-03)의 명시적 `operator_retry=True`를 추가했다. 저장 목록은 잠그고 정확한 거부에서만 `같은 교체 재시도`를 열어 실제 건수·관리자 조치 확인을 받는다. 기존 background thread/lane과 lease gate를 확인하고 원 command/key를 receipt-first로 검증한다. 불확정은 review에 남아 다음 자동 POST를 허용하지 않으며, fresh 조회 중 exact ACK 또는 POST 이후 늦은 오류에도 ACKED를 보존한다. 새 잠금 framework·CLI·timer·저장소는 없다.

**PROVEN — 소스·격리 fake/SQLite:** 변경 전 해당 명시적 호출은 예상한 **1 FAIL/1 PASS**, 중간 coordinator **65 PASS/63 deselected**, UI **12 PASS/47 deselected**, 실제 public F4 진입·daemon 선택 **9 PASS**다. 최종 sealed-exchange 전체와 영향 F4/UI/daemon·writer 네 사례는 **142 PASS/20.88s, 정상 종료0**다. 같은44개 writer identity/guard를 보존하고 source pin을 `f4c17894484634f9a15a63577da19880f3aad85ca76d4105f2a69e8bcca9595d`로 갱신했다. [정확한 diff·기준·검증·문구·한계](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-explicit-retry-20260910/RESULT.md)를 따른다.

**Claude ee73 후속:** 위142 PASS와 실제 Claude 검토를 보존하고, 잘못된 정적 안내를 먼저 기존 UI 시험의 **1 FAIL/1.39s**로 확인했다. 저장 목록 안내·guard 거부 이유·불확정 목록의 읽기 전용 재열기·저장 목록 재구성 실패 안내·비활성 적용 버튼 문구를 보정했다. widget은 문구 대신 기존 submit callback으로 결속하고 새 deepcopy는 재시도 경로에서만 수행한다. guard 거부 이유는 메모리 반환값이며 DB schema/row/error/attempt를 바꾸지 않는다. receipt/fresh 조회 실패 중 이미 기록된 exact ACK도 최신 row로 보존한다. 기존 자동 receipt-only와 명시적 불확정의 재전송 금지는 유지한다.

최종 후속은 coordinator 전체·F4 draft/review **137 PASS/13.54s**와 기존 lookup/lease/daemon/writer **10 PASS/9.61s**, 각각 정상 종료0이다. 두 선택은 중복 없는 **147개** 소스 사례이며 native 화면·업무 수용이 아니다. 동일44개 identity/guard의 후속 pin은 `32fcd7537f9c961233247e83e00ba99b403a5cd52d057ad78b501c31ea4ef587`이고 [정확한 교정·문구·증거](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-explicit-retry-20260910/claude-followup/RESULT.md)를 따른다. ee73 E-only packet은 과거 후보로 보존하며 후속 source의 배포 증거로 쓰지 않는다. 최종 Claude/paired backend 수용과 정상 guest 적용·실제 재시도는 아직 남는다.

**Claude a362 최종 F1–F3:** 저장 command가 있는 다른 OPERATOR_REVIEW도 기존 F4에서 읽기 전용으로 재열고, 실제 재시도 허용은 기존 `operator_retry_available`로만 판단한다. 결과 보존 안내는 접수 가능성으로 표현하며, lease에 막힌 재시도는 다음 작업자 요청에서 다시 확인할 수 있는 일시 보류로 표시한다. 변경 전 기존 시험 **3 FAIL/2.93s**를 보존하고, 영향 F4 draft/review·lookup/lease·daemon·writer는 **25 PASS/19.77s, 정상 종료0**이다. coordinator 소스는 a362와 같아 위147 사례의 해당 근거를 재사용하며 새 전체 통과로 합산하지 않는다. 동일44개 writer identity/guard의 pin은 `e2fadfa0e152e2b6b0ba7003048b00f14b2aa27cb95a562eb89493d89e2a252a`다. [최종 diff·문구·기존 시험](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-explicit-retry-20260910/claude-final/RESULT.md)을 따르며 ee73/a362 후보와 원 실패는 보존한다.

**별도 실제 transport:** Main이 각각 검토한 CSV 두 행의 지원 acknowledgment 뒤 자연 cycle183은13:22:14Z PASS/native0·idle, queue13 ACKED였다. 두 번째의 UNKNOWN receipt·6 raw inserts/3 quarantines·spool과 원 F4/저장 상태 hash는 보존됐다([정확한 두 번째 ACK](E:/KMTech/optimization-implementation-20260909/Label_Match/business/ui-relay-cad3b68-01/TRANSPORT-SECOND-ACK-RESULT.md)). 이는 F4 업무 성공이나 quarantine 해소가 아니다. 후보 source의 독립 검토·정상 적용, 명시적 재시도의 실제 화면·입력·backend 수용, 새 QR·F3/ACK·후속 업무 및 full-GUI Today 전후 비교는 **UNPROVEN / NOT TESTED**다. 이번 소스 단위에서 live source·GUI/VM lifecycle·guest 업무 입력과 host 입력을 변경하지 않았다.

<a id="relay-exception-location-20260910"></a>
## 2026-09-10 · 상주 relay 예외 위치의 제한된 기록

**자연 실패 확인과 후속 교정:** 정상 a70→`395113e`의 세 파일 적용·own fresh marker CAS release0 뒤 같은 계정/session1에서 GUI12572·relay12440을 정상 시작했다. after-close DB/상태·identity를 보존했고 GUI 복구는 원본 PHS2 한 스캔을 재사용했다. 첫 자연 cycle은 `user_relay.py:_run_command:215`의 `completed.stdout[-2000:]`에서 TypeError가 난 것을 일곱 안전한 frame으로 확인했다. guest cp949/UTF8mode0, launcher의 UTF8 child 출력 및 non-ASCII 실패 파일명은 decode 가설과 맞지만 실제 버려진 reader 예외 자체는 관측하지 않았다. [정확한 정상 적용·자연 실패](E:/KMTech/optimization-implementation-20260909/Label_Match/relay-location-20260910/RESULT.md)를 따른다.

유일한 유지 소비자는 child status/returncode와 기존 UTF8 JSON 상태 파일만 사용하므로 쓰이지 않는 stdout/stderr 텍스트 capture·tail slicing을 제거하고 두 출력은 DEVNULL로 보낸다. timeout/no-window·exit code 판정·UNKNOWN launch 오류·writer/retry·안전한 frame 기록은 유지한다. Windows의 두 pipe reader thread와 두 tail 생성이 없어지며 처리 시간 향상은 측정하지 않았다. 같은 실패를 일으키는 무해한 실제 child0/2 시험은 교정 전 **2 FAIL/4 decode 경고/1.05s**, 교정 뒤 기존 loop/diagnostic/writer 포함 **8 PASS/5.97s, 정상 종료0**다. 동일44개 writer의 pin은 `15531452475b8340ebad713dedbd4e2e855823f253af4c973a4367c6ca7ea6cc`이고 [기준·diff·결과](E:/KMTech/optimization-implementation-20260909/Label_Match/relay-child-output-20260910/RESULT.md)에 결속한다. 실제 child의 별도 `existing_terminal_blocked`와 enqueued0/attempted1/terminal1을 성공으로 바꾸거나 지우지 않는다.

**cad3b68 실제 부분 확인:** Main 승인 뒤 정상395→cad 세 파일 적용·123개 readback, after-close DB/상태/identity 보존과 새 own marker CAS release0을 확인했다. 동일 사용자/session1의 GUI7660·relay4060 정상 시작 후 관측한 자연 cycle2(11:49:32Z)는 TypeError 없이 process FAIL/exit2와 `relay_status=existing_terminal_blocked`를 기록한다. 실제 reader 예외의 원인이나 transport 성공까지 입증한 것은 아니다. 정상 복구 Yes(11:51:46.324Z) 뒤 원본 PHS2/세트·멤버901/902/903·seal1·네 durable 표0을 보존했다. schema-info의 정상 재시작 rowid23 외 업무 표는 동일하고 Apply/F3/새 입력0이다. 앞선 relay exit1과 모든 실패를 보존하며 원래 F4/F3 연결 흐름·native 전체 UI·Today 비교는 미완료다.

실제95b와 a70 상주 relay의 `UNKNOWN/TypeError`에는 실패 위치가 없었다. 기존 `run_persistent_relay_loop` 예외 기록에 가장 안쪽 여덟 frame의 파일명·함수명·행 번호만 덧붙인다. 기존 status/reason/error_type, 30초 정상 재시도와 상태 파일은 유지하고 메시지·locals·인자·소스 본문·child payload나 새 logger를 기록하지 않는다. 원인이나 전송 여부는 UNKNOWN만으로 판단하지 않는다.

기존 실패→성공 시험에 예외 사례를 추가한 기준선은 **1 FAIL/1 PASS, 0.33s**이며 후속 기존 loop/writer 선택은 **6 PASS/6.01s, 정상 종료0**다. 안전한 위치 기록의 상한·비밀 모양 메시지/payload 부재와 다음 성공 재시도를 확인했다. 당시 동일44개 writer identity/guard의 source pin은 `cdce2579483fa9c166cc3d1ddd4f77971735a2175bdd3c20ec23aa188ace0313`이다. [기준·diff·결과](E:/KMTech/optimization-implementation-20260909/Label_Match/relay-location-20260910/RESULT.md)를 따른다. 초기 무해한 ASCII child probe는 TypeError를 재현하지 못했으며 그때는 동작 교정을 하지 않았다. 이후 실제 자연 실패 위치와 별도 undecodable-output 회귀를 위 후속 근거에 연결한다. 수동 full cycle·업무 replay·live hook·강제 종료는 쓰지 않는다.

<a id="root-ui-20260910"></a>
## 2026-09-10 · 대기 중 화면 깜박임·작업 화면 잘림 교정

Main의95b 실제027(200프레임/29.969s)은 입력 없이 `저장된 현품표 · 중앙 확인 중` 제목과 회색 업무 버튼이 잠깐 나타나는 현상을 포착했다. 원인은 주기 작업이 후보를 확인하기 전에 UI lane을 점유하고, 후보가 없어도 busy 화면을 그리는 경로다. 기존 로컬 readback worker/queue에서 같은 후보 selector를 먼저 확인하고 실제 작업만 기존 lane에 접수한다. 열린 세트에서는 materialization 후보 조회를 생략하며 접수 직전의 현재 세트 확인, 실제 claim/세대/입력 guard, 정상 종료 취소 뒤 재예약은 유지한다. SQLite를 Tk로 옮기거나 별도 scheduler를 만들지 않았다.

단계·상태 열은 실제 본문/머리글 글꼴 폭을 사용하고, 최근 완료 열은 보이는 tree 폭 또는 탭 내부 여백을 뺀 폭을 사용한다. 좌측 현재 작업은 테두리 안에서 줄바꿈한다. 대기 상세는 전체/대기 건수·최장 시간·정확한 한국어 상태·서버확정 후 로컬반영 대기·완료/미완료 종결을 남기고 내부 식별자 덤프를 제외한다. 원래 readback의 상세 필드는 유지하며, 기존 Text/scrollbar는 읽을 수 있는 글꼴의 다섯 줄 높이를 사용한다. 영문 혼합 badge/경보도 한국어로 표시한다. 소스 전수 점검에서 같은 확장 영역 우선 배치를 발견한 두 F5 확인/선택 창은 기존 footer를 먼저 아래에 할당했다. callback·QR·인쇄·업무 상태는 변경하지 않았다.

소스 기준선 **13 PASS/7.34s**, 첫 선택 **19 PASS/3 fixture setup ERROR**를 보존한다. 실제 Tk가 없는 시험의 글꼴 측정 stub을 정리한 뒤 **27 PASS/8.40s**, 마지막 열린 세트 조회 생략/도중 입력 보호 보정 뒤 해당 scheduler8+writer3은 **11 PASS/6.84s**다. 새 시험 파일은 없고 기존 scheduler 시험에 idle/실제 검증/복원/기존 입력/읽기 실패/종료 취소 사례를 넣었다. 동일44개 writer identity/guard이며 네 Label source digest와 세 source 위치가 바뀌고 최종 pin은 `61589c2cdc1393a6936a188ae8e18d873a7d3a04adbc65b823a68a8a45eae8d2`다. [기준·정확한 diff·소스 화면 점검·결과](E:/KMTech/optimization-implementation-20260909/Label_Match/root-ui-20260910/RESULT.md)에 결속한다.

**94c 후속 검토 교정:** 대기 상세 render의 예상 밖 예외는 그대로 노출하면서 후보 접수/재예약을 `finally`에서 계속한다. background 검증의 예상된 busy 거절만 작업자 입력 거절 화면을 생략하고, 기존 coalescing 재시도와 실제 작업자·broken·closing 거절은 유지한다. `retry_schedule`이 비어 있지 않을 때 기존 다섯 줄 상세에 `자동 재시도가 예정되어 있습니다.`를 덧붙인다. 이 목록은 최대100개의 시각 지정 retry로 제한되므로 전체 retry 건수로 표시하지 않으며 SQL/readback 계약은 그대로다.

교정 전 기존 시험 확장에서 **2 FAIL/2.78s**, 교정 뒤 scheduler9·busy/입력·남은 gate·상세2·readback·writer 선택은 **24 PASS/13.35s, 정상 종료0**다. 기준선 실패 시 정리 누락으로 남은 독립 host 시험 thread/process만 식별해 종료하고 시험 정리를 보정했으며, 실패 출력과 종료 근거를 보존했다. 실제 앱/guest 종료가 아니다. native 시험의 잘못된 font module 별칭도 바로잡았지만 실제 Tk 시험은 실행하지 않았다. 동일44개 writer identity/guard의 후속 pin은 `704866984db7afc430902bb8aa605b22752df7f7e05e02ab56a450a54c5412b3`이며 [후속 기준·실패·diff·결과](E:/KMTech/optimization-implementation-20260909/Label_Match/root-ui-20260910/followup/RESULT.md)에 결속한다.

**a70 실제 부분 확인:** Main의 guest 소유권 반환 후 정상95b→a70 적용/복구를 했다. 실제 idle241프레임/36.016s에서 다섯 버튼·제목 각각 단일 pixel hash를 확인했고, 좌우/루트와 네 탭의 선택 영역을 확인했다. F4 기본776×539 창의 `교체 목록 0건`·안내·다섯 footer 버튼도 잘리지 않는다. 007의 실제 F4 열기121프레임/15.672s는 클릭/modal 전환만 포착해 짧은 busy 구간의 native 입력 보호는 **UNPROVEN**이다. Apply/F3/새 바코드 입력은0이며 원본 PHS2·세트·멤버901/902/903·seal1·네 durable 표0을 보존했다. [정확한 적용·실패·보존·화면 근거](E:/KMTech/optimization-implementation-20260909/Label_Match/business/ui-root-a70d993-01/NATIVE-RESULT.md)에 preclose/afterclose hash 비교 오류와 caller의 잘못된 후속 실행, 별도 relay 자연 종료1/TypeError를 보존한다. 설정/날짜/원문/오류/F5 창, 실제 busy 입력 유지, F4 적용·QR·F3/ACK·후속 업무와 Today 전후 성능은 미완료다. Host 입력은 없으며, 새 증거와 무관한 RedrawWindow 플래그는 변경하지 않았다.

<a id="f4-editable-list"></a>
## 2026-09-10 · F4 편집 목록·대상 멤버 수 교체

**조작자 안내 문구 후속:** Claude28과 Main이 검수한 문구로 `교체 목록 N건`만 표시하고, 대상→새 양품 순서의 반복 입력·목록 전체의 단일 `[교체 적용]`·적용 전 닫기의 목록 소실을 기존 안내 라벨에서 알린다. 상한에 닿았을 때만 현재 현품표의 실제 제품 수를 표시한다. 오류/대기와 미완성 입력 문구도 실제 상태와 조작에 맞췄다. `입력 취소`는 편집을 취소해도 기존 쌍을 유지하고, 적용 후 이미 생긴 durable 작업은 닫기로 취소되지 않는다. 버튼·callback·검증·업무 계약과 b6의 폭/줄바꿈 교정은 유지한다.

**문구 교정 검증:** 기존 F4/draft/writer와 네트워크 전 수량 거부 선택 **12 PASS/7.96s**, 마지막 W3 문장 정렬 뒤 writer binding **3 PASS/4.81s**다. 기존 시험의 영문 오류 match 한 곳만 한국어로 맞췄고 새 시험은 없다. 동일44개 writer identity/guard/위치, 일곱 source digest와 두 pin만 갱신했으며 최종 pin은 `85f7e8174c100b56f06cd92ca5a36efceb375a79a0a94a8232a9ca1719fd42ff`다. [사전 기준·최종 diff·결과](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/wording/RESULT.md)를 보존한다. Main의95b 실제018–024는 추가·수정 진입·입력 취소·삭제·정상 닫기를 확인했고 Submit/F3는0이다. 최종 문구/전체 버튼의 native 수용은 Main의 정상 적용 뒤 확인하며 배포 기준은 미적용b6가 아닌 현재95b다.

**95b 실제 화면·가로 폭 후속:** 정상 적용·복구 뒤 08:17Z 기본 760×500 화면은 `교체 목록 0건 / 제출 시 0건 반영`과 네 버튼의 세로 배치를 확인했지만, 다섯째 `닫기`와 안내 문구 끝이 오른쪽에서 잘렸다. [원본 화면·무입력 Main 인계](E:/KMTech/optimization-implementation-20260909/Label_Match/business/ui-f4-95b7a85-01/F4-95B-NATIVE-HANDBACK.md)를 보존한다. 기존 네 동작 버튼 생성문과 닫기 생성문에만 `width=0`, 안내 라벨에는 기존 상태 라벨과 같은 `wraplength=700`을 지정한다. 창 크기·글꼴·padding·pack·callback·초안/제출 검증은 유지한다.

**소스 검증과 남은 실제 수용:** 최종 세 옵션 교정의 기존 F4/draft/writer 선택은 **11 PASS/9.70s**, 동일44개 writer identity/guard/위치이며 새 source pin은 `71edccc784ac31424448cf1587000beef9d87976a93536d39ad6184424dbc9ea`이다. [사전 기준·정확한 diff·한정 결과](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/footer-natural-width/RESULT.md)를 따른다. Main이 guest 입력을 소유하며 이 worker는 source copy·guest 수명주기·입력을 하지 않는다. 다섯 버튼과 안내 문구의 실제 전체 가독성/조작 및 목록 편집·제출 성공은 후속 native 증거가 필요하다.

**후속 실제 footer 실패·최소 교정:** Claude25는 실제 `16b6050`의 760×500 화면에서 아래/오른쪽 테두리는 보이지만 다섯 footer 버튼이 모두 가려지는 실패를 확인했다. 기존 버튼·상태를 먼저 아래에 배치하고 목록이 남은 높이를 쓰게 하는 4줄 교체와 `교체 목록 N건 / 제출 시 N건 반영` 문구로 바로잡았다. 기존 창 크기·8행·스타일·검증·제출 callback은 유지한다. [사전 기준·원 실패·한정 결과](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/footer-fix/RESULT.md)에 결속한다.

**PROVEN — 선행95b의 소스 범위:** 기존 F4/draft/writer 선택 **11 PASS/10.06s**, 새 시험 파일 없음. 동일44개 writer identity/guard의 source pin만 `59220bd71cd92f21794ce5224aff5aac87434d0b6336f7cbd8619c9edb32a400`으로 갱신했다. **당시 실제 화면·입력 미검증:** 해당 소스 확인 시점의 guest16b는 old 입력 수용/new 입력 미확정·목록0건·Submit0을 보존했다. 07:13:18Z 읽기 전용 확인에서 원본 PHS2/멤버/봉인1과 저장 상태 hash가 같고 exchange/package/lease 모두0이었다. Host 입력은 중단했고 live source 교체·정상 재개·새 화면 수용은 그 source 확인에 포함되지 않았다. 위95b 실제 화면과 아래5f 정상 종료·native 미실행 기록을 각 단계의 증거로 구분한다.

Main의 후속 empty-members 검토는 3bdd의 count-only 초안이 가능한 실제 분기를 확인했다. [한정 후속 교정](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/missing-members/RESULT.md)은 exact nonempty 목록을 요구하고, 명시적 레거시 direct-seal만 기존 worker의 실제 target/seal 조회로 목록을 공급한다. 기존 팝업 회귀의 missing-members **1 FAIL→PASS**, 한정 F4 **7 PASS/3.23s**와 기존 draft/writer **4 PASS/5.97s**이며 전체 suite/GUI는 재실행하지 않았다. 동일44개 guard/identity를 유지하고 pin을 `75278d9927e13288dc967e0a2112f3e65f45abd31c03810b2b077576253bd933`으로 갱신했다. 아래 130/66과 원래 pin은 선행3bdd의 보존된 결과다.

Main의 최종 추가 capability 합의와 원본 C 저장소 구현 GO 뒤, 깨끗한 parent `945e126d7ec76fd517c8011e0ecc8eab4acbb1cb`에서 [사전 구현 기준](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/IMPLEMENTATION-CRITERIA.md)을 고정했다. 기존 5f 설계·baseline, 945 이력표 교정과 모든 과거 실패를 보존한다. 수량 대화상자·자동 제출을 old→new 목록과 명시적 단일 적용으로 바꾸고, 기존 모듈 안의 작은 메모리 초안과 정상 SQLite migration으로 [C-03](contracts.md#c-03)을 구현했다. 상세 변화·커밋·파일 크기는 [RESULT](E:/KMTech/optimization-implementation-20260909/Label_Match/f4-draft-list/RESULT.md)에 결속한다.

**PROVEN — 소스·격리 fake/SQLite:** 기존 기준선 **115 PASS**, 새 요구의 변경 전 **13 FAIL**을 보존했다. 첫 구현 확인은 **11 PASS/2 FAIL**이며 두 실패는 새 테스트 donor fixture의 identity 변경 뒤 membership hash 미갱신이었다. fixture만 바로잡은 두 사례 **2 PASS**와 전체 영향 선택 **130 PASS/9.17s**를 기록했다. 3쌍 전체/부분 대상·기존 base-only 2쌍, 추가 capability 거부 시 intent 0건, 목록 편집·미완성 입력·자동 제출 부재·한 번 제출·prepare 뒤 첫 load 실패의 잠금, 같은 저장 명령/receipt의 ACK 유실·재시작 및 물리 PHS2 보존을 검증했다.

후속 **66 PASS/2 deselected/6.29s**는 기존 비 GUI layout/workbench, writer binding/POST guard 3개와 migration 외부 view/FK 거부 2개다. 정상 migration의 다섯 상태·ordered rowid·opaque JSON·index/trigger 보존·재실행 및 copy 이후 예외 rollback도 앞선 영향 선택에 포함된다. writer identity/guard **44개 동일**, 기존 두 source pin만 `c506e8f8e91a8dadb402dde42848fa7b7ea2e73cf8cf099de7caea00cb8d7a0a`로 갱신했다. TEMP/TMP·pytest DB/output은 E 아래이며 새 테스트 파일·실행 framework는 없다. 시간은 테스트 실행 시간이다.

**UNPROVEN — 실제 수용:** 760×500 새 F4 화면의 실제 가독성/입력, 현재 paired server 후보의 배포·3쌍 이상 실제 원자 교체, 전자 seal 확인·F3/ACK 장애 복구·후속 출고, 최종 Claude F4/native layout 검토와 full-GUI Today before-a7→5f 비교는 남는다. 원 guest는 5f에서 원 PHS2 세트를 교체/F3 없이 정상 종료했고 [PAUSE-RESULT](E:/KMTech/optimization-implementation-20260909/Label_Match/business/ui-fault-pass-20260910-01/PAUSE-RESULT.md)의 identity/데이터 보존을 유지한다. 이 소스 단위에서 guest·서버·relay·grant·live DB를 실행하거나 변경하지 않았다. live 086에 3쌍 이상을 제출하지 않으며 후속 Main 배정에서 exact 후보와 새 실제 증거를 연결한다.

<a id="history-layout-20260910"></a>
## 2026-09-10 · 이력표·우측 폭 최소 교정

`5f19ce3`의 사용자 crop과 실제 최대화 `014-refreshed-layout.png`에서 시간 머리글 잘림을 확인했다. 구현 전 기준과 원래 실패는 [RESULT](E:/KMTech/optimization-implementation-20260909/Label_Match/ui-layout-fix/RESULT.md)에 보존한다. 기존240/311px 열 측정과1366/1440/1920 설계 앵커를 사용하며, 실제 guest DPI를 추정하지 않았다. 본문 전체 최소폭 대신 머리글 우선 폭을 배분하고, 짧은 창의 우측410px 상한을 기존 최대720px로 통일했다. 우측 Notebook 스타일·선택 원문 요청 높이만 함께 교정하고 날짜 도구열의 반응형 줄바꿈·선택/복사·원문과 업무 상태는 유지했다.

**PROVEN (소스·CPython 3.12 비 GUI):** 구현 전4 FAIL→같은4 PASS, 추가 원문 높이 상한1 FAIL 보존, 최종 기존 focused **64 PASS/실제 Tk 2개 제외**. 동일44개 writer identity/guard의 정규화 source·line 파생 pin만 양쪽에서 `5457ddb4bca2d097ebe498599e7bcaaa35e42a841f3b826c926ff320e334e750`로 갱신했다. 새 검사 파일·실행 도구는 없다. **UNPROVEN:** 수정 후보의 실제 VM 화면, +/-·창 크기 왕복/탭·도구열 비겹침과 실제 원문 높이 포함 관계, 최종 Claude 수용. Main의 배정된 GUI 슬롯에서 확인한다. 이 결과는 성능 개선이나 F4 목록 제출이라는 별도 새 요구의 완료를 주장하지 않는다.

<a id="s05-simplification"></a>
## 2026-09-09 · S05 소스 단순화와 한정 검증

Main이 선택된 여섯 프로그램 qualification을 독립 종결한 뒤 승인한 소스 작업이다. 새 commit 자체로 Full, native rebuild/reinstall, F1/F4/F3·출고 replay 또는 서버 갱신을 요구하지 않는다. 아래 Stage/Setup/Full·당시 배정/대기 기록은 역사 근거다. 필요한 추가 검증은 바뀐 동작·공급자·배포 범위와 남은 구체적 위험에 따라 정하고, 변경 없는 입력의 기존 근거를 재사용한다. 새 대상·실제 설치·보안/데이터/공유 자원 작업의 권한과 독립 검토는 유지한다.

`Label_Match.py`에서 옛 `_create_legacy_widgets`와 미사용 보조 함수 8개, `deferred_intent_capture.py`의 미사용 entropy wrapper, `item_catalog_sync.py`의 미사용 cache 판별 함수를 제거했다. 실제 시작은 `main`/portable entry → guarded application → `_create_widgets`이며 그 builder와 F1/F4/F3 처리 함수의 동작은 변경하지 않았다. `tools/measure_zero_pe_label_parity.py`는 전환기 Git/Pillow 비교 출력만 생성하고 현행 package closure·installer·runtime·test·runbook 소비자가 없어 제거했다. 기존 출력과 base commit의 원본은 보존한다.

**PROVEN (소스·격리 host CPython 3.12.10):** 기존 cancellation, sealed-transfer, operator action gate, workflow snapshot/presenter, inline warning/idle 및 entropy/cache/writer focused 선택 **217 PASS**. 직접 호출만 남았던 옛 completion helper 검사 두 개는 제거했으며 raw/parsed snapshot 렌더링, full/partial 구분과 durable 완료 뒤 idle 복귀 시나리오는 유지했다. 실제 Tk/설치 제품·서버·VM을 시작하지 않았고 temporary data와 JUnit/log는 E: 작업 루트에만 기록했다. 동일44 writer identity/guard의 새 양쪽 pin은 `7205c9ac726db320fdb7ea79b8b28bf7024d6cca0eed1a37d8107e40fab94e04`다. [RESULT 및 정확한 실행/한계](D:/KMTech/s05-simplification-20260909/Label_Match/RESULT.md)를 정본으로 한다. 시험은 E: editable clone에서 완료했다. 사용자 지시에 따라 Main이 기존 C: 변경을 `a57d50b52695030bc91eb2094469c9e3fd730a7c`로 먼저 commit하고 S05 delta만 원본 C: 저장소에 한 번 반영했다. 원본 C:에서 같은44 inventory record/pin과 세 production module의 retained AST 일치를 직접 확인하여 변경 없는 시험 근거를 재사용했다. E: source는 checkpoint로 보존하며 후속 편집·commit은 원본 C:에서 수행한다. 이 결과는 S05 artifact의 native/배포 수용을 추가하지 않는다.

[제품·기능](README.md) · [데이터·통합 계약](contracts.md) · [남은 일](BACKLOG.md) · [중앙 준비도](../../../Program_Spec_Hub/READINESS.md)

최초 기준일은 **2026-09-07**, 후속 갱신은 **2026-09-08**이며 소스와 네 판단 축은 [README](README.md#evidence-scope)를 따른다. 기존 문서 기준선·잔여 소스 종결에서는 앱·테스트·VM·프린터를 실행하지 않았다. 이후 이번 [relay custom 경로 교정](#relay-custom-root-evidence)은 격리된 호스트 headless 검증 26 PASS를 직접 기록했다. Main이 별도로 수행·수용한 [M3/N6](#residual-source-evidence), [ProducerClose](#producer-close-evidence), [SaveRoot13](#saveroot13-evidence)는 각각 원래 소스·환경에 남으며 나머지 복구·수용 항목을 실행 결과로 간주하지 않는다.

<a id="task10-current-user-reuse"></a>
<a id="resident10-alignment"></a>
## 2026-09-09 · 사용자 승인 resident 정렬과 PHS09 실제 실패 보존

[receipt12 최종 native continuation](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)은 수용 source `da60e05e9bf07855f701fff328df70c10db0a094` / ZIP `782d585bf09396da8c33fbb3f1c3b24afc6a8716f73e0b4983b951a896633d6e` / manifest `829f5f2230add6f0245106c5b4dac40547f56941792c2da46dbf5318effa6111`에 결속된다. 원 assigned VM에서 동일 artifact의 정상 설치 attempt02 native0, exact retained14의 지원 `ack-reviewed` native0, 지원 current-user 제거·code-only 제거, 수용 timing06 prior-code 설치/보존, 최종 receipt12 복귀 설치03:42:16Z/native0와 readback0을 완료했다. GUI13은 기존 resident를 재사용하여 PHS2 대기·이전 완료 이력을 표시하고03:44:40Z/native0으로 정상 종료했다. 실제 정상 shutdown 뒤 Off/uptime0을 관측하고 새 boot03:46:01Z·Explorer4640/session1에서 원 SID의 resident7556이 03:46:22Z에 자동 시작한 것을 확인했다. 수동 product 시작 전 단일 resident·정상 HKCU Run·canonical task0·stop/fence 없음이며 postboot native0은 payload3,391개와 보호 파일25개, 최신 post-review backup의 업무11 tables/40 rows를 확인한다. 전체 상태 메타데이터는 40/51개 동일하며 나머지 11개 status/control/settings/log 변경을 기록했다. app_settings 필드별 차이는 미확인이고 전체 config/DB/schema/hidden-rowid 동일성이나 새 대상 설치를 주장하지 않는다. [최종 보존 readback0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-PRESERVATION02.json)은 원 receipt/metadata/spool/attempt1·audit1과 inserted10/quarantined4를 유지하고 queue11ACKED를 기록한다. 원 event CSV prefix 두 개가 동일하며 APP_START1·APP_CLOSE1만 추가됐다. 원 F1/F4/F3·shipping·로컬7 업무나 pre-F3 DB 복원을 반복하지 않았다. 첫 설치 native1/UAC 취소·지원 rollback, 별도 pre-execution policy rejection과 reader 실패는 이력으로 보존한다. [Main의 선택 범위 최종 native 수용](E:/KMTech/resume-after-input-20260908/LABEL-RECEIPT12-FINAL-NATIVE-ACCEPTED.json)은 `PROVEN_SELECTED_LABEL_FINAL_NATIVE_ACCEPTED`이며 Main msg_b42de21979ea 승인에 따른 정상 guest shutdown 뒤 [최종 VM Off/uptime0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-VM-OFF.json)을 03:58:51Z에 확인했다. 이 수용은 기존 assigned VM의 선택된 Label 범위에 한정한다. 전체 제품 Ready는 **0/6**이며 여섯 프로그램의 현재 조합 판정과 다음 S05 source workspace 결정은 Main 소유다.

[receipt12 공개 이벤트·strict RAW 교정](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE.md)은 `SET_CANCELLED`를 공개 `set_id`만 기록하도록 제한하고 `SEALED_TRANSFER_EXCHANGE_APPLIED`에서 `old_seal_qr_payload`·`new_seal_qr_payload`만 제거한다. 공개 set/intent/receipt/bundle·멤버 목록·version과 private 취소·복구·apply 상태는 유지한다. strict RAW allowlist에 정확히 두 이름만 추가하며 receipt/identity/hash/bytes/행·event 합계, `OBSERVED/RAW_EVIDENCE_ONLY/NOT_PROJECTED/NO_STAGE1_REDUCER`, quarantine/errors=0 및 runtime fence를 그대로 요구한다. 저장된 bytes와 근거 hash를 확인하여 focused60·inventory4 PASS와 parent causal6 FAIL을 재실행 없이 재사용했다. 동일44 writer identity/guard의 pin은 `507da9c952a88649cd06c090f45fb6e1bb5129de12260a747741b744ccc1d4cb`다. receipt11 `7c4abfa`/ZIP2094ba6a를 parent/recovery로 보존하고 단 한 번 고정·빌드한 수용 source12는 `da60e05e9bf07855f701fff328df70c10db0a094`다. Web05 normalizer의 기존 공개 형식을 사용하며 reducer/비밀 검증을 변경하지 않는다. [native 완료 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)와 [최종 source 소유권 지도](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE-OWNERSHIP.md)를 연결한다. 이 문서의 최종 native 기록만 갱신했으며 고정 artifact는 재빌드하지 않았다. Ready **0/6**이다.

resident10의 실제 설치 전 public uninstall/native0는 추적 사용자 상태29개를 모두 보존했다. 설치 뒤26/29개가 그대로이고 정상 onboarding/control 상태3개만 달라졌다. [pre-GUI custody](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-RESIDENT10-READBACK.json)는 원 F1 취소v8·원 F4 PREFETCHED/fence2·원 F3 ACKED/local completion1 및 원 case04 command/attempt2/NULL receipt를 확인한다. GUI09는 기존 resident6916(23:09:28.3691100Z)을 그대로 유지해23:16:09Z 정상 기본 시작했고, [자동 복구 안내 확인](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RESIDENT10-GUI09-RECOVERY-ACK01.json)은 새 F4나 재스캔 없이 한 번만 수행했다. 동일 원 command는 GUI09 시작23:16:23Z에 ACKED/attempt3로 복구되었고 receipt_04cc7f6eff4e4064a432a291681ba4d1이 보존됐다. target 새 membership hash는 a2657b636b78a04194f7e59fd6bf97ad9049bb8852894913346e1ee87d22c1f7/count3이며 이전 두 unit은 damage에 반영됐다. 원7 custody 묶음은 full row/rowid hash까지 유지된다. seal/local apply 및 case04 F3는 아직 미실행이며 정상 새 전자 봉인 확인 UI가 열려 있다.

[Web의 독립 중앙 확인과 보존 범위](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RESIDENT10-CENTRAL-F4-SCOPE.md)는 같은 receipt·targetv2/sealrevision2·new3/hash 및 donor origin 보존/current IIN 재결속을 확인한다. 로컬7 custody 묶음 hash 불변과 중앙 상태는 구분한다. 중앙 원 F1은19 row identity와9/10 table hash를 유지하지만 old operation_leases가 EXPIRED_UNRECONCILED로 관측됐다. 여전히fence2/unconsumed/unreconciled이며 담당자가 전이 원인을 추적한다. Main은 이 lease를 그대로 보존하고 새 case04의 seal/F3와 분리했으며 release/recreate/reconcile을 수행하지 않는다.

GUI08는 정확한 같은 사용자 예약 작업이 Running인 상황에서 기존 무조건 거부 guard를 먼저 지나 `CurrentUserScheduledTaskError`로 종료했다. public 설치 native0와 첫 시작 native4를 구별한다. 정상 오류 확인 뒤 GUI 자연 exit21:41:29Z, public 사용자 제거 native0/PASS_DATA_PRESERVED21:42:46Z와 실제 pause02를 보존한다. pause02는 canonical process/task/Run0·실제 writer-session/active.json 부재·stop marker 존재, 원 custody7묶음·원 command fullrow hash·attempt2와 case04 CREATE_PACKAGE/lease0을 확인한다. 최초 mouse ButtonIndex0의32773 및 올바른1-based 처리, 최초 잘못된 fence 파일명 관측과 교정도 원 증거에 남긴다.

Main이 전달한 사용자 결정은 task10 guard-only를 supersede하고 [현행 계약](contracts.md#current-user-resident-relay)처럼 기존 HKCU Run/resident 구현을 재사용한다. 예약 creator/one-shot runtime/parser/활성 product mode는 제거했고 옛 argv는 exit2로 종료한다. 기존 healthy owner는 normal startup에서 재사용하며 정상 사용자 제거는 resident ABSENT 뒤 task를 이행한다. 실제 이전 버전 consumer가 남은 canonical rollback과 elevated historical SYSTEM uninstall만 exact ownership/action 및 정상 quiescence로 유지한다. 새 watchdog/relay/framework 또는 업무 변경은 추가하지 않았다.

영향 source 결과는 [onboarding/resident/product-host59PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-affected02.xml), [current-user native task fixture38PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-retirement02.xml), [historical SYSTEM uninstall fixture23PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-legacy01.xml), [GUI/등록/installer 소비자12PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-consumers01.xml), [최종 inventory4PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-inventory02.xml)다. 각 native0/log/XML이 있고 host/guest task나 업무를 fixture로 조작하지 않았다. affected01의 UNKNOWN downgrade1FAIL과 final ownership/unknown count의 [causal3FAIL](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/resident-review-red01.xml)을 교정 후 증거와 같이 유지한다. [inventory delta02](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RESIDENT-INVENTORY-DELTA02.json)는 기존46개 중 scheduled_relay/scheduled_task_install만 제거하고44개 source identity/guard를 보존한 pin `a312e444c0c510594011b97804cb57a5da25f70ccabf39ca3892462ad79ee9ee`를 결속한다.

이전 [task10 guard-only59PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/task10-final.xml)·원 RED·rollback RED 및 source preimage는 폐기한 후보의 한정 이력이다. 해당 후보의 freeze/build/deploy는 없었다. PHS09 업무 source79PASS와 기존3266 providers는 변경하지 않아 원 검증 범위를 보존하며 Full/provider를 재실행하지 않는다. 실제 수정본 default startup/live resident·GUI close 후 sync·login/coldboot/single-instance·지원 migration/remove/reinstall/data 보존은 최종 동일 artifact로, 원 F4 command/receipt/새 seal/local apply/F3 및 normal prior-code recovery는 실제 custody를 이어 검증한다. 22:33Z 준비 중 VM Off/22:53Z 정상 부팅은 설치 전 이력이며 최종 resident10 coldboot 증거가 아니다.

## 실행 구성과 소유 경계

현행 qualification의 ERROR_INPUT 후속 교정은 [Web 독립 receipt](E:/KMTech/web-integration-20260908/label-terminal-receipts-readback.json)의 exact batch/request·2523B/hash·2개 SCAN_ATTEMPT/ERROR_INPUT raw row에 근거한다. `_is_raw_lifecycle_receipt`의 허용 event에 ERROR_INPUT만 추가했으며 전체 spool hash/byte·install/source identity·행별 count/classification·HTTP/commit·runtime fence와 business COMPLETE 요구는 유지한다. [수정 전](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/input-receipt-red.xml) 4 FAIL 뒤 [focused](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/input-receipt-green.xml) **66 PASS**는 inserted/replayed 입력 오류·불완전 receipt·혼합 business 거부를 포함한 host fixture 범위다. 실제 terminal row는 자동 재실행하거나 삭제하지 않는다.

기존 정상 `tools/direct_sync_relay_operator.py ack-reviewed`가 portable tool closure에서 빠져 있어 [builder](../../tools/build_portable_release_candidate.py)의 기존 외부 tool 목록에 포함했다. [기존 복구 guard·portable closure·inventory](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/recovery-closure.xml)는 **14 PASS**, 파생 pin `566e4e7ea051107f2470fd4f217f60da79f549e0c04de6f30703b4e1d10ea9d7`과 기존 45 writer identity/guard가 일치한다. 설치된 엄격한 consumer로 실제 2행 receipt를 다시 검증한 뒤 Main이 승인한 exact relay/request/hash/error와 Web 증거에 한정하여 정상 `ack-reviewed`를 실행했다. [native 0 결과](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/reviewed-recovery-02/EXIT.json)는 11:36:36Z이며 [읽기 전용 queue](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RELAY-LINEAGE-03.json)는 해당 ACK와 다음 mixed delta의 정상 자동 ACK attempt 1을 별도로 확인한다. [Web readback](E:/KMTech/web-integration-20260908/inspection-x09-and-label-delta-readback.json)의 뒤 6행 중 오류 TRAY_COMPLETE 2행은 PROJECTED, 나머지 관측 4행은 raw/NOT_PROJECTED다. 오류 행의 projection과 operator-reviewed raw ACK를 F3 성공으로 취급하지 않는다. 첫 PowerShell observer preflight 실패와 최초 terminal receipt·spool·identity·이력을 보존한다.

<a id="label-fresh-qualification"></a>
### 2026-09-08 새 VM 정상 설치·등록 복구와 relay 재시작 교정

14:43Z 중지 이전 timing06 설치는 Main `msg_4985c5302883`이 독립 수용한 [timing06](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-timing06/REVIEW.md) source `6961456819c2ff8be15c1855a5e05284c34e7171` / tree `5975b2f63aae47021add9d3ac207e9b656eb0a70`이다. archive 26,141,411 B / SHA `e2f2f6fb4f6d54dddd38ec9048385aa240295b7465b90437c7ba765f0238f2e1`, manifest `02751230e193481cc9882d0d229ed9a3f42cb47610bcd8a505abb6c6077cd8b1`과 provider 3,266개 원래 bytes를 유지했다. [정상 코드 uninstall](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/UNINSTALL-TIMING06-READBACK.json)은 native 0·27개 상태 hash 동일이며, [정상 UAC 재설치](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-TIMING06-READBACK.json)는 14:27:29.0191024Z native 0, 설치 3,392파일 중 payload 3,391개 전체 재해시 PASS, READY/REUSED·HEALTHY_CURRENT_USER·exact launch PROVEN·writer fence RELEASED_AFTER_RESTORE다. 설치 뒤 원래 27개 상태 중 23개는 동일하고 4개 stop marker/onboarding/relay 상태만 정상 변경됐다. [교체 전 백업](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/BEFORE-TIMING06.json)의 eb79 설치 3,392파일 preimage와 두 SQLite 일관 백업은 guest private에 보존했다. 정상 제거/재설치를 exact rollback 또는 cold boot로 계산하지 않는다.

Main `msg_54b72bd836bc`은 native 결과 보존 후 X09 공유 중지를 지시했다. 이번 설치 뒤 새 GUI/업무/cold boot를 시작하지 않았고, 기존 `--remove-current-user-setup` 한 번은 [14:41:14.3185537Z native 0/PASS_DATA_PRESERVED](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/shared-pause-remove-02/EXIT.json)였다. [14:43:19.3152506Z actual pause](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-TIMING06.json)는 canonical 제품 프로세스/task/Run 0, writer-session active 파일 없음, stop marker 존재, 기존 Explorer4200/session1을 확인한다. GUI04의 앞선 정상 종료를 재사용하며 새 GUI 종료를 꾸미지 않는다. [읽기 전용 업무 확인](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-TIMING06-BUSINESS.json)은 원래 F3 ACKED/local completion1과 F4 set `1788871769401472200`, 정확한 물리 입력·fence2 PREFETCHED·issue attempt ACTIVE를 모두 보존했고 교체 intent/포장 명령은 0건이다. Main `msg_71c3d4b5c50f`에 이 정지 경계를 보고했으며 재개 전 guest/source diagnosis를 중지한다. 원래 F4를 해제하기 위한 ADMIN machine credential/permission 우회나 새 취소 체계는 승인되지 않았고 실행하지 않았다. 새 정상 intake/disassembly/Inspection donor batch는 Main/Web 준비 단계이며 새 stock/demand/lease mutation은 없다.


후속 **timing06 source 교정**은 Main의 초기 validation/F3 권한 분리 지시를 반영했다. 새 source plan은 읽기 전용 1단계·기존 claim 만료에 결속된 VALIDATED snapshot으로 끝나고 F3 때 기존 경로가 lease를 얻는다. [causal RED](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-red.xml)는 초기 grant/발급·materialization/F3 경계 5 FAIL, F4 잔여 guard 4 PASS였으며 수정 후 기존 deferred **111 PASS**, lane/package/lease **218 PASS**, inventory **4 PASS**다. 첫 교정의 bounded-expiry 누락은 5 FAIL/106 PASS로 보존하고 기존 claim expiry를 전달해 해결했다. 줄바꿈 보호 edit가 실패한 뒤 동일 실패를 다시 관측한 로그도 보존한다. [source 비교](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/TIMING06-SOURCE-CHECK.json)는 Label class에서 `_execute_deferred_label_validation`만 바뀌었고 F3/F4/초기 resolver 메서드는 동일하며, 기존 45 writer의 identity/path/guard와 파생 pin `68b8abc21ab01737ed99d9b8dcb76ba3994ece6f77f072cbf87093ecff85e6b5`를 확인한다. [별도 review packet](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-timing06/REVIEW.md)의 Main 수용 뒤 위 정상 교체를 수행했다. 이 source PASS와 설치 native 0을 native F4·새 F3 결과로 계산하지 않는다.

Main은 receiving `0ffe`를 13:37:19Z ACCEPTED로 독립 확인한 뒤 공유 hold를 해제했다. 이전 hold 해제 뒤 Label은 timing06 검토·교체 전까지 자발적으로 기존 GUI/relay 정지를 유지했으며, 현재는 위 새 X09 요청으로 다시 중지 상태다. 원래 F4 fence2는 package-owned로 보존하며 맹목적인 재시도·새 early-prefetch scan·backend rewind 없이 별도 정상 대상/처분 지시를 따른다.

직전 eb79 설치 이력은 [동결 소스](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-idle05/SOURCE-FREEZE.json) `eb79e519c24c0a1a7bf917080d538b8ba404edb5` / tree `db7911429fd2d37e0a8ef980fe921ada2198ef34`, archive `9e168e7138eb246870c3ecb62a7ef75eb58972a131f0831ab2cf5244094acb99`(26,141,583 B), manifest `aef3c4a9cac2f266aa0bbb4aa58c4d3ae2824559341595b21446e7ec87a6a0d1`다. source.bundle `99c98a23e3d6cbeb1a642a7847e6c5216f2a1432420fee78cbf7f6832165f9ec`와 원래 provider 3,266파일을 보존한다. [portable delta](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-idle05/PORTABLE-DELTA.json)의 7개 byte 차이 중 3개는 checkout 줄바꿈 차이뿐이다. 원래 실패·빌드 시점 NOT_TESTED와 후속 실제 설치 결과는 별도 파일로 유지한다.

정상 PlanOnly native 0 뒤 GUI03 정상 종료, 사용자 제거 native 0(12:35:34Z), UAC 코드 uninstall native 0과 [기존 상태 26파일 유지](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/UNINSTALL-BEFORE-IDLE05-READBACK.json), [정상 재설치](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-IDLE05-READBACK.json) native 0(12:43:45Z)·3,392파일 integrity PASS·HEALTHY_CURRENT_USER·READY/REUSED·writer fence 정상 해제를 확인했다. 직전 설치 3,392파일의 exact preimage/DB backup은 `private/before-idle05`에 보존했으며 다른 버전 rollback은 실행하지 않았다. [GUI04](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI04-REUSE-READBACK.json)는 PID2612에서 기존 relay4312를 재사용했다. [정상 F4 원본 입력](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI04-F4-INPUT-PERSISTENCE.json)은 139자 hash가 즉시와 12초 뒤 모두 일치하고 fresh copy sentinel이 바뀐 뒤에만 Enter를 보냈다. 새 코드의 F3 완료 후 입력은 아직 별도 미실행이다.

보존한 원래 F4 사례는 [정확한 2 Pcs/full_single_transfer snapshot](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/F4-BEFORE-EXCHANGE.json)을 얻었지만, 정상 START에서 발급한 `operation-lease-6dd46054-7345-4c4d-8a95-2df3b4f0ce31` fence2의 PREFETCHED/ACTIVE 상태 때문에 [포장 처리 우선 경고](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/F4-QUANTITY-DIALOG.json)로 수량 입력 전 차단됐다. expiry 13:19:29Z가 지나도 로컬 gate는 상태만 검사하며 production retirement caller가 없다. 직접 seal QR은 START 입력으로 지원되지 않는다. Main은 이를 [C-03 초기 lease 발급 시점 결함](contracts.md#c-03)으로 확인해 위 source 교정을 지시했고 lease/token/receipt/DB를 수정하지 않았다. GUI 종료를 위한 Alt-F4가 같은 경고를 다시 연 관측도 보존하며, 수량·old/new pair·교체 명령은 제출하지 않았다.

Main의 공유 backend 전환 요청으로 정상 창 닫기와 확인 버튼을 거쳐 GUI04는 13:25:51Z native 0으로 종료했다. 지원 `--remove-current-user-setup`은 13:22:50Z native 0/PASS_DATA_PRESERVED였으며 [13:26:44Z 중지 확인](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-02.json)은 제품 프로세스·canonical task·Run 항목이 모두 없음을 기록한다. 최초 keyboard/close guard 및 확인 관측 실패는 개별 파일에 남겼다. [중지 DB 사본의 읽기 전용 확인](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-BUSINESS.json)은 integrity OK, 원래 F3 ACKED/local completion1, F4 원본·set·lease 보존, F4 교체 intent/포장 명령 0건을 확인했다. 이 관측 당시 Main 재개 전까지 client 쓰기를 중지했고, 이후 hold 해제와 현재 정지 범위는 위에 기록했다. 기존 F3의 서버 확정은 직전 후보에, 새 설치의 확인은 기존 업무 상태 보존에 각각 한정한다.

아래 단락은 앞선 후보·실패·복구의 시점별 이력이다. 현재 결과는 위 현행 후보와 중지 상태를 기준으로 읽는다.

`8425fe1`의 F3 다음 실제 입력에서 Entry가 회색으로 남고 copy readback이 실패하여 Enter를 보내지 않았다. `_submit_ui_lane_task`의 finish/fail/settle render는 TkSerialUiLane가 아직 BUSY일 때 실행되고 실제 IDLE 뒤의 render가 없었다. 기존 callback 뒤 정상 admission을 다시 계산하는 `idle_on_tk`와 caller `on_idle` 보존만 추가했다. 실제 presenter와 lane을 함께 쓴 [RED](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/idle-admission-red.xml)는 success/failure/stale 3 FAIL, 잔여 gate 4 PASS이며 [기존 lane/action gate/protocol](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/idle-admission-green.xml) 전체 **97 PASS**다. [기존 45 writer identity/guard 및 원래 Label bytes 보존](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/INVENTORY-IDLE-FIX.json)과 파생 pin `cb277164040b73c9bd20fdea4e527feee25a808583e51bf3badd100650b861da`의 기존 static **4 PASS**를 별도로 확인했다. Main은 작은 delta를 독립 수용했다. 위 successor의 정상 패키지·설치·초기 입력은 확인했으며 F3 후 입력은 별도로 남는다. `8425fe1`의 기존 F3는 [Web 독립 근거](E:/KMTech/web-integration-20260908/label-f3-and-x09-claim-readback.json)가 정확한 2 Pcs edge/receipt 한 건과 lease fence2 CONSUMED를 확인했으며 새 코드에 자동 상속하지 않는다.

직전 실제 설치는 [동결 소스](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-relay04/SOURCE-FREEZE.json) `8425fe167830e832ea996e90059aa089fadbaf70` / tree `5d829abdb8e804428d486f91a8baa1e1fbb1887c`, archive `c1f396d7bec923983a5a6336c61530ad9dbbc9f0c9dde4f7ab4108d30842279e`, manifest `dfc912f358a45292d7ec75c2efd41d85516b680acbe586b2768ebb8e40d82d72`다. 첫 in-place 시도 native 1 `WRITER_TRANSITION_SOURCE_SET_DIFFERS`는 기존 동일 소스/AST transition 한계이며 이를 완화하지 않았다. [정상 사용자 제거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/remove-old-01/EXIT.json)는 11:23:39Z native 0/PASS_DATA_PRESERVED, 정상 elevated 코드 uninstall은 11:26:23Z native 0이며 [21개 기존 상태 파일 hash](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/UNINSTALL-READBACK.json)가 모두 유지됐다. [정상 재설치](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-READBACK.json)는 11:30:54Z native 0, 3,392개 파일/integrity PASS, HEALTHY_CURRENT_USER·READY/REUSED·writer fence 정상 해제를 기록한다. [GUI03](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI03-REUSE-READBACK.json)는 PID8272에서 기존 relay8524를 실제 재사용했다. 이는 앱 설치/재기동 범위이며 cold boot나 다른 버전의 exact rollback PASS가 아니다.

GUI03의 초기 empty input은 foreground/caret만으로 Tk 입력 가능 상태를 입증하지 못했다. native provider는 실제 Entry bounds를 TkChild Pane으로만 보고하고 값·readonly pattern을 제공하지 않았다. 앱의 정상 scan-label click focus 경로 뒤 [fresh-sentinel 복사 readback](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI03-NORMAL-FOCUS-READBACK.json)이 원본 PHS2 125자/hash 전부와 일치한 다음에만 Enter를 전송했다. [실제 snapshot](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CASE02-BEFORE-F3.json)은 CA가 이관한 TRANSFER-A80D17528B6D4E261F78BBB7의 정확한 2 Pcs, membership hash, full_single_transfer·entity v1과 operation lease fence2를 기록한다. [정상 F3 confirmation 후](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CASE02-AFTER-F3.json) CREATE_PACKAGE `label-package-cmd-e577ce6d5f8547433c86d0c9` / `receipt_29908880cf5f40c5a9ca09e885570dac`는 12:08:58Z COMMITTED, 로컬 ACKED attempt1·local completion committed·lease ACKED다. 실제 랩핑/물리 scanner/프린터 수용은 이 VM 합성 입력으로 주장하지 않는다. F3 서버 readback은 Web과 Main이 별도로 수용했다. F4·partial/duplicate·후속 GUI 복원·cold boot·rollback은 위 현재 상태를 따른다.

Main이 배정한 `KMTech-Label-Qualification-20260908-01`(GUID `ab171ce3-7967-4820-bbf0-9df50b00de23`, `172.22.143.81`)에서 [수용된 portable](#label-black-continuation) `d39511ab`, archive SHA-256 `727cedcb4e224bb547424f348c344d2dcfea43e04b1f01b5a439a7038681a272`로 최초 설치를 시작했다. 직전 설치는 아래 별도 동결 successor `8425fe1`이었으며 원래 archive/실패를 보존한다. 이전 VM01은 소유·사용하지 않는다. [초기 상태](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/PRESTATE.json)는 Label code/state/persistence 부재와 원래 MachineGuid/SID·일반 사용자 session 1을 기록한다. E: VHD에 한정하며 호스트 입력·대상 외부 수리·identity/key reset 없이 정상 UAC를 승인했다.

[최초 설치](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/install-01/audit.json)는 native 0으로 정본 코드 3,390개와 integrity record를 배치했지만 정상 enrollment의 HTTP 409 `producer_identity_conflict` 때문에 **RECOVERY_REQUIRED**였다. native 0을 활성화 성공으로 바꾸지 않는다. Web이 producer/install/source 세 필드의 기존 계보를 읽고 별도 일회용 관리자 승인을 발급했다. 설치된 public dry-run에서 manifest `cffe629989757c530295ff7fd97ee207da5fd12c7987052bfeba4bb5adef52fa`를 독립 계산하고 기존 CNG 키를 생성·재설정 없이 열었다. 역사 server hash와 실제 candidate hash는 별도 근거이며 이번에는 값이 같았다.

[정상 복구](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/recovery-01/registration.json)는 09:35:28Z native 0 / **ADMIN_RECOVERY_REGISTERED**, epoch 4와 server rotation·로컬 producer/logistics credential·문서 확정·guest authorization 삭제를 보고했다. 이어 [일반 사용자 onboarding](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/onboard-02-final/onboarding.json)은 09:37:47Z native 0 / **READY·REUSED**, 정본 integrity **PASS**였다. 이는 앱 onboarding 상태이며 제품 Ready가 아니다. 원래 409·관리자 승인·현재 사용자 DPAPI/CNG·공개 CA의 chain/hostname 검증 근거를 보존한다. [진행 보고](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REPORT.md)가 실제 서버 독립 readback, 기본 GUI·CA가 이관한 실제 2개 GOOD transfer, F3/해당 F4·거부·재시작·재설치·rollback의 후속 결과를 연결한다. 현재 이 후속 수용은 **진행 중/UNPROVEN**, 제품 Ready **0/6**이다.

기본 GUI 첫 실행과 정상 종료(native 0)는 확인했으나 실제 PHS2 입력은 125자 전체 수신을 입증하지 못했다. CSV의 두 SCAN_ATTEMPT는 101자 suffix와 14자 prefix로 모두 오류이며, 오류 종료 TRAY_COMPLETE를 F3 성공으로 계산하지 않는다. GUI01/02 당시 실제 CA case02의 2 GOOD transfer는 미소비였고, 이후 GUI03의 F3가 위 기록처럼 소비했다. 두 번째 정상 실행은 **FAILED / SETUP_FAILED**: 이미 상주하는 relay의 instance lease 때문에 새 child가 0으로 종료했는데 onboarding이 이를 생존 실패로 처리했다. [원래 실패](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/restart-failure-01/onboarding.json)와 [기존 owner 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RELAY-OWNER-01.json)는 PID 1988·birth·SID·session·정본 command를 보존한다. 원래 consumer가 새 child PID를 기록하지 않아 해당 PID는 NOT_CAPTURED이며 exit 0만 확인했다.

[user_relay](../../user_relay.py)의 한정 후속 구현은 기존 `_fresh_persistent_owner` 계약을 재사용한다. stop marker 부재, 이미 점유된 instance lease, fresh RUNNING·live PID·동일 app/state 경로를 정상 survival window 전후로 확인한 동일 owner만 ALIVE로 반환한다. 증거 없는 child exit 0은 계속 실패한다. [focused RED](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/relay-reuse-red.xml) 1 FAIL 뒤 [전체 relay 파일](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/relay-reuse-green.xml) **32 PASS**는 격리된 호스트 pure/mock 검증이며, 새 동결 successor의 정상 설치·재시작 native 수용은 아직 미실행이다. 원래 accepted archive/설치 실패는 보존하고 in-place patch하지 않는다. 소스 변경으로 파생되는 writer inventory digest의 기존 두 pin도 함께 갱신했다. 최초 a614058 build/PlanOnly는 native 0이지만 기존 inventory assertion 1 FAIL로 설치하지 않고 보존했다. 새 digest `07b383af5ac9c50c5a3ce0d2f492b770ec9fc19bdec899625c4cb8b1ad76b90f`의 Python/PowerShell 일치·45 writer coverage·LF/CRLF 동일성 검증은 **4 PASS**이며 후속 동결 후보의 실제 설치는 별도다. 상주 liveness와 별개로 relay transport는 `existing_terminal_blocked / existing_terminal_delta_blocked`여서 Web의 계보 readback을 요청했으며 queue/identity/서버 상태를 reset하지 않았다.

`Label_Match.py:main`은 product-host 전용 인수를 먼저 분기하고 일반 앱에서는 factory 계약 검증, 조건부 current-user onboarding, 데이터 범위 mutex, 품목 갱신, Tk 앱 순서로 진행한다. UI lane은 외부 작업과 Tk 결과 적용을 분리하며 DataManager가 CSV/current-state를, SQLite가 명령·취소·operation lease·접수·교체 intent를 보존한다. producer relay는 포장 명령과 독립된 관측 전송이다. 근거: [main/DataManager](../../Label_Match.py), [product host](../../label_match_product_host.py), [TkSerialUiLane](../../tk_serial_ui_lane.py), [C-04/05](contracts.md#c-04).

완성된 relay successor `f6353f5`의 정상 교체는 10:59:41Z **native 1**, `Healthy lifecycle task belongs to another command`로 placement 이전에 멈췄다. [실제 task](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SCHEDULED-OWNER-01.json)는 기존 creator가 만든 Limited/Interactive·정본 runtime/app cwd이며 정상 `--log-path`를 포함하지만 installer consumer는 이를 누락했다. 설치된 `d39511ab`·relay 1988·stop-marker 부재가 유지되며 [원래 실패](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-relay02/install-first/EXIT.json)를 보존했다. 후속 [정본 installer](../../INSTALL_CANONICAL_PORTABLE.ps1)는 현재 사용자 정본 DirectSync logs의 정확한 suffix를 요구하도록 교정했다. 실제 creator/읽기 전용 PowerShell admission을 연결한 [RED](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/task-admission-red.xml)는 정상 경로 1 FAIL/foreign 경로 1 PASS, [GREEN](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/task-admission-green.xml)은 두 경로와 기존 inventory 4개를 포함해 **6 PASS**다. task나 설치 PC를 외부 수정하지 않았으며 새 동결 후보의 native 재개는 별도 남는다.

첫 GUI에서는 전달한 원본 125자와 실제 `SCAN_ATTEMPT`를 비교해 101자 suffix 및 14자 prefix의 불완전 입력을 보존했다. 거부 후 `TRAY_COMPLETE` 오류 기록은 F3 포장 성공이 아니다. [읽기 전용 store 관측](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/STORE-SCHEMA-01.json)은 package command·operation lease/attempt·deferred·F4 intent가 모두 0임을 확인했다. Main의 독립 소스 검토와 CA/Inspection의 실제 입력 helper를 대조했으며 전체 entry가 확인되지 않은 후속 입력은 Enter를 보류했다. [첫 정상 종료](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/gui-01-final/EXIT.json)는 10:18:51Z native 0, 기존 relay PID 1988 유지이고 [재시작 전 보존](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/BEFORE-RESTART.json)은 SQLite backup integrity `ok`와 19개 파일의 digest만 기록했다. 암호화 credential 본문은 guest 보호 경로에 남긴다. [lifecycle 준비](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/LIFECYCLE-PREPARATION.md)의 동일 후보 재기동·제거·재설치와 실제 rollback preimage 구별은 후속 실행 결과와 별도로 유지한다.

| 구성 | 현재 근거와 적용 한계 |
| --- | --- |
| 현행 portable 후보 | [기존 builder](../../tools/build_portable_release_candidate.py)는 CPython3.12.10과 curated 의존성·앱 소스를 배치하며 [정본 installer](../../INSTALL_CANONICAL_PORTABLE.ps1)의 기본 대상은 `C:\KMTech\Apps\Label_Match\current`다. [RELEASE_GATE_CONTRACT](../../RELEASE_GATE_CONTRACT.md)의 PyInstaller/`INSTALL_THIS_PC.ps1` 문구는 별도 legacy 경로다. mutable identity·profile·ledger·relay 등록과 실제 설치 수용은 후보 build와 구분한다([후속 근거](#label-black-continuation)) |
| 일반 사용자 실행 | [current_user_onboarding](../../current_user_onboarding.py) `resolve_current_user_onboarding_paths/apply_current_user_runtime_environment`가 사용자별 데이터·설정·relay·프로필 위치를 정하고 환경에 적용한다. Machine logistics anchor와 기존 사용자 설정의 영향은 아래처럼 별도 확인 |
| relay | 현행 릴리스 계약은 HKCU `KMTech.LabelMatch.Relay`, 동일 제품의 `--label-match-user-relay`/`--label-match-direct-sync-relay`, `:18456` 계약과 로그인 사용자 실행을 사용한다. SYSTEM/AtStartup task를 현행 지원 배포로 안내하지 않는다. 과거 scheduled argv는 exit2로 거부하고 실제 이전 버전 migration/rollback 소비자만 유지함([product host](../../label_match_product_host.py), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md)) |
| source/portable 진입 | [portable/main.py](../../portable/main.py)도 같은 `Label_Match.main`을 부른다. checked-in 대체 진입점 존재와 현행 배포 후보의 실제 포함·선택은 다르다. [requirements-release](../../requirements-release.txt), factory bundle/pin과 실행 산출물 조합을 함께 식별 |
| provider·overlay | 이 조사에서는 Label 로컬 모듈과 서버 소비 경계를 확인했다. Rework의 vendor/sibling 선택 규칙을 Label에 적용하지 않는다. 설치된 Python/라이브러리·실제 로딩 파일·패키징 변경·공급자/overlay 유무는 해당 후보 근거로 확인할 항목이며, `APP_VERSION`이나 저장소 HEAD로 추정하지 않는다([LM-B04](BACKLOG.md#lm-b04)) |

<a id="configuration"></a>

<a id="phs09-source-recovery"></a>
#### iin08 실제 정상 설치·원 command 거부 / phs09 지원 복구 source

[실제 IIN08 설치·F4 attempt2](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/IIN08-NATIVE-F4-ATTEMPT02.md)는 source `1f4943750d11b25bfa036e81186eb1c00abfe423`, manifest `8bec4dc00df74c94a2aad826b9bf24e4e2b2def8ca1f99492602195aad6abdd1`에 결속된다. public uninstall19:32:01Z/native0 뒤 기존29파일·원7개 custody group과 원 실패 row가 같았고, public reinstall19:37:51.809017Z/native0와 전체 declared payload hash·READY/REUSED·HEALTHY_CURRENT_USER를 확인했다. 원 state preimage와 SQLite 일관 백업, 설치된 cancel07의3,392 declared/code파일 및 기존32개 runtime pycache 추가파일을 모두 보존했다. 최초 fixed-count observer 실패 및 inline __file__ 준비 실패·Plan audit collector 실패는 지우지 않으며 재실행한 업무로 계산하지 않는다. 관측은 declared payload·business state를 검증하고 부가 cache 수를 고정하거나 삭제하지 않는다.

GUI07은 기본 launcher로19:40:23Z 시작했다(PID1276/birth19:40:23.2711340Z, relay8284/birth19:37:44.5061980Z). 기존 drain은 안내 확인 전에 원 case04 intent를 정상 bind했고19:40:35.601553Z `PHS_REPLACEMENT_INSTRUCTION_CONFLICT`로 OPERATOR_REVIEW/attempt2를 저장했다. command_id `label-sealed-transfer-exchange:6b64a075eac51ea2accb02eaa111ca5994f96a0d63b61e7ad47ed077326f69a5` / hash `eb435f8d664ee8c84e1d816bfb1407345acc798737bae8a5999da8c5aca4c251`은 immutable이며 receipt NULL·seal/local-applyPENDING이다. 정상 자동 복구 안내 확인 한 번 외 원 입력 재스캔·수동 재시도·새 seal 확인/F3는 없었다. [readonly native0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CASE04-AFTER-IIN08-AUTO-RECOVERY.json)은 원7개 custody group·원 물리125B입력/hash/cache를 보존했고, [중앙 비교](E:/KMTech/web-integration-20260908/case04-after-iin08-conflict-central-comparison-01.json)는 target8개 구조·두 completed donor·원F1의10테이블19기록/hidden rowid 무변경과 replacement receipt0을 확인한다. 앞선18:53 pause는 이 실행 이전의 역사 상태다.

원인: 첫 donor는 원 완료 계획/세션 target2·binding2를 보존한 정상 partial completion(GOOD1/NG0/group1)이다. Web은 nominal target=actual GOOD+NG=physical members를 요구하던 guard의 교정과 completed session/receipt/group 무결성·corrupted nominal target rollback 회귀를 소유한다. frozen26 service.py의 immediate_transaction8563→group 검사8854→유일한 raise1482는 command 저장9380 전이며 connection.py의 예외 rollback 경계 안이다. 실제 없음/보존 증거와 이 precommit attestation을 paired source 검토에 결속한다.

Label은 Main `msg_444fe70b834c`의 승인에 따라 기존 attempt/drain과 상태만 사용한 [좁은 복구](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/PHS09-SOURCE-RECOVERY.md)를 구현했다. [C-03](contracts.md#c-03)의 exact prior error/stage·canonical command/hash·strict receipt404/code/committed=false·fresh command 완전 일치와 반복 terminal 거부 중지 경계를 따른다. [affected 최종79PASS/native0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/phs09-affected-final01.xml)는 exchange75개와 writer inventory4개다. causal RED1FAIL과 첫76PASS/3immutable-command fixture setup FAIL을 보존했으며 해당 fixture만 isolated read projection으로 바꿨다. [inventory](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/PHS09-INVENTORY-DELTA.json)는46개 identity/path/guard가 같고 pin `119ecdeb03ed7c56afc69815a9c3bdb85b7f57bc7994f676f6d2afb9824cf58e`다. production SQL trigger/schema/API/GUI를 추가하거나 원 durable command를 rebind하지 않는다.

현재 installed IIN08는 변경하지 않았고 phs09는 source/portable 준비 단계다. Main의 exact paired server/client 검토·현재 정상 grant/custody와 정상 shared writer 전환 뒤 원 command 복구→새 전자 seal 확인/로컬 적용→F3·GUI 업무/최종 lifecycle이 남는다. 알려진 blocked candidate에 새 coldboot/fresh-target/rollback qualification을 추가하지 않으며 Main이 최종 대상/전원을 소유한다. 호스트79PASS와 과거 cancel07 F1 수용은 새 artifact의 native 수용을 뜻하지 않으며 Ready0/6 유지.

<a id="iin08-source-recovery"></a>
#### iin08 accounting IIN 호환 · 원 pre-command intent 복구 준비 (역사 source/pause)

[소스 판정·검증·pause 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/IIN08-SOURCE-RECOVERY.md)와 [C-03](contracts.md#c-03)을 따른다. Main/Web이 수용된 서버의 donor→target accounting 이동/rebind와 immutable origin 보존을 확인해 예전 equality만 교정했다. exact legacy error와 완전히 미전송인 단계에 한해 기존 drain이 같은 intent를 fresh 검증하고 command를 durable bind한다. 새 복구 화면·SQLite 상태 reset·새 intent를 만들지 않는다.

- 검증: `iin08-affected-final01.xml` **48 PASS/native0**. 첫 equality RED1/6PASS, 첫 pin mismatch23PASS/1FAIL, recovery RED1FAIL 및 잘못된 selector native4/0실행, source-line까지 비교한 inventory 준비 실패를 보존한다. 최종 inventory46개 identity/path/guard 유지, Python/PowerShell pin `e1e00ec505a4edcb62a4d0c8d7742bdbe161b9389dd43f499ca888c83a038284`다. [XML](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/iin08-affected-final01.xml), [파생 inventory](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/IIN08-FINAL-INVENTORY-DELTA.json).
- 실제 pause: [18:53:13.4288352Z](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-CASE04-CANCEL07.json), 설치 cancel07 유지, canonical process/task/Run0·active writer fence 없음·지원 stop marker 있음. public removal native0/PASS_DATA_PRESERVED. [readonly observer native0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CASE04-AFTER-TRANSITION-PAUSE.json)는 실패 F4 full row와 원래7그룹·원125B입력/cache를 보존했다. Main `msg_514bd6ce8cf0`가 writer boundary를 독립 수용했다.
- 종료 한계: 실패 popup과 root를 정상 닫고 확인을 한 번 전달했으며 GUI4168/launcher7120은 없다. wrapper task0xC000013A/EXIT 누락으로 GUI native0은 **UNPROVEN**이다. [원 결과](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CASE04-GUI06-ABSENT-EXIT-UNPROVEN.json)를 보존하고 clean exit를 만들기 위해 재실행/강제종료하지 않는다. stale relay status 파일의 RUNNING을 live process 근거로 쓰지 않는다.
- 당시 재개 조건은 이후 Main의 exact iin08 source/recovery·shared26 및 현재 grant 수용으로 충족됐으며 위 실제 설치/attempt2를 수행했다. 다음 기록은 당시 준비 기준이다: 별도 동결·portable 준비 후 Main의 exact source/recovery 및 공유 서버 수용, 현재 정상 grant를 먼저 확인한다. corrected client 시작 시 기존 drain이 저장된 원 intent로 첫 command를 보낼 수 있으므로 수용 전에 재등록/GUI 실행하지 않는다. 이후 기존 새 전자 seal 확인·로컬 적용→F3 및 최종 lifecycle을 실제 검증한다. host PASS와 앞선 cancel07 F1 수용을 새 artifact 전체 수용으로 승계하지 않으며 Ready0/6 유지.

<a id="f1-cancel07-native"></a>
#### cancel07 정상 설치 · 원래 F1 한 번 · 정상 재시작 (실제)

현재 설치는 Main이 수용한 cancel07 `61605a4b43e965201958cc226694f1ce139e9c14`다. [정상 설치 native 0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-CANCEL07-READBACK.json)(16:36:44Z) 뒤 payload 3,391개 전체 hash·설치 3,392파일·READY/REUSED·HEALTHY_CURRENT_USER·writer fence 해제를 확인했다. 원래 F4에서 정상 F1을 한 번 실행해 16:46:59.862422Z에 exact capture만 VALIDATED/v7에서 CANCELLED/v8로 바뀌고 TC_CANCEL 한 건과 빈 화면/cache를 확인했다. 원래 lease/ACTIVE attempt/검증 이력/F3 ACKED의 전체 row hash는 같고 [Web의 중앙 10테이블/19기록 비교](E:/KMTech/web-integration-20260908/label-original-f1-central-comparison-01.json)도 reservation·target·receipt 무변경이다. 정상 GUI 종료 native 0과 재시작 뒤 16:56:12Z에도 취소 세트는 복원되지 않았다. [정확한 native 근거·실패·한계](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/NATIVE-CANCEL07-F1.md)를 따른다. case04 첫 F4(18:03:02Z)는 예전 클라이언트의 accounting IIN equality에서 OPERATOR_REVIEW/attempt1로 멈췄고 command/receipt·새 seal/F3는 없다. 이후 Main/Web은 수용된 중앙 경로가 bundle 내부 accounting binding과 immutable origin을 보존하며 공여→대상 IIN 이동/rebind를 이미 지원함을 확인했다. [iin08 교정·동일 intent 복구의 host 48 PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/IIN08-SOURCE-RECOVERY.md)는 이 equality만 제거하며 실제 업무 성공은 아직 아니다. 18:53:13Z 정상 current-user removal0 뒤 no-writers와 실패 row·원래 7개 custody group·물리 입력 보존을 확인했다. GUI/launcher는 없으나 wrapper0xC000013A/EXIT 누락으로 GUI native0은 UNPROVEN이다. 공유 서버 및 exact successor/recovery 수용 전 재실행·재등록·업무 재시도는 보류한다. 최종 artifact cold boot/fresh target/public code rollback은 별도 배정이며 Ready **0/6**을 유지한다.

- 원래 local F4의 [직전](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CUSTODY-BEFORE-F1.json)·[취소 직후](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CUSTODY-AFTER-F1.json)·[정상 재시작 뒤](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CUSTODY-AFTER-RESTART.json)를 분리한다. state/row_version/last_reason_code/updated_at만 바뀌었고 payload·seal·두 원래 lease·ACTIVE issue attempt·검증 이력·F3 명령은 전체 row/hidden rowid hash가 같다. TC_CANCEL은 한 건이며 F4 package/exchange command는 0이다. 취소는 중앙 lease 해제·포장 완료·재고 반환이 아니다.
- 14:43Z 중지 뒤 Windows servicing이 15:13:28Z guest를 재부팅했다. 첫 Plan admission의 구형 Explorer guard 거부와 [실제 boot/SID/session 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-DESKTOP-PROVENANCE.json), planned MoUsoCoreWorker/TrustedInstaller 이벤트를 보존했다. 같은 VM·SID/session·설치 code/current-set hash를 확인하고 새 desktop guard로 정상 설치를 진행했다. worker의 VM 재부팅 또는 최종 artifact coldboot PASS로 계산하지 않는다. 첫 UI observer의 CIM/native birth 정밀도 불일치도 보존한다.
- GUI05 정상 종료 0(16:52:46Z), GUI06 PID4168/birth16:53:08.795497Z의 [UIA](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-GUI06-RESTART-UIA.json)와 [빈 화면](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/cancel07-after-restart-focused.png)을 연결한다. 기본 launcher/helper console이 가린 첫 화면도 보존하고 정확한 guest GUI를 정상 focus해 다시 관찰했다. 당시 relay5708은 정상 lifecycle로 유지됐으며 아래 18:53Z 공유 pause에서 public removal로 정리됐다.
- 당시 Main `msg_2c1058cb859f`는 case04 `TRANSFER-6200A489708338C79D686F46`/품목 `AAA2287560200`/Sep8 `IIN-20260908-1A5C5E2E`를 조건부 배정했다. CA4001/CA4002를 두 실제 Sep9 COMPLETE singleton LSD001/LSD002로 교체하고 CA4003은 유지한다. 두 donor receipt와 Web의 직전 AVAILABLE3/member/version/CREATE_PACKAGE reservation 확인 뒤 원본 PHS2 → 두-member F4 → 새 seal 확인 → F3 한 번은 별도 routine GO 없이 허용된다. 조건 충족 전 신규 capture/lease 요청은 하지 않는다. 선택 원자·원본 125-byte PHS2 hash·남은 수용은 [native 보고](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/NATIVE-CANCEL07-F1.md)를 따른다. 이후 실제 backend 계약 확인으로 narrow equality 교정과 원 pre-command intent 복구 준비가 승인됐다. 현재는 정상 no-writers 경계에서 공유 전환 및 corrected-source 수용을 기다리며 업무 재시도는 하지 않는다. [지원 prior-version 준비](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/LIFECYCLE-PREPARATION.md)는 업무 상태의 정상 safe point 뒤 수행한다.

<a id="f1-cancel07-qualification"></a>
#### 2026-09-09 · cancel07 F1 correction / timing06 guest quiescent

cancel07은 Main의 F1 source audit `msg_8450ebdcf336`와 lifecycle 구분 `msg_177239e44868`을 반영한 후속 후보다. [frozen source·provider·portable·검토 packet](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/successor-cancel07/REVIEW.md), [계약](contracts.md#f1-capture-cancellation), [전체 보고](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REPORT.md)에 근거를 연결한다. 이 source 동결 시점(15:59Z)의 실제 설치는 timing06이고, 아래 후속 native 단락에서 cancel07 정상 설치/F1/재시작을 별도로 기록한다.

- Host 검증: [영향 회귀 370 PASS, native 0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/cancel07-affected-final02.xml) — F1 취소 순서·exact identity/CAS·durable audit 실패 rollback·확인된 legacy lease와 UNKNOWN 구분·다음 source FIFO·stale cache/crash·늦은 materializer·installed DB trigger 갱신·F3/F4/F5/in-flight 보존 및 기존 deferred/lane/package 회귀와 writer static 4사례. 46 writer 중 원래 45 identity/path/guard를 모두 보존하고 `gui_deferred_capture_cancel`을 새로 등록했다. 이 테스트는 실제 server/GUI/hardware 수용이 아니다.
- 만료된 원래 F4 custody 조건도 확인하려고 lease fixture의 보존 만료시각만 과거로 설정한 뒤 [최종 F1 22 PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/cancel07-expired-lease-final.xml)를 추가 확인했다. production source는 370 PASS와 동일하며 기존 lease fixture의 기본값·다른 회귀 입력은 유지한다.
- 실패 보존: 최초 import 오류 native 2, 초기 red02 11 FAIL(당시 rw13 해석을 포함하므로 모두 승인된 causal 요구로 계산하지 않음), green01 15 PASS, boundaries01 358 PASS/1 schema-idempotence FAIL, green02·03의 legacy fixture/evidence 연결 실패, green04 18 PASS, final01의 잘못된 static selector native 4/0 실행을 보존한다. 기존 요청 hash가 raw issue body를 가리킨다는 계약에 맞춰 교정했고 final02는 전부 통과했다.
- 실제 재개: Main의 exact frozen successor 수용 뒤 정상 public 설치 경로로 교체하고, 보존된 원래 F4에서 정상 F1을 실행하여 exact capture만 CANCELLED가 되는지, 원래 lease/attempt/서버 reservation/receipt가 유지되는지 확인한다. Web이 실제 donor receipt로 준비하고 Main이 배정한 새 정상 source에서 FIFO 진행·F4/새 seal·F3/그 뒤 입력 및 필요한 duplicate/partial 거부·GUI 복원을 검증한다. 원래 source에 ADMIN 우회·강제 retirement·새 credential을 적용하지 않는다.
- 최종 lifecycle: timing06의 정상 remove/uninstall/reinstall 및 provider/state preservation 중 변하지 않은 근거를 재사용할 수 있지만 cancel07 실제 cold boot/fresh target/exact public code rollback으로 이름을 바꾸지 않는다. Main이 소스·업무 안정 후 최종 artifact 대상으로 배정한다. 조건부 F5, inactive warehouse compatibility, 실물 printer/factory deployment는 이 선택된 native software 범위의 새 blocker가 아니다. 제품 Ready 0/6 유지.


## 설정 위치와 우선순위

<a id="enrollment-ip-policy"></a>
### 등록·관리자 복구의 IP 기준 승인

[등록 도구](../../tools/register_label_match_worker_pc.py)의 정상 등록과 `--admin-recovery-secret-file` 복구는 네트워크 승인 토큰 없이 서버 허용 IP 정책을 사용할 수 있다. 이 PC의 접속 IP를 서버 허용 목록에 등록한 뒤 토큰 인수·파일·`PRODUCER_SELF_ENROLL_TOKEN` 환경 값을 비워 실행한다(환경 값 사용 제외: `--enrollment-token-env ""`). 빈 값은 `X-Producer-Enrollment-Token` 헤더나 등록 토큰 JSON 필드로 전송하지 않는다. 서버가 `enrollment_unauthorized`로 거부하면 HTTP 상태·오류 코드를 보존하고 CLI 출력과 등록 보고서 `blocked_reason`에 `서버 허용 IP 목록에 이 PC 를 등록하거나 토큰을 입력하세요`를 표시한다. 토큰을 제공하면 기존 인증을 유지하며 잘못된 토큰으로 거부된 요청을 자동으로 토큰 없이 재전송하지 않는다. 관리자 복구에는 별도의 일회용 승인 파일(`recovery_token`), 명시 identity·서버 활성 manifest hash·TLS CA와 현재 사용자 소유 키가 계속 필요하다. IP 허용은 이 복구 검증이나 등록 후 업무용 기계 자격증명을 대체하지 않는다.

**코드의 fallback, onboarding이 적용한 위치, 실제 저장 위치를 구분한다.** 비밀 값은 수집·문서화하지 않고 선택 경로·비밀 없는 identity·안정된 오류 코드만 근거에 연결한다.

| 항목 | 현행 선택 규칙 | 근거·주의 |
| --- | --- | --- |
| 사용자 설정 | `LABEL_MATCH_SETTINGS_PATH` → `%LOCALAPPDATA%\KMTech\Label_Match\config\app_settings.json`. 파일이 없으면 packaged `config/app_settings.json`을 template로 복사 | [앱 `_default_label_match_settings_path/_setup_paths`](../../Label_Match.py). CODEX의 packaged 경로를 일반 사용자 쓰기 위치로 해석하지 않음 |
| onboarding 데이터 | GUI·guard·relay와 같은 `resolve_data_scope`로 settings custom → env → 등록 수명주기·onboarding 상태에서는 항상 LOCALAPPDATA | 최초 등록 진입부터 ProgramData 업무 파일을 무시한다. ProgramData fallback은 등록 없는 standalone 실행 전용이다. 기존 onboarding ledger만 다른 루트에 있으면 `ledger_path`를 유지하고 `SPLIT_PRESERVED`로 진단한다 |
| 앱의 실제 데이터·GUI mutex | nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR`; 둘 다 없으면 아래 기존 설치 감지 규칙 | [공통 resolver](../../label_match_single_instance.py)를 앱·onboarding·relay가 소비한다. null/빈 설정 fallback과 settings template 복사 전 선택도 같은 규칙이며 두 process의 native writer callback 제외·해제를 headless 검증했다 |
| producer 상태 | onboarding은 `LABEL_MATCH_DIRECT_SYNC_ROOT` 또는 호환 `LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT` → `%LOCALAPPDATA%\KMTech\DirectSync\label_match`를 선택 | [onboarding](../../current_user_onboarding.py). 앱의 오래된 ProgramData bootstrap 상수만 보고 현재 relay 상태 위치를 정하지 않음 |
| relay의 CSV 발견 위치 | 명시 `--scan-source-dir` → 공통 business root | 명시 scan override와 producer queue/spool·identity·profile 위치·producer/key/endpoint binding은 유지한다. 기존 미전송 payload를 이동·재작성·종결하지 않는다 |
| logistics Machine anchor | 일반 Windows 경로에서 Machine `KM_LOGISTICS_PROFILE_PATH`/`KM_LOGISTICS_REQUIRED` 중 하나라도 있으면 두 process 값보다 Machine 쌍을 우선 | [logistics_runtime_profile._runtime_environment](../../logistics_runtime_profile.py), [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md). 테스트 전용 엄격한 TEST1 분기는 일반 운영 override가 아님 |
| logistics 프로필 위치 | onboarding은 명시 경로 또는 `%LOCALAPPDATA%\KMTech\Logistics\profiles\Label_Match\runtime-profile.json`을 적용. 일반 resolver 기본은 `%ProgramData%\KMTech\Logistics\profiles\Label_Match\runtime-profile.json`; 앱별 파일이 없고 옛 공통 파일이 있으면 `...\Logistics\runtime-profile.json` 호환 선택 | [selected_logistics_runtime_profile_path](../../logistics_runtime_profile.py). explicit path·Machine anchor·legacy 공통 경로 재선택을 함께 대조. 설치된 최종 선택은 미확인 |
| 권한·비밀 범위 | 현행 current-user 계약은 사용자 DPAPI와 AUTHORITATIVE profile을 사용. 기존 Machine profile의 machine-scope DPAPI/ACL 계약은 별도 구성 | [onboarding](../../current_user_onboarding.py), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md). 프로필 JSON의 secret reference와 실제 비밀을 혼동하지 않음 |
| 필수 물류 | required 모드의 프로필·HTTPS·authority·인증 실패는 중앙 업무를 차단한다. 비필수 legacy 환경 fallback은 별도 호환 분기 | [package_client_from_env](../../package_logistics.py), [load_logistics_runtime_profile](../../logistics_runtime_profile.py). 필수 모드를 임의로 해제해 완료하지 않음 |
| 업데이트 | `LABEL_MATCH_UPDATE_PROVIDER` → packaged `update_settings.provider` → 코드 기본. provider 미지정 기본은 `off`; `github/private_manifest` 후보·서명 조회를 보존하고 도달 불가 앱 내부 apply/batch/prompt는 제거했다 | [앱 update 함수](../../Label_Match.py). 명시 channel/manifest 설정도 별도 선택; 운영 값·원격 feed를 조회하지 않음. 기존 CODEX와 차이는 [LM-B09](BACKLOG.md#lm-b09) |

활성 프로필의 scope/epoch/plane·device/source identity, 서버 capability, 실제 endpoint 및 `PROJECTION_API_READ_ENABLED`를 후보별로 대조해야 한다. 토큰의 존재만으로 호출 권한이나 소비 화면 반영을 입증하지 않는다([C-00](contracts.md#c-00), [C-05](contracts.md#c-05)).

<a id="storage-root-residual"></a>
**LM-C03 공통 resolver와 기존 위치 보존:** settings custom → env를 먼저 사용한다. 둘 다 없으면 등록 수명주기(진입·진행 중·등록 직후 환경 적용·relay 시작·재시작)에서는 항상 LOCALAPPDATA(A)를 선택한다. 기존 current-user onboarding 상태(identity·manifest·등록/onboarding report 파일)도 A를 선택한다. 두 경우 모두 ProgramData(P)의 과거 업무 파일을 검사하거나 선택하지 않으므로 최초 legacy P 등록에서도 모든 소비자·첫 ledger가 A로 일치한다. P fallback은 onboarding 문맥·상태가 전혀 없는 순수 standalone 실행(등록 없이 직접 GUI/guard)에만 적용한다. 필요한 상태 감지는 매번 실제 파일 존재를 읽으며 부재 캐시를 쓰지 않고, standalone의 구형 디렉터리 읽기 실패는 부재로 바꾸지 않는다.

구형 standalone의 ProgramData 데이터는 자동 이동하거나 `custom_save_path`에 자동 기록하지 않는다. current-user 등록으로 전환한 뒤에도 ProgramData를 계속 쓰려면 운영자가 `custom_save_path`를 수동 설정해야 한다. 최초 legacy 등록 전이는 기준 `e45ec3e`의 A 동작으로 검증하며 데이터 이관은 제공하지 않는다.

기존 custom C와 onboarding ledger A가 갈린 설치에서는 업무 CSV/DB·GUI mutex·relay는 C, onboarding ledger는 기존 A를 계속 쓴다. 환경을 적용하거나 다음 실행의 env가 달라져도 기존 onboarding report의 ledger 경로를 재사용한다. custom/env가 명시적으로 ProgramData를 선택한 갈린 설치도 같은 호환 경로다. identity·profile·설정·등록 manifest·queue/spool의 위치/내용을 옮기거나 identity를 재등록하지 않는다. `WARNING storage_root_selected`는 선택 규칙·경로 한 줄, `WARNING storage_root_split_preserved`는 두 위치를 같은 Python startup 로그 채널에 남긴다. 기본 effective level WARNING에서도 기록되며 전역 로그 수준은 변경하지 않는다. onboarding report의 `storage_root_compatibility`에도 `SPLIT_PRESERVED`를 남긴다.

현재 headless 수용: [Wave 3 등록 수명주기 수정 RESULT](D:/KMTech/program-improvement-20260912/work/Label_Match/w3fix2/RESULT.md). 검토의 일곱 fixture 입력·원 bytes를 유지하며, 원본 legacy 등록 재현을 그대로 회귀에 편입해 `e45ec3e`와 대조한다. 최초 등록의 env·guard·relay·READY report·ledger·재시작이 모두 A이며 실제 GUI resolver도 A를 선택하고 settings·P CSV의 SHA는 불변이다. A의 기존 완료 CSV/pending ledger/onboarding report와 P의 과거 CSV가 함께 있어도 모든 소비자·ledger가 A다. custom/env 및 두 process callback 제외/소유권 해제, split fixture의 기존 데이터·identity·미전송 bytes 보존도 검증한다. guard 이전 onboarding ledger 초기화 순서는 유지한다. 실행 중 설정 변경의 race·모든 filesystem alias·실제 Tk/설치/서버 enqueue/ACK는 별도 수용이며 데이터 migration은 제공하지 않는다.

A=LOCALAPPDATA, P=ProgramData, E=SAVE_DIR env, C=settings custom. 기존 custom/env 통일의 의도된 변화는 다음과 같다.

| 같은 입력 | `e45ec3e` | 현재 |
| --- | --- | --- |
| custom C + env A/B, 신규 onboarding/ledger | E | C |
| custom C + env A/B, GUI/guard/relay | C | C |
| onboarding 상태 + A/P 업무 파일, 모든 소비자·ledger | A | A |
| env만 지정, 모든 소비자·ledger | E | E |
| onboarding 없는 standalone, 직접 GUI/guard | P | P |
| P 업무 파일 + 최초 등록, env·guard·relay·READY report·ledger·재시작 | A | A |

<a id="relay-custom-root-evidence"></a>
### 2026-09-08 · relay custom 경로의 재시작 후 발견

[변경·실행 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/CHANGE.md)는 clean parent `394c21fd535ae565d862a77aeeb680f463496cdb`와 후속 working source를 구분한다. 수정 전 [Baseline02](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Baseline02.xml)는 **custom 4 FAIL / 나머지 10 PASS**다. 최종 [Final02](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Final02.xml)는 **26 PASS, failure/error/skip 0**: relay 모듈 23개(신규 14개 포함)와 기존 writer inventory 검사 3개다. 별도 cohort와 합산하지 않는다.

Windows 호스트 CPython **3.12.10**, pytest **9.0.2**에서 실제 두 relay 진입점·command builder·CSV scanner를 사용했다. 상주/예약 × custom 기본·env 충돌/명시 scan override/empty/null/missing/invalid JSON을 확인하며, 각 사례는 두 번의 진입 사이에 CSV를 추가하고 process 환경을 되돌려 다음 발견을 대조한다. child process/transport와 profile loader·relay lease는 대체물이다. 같은 queue 경로와 기존 spool 파일·settings bytes 보존을 확인했지만 실제 queue enqueue/ACK나 OS 재부팅의 증거는 아니다. TEMP/TMP·pytest·상태·로그는 지정 E 작업 루트에 격리하고 bytecode/cache와 외부 pytest plugin 자동 로딩을 비활성화했다. 최초 Baseline의 14 FAIL은 긴 E 경로의 Win32 파일 열기 실패이며 원본을 보존했다; 이후 시험은 같은 E 루트의 extended-length 경로를 사용했다.

기존 inventory pin은 수정 전부터 실제 clean source와 달랐다(`a3852e…` vs `f3f9bc…`). 같은 [기존 scanner](../../writer_sink_inventory.py)로 후속 source를 계산해 [Python](../../writer_session_fence.py)과 [PowerShell](../../tools/label_writer_fence.ps1)의 pin을 `852dd0a0a0cb8377bf4e7ff5bd261806ab878100848bcb0cb0df0ad59c299e12`로 맞췄다. [대조 원본](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Writer-Inventory.json)은 45개 source 식별자·위치·qualified name·guard 종류가 동일함을 확인한다. pin/coverage 검사 3개와 PS5 구문 오류 0은 **PROVEN**이며 설치 시 실제 fence 동작·qualification을 입증하지 않는다.

원래 `label-clean-full-prepare`의 Stage/MainStage **9,445파일·issues 0** 수용은 `394c21f`의 역사 근거로 보존한다. 이 후속 소스에는 상속하지 않는다. [추가 source 입력과 다음 단계](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/CHANGE.md)를 수용하고 successor source/host bindings를 새로 결속한 뒤 Main이 CA 정리 후 VM01을 배정해야 한다. Setup/FULL·native F3/F4·설치·실제 backend는 이번에 실행하지 않았고 Ready **0/6**을 유지한다.

<a id="relay-successor-preparation"></a>
### relay 독립 소스 종결·후속 FULL 준비

[독립 검토·소스/후속 입력 결속 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-relay-closure-review/REVIEW-PREPARE.md)가 이 작업의 exact commit, 원래 394c21f와 새 후보의 차이, 새 host/guest bindings 및 정적 검증 범위를 기록한다. 최종 26 PASS를 만든 product/test bytes를 보존하고 45개 writer identity·guard 종류와 두 pin의 일치를 독립 대조했다. 이번 검토에서 앱·pytest·VM을 실행하지 않았으며 기존 Baseline/Baseline02 실패도 보존한다. 기존 `label-clean-full-prepare`와 `label-full-host-reader-fix`의 verified entry 경로를 통해 새 후보별 Stage → 별도 MainStage → Setup → ExportSetup → MainSetup → Full → ExportFull → MainFull을 분리한다.

[successor02 제어 변경 독립 검토](E:/KMTech/label-rp-0908/REVIEW.md)는 2026-09-08T01:05:06Z 실제 호스트 readback에서 기존 entry 8개·host 33개·원본 40개 pin, E 재구성의 raw 405개와 Git index blob/mode 405개·HEAD/tree·clean status를 대조했다. raw 복원·hash 검사 뒤 실제 `git add --update --` 한 단계를 추가하며, 기존 source check의 staged/unstaged 거부를 유지한다. 두 유한 log export와 PS5/PS7 각 11개 파일의 기존 parse 근거도 현재 bytes에 일치한다. 최초 clean-status 및 stat-only refresh **FAILED**는 보존하며 product test·guest body는 재실행하지 않았다. 후보는 `8b55bd30284617703c65a99526ad06199e592e83`, packet SHA256 `195d707148ef524f91ec3f8246fb3aacfdf28b9a84555bb4cf747920f9738fc8`로 유지한다. 이번 운영·백로그 변경은 문서 상태만 갱신하며 동결 packet의 입력으로 재결속하지 않는다.

**이력 · 당시 QUEUED — 후속 VM 배정을 위한 제어 검토 완료.** 현행 인계에서 VM01은 Inspection 소유이고 Defect가 대기하므로 Label의 guest 조회·실행 권한은 없다. Main의 packet 수용·독점 VM01 배정과 기존 VM/desktop/provider live admission을 통과한 뒤에만 보고서의 literal Stage → 별도 MainStage → Setup 순서를 시작한다. 새 Setup/FULL·native 로그인/재시작·실제 queue/producer receipt·F3/F4·설치/cold boot/재설치/제거/rollback·통합 E2E는 **NOT TESTED / UNPROVEN**이고 Ready **0/6**을 유지한다.

<a id="relay-vm01-continuation"></a>
### 2026-09-08 · VM01 개발 소스 FULL 계속 검증

Main은 2026-09-08T06:03:27Z에 Defect가 반환한 VM01을 Label에 독점 배정했다. 최초 인계의 VM3 계획을 대체하며 실제 대상은 VM01 `8d9fd433-02bb-4609-93fa-f992ef8a1838`이다. [현재 보고](E:/KMTech/label-vm3-20260908/vm01-continuation/REPORT.md)와 [실제 admission](E:/KMTech/label-vm3-20260908/vm01-continuation/ADMISSION.json)에 기존 사용자/SID, session1, 실제 Explorer4184 birth, unlocked 상태와 새 lane/task 부재를 기록했다. 호스트 입력·생산/ERPnext 접근·VM/network/security 변경은 하지 않았다. 소스 `8b55bd3`와 기존 26 PASS, 과거 소스 재구성 실패·각 한정 수용은 그대로 보존한다.

첫 실제 Stage는 9,447개 기존 snapshot 파일을 관측했지만 MainStage는 **FAILED**다. [진단](E:/KMTech/label-vm3-20260908/vm01-continuation/PROVIDER-MEMBERSHIP-DIAGNOSIS.json)은 모든 기존 pin bytes가 같고 Python 표준 라이브러리에 `Lib/encodings/__pycache__/utf_8_sig.cpython-312.pyc` 하나만 추가됐음을 확인했다. 원래 Stage/MainStage·native 기록과 cache 파일을 지우거나 PASS로 바꾸지 않는다.

[제어 교정](E:/KMTech/label-vm3-20260908/vm01-continuation/CACHE-ISOLATION-CHANGE.json)은 모든 직접 Python 자식에 명시 `-X pycache_prefix`를 주고 pytest에는 같은 lane cache 환경과 실제 prefix assertion을 적용한다. 직접 제어 interpreter의 cache 경로를 격리하면서 provider cache pin 984개와 cache membership gate를 측정에서 제외하고, membership 조회가 같은 파일을 다시 hash하던 중복도 제거했다. 원래 manifest는 보존하며 소스·패키지·native library·startup hook·비cache membership·일반 사용자·isolation·업무/결과 assertion은 유지한다. 새 guest lane `label-full-relay-g7-02`에서 변경한 제어만 재결속하고 기존 Python/Label22/Git/PowerShell 공급자를 재사용한다. PowerShell 11개·Python 5개 구문 확인과 호스트 CPython3.12.10의 실제 cache 경로 선택 확인은 **PROVEN**이며 [교정 후 MainStage](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/MAIN-Stage.json)는 **PROVEN / evidenceComplete=true**, 8,463개 측정 입력·문제 0이며 Setup을 한 번 시작했다. Setup/FULL의 실제 종료 결과는 아래에 기록한다.

**Cache 근거 한계:** Main의 동결 소스 검토는 9개 실제 `-I` 자식 launch와 installer PowerShell 자식·공유 stdlib fixture venv를 확인했다. 이 자식들은 `PYTHONPYCACHEPREFIX`를 무시할 수 있고 `-B`는 cache 읽기를 막지 않는다. 따라서 직접 제어 Python/pytest의 prefix 확인만 **PROVEN**이며 제외한 984개 cache가 Full 전체에서 미소비·불변이라는 주장은 **UNPROVEN**이다. 이미 시작한 Full과 완료한 Setup은 보존하며 실제 test outcome과 runtime cache 한계를 분리한다. 이를 위해 제품 argv를 넓게 변경하거나 provider/Setup을 다시 구성하지 않는다.

[Setup 실제 결과](E:/KMTech/label-vm3-20260908/vm01-continuation/SETUP-SETTLED.json)는 inner/outer/task 자연 종료 0, [Git source readback](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/export-Setup/evidence/Setup/git-source.json)은 exact HEAD/tree·405파일·clean status, [dependency readback](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/export-Setup/evidence/Setup/dependencies.json)은 25 versions/26 imports를 기록했다. 기존 Label22를 재사용하고 부족한 세 package만 새 lane에 추가했다. 최초 ExportSetup FAILED는 Windows PowerShell5의 JSON 배열을 한 번 더 감싸 28개 path가 한 항목으로 전달된 제어 오류다. [세 루프 교정](E:/KMTech/label-vm3-20260908/vm01-continuation/EXPORT-ARRAY-FIX.json) 뒤 [동일 Setup의 28파일 export](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/EXPORT-Setup.json)는 stable·missing/overflow 0이다. 실패 native 기록과 기존 host 제어 원본은 보존하며 guest·소스·provider를 다시 구성하지 않았다. [MainSetup](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/MAIN-Setup.json)은 **PROVEN / evidenceComplete=true**, 9,062개 입력·native 8단계 종료 0을 대조했다. 원래 Full은 자연 종료했으며 아래의 FAILED 결과를 보존한다.

[Full 원본·Main 대조](E:/KMTech/label-vm3-20260908/vm01-continuation/packet-cache-isolation/MAIN-Full.json)는 **FAILED / evidenceComplete=false**다. pytest 2,345개 수집, JUnit 2,347개 case(수집 단계 외부 서버 skip 2개 포함), **2,259 PASS / 72 FAIL / 16 SKIP**, pytest 종료 1·inner/outer/task 종료 2다. 27개 export 파일은 stable·missing/overflow 0이고 observer 9,500 event와 native 두 stream EOF를 보존했다. [보존 차이](E:/KMTech/label-vm3-20260908/vm01-continuation/FULL-PRESERVATION-DIAGNOSIS.json)는 중첩 pytest의 `.pytest_cache` 출력 5개 추가뿐이며 기존 9,062개 측정 파일의 변경·삭제는 없다. 이를 pytest 실패 또는 제외한 runtime cache의 불변 증거와 혼동하지 않는다.

[후속 593개 기존 검사](E:/KMTech/label-vm3-20260908/vm01-continuation/affected-01/RESULT-REVIEW.json)는 새 `label-fixes-g7-01` 소스 사본·11개 명시 test patch에서 **571 PASS / 22 FAIL / SKIP 0**, 자연 종료 1이다. 수집 node/order·JUnit 593개가 일치하고 소스 405개 bytes는 실행 전후 같으며 19파일 export가 stable, observer와 native 두 EOF가 완전하다. PATHEXT 누락, OEM PowerShell stderr를 UTF-8로 해독하던 fixture, 상속 legacy 저장소/profile, 고정 소스 줄 번호, pycache 위치, venv 실행 PID·FIFO tie 가정과 누락 `settings_path`를 좁게 교정했다. 남은 실패는 healthy lifecycle 15개, writer transition 6개, Display2 waiting 행 수 1개(실제 5/기대 1)다. fixture의 격리 자식이 `requests`를 찾지 못하는 경로와 관련 rollback/receipt 실패는 [원본](E:/KMTech/label-vm3-20260908/vm01-continuation/affected-01/export/out/stdout.txt)에 남아 있으며 해소됐다고 주장하지 않는다.

**계정 인계 당시 미검증 소스:** Main 검토에 따라 실제 복구의 개발 호스트 E: 전용 조건을 제거한 [로컬 계약 변경](contracts.md#recovery-evidence-paths), 해당 initializer 도구, 기존 portable fixture의 curated dependency 복사 교정은 593개 실행에 포함되지 않았다. writer inventory pin은 기존 값과 같은 것을 정적으로 확인했으며 기존 PASS를 이 변경에 상속하지 않는다. Main의 2026-09-08 계정 전환 지시로 추가 cohort/build/install을 시작하지 않고 [계정 인계](E:/KMTech/label-vm3-20260908/ACCOUNT-HANDOFF.md)에 미검증 수정과 다음 작업을 남겼다. [VM01 반환](E:/KMTech/label-vm3-20260908/VM01-RETURN.json)은 07:33:14Z에 소유 task 세 개 Ready(0/2/1), active owned process 0, 같은 Explorer/session을 기록했다. replay 세션은 Fast를 변경하지 않았다. 후속 실행은 [새 blackdwarfian 범위](#label-black-continuation)에 분리하며 제품 Ready는 **0/6**이다.

개발 소스 시험과 설치본/실제 서버/재시작·제거·재설치·rollback·장비·통합 E2E는 별도다. Ready **0/6**, LM-B04/05/06/07/10/11의 해당 잔여 범위는 유지한다. 공통 INT-09/Q08·VM 배정·중앙 준비도 갱신은 Main이 소유한다.

<a id="label-black-continuation"></a>
### 2026-09-08 · blackdwarfian 복구·portable 준비

[후속 보고](E:/KMTech/label-black-continuation-20260908/REPORT.md)는 새 blackdwarfian managed home `70573d48-149e-487f-90b3-e4d5dbbfe4af`, 실제 Fast off와 설정 tier default를 확인하고 기존 17개 pending 경로의 해시 일치·별도 백업을 기록한다. [VM01 admission](E:/KMTech/label-black-continuation-20260908/ADMISSION.json)은 같은 사용자/SID·unlocked session1·Explorer4184 birth, 이전 task Ready 0/2/1과 active owned process 0을 확인했다. 호스트 산출물은 새 E 작업 루트, guest 소스·산출물은 지정 E-backed VHD에만 생성하며 기존 provider/Setup·전체 Full/593을 반복하지 않는다.

복구의 E: 전용 조건을 제거하면서 파일 identity·hash/SQLite integrity·정상 인증/CAS·forward-only/모호한 응답 거부를 보존했다([로컬 계약](contracts.md#recovery-evidence-paths)). Display2 waiting은 격리 환경의 `KM_LOGISTICS_REQUIRED=0`·client 부재에도 authoritative 1행을 기대하던 fixture 오류였다. 시험에 실제 표시 상태를 명시해 1행 PHS2·기존 legacy 5행·exact membership·실제 Tk 배치/잘림/DPI oracle을 유지했다. 실제 모니터 metadata만 가상이며 물리 DISPLAY2/스캐너 수용은 아니다. 파일 경로로 실행되는 initializer의 portable 누락은 기존 builder external tool 목록에서 교정했다. writer 45개와 inventory pin `852dd0a0a0cb8377bf4e7ff5bd261806ab878100848bcb0cb0df0ad59c299e12`는 동일하다.

| 실행·입력 | 실제 결과·범위 |
| --- | --- |
| [focused-01](E:/KMTech/label-black-continuation-20260908/focused-01/RESULT-REVIEW.json), 원래 HEAD+현재 20개 patch/405파일 | **FAILED**, 69 collected: 32 PASS / 37 setup ERROR / SKIP0, 30.66초·자연 종료1. recovery21, Display2 1, portable closure4 및 fixture 비소비6은 PASS. 37개는 chardet5.2.0 부재. 소스405 불변, stable export19·observer complete·dual EOF |
| [focused-02](E:/KMTech/label-black-continuation-20260908/focused-02/RESULT-REVIEW.json), 같은 소스/37개 setup-error 선택 | **FAILED**, 37 setup ERROR, 5.79초·자연 종료1. 추가 wheel의 실제 metadata는 5.2.0이나 pip staging의 관리자 소유 ACL 때문에 제한 사용자에게 Version이 보이지 않음. 소스·19개 stable export 보존 |
| [provider ACL 진단](E:/KMTech/label-black-continuation-20260908/PROVIDER-ACL-DIAGNOSIS.json) | 새 provider에 1,342파일의 동일 bytes를 일반 상속 권한으로 복사. 기존 측정 package8과 보존 chardet wheel1을 재사용하고 실패 provider/ACL도 보존. 기존 환경·보안 설정 변경 없음 |
| [focused-03](E:/KMTech/label-black-continuation-20260908/focused-03/RESULT-REVIEW.json), 같은 소스/37개 | **FAILED**, 36 PASS/1 FAIL, 1,340.00초·자연 종료1. writer 전부 PASS; fresh identity conflict의 격리 child가 pytest 의존 test module 전체를 import해 실패. 소스405 불변·stable export19·observer complete·dual EOF |
| [focused-04](E:/KMTech/label-black-continuation-20260908/focused-04/RESULT-REVIEW.json), 변경 fixture 포함406파일 | **PROVEN**, fresh install2와 shared fixture 소비자12, 합계14 PASS/42 phase PASS, 88.40초·자연 종료0. 두 fresh 사례는 실제 registration report의 409/producer_identity_conflict와 401/enrollment_token_invalid까지 확인해 임의 child 오류의 rollback을 성공으로 인정하지 않음. 소스406 불변·stable export19·observer complete·dual EOF |
| [headless capture 검사](E:/KMTech/label-black-continuation-20260908/host-headless/junit.xml) | Windows 호스트 CPython3.12.10, 5 PASS/2.22초. 불필요한 source line metadata 제거의 세 helper 소비자·exact product 문자열/렌더링 assertion 유지; 실제 GUI 실행 없음 |

fresh native adapter는 이제 [순수 공유 key fixture](../../tests/_possession_fixture.py)만 읽는다. [registration 검사](../../tests/test_register_label_match_worker_pc.py)도 같은 값을 import하며 기존 test module 전체의 runpy 실행과 중복 descriptor 정의를 제거했다. pytest를 제품 dependency에 추가하지 않았다. [사례별 연결](E:/KMTech/label-black-continuation-20260908/AFFECTED-CASE-MAP.json)은 최초 선택69개의 마지막 PASS를 focused01/03/04별 실제 입력에 연결한다. 이를 단일 전체 재실행이나 설치 수용 PASS로 합산하지 않는다.

portable는 [기존 builder](../../tools/build_portable_release_candidate.py)의 clean frozen source·CPython3.12.10·동일 환경의 curated9개·기존 정상 consumer public key로 **build PROVEN**이다. [실행 결과](E:/KMTech/label-black-continuation-20260908/PORTABLE-EXECUTION.json)는 guest 전용 snapshot commit `d39511abc45c35abf44573adff5004de326a44dd`, tree `5a659a550124cafef8fc2a8373d890f260a67733`, 3,390파일과 initializer helper 포함을 기록한다. 앱/test 실행 bytes는 focused04의406파일과 같으며 빌드 직전 상태 문서만 갱신했다. host main/기존17경로를 commit/reset하지 않았다.

[INSTALL_CANONICAL_PORTABLE.ps1](../../INSTALL_CANONICAL_PORTABLE.ps1) `-SourceRoot <candidate> -PlanOnly`도 자연 종료0, `install_status=PLAN_ONLY`, `registry_changed=false`를 반환했다. 기본 대상 `C:\KMTech\Apps\Label_Match\current`는 전후 모두 부재이며 signature validation·canonical layout을 우회하지 않았다. `INSTALL_THIS_PC.ps1`은 별도 legacy 배치 진입이다. 최초 archive export는 PS5가 stdout 문자열의 provider 속성까지 JSON에 저장해 값 비교에 실패했다. [진단](E:/KMTech/label-black-continuation-20260908/PORTABLE-EXPORT-DIAGNOSIS.json)은 원래 JSON·native log를 보존하고 값만 추출한 compact 결과와 원본 stdout을 대조하며 build/PlanOnly를 재실행하지 않았다. 후보 archive·source bundle·manifest·전송 해시는 [보고](E:/KMTech/label-black-continuation-20260908/REPORT.md)에 연결한다.

새 focused 제어에서는 소비자가 없는 기존 preservation/overlay 함수 5개만 제거했고 source·desktop·업무 assertion과 원본 제어는 보존했다. 제외 cache의 전역 미소비/불변은 **UNPROVEN**이다. 현재 VM01은 개발 fixture 환경으로 fresh install 증거가 아니며 실제 설치·업무·서버·장비·재시작/제거/rollback·통합 및 Ready **0/6**은 별도다. 중앙 요약·새 설치 대상 배정은 Main이 소유한다.

[후속 VM01 반환](E:/KMTech/label-black-continuation-20260908/VM01-RETURN.json)은 08:49:15Z에 이전3개와 새4개 task 모두 Ready(0/2/1/1/1/1/0), active owned process0, 원래 unlocked session/Explorer birth와 소스406 불변, canonical install 부재를 확인했다. 모든 실패 lane·provider·산출물을 보존했고 [후보 전송 검증](E:/KMTech/label-black-continuation-20260908/PORTABLE-REVIEW.json)의 archive/source bundle을 Main에 전달했다.

<a id="devices"></a>
## 스캔·포커스·사운드·출력

| 관측 대상 | 현행 동작 | 아직 확인할 장비 조건 |
| --- | --- | --- |
| 제출·키보드 | 작업 입력은 Tk `<Return>` → `_handle_scan_enter/process_input`, 문자열 앞뒤 whitespace 제거. busy이면 입력칸을 비우지 않음. F1–F4는 버튼·키보드 공통 gate를 사용 | 스캐너 CR/LF/CRLF/Tab suffix, keyboard layout/IME, 연속 입력 간격, 중복 Enter, modal/오류 안내 후 포커스. Enter가 안내 확인으로 소비되는 경우 실제 다음 입력 확인 |
| QR/문자 | compact PHS2의 여섯 필드·순서·HSH를 검사. 첫 입력에는 조건부 encoded 문자열 UTF-8 decode 호환도 존재하지만 decode 자체는 중앙 유효성 승인이 아님 | scanner model·펌웨어·문자 인코딩·제어문자 보존·길이·QR 표시 배율. Tab·직렬 장비 지원을 이번 검색으로 보장하지 않음 |
| 소리 | `LABEL_MATCH_AUDIO_ENABLED`로 비활성 가능하며 automated-test mode는 무음. 성공음 호출은 F3 durable 경계 후에 있음 | 실제 소리 장치·볼륨·환경 소음·고장 시 시각 피드백. 소리만으로 중앙 ACK를 판정하지 않음 |
| F4 전자 QR | 중앙 교체 뒤 새 전자 QR을 화면에서 스캔해 확인하고 원본 물리 PHS2는 유지 | 화면 반사·해상도·배율에서 판독, 잘못된 QR 거부와 재시작 후 확인 단계 복구 |
| F5 물리 출력 | Windows GDI proof는 printer/job/document/time/EndDoc를 저장하는 spool 증거 | 용지 크기·driver·실제 배출·QR 가독성·기본 프린터 보존·paper-out. 큐 접수 성공은 실물 출력 성공과 별도 |

근거: [입력·단축키·audio](../../Label_Match.py) `process_input/_handle_scan_enter/_handle_workflow_shortcut/_label_match_audio_enabled`, [F4 확인](../../Label_Match.py) `_prompt_new_seal_verification`, [F5 프린터](../../phs_label_workflow.py) `WindowsGDIPhysicalLabelPrinter/PhysicalPrintEvidence`. 전체 장비 수용은 [LM-B04](BACKLOG.md#lm-b04), 업무 기준은 [LM-05/10/12](README.md#lm-05)에서 연결한다.

<a id="recovery"></a>
## 저장·오프라인·중단 복구

<a id="business-fixtures-w7lmfix"></a>
### W7 후속 업무 fixture 사용 범위

[business_factory](../../tests/test_business_flow_fixtures.py)와 [지원 코드](../../tests/_business_flow_fixture.py)는 W7-V4의 세 후속 행을 위한 격리 headless 시험이다. 실제 `DataManager`, 공유 SQLite outbox·exchange·lease store, `PackageLogisticsClient`의 주입 transport, 기존 시험 signer/keyring을 사용한다. 제품 source·guard·서명 검증·flush/fsync·marker·receipt 검증은 변경하지 않는다. `run_tests=False`이며 UI·오디오·telemetry 전송·clock만 외부 seam이다.

| 행 | 재사용 진입·상태 | 입증 한계 |
| --- | --- | --- |
| F4 저장 목록/원자 교체 | `business_factory(exchange=True)` → `prepare_exchange(case)`. 기존 `BatchClient` 2쌍·target/donor/receipt로 목록 수정·삭제·순서를 검사하고 실제 coordinator/store에 저장한다. 초기 DB 상태는 **PREPARED**이며 audit의 일반적인 pending 표현과 구별한다. 실제 409→OPERATOR_REVIEW, 검증 receipt→ACKED + seal/local PENDING, 잘못된 QR 거부→정확한 QR 확인→로컬 APPLIED와 동일 set/raw 복원을 검사한다. | 기존 단일-source projection과 제품에서 검증한 seal을 결합한 exchange 시험이다. 최신 full-single work-group topology의 첫 PHS2→F4 팝업 진입·widget 목록 편집은 **UNPROVEN**. 3쌍 capability 부재, 기존 F3 lease 및 client 부재 거부는 유지한다. |
| F3 local/pending/ACK/conflict | `case.accept()` → `_begin_central_package_submission()` → `case.drain()`. 실제 source 조회·서명 lease·current-state→intent→CSV flush/fsync→marker/lease transaction 뒤 로컬 완료 상태와 입력 초기화를 검사한다. provider `offline/online/conflict`로 marker=1 PENDING/ACKED/CONFLICT 및 lease LOCAL_COMPLETED/ACKED/OPERATOR_REVIEW를 구분한다. | `_draft/_projection/_receipt`의 기존 단일-source 호환 계약 범위다. marker=0은 전송되지 않고 충돌도 완료 CSV를 지우지 않는다. 실제 문구·화면, 서비스 인증·lease 발급, async ACK 화면·native F3 lane은 **NOT TESTED**. |
| 자정/lost ACK | `provider.mode='lost_ack'`에서 provider 효과 1회·응답 유실 → `case.restart(at=...)`로 23:59 저장/writer 종료 후 00:01 reopen → 같은 intent/command/key의 receipt-first 복구. 별도 marker 쓰기 실패도 같은 signed lease·원 CSV 1행으로 복구한다. | 실제 store와 current-state loader를 사용하지만 Tk `on_closing`·프로세스 재기동·전원 장애는 **NOT TESTED**. provider 효과 1회는 실제 서버 원자 transaction의 증거가 아니다. OS 시계를 바꾸지 않는다. |

다른 pytest에서 `from tests.test_business_flow_fixtures import business_factory, prepare_exchange`로 fixture와 F4 준비 함수를 재사용한다. `case.app`, `case.root`, `case.provider`가 각각 실제 저장 소비자·격리 데이터·외부 응답 제어점이다. `case.restart(at=datetime(..., tzinfo=timezone.utc))`는 저장→writer flush/close→합성 clock 이동→새 store/current-state 복구를 수행한다. 종료 fixture는 writer를 닫고 검사하며 DB/CSV/키 자료는 지정 basetemp에 남긴다. 운영 프로필·데이터를 복사하지 않는다.

실행 전 TEMP/TMP, `LABEL_MATCH_SAVE_DIR`, `LABEL_MATCH_SETTINGS_PATH`, `LABEL_MATCH_DIRECT_SYNC_ROOT`, LOCALAPPDATA/APPDATA/PROGRAMDATA/USERPROFILE을 지정 D: 작업 루트로 격리하고 `PYTHONDONTWRITEBYTECODE=1`을 설정한다. 명령은 `python -m pytest -q -p no:cacheprovider tests/test_business_flow_fixtures.py --basetemp <D: 작업 루트의 새 tmp 경로> --junitxml <D: 로그 경로>`다. 실행 결과·정확한 회귀 노드·실패 원본은 [W7LMFIX RESULT](D:/KMTech/program-improvement-20260912/work/Label_Match/w7lmfix/RESULT.md)에 연결한다. 기존 W7-V4 store 0행·실패 및 표시 DTO ACK 판정은 소급 변경하지 않는다. 새 fixture를 이용한 VM 재장면은 별도 사용자 승인 전 실행하지 않는다.

### 복구 상태와 대응

같은 데이터 루트의 `_current_set_state_packaging.json`, event CSV, `package_logistics_outbox.sqlite3`와 `package_operation_lease_keyring.json`을 업무 identity와 함께 보존한다. 같은 SQLite 파일에 생성·취소·lease·접수·교체 상태가 연결되며, F5는 `phs_label_exchange/phs_label_exchange_recovery.json`과 `labels`를 사용한다. producer의 queue/spool/status/receipt는 별도 root다. 근거: [앱 초기화/DataManager](../../Label_Match.py), [package_logistics](../../package_logistics.py), [보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md).

| 사건 | 현행 경계와 작업자/지원 담당의 다음 행동 | 종료·확인 기준 |
| --- | --- | --- |
| 시작/품목 실패 | 중앙 등록은 검증된 이전 cache 복구 가능; 유효 cache가 없으면 시작 차단. 표시된 profile·catalog 진단을 확인 | 임의 Item.csv 성공으로 바꾸지 않고 정상 snapshot 또는 검증 cache가 선택됐는지 확인([LM-02](README.md#lm-02)) |
| 미검증 접수·오프라인 | `CAPTURED_UNVERIFIED`는 저장 접수다. F3는 현재 snapshot과 일치하는 유효 lease가 필요. 재사용 lease 없는 단절에서는 완료 차단 | 재연결 뒤 같은 capture/set identity의 검증·중앙 결과·로컬 적용 상태가 일치([C-04](contracts.md#c-04)) |
| 디스크/CSV/marker 실패 | 성공을 선행 표시하지 않고 현재 세트·intent·기존 CSV를 유지. writer 오류를 정상 flush로 오인하지 않음 | 같은 작업으로 복구해 이미 있는 완료 event를 대조하고 marker와 lease 경계를 연결; 새 set/key로 우회하지 않음([LM-06/08](README.md#lm-06)) |
| 이력 조회 불완전 | 마지막 완전한 활성 색인을 유지하며 손상 행 수를 경고. 이력 상세에서 파일·행 위치를 확인하고 원본을 보존해 지원 담당에게 인계 | 오늘 기록을 완전하게 다시 읽어야 작업 차단 해제. 부분 집계나 빈 표를 정상 수량으로 사용하지 않음([C-08](contracts.md#c-08)) |
| 관리자 경고 조회 실패 | 생성·취소 경고와 마지막 확인 목록을 보존하며 `조회 실패 · 오래됨` 표시. 실물 구분·반출 보류 안내를 계속 따른다 | 정상 조회로 현재 상태를 확인한다. 확인된 빈 목록만 경고를 해제하며 DB 오류를 사건 종결로 간주하지 않음 |
| 로컬 완료 후 전송 대기 | `로컬 완료 저장됨 · 중앙 전송 대기`는 marker=1 로컬 완료이며 due 재시도; 다음 준비 작업 가능. ACK 유실은 저장 명령의 receipt부터 조회 | `중앙 확정`은 중앙 COMMITTED receipt와 원래 명령 identity 검증 뒤의 상태. 오래된 pending 실물 인계 기준은 [LM-B02](BACKLOG.md#lm-b02) |
| 중앙 충돌/다중 PC 경합 | marker=1 conflict는 로컬 완료를 보존하고 검토 사건으로 격리. marker=0을 완료로 승격하지 않음 | 작업자는 원본 PHS2·실물 구분을 유지하고 리더/지원 담당이 set/key·중앙 결과·원인을 대조. 사건 종결 권한·실물 처리 기준은 미확정([LM-B06](BACKLOG.md#lm-b06)) |
| 자정·재시작 | 미확정 PHS2/outbox를 날짜만으로 삭제하지 않음. 중앙 캐시 timestamp 파싱 실패도 원본을 보존한 복구 잠금이며 관리자에게 확인·수리 요청 | 작업일 변경과 관계없이 기존 identity로 완료/검토 상태에 수렴. 시각을 현재 값으로 수동 치환하거나 원본 삭제로 우회하지 않음 |
| F1/F2 | F1은 gate가 허용하는 미완료 초기화. F2 취소는 CREATE ACK dependency와 별도 취소 key 사용 | 취소·중복 요청이 원래 PACKAGE에 연결되고 SHIPPING-WAIT 재고 유지. 실물 해체·재고 반환으로 해석하지 않음([C-06](contracts.md#c-06)) |
| F4 응답/로컬 확인 실패 | 저장 intent/receipt부터 복구; 중앙 성공 후 새 QR 확인·로컬 적용은 별도 단계 | 중복 교체 없이 현재 membership/seal을 확인한 뒤 후속 gate 해제([C-03](contracts.md#c-03)) |
| F5 출력 결과 불명 | `recover_current/recover_reconciliation`과 journal·서버 status·출력 evidence를 먼저 대조 | 재출력 여부를 결정하고 prepare/print/activate를 조정. 새로운 출력부터 반복하지 않음([C-07](contracts.md#c-07)) |
| spool 유실/손상 | 통신 재시도로 파일을 재생성하지 못함. `failed_permanent`/검토 prefix를 보존하고 원본 range와 identity를 대조 | 원본에서 복구 가능한 범위와 손실을 구분하고 유효 accepted receipt까지 진척을 확정하지 않음([C-05](contracts.md#c-05)) |
| 처리 중 종료 | UI lane drain·현재 상태/로그 저장·세션 동기화의 종료 경계를 따른다. BROKEN/timeout을 정상 저장으로 간주하지 않음 | 재시작으로 durable 상태를 확인. 강제 종료 후 성공 여부는 소리·창 닫힘으로 판정하지 않음([TkSerialUiLane](../../tk_serial_ui_lane.py), [앱 `on_closing`](../../Label_Match.py)) |

지원 인계에는 비밀 없는 set/package/key·발생 시각과 업무일·로컬 marker/status·중앙 receipt 또는 오류 코드·producer 상태·관련 산출물 경로·실물 구분을 연결한다. 원본 로그 전문을 복사하거나 DB/status를 수동 편집해 복구 성공을 만들지 않는다. 요구 RPO/RTO·재시도 후 인계 시점·최종 승인 역할은 아직 미정이다([LM-B02](BACKLOG.md#lm-b02), [LM-B07](BACKLOG.md#lm-b07)).

<a id="wave1-failure-handling-validation"></a>
LM-W1 실패 처리의 격리 host 검증은 감사 clock32/package156/producer45/lane66의299 PASS와 직렬 내구33 PASS(기존32+새 CSV1), 새 회귀19의 PASS를 [JUnit node 대조](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/acceptance-summary.json)로 연결한다. 감사 필수331개 누락은0이며 새 회귀 포함350개다. 최초 병행 내구 실행의2 FAIL/31 PASS는 원인 미확정으로 보존했고 진단2/직렬33 PASS에서 재현되지 않았다. [RESULT](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/RESULT.md)의 전후 probe와 한계를 따르며 GUI·VM·서버·clean build 증거는 아니다.

## 설치·업그레이드·제거·백업·롤백

<a id="bootstrap-integrity-order"></a>
`d0e504e` 이하에서 기록·읽기의 ordinal/casefold 순서 차이로 재시작 시 `canonical bootstrap integrity readback differs`가 발생할 수 있다. `w9labelintegrityorder` 수정본은 installer와 같은 ordinal 열거 및 순서 비민감 행 비교를 사용하고, 기존 v1 aggregate는 저장된 순서로 검증하여 원본을 보존한다. 추가·누락·내용·크기·경로·digest 불일치는 계속 차단한다. capture6은 수정 커밋의 stock portable 후보를 정상 설치한 뒤 같은 사용자의 재시작·정상 종료와 기존 stop-marker/receipt 절차를 확인한다. 호스트 headless 검증과 guest 재설치 수용은 구분하며 guest 확인은 아직 NOT VERIFIED다.

<a id="bootstrap-integrity-writer-transition"></a>
### 무결성 순서 수정본으로 writer 업그레이드

`0cf5bf6` 자체의 installer는 `d0e504e`와 onboarding AST가 달라 교체 전에 거부한다. `w9labelwriterupgrade`가 포함된 stock portable을 사용하면 정확한 전/후 onboarding hash 쌍만 `PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX`로 인정한다. 파일 집합·writer/guard·runtime·계약·설치본 bootstrap 검증은 유지한다. 검증 순서만 바뀌고 data/identity/queue/spool/settings/receipt schema는 같으므로 데이터 변환은 필요 없다. 임의 이전/이후 릴리스나 역방향 downgrade를 허용하는 옵션은 없다.

같은 사용자로 앱을 정상 종료하고 검토된 새 후보의 `INSTALL_CANONICAL_PORTABLE.ps1 -SourceRoot <후보 경로> -EvidencePath <새 감사 JSON 경로>`를 실행한다. `-PlanOnly`는 writer 전환 검증을 실행하지 않으므로 성공 근거가 아니다. 기존 conflict 상태는 후보 전체 inventory에 결속된 정상 receipt가 먼저 필요하다. 이전 후보용 receipt 재사용·수동 수정·stop-marker 삭제 또는 code-only 제거로 guard를 우회하지 않는다.

설치 전 사용자 상태의 보존본과 원 설치본을 유지한다. 성공은 감사의 `PASS`·위 compatibility·candidate/installed writer pin과 현재 bootstrap 검증으로 확인하고 같은 사용자의 시작→정상 종료를 두 번 확인한다. 교체는 새 bootstrap record를 생성하고 `.current.rollback.*`에 이전 code/record를 보존하며, 실패 시 기존 transaction이 검증한 preimage로 복원한다. 역방향 설치는 거부한다. 실패 복원된 `d0e504e`에는 원래 재시작 순서 결함이 남으므로 정상화로 간주하지 않는다. host 재현·검증은 [RESULT](D:/KMTech/program-improvement-20260912/work/Label_Match/w9labelwriterupgrade/RESULT.md), 실제 guest 재설치·업무 수용은 capture7의 별도 확인 범위다.

portable builder의 `THIRD_PARTY` 9개 version은 `requirements-release.txt`의 hash lock과 [자동 대조](../../tests/test_zero_pe_conversion.py)한다. `chardet==5.2.0`의 pure Python wheel을 명시하고 source runtime에는 계속 chardet을 복사한다. lock의 charset-normalizer는 다른 build/test closure를 위해 유지하며 portable zero-PE 대체 의도는 바뀌지 않는다. 이 입력 정합 검사는 clean 설치·portable build 실행 증거와 별개다.

현행 [릴리스 계약](../../RELEASE_GATE_CONTRACT.md)의 코드 배치와 첫 사용자 등록을 구분한다. `--remove-current-user-setup`은 정확한 사용자 persistence 제거·relay 종료와 lock 부재를 확인하면서 identity/profile/settings/ledger/queue/spool/status/logs/receipts를 보존하는 계약이다. 이후 elevated `INSTALL_THIS_PC.ps1 -Uninstall`이 코드를 제거하며 relay persistence가 남아 있으면 거부한다. 이 명세에서 설치·제거 명령을 실행하지 않았다.

업데이트 후보·서명/manifest 및 archive 검증은 유지하며, 앱 내부 코드 적용·batch workspace·레거시 prompt는 제거했다. `threaded_update_check`와 GUI worker/poll은 조회 결과만 알린다. `tools/sign_release_executables.ps1`은 외부 수동 운영 소비 여부가 미확인이므로 보존한다. 코드 교체와 integrity 재생성은 별도 installer의 책임이며, 이전 릴리스/현재 dirty 소스의 결과를 서로 자동 상속하지 않는다. 실제 업그레이드·재설치·rollback 뒤 기존 identity, current-state, 미전송/검토 row, F5 journal, receipt 정합성은 [LM-B07](BACKLOG.md#lm-b07)의 남은 수용 범위다.

백업·복원은 데이터와 미전송 효과를 함께 다뤄야 한다. 현행 [보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)은 미확정 spool/status 삭제 금지와 ACKED retention의 read-only 후보 판정을 제공하지만, 완전한 운영 백업 도구·주기·RPO/RTO·검증된 복원본을 이 조사에서 확인하지 않았다. DB 단독 복사나 코드 rollback만으로 앱 CSV/queue/keyring/F5 journal까지 일관된 시점으로 복원됐다고 할 수 없다. 서로 다른 PC의 DPAPI/identity를 단순 파일 이동으로 복구할 수 있다고 가정하지 않는다. 실제 복원 방식·중앙 receipt 대조·중복 방지 확인을 소유 운영 절차에 확정할 필요가 있다.

<a id="performance"></a>
## 처리량·동시성·최신성

<a id="responsiveness-w2"></a>
### 감사 Wave 2 · Tk callback 응답성

최종 w2fix3 격리 host 실행은 **469 PASS / 579.16초**, FAIL/ERROR/skip0이다([JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix3/logs/focused.xml)). W1 감사331+추가19=350·내구33·이력40과 B01/B03 응답성10개를 포함한 기존469 노드 누락0을 [JUnit 노드 대조](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix3/logs/acceptance-summary.json)로 확인했다. 두 검토의 R1–R5는 제거된 index 내부 조작을 CSV/obsolete artifact 경계로 전환한 동일 재현 **28 PASS**다([재현 JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix3/logs/r1-r5.xml)). 원 실제 commit/writer·capture 파일은 그대로이며 R4 rename 중 1→2행과 R5 `int(Thread)` 기준4 FAIL도 보존했다. [1차 검토](D:/KMTech/program-improvement-20260912/work/Label_Match/w2/REVIEW.md), [재검토](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix/REVIEW.md), [철회 결과·정확한 전환·전후표](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix2/RESULT.md)를 따른다. GUI·서버·전체 suite·운영 p95 수용은 아니다.

이력은 완전 결과의 활성 색인을 먼저 설치한 뒤 삭제·삽입·집계 표시를 최대100회 연산/8ms 작업 예산의 after 배치로 나눈다. 오늘 결과 적용 중에도 스캔이 가능하고 새 조회는 이전 배치를 중단한다. 활성 완료/취소와 새 집계의 revision을 보존하며 과거 조회는 활성 행을 표시하지 않는다. 기존 history40 PASS와 신규4 PASS(5,000행 삭제+5,000행 삽입+집계2,000, 적용 중 실제 입력 admission·generation 취소·완료/취소·과거 조회), [신규 JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w2/logs/b03-final.xml). 기존 읽기 중/과거 조회 gate 자체와 D03의 별도 조회 창 분리는 후속이다.

LM-B02 위치 색인은 철회했다. 첫 검토의 동일 metadata 변경·외부 append·lookup 손상(R1–R3)을 보완한 뒤에도 rename(R4)으로 중복 완료가 발생했다. 전체 바이트 해시 때문에 정상 날짜 365파일 warm도 약25→155ms로 늘어 정확성 위험에 비해 남은 이득이 작다는 코디네이터 결정이다. 색인 모듈·사용·writer 갱신·재구축을 제거하고 `031f6d9`의 같은 PC prefix 전체 CSV 직접 검색과 durable writer를 복원했다. 검색 중 파일 소실만 현재 목록으로 한 번 재검색하며 반복 이동·열거 오류는 부재 대신 완료 중단이다.

기존 색인/임시 파일은 정리하거나 변경하지 않고 무시한다. 부재 판정에 캐시·색인은 없다. CSV 적중 flush→재대조→fsync, durable→성공 표시→중앙 ACK, 같은 key·OPERATOR_REVIEW 격리를 유지한다. 완료 존재 확인 비용은 보관 파일 수에 비례하므로 보관 정책으로 관리하는 후속 항목이다. 최종 검색 이후 다중 프로세스 CSV 동시 쓰기는 직렬화하지 않는다. GUI·운영 p95·cold OS cache·독점 부하는 미검증이다.

주기 package review의 SQLite 정리·조회와 workbench F4 표시 조회는 기존 package worker에서 수행한다. Tk poll은 현재 set·generation·업무 epoch가 같은 불변 결과만 경고에 적용한다. 행동 직전 검사·복구 적용 경로는 유지한다. 격리 headless review20 PASS(200ms 실제 SQLite writer lock·stale 결과·기존 경고 회귀), [로그와 JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w2/logs/b01-new.xml). 실제 GUI·스캐너·운영 p95는 미검증이다.

R5 capture 소비자는 `int(Thread)` 대신 worker 완료 snapshot을 받아 기존 guard로 적용하고 conflict 개수·경고를 확인한다. waiting/취소 conflict 원 reviewer2와 편입2·기존 warning renderer1은 **5 PASS**다([JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix2/logs/r5-02.xml)). capture 도구의 제한된 대기만 추가했으며 제품 B01 worker·행동 직전 정본 guard는 변경하지 않았다. headless 가짜 presenter 검증이며 실제 화면 capture는 미실행이다.

R6 actual-input walkthrough는 `review_only=True`를 기존 worker에 위임하고 일반 outbox 전송 억제를 유지한다. 원 reviewer 검사는 기준031f6d9 PASS·수정 전b44897d FAIL·수정 후 PASS이며, 편입 검사는 실제 subclass/workbench에서 worker 조회·poll snapshot 적용·충돌 경고·전송 억제를 확인한다. B01/W2 반환 계약 변화는 worker 시작·review 갱신 두 메서드이며 관련 snapshot을 포함한11개 API 이름의 앱·도구·테스트42개 scope를 AST/rg로 대조하고 소비자 집합 회귀1개로 고정했다. [w2fix3 결과·소비자 표·로그](D:/KMTech/program-improvement-20260912/work/Label_Match/w2fix3/RESULT.md)를 따른다. 실제 생성자·widget과 poll 후 업무 복구는 headless 대체이며 GUI·VM·서버·운영 p95는 미검증이다.

<a id="history-optimization"></a>
### 2026-09-09 이력 적용의 반복 작업 제거

기준 소스 `a7e57b7f7c3d815ad5aff945e302b916c5e68f4e`에서 [구현 전 기준·목표](E:/KMTech/optimization-implementation-20260909/Label_Match/BASELINE-AND-TARGETS.md)를 고정했다. 새 helper로 옮기는 LM-S1 추출은 실질 제거가 없어 채택하지 않고, 빈 셀의 열 너비 조회와 날짜 모드 적용 뒤 중복 workbench 호출만 제거했다. 기존 dict·상태 공유·generation·pending, 한 callback의 논리 설치/전체 적용 및 F3/F4·durable 순서는 유지한다. 새 cache/queue/timer/DTO/batching은 없다.

**PROVEN — 한정된 합성 component 측정:** Windows 11 build 26200 / CPython 3.12.10, 같은 E-only CSV와 fake widget으로 오늘/과거 각각 30회. 1,000행(80% 중앙 1스캔·20% 호환 5스캔)의 열 너비 조회는 4,000→800회, loading 요청 포함 workbench 호출은 오늘 4→3회·과거 3→2회다. 첫 after의 전체 결과 callback 중앙값은 오늘 7.0007→3.7152 ms, 과거 7.4433→3.7997 ms이며 행/집계 표시 digest와 오늘/과거 상태·입력 보존 assertion이 일치한다. Python allocation peak 증가는 모든 사례에서 8 bytes 이하로 측정 한계 내다. 파일 파싱·worker 자체는 변경하지 않았으며 그 시간 차이를 최적화 효과로 주장하지 않는다.

**실패·한계 유지:** 첫 after의 빈 과거 이력 total 중앙값 1.2662 ms는 고정 상한 0.8755 ms를 초과했다. 변경하지 않은 요청/thread 시작 구간에서 증가했고, 별도 paired control은 baseline 0.4979 / candidate 0.4463 ms였으나 첫 실패를 지우지 않는다. 후속 confirmation은 Main의 07:26:59Z 공유 디스크 복사 구간과 겹쳐 worker 증가 및 1,000행 과거 total 45.2078 ms를 기록했다. 이는 같은 부하 조건의 비교가 아니며 원 결과와 시간 창을 보존한다. Main의 07:29:55Z 복사 종료 확인 뒤 같은 driver를 새 process에서 각30회 실행한 결과는 **고정 component 제한 전부 PASS**다. 빈 과거 total은 0.5772 ms, 1,000행 callback은 오늘 3.5365 / 과거 3.9916 ms, total은 19.9540 / 22.2102 ms다. 이 fake 측정의 당시 driver bytes/hash는 동결되지 않았으며 후속 변경과 한계를 [provenance 기록](E:/KMTech/optimization-implementation-20260909/Label_Match/DRIVER-PROVENANCE.md)에 보존한다. [결과·잔여 작업](E:/KMTech/optimization-implementation-20260909/Label_Match/RESULT.md), [VM 행동·기대 결과](E:/KMTech/optimization-implementation-20260909/Label_Match/VM-WORKFLOW.md).

소스 회귀는 기존 history/summary/gate/F3/F4/lane 종료 선택 **64 PASS**, 명시 snapshot-adapter와 기존 writer pin 소비자 **3 PASS**다. 파생 pin은 변경 파일 hash에 맞추며 **44개 writer identity/guard 종류**는 동일하다. 이 증거를 실제 Tcl painting·스캐너·중앙 ACK·VM 업무 또는 설치 증거로 확대하지 않는다.

**PROVEN — 배정 VM의 실제 Tk component:** 소스 `0077412b609247b999fb6b0c7cfc515b24d188e2`와 원 기준 소스를 동결한 v2 driver로 오늘/과거 각30회 비교했다. [사전 native 기준](E:/KMTech/optimization-implementation-20260909/Label_Match/NATIVE-CRITERIA.md), before 뒤·after 전 고정한 [수치 제한](E:/KMTech/optimization-implementation-20260909/Label_Match/native/BASELINE-LIMITS.json), [비교 결과](E:/KMTech/optimization-implementation-20260909/Label_Match/native/COMPARISON.json)는 모두 보존한다. callback 중앙값은 오늘 20.3657→12.0721 ms·과거 20.0075→12.5966 ms, 요청부터 worker/100 ms polling/Tk idle painting까지 total 중앙값은 141.4354→133.2161 ms·140.0262→133.7138 ms다. 10 ms probe의 표본별 최대 지연에 대한 관측 p95는 20.6050→13.2542 ms·19.9981→17.1616 ms이며 모집단 p95로 확대하지 않는다. 모든 고정 제한, 정확한 work 감소, 행/집계 digest, pending 입력 보존과 오늘/과거 gate·색인 assertion이 통과했다.

VM `76a3f7a6-efc0-452c-ab73-f40ccd33594c`, Windows 11 build 26200 / Python 3.12.10 / Tcl·Tk 8.6.15 / vista / scaling 1.33489 / kmadmin session1의 1366×768 화면에서 1000×700 이력·집계·세션·입력 위젯이 mapped 상태였다. 다른 workbench 위젯과 적응 글자 맞춤은 fake이며 실제 앱 시작·PHS2/F4/F3·중앙 ACK·출고 및 Computer Use 전체 업무는 아직 **NOT TESTED**다. 측정 후 버전 조회 2회 실패와 교정을 보존했고, [정리 readback](E:/KMTech/optimization-implementation-20260909/Label_Match/native/CLEANUP.json)은 Label task/Python process 0·자체 PSSession 종료를 확인한다. tkcalendar 미설치는 전체 업무용 client 준비에서 Main이 해결할 항목이다. 제품 최적화 전체 완료는 남은 실제 업무 검증으로 판단한다.

<a id="history-render-dedup"></a>
### LM-A2 · 상태 갱신 뒤 중복 render 추가 제거

`fa147df` 기준에서 [구현 전 work/state 목표](E:/KMTech/optimization-implementation-20260909/Label_Match/lm-a2/BASELINE-AND-TARGETS.md)를 고정한 뒤 `_prompt_exact_rescan`, `_process_exact_rescan_product`, 오늘 분기의 `_apply_history_view_mode`에서 `_update_status_label` 직후 같은 workbench를 다시 그리는 호출 3줄만 제거했다. 앞 두 곳은 중앙 PHS2 표준 교체가 아닌 호환 전체 재스캔 경로다. timer/복구에서 도달할 수 있는 `_apply_acked_sealed_transfer_exchange`의 명시 render와 메서드 AST는 보존했다. init 이전을 꾸민 테스트, A1 polling 변경, A4 날짜 기반 CSV 범위 축소 및 새 helper/state는 없다.

**PROVEN — 결정적 work/state:** 기존 기록 위젯·실제 status/presenter/render 메서드의 [before/after 비교](E:/KMTech/optimization-implementation-20260909/Label_Match/lm-a2/COMPARISON.json)에서 0/1,000행 오늘 render는 loading 요청 포함 3→2, 과거는 2 유지, 재스캔 시작·첫 제품·완료는 각각 2→1이다. 열 너비 조회 0/800과 행/집계/입력 gate, 모든 재스캔 단계의 current state·view·QA/exact 행·tab·status/button·log·save digest가 동일하다. 사전 기존 선택 7 PASS, 강화한 기존 실제 DataManager/render 테스트의 변경 전 1 PASS, 변경 후 선택 7개+기존 pin 2개 **9 PASS**다. 동일 44개 writer identity/guard에 파생 pin만 갱신했다.

이 후속 단위는 product 3줄과 기존 테스트 1줄을 줄이며 새로운 실행 시간·native 화면 수치를 주장하지 않는다. 위 native 측정과 당시 render 3/2 기준은 `0077412`의 보존된 증거다. LM-A2의 work 기준 2/2를 과거 native 결과에 소급 적용하지 않으며, 최신 source의 전체 Computer Use 업무는 후속 배정에서 확인한다.

| 항목 | 요구 목표 | 코드·계약에서 확인한 값 | 실제 측정 |
| --- | --- | --- | --- |
| 입력 간격·처리량·최대 membership | 미정; ERPnext 제한 전용 금지 | 표준 PHS2 1회, F4는 실제 대상 멤버 수 이내의 교체 목록을 한 번에 적용. 처리량 SLA가 아님 | 이 작업 NOT TESTED |
| 물류 요청 시간 | 운영 응답 목표 미정 | 일반 profile timeout 기본 10초, 허용 0.1~60초; legacy client 기본 8초([profile](../../logistics_runtime_profile.py), [client](../../package_logistics.py)) | 설치값·지연 분포 미확인 |
| 전송 claim 회수 | 업무 lease 허용 기간과 별도 | outbox stale SENDING 300초, due-time/last-attempt 순서 선택([C-04](contracts.md#c-04)) | 장애 후 회복시간 미측정 |
| relay/종료 | RTO·허용 대기 미정 | 릴리스 사용자 relay 30초 retry loop; 앱 종료 총 예산 상수 105초/로그 10초([릴리스](../../RELEASE_GATE_CONTRACT.md), [앱 상수](../../Label_Match.py)) | 최대 소요 보장 또는 측정 p95로 해석하지 않음 |
| 동시 작업 | profile 문서의 10~30대 전환은 rollout 계획, 검증 capacity 아님 | GUI guard의 기존 custom 우선 선택을 writer와 일치시킨 소스 교정. Windows lexical 경로 정규화·mutex 수명 및 중앙 CAS/idempotency는 유지. [잔여 경계](#storage-root-residual), [LM-B10](BACKLOG.md#lm-b10) | [SaveRoot13](#saveroot13-evidence)의 단일 process native 제외·해제 후 재진입만 입증. 두 GUI writer·다중 PC 처리량은 미실행 |
| 지표·시계·보존 | 발생→ACK→수신→화면 지연, 시계차, 업무 cutoff, 보존 기간·RPO/RTO 미정 | [C-05](contracts.md#c-05)의 전송·projection/flag와 [수량·시간](contracts.md#quantities) | 실제 설치 조합의 신선도·손실·복원 측정 없음 |

<a id="verification"></a>
## 수용 기준과 기존 증거의 연결

아래는 핵심 확인 묶음이며 테스트 개수나 전체 완성도 분모가 아니다. 카드의 현행 계약 확인 기준을 입력/장애/관측 결과로 묶었다. 목표 설치 조합의 필수·조건부·비적용은 담당자가 근거와 함께 확정하고, 이미 확보된 실행을 먼저 연결한 뒤 실제 부족한 범위를 검증한다. **M3/N6의 V03 저장·복구 및 V07 worker/Tk 적용 경계, SaveRoot13의 V01 저장 경로, ProducerClose의 V06 receipt 회귀는 각각 아래 지정 범위에서 PROVEN**이며 V01–V08 전체를 통과로 바꾸지 않는다.

| 묶음·연결 | 입력·장애와 기대 결과 | 현재 연결할 근거·공백 |
| --- | --- | --- |
| V01 · LM-01/02/03 | 사용자 시작·catalog 정상/검증 cache/손상, 정상/오형식 PHS2 → 선택 경로·권한·멤버십 일치 또는 차단. 서로 다른 env + 같은 custom 저장소 → 실제 writer와 같은 mutex 및 중복 callback 제외; null/빈 custom fallback 대조 | [main](../../Label_Match.py), [품목](../../item_catalog_sync.py), [회귀 소스](../../tests/test_label_match_single_instance.py). [원래 선택 제안](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/FUTURE-VM.md)에서 결속한 [focused packet](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/PREPARATION.md)의 **실제 13 PASS/39 phase PASS**를 [아래](#saveroot13-evidence)에 연결. 저장 경로·단일 process native mutex만 해당하며 catalog/PHS2·실제 GUI·설치 provider·장비는 이 실행으로 입증하지 않음([LM-B04](BACKLOG.md#lm-b04)) |
| V02 · LM-05 | F4 1/2쌍, 다른 품목·UOM·stale·부분/복수 work-group, ACK 유실·QR 중단 → 원자 교체 또는 무변경 거부·동일 identity 복구 | [test_sealed_transfer_exchange](../../tests/test_sealed_transfer_exchange.py) 설계; 실제 QR/실물 [LM-B04](BACKLOG.md#lm-b04) |
| V03 · LM-04/06/08 | 유효 lease 유무별 offline, intent/CSV/marker 경계 중단·writer 실패·자정 → 거짓 성공 없음·같은 set 1회 복구 | [M3 변경 사례 #83–84, #160–168, #179 및 N6 #1–6](#residual-source-evidence)의 실제 저장/중단·재동기화 범위 **PROVEN**. core는 lease 없음, materializer는 로컬 lease/검증 double, N6는 lease 없는 역사 ACKED row다. real lease-bearing orphan 통합 재시작·원격 lease/backend·native F3는 **UNPROVEN**([LM-B05](BACKLOG.md#lm-b05)) |
| V04 · LM-07/09 | 다중 PC·lost ACK·429/409·취소 dependency → receipt 수렴, 후속 due row 진행, 로컬 완료 보존, 취소 후 재고 유지 | [test_package_logistics](../../tests/test_package_logistics.py) 설계와 [서버 계약](contracts.md#c-04); 중앙·실물 인계 [LM-B02](BACKLOG.md#lm-b02) |
| V05 · LM-10 | F5 출력 실패/출력 후 ACK 유실/재시작 → journal 단계 조정·중복 출력 통제·실물 판독 | [F5](../../phs_label_workflow.py) 정적 근거; 실제 프린터 [LM-B04](BACKLOG.md#lm-b04) |
| V06 · LM-11 | 정상·취소·중복·부분 이벤트, 명령/CSV 순서 바꿈, spool 손상 및 F3 ACK 뒤 raw APP_CLOSE → 수신·세트 projection·화면 별도 확인. [ProducerClose 실제 50/150 PASS](#producer-close-evidence)는 receipt 회귀에 한정하며 native/서버·화면 연동은 미실행 | [양쪽 전송/소비](contracts.md#c-05); 실제 flag·화면·지연 [LM-B06](BACKLOG.md#lm-b06) |
| V07 · LM-12 | busy/과거 날짜/연속 Enter/종료·stale 결과 → 입력 보존, gate 일치, 중단 후 저장 정합 | [M3 #287](#residual-source-evidence)의 real SQLite/DataManager worker 처리와 marker/lease commit 뒤 owner-thread UI 적용 **PROVEN**. FakeTkRoot이며 native F3/close/reopen·실장비/배율 및 V07 전체 수용은 **UNPROVEN** |
| V08 · 설치·복원 | exact candidate 설치→첫 업무→cold boot→업그레이드/제거·재설치→rollback → 데이터·미전송 identity 보존. 기존 custom 저장소의 owner 종료/중단 후 같은 위치 복구와 onboarding/relay A/C 선택을 별도 확인 | [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [중앙 준비도](../../../Program_Spec_Hub/READINESS.md); 전체 범위 [LM-B07](BACKLOG.md#lm-b07), [LM-B10](BACKLOG.md#lm-b10) |

<a id="residual-source-evidence"></a>
### M3/N6 · 완료 CSV·복구 잔여 소스의 기존 수용 매핑

2026-09-08 [독립 전체 diff·증거 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-review/REVIEW.md)에서 전체 tracked diff 1,575줄/71,863 bytes와 신규 완료 CSV 모듈 10,540 bytes를 대조했고 Main이 다섯 경로+네 명세의 한 소스 단위 종결을 승인했다. [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)은 테스트 당시 working bytes를 유지하고 Git EOL 정규화 staged/commit blob pins를 별도로 남긴다. 아래는 기존 실제 실행의 매핑이며 신규 실행·collection·VM 관측이 아니다.

**PROVEN:** M3는 **313 PASS / 939 ordered setup/call/teardown PASS**(deferred 106 + core 174 + lane 33), 별도 N6는 **6 PASS / 18 ordered phase PASS**다. M3의 변경 13사례는 이미 그 313개에 포함되며 Candidate13과 겹친다. N6는 M3 밖의 별도 선택이다. 모두 원래 JUnit 순번이며 아래 각 사례의 세 phase는 PASS다.

| 원래 사례 | 변경 경로·정확한 함수/parameter | 입증된 경계 |
| --- | --- | --- |
| M3 #83–84 | `test_deferred_intent_capture.py::test_validated_materializer_flows_through_f3_durable_completion[first-success/marker-commit-refusal]` | 같은 command/lease/capture/CSV identity; marker/lease 외부 동시 가시성·거부 rollback·1회 복구 |
| M3 #160 | `test_label_match_core.py::test_central_finalize_commits_intent_and_local_event_before_ui_success` | 독립 DB/CSV read가 성공음·이력·count보다 선행 |
| M3 #161–165 | `test_label_match_core.py::test_f3_real_storage_refusal_preserves_packaging_and_recovery[intent_insert/intent_commit/csv_flush/csv_fsync/marker_commit]` | 각 실제 저장 거부에서 입력·복구 보존, 새 writer로 같은 작업 복구 |
| M3 #166 | `test_label_match_core.py::test_f3_pending_first_keeps_second_prepared_input_on_reopen` | 첫 pending과 다음 준비 입력을 재시작에서 각각 보존 |
| M3 #167–168 | `test_label_match_core.py::test_f3_crash_recovery_keeps_one_completion_across_midnight[csv_durable_marker_uncommitted/committed_before_ui]` | exit-73 두 중단 경계·자정 뒤 동일 완료 event 1개 |
| M3 #179 | `test_label_match_core.py::test_data_manager_fsyncs_local_completion_before_flush_returns` | 실제 fsync gate가 flush 반환 차단 |
| M3 #287 | `test_label_ui_lane_integration.py::test_f3_lease_outbox_and_flush_run_off_tk_before_ui_apply` | worker에서 real SQLite/DataManager; marker hold 때 0/PREFETCHED, commit 뒤 1/LOCAL_COMPLETED와 owner-thread apply/sound/drain |
| N6 #1–3 | `test_completion_csv_durability.py::test_acked_orphan_requires_existing_csv_sync_before_marker[None/fsync/write_open]` | write-capable fsync 뒤 marker; 거부는 marker0·동일 input/retry block·event identity 유지 |
| N6 #4–6 | `test_completion_csv_durability.py`의 `test_matching_csv_keeps_real_writer_failure_sticky`, `test_matching_csv_is_revalidated_after_writer_barrier`, `test_malformed_or_unrelated_discovery_rows_remain_absent` | writer 오류 유지, barrier 뒤 바뀐 row 거부, malformed/unrelated row 제외 |

다섯 working pins는 원래 P109와 PC112 `control/SOURCE-MANIFEST.json` 모두에 일치한다. P109 manifest SHA256 `40b2104364b6cc15476328f8b6318ae03f32867be52f590e4d4cf6f43f913b7f`, [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/control/SELECTION.json) `e91a79cfee33e6273619632d2e204d48f03ff47772a4517fc2d06191d65870f2`다. 환경은 VM01 Windows/Python 3.12.10 x64/pytest 9.0.2, 22 versions/19 imports와 원래 tkcalendar warning, guest `C:\Qualification\vm-test-g6\label-b1-pid-03\source`다.

| 파일 | 테스트 당시 bytes / working SHA256 |
| --- | --- |
| `Label_Match.py` | 886147 / `a205c587c3ca0a518f80410729a4e6d28bb9c267282d21aa2957c1cb780bece5` |
| `tests/test_deferred_intent_capture.py` | 129177 / `dcb552cb26c747161f2621281c432124f274dbfc83f2c8d4657940265a335bb9` |
| `tests/test_label_match_core.py` | 242285 / `9ea2f893cab73ded5ac496bf8454be809529bad57d3ea365ab31172bf5ce902b` |
| `tests/test_label_ui_lane_integration.py` | 68783 / `7de68a05c4845653c76299f64c4a442566cf2445075c744fa38e086c7320ed55` |
| `tests/test_completion_csv_durability.py` | 10540 / `335bb278196e620ee5596b3a7643430eb73b42ebedb53997c43687f2495dd46d` |

[M3 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-modules3-reader-fix/MAIN-Modules3-READBACK-REPR.json): **2,815,193 bytes / SHA256 `d0278b9eb6d6e1b5f118889e6be22082619bba0c4410d02ca8e502261a26ee12`**, Main 수용 `2026-09-07T08:11:02.5780662Z`. [N6 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/MAIN-NewProduct6-READBACK.json): **2,479,468 bytes / `7b0e39fe772203b770627969a1562f03e20a3ed9744f40e68c27da1a8c4f3a6a`**, 수용 `2026-09-07T07:12:01.0865971Z`. 독립 검토에서 원본 XML/phase를 대조하고 M3 21개/N6 17개 export 전부 재해시해 불일치0, saved source/provider/stream gates 일치를 확인했다. 원시 XML/hooks는 `label-next-prepare/evidence-Modules3`, `evidence-NewProduct6`에 보존한다.

**FAILED 이력:** 원래 M3 reader의 11 repr ID/33 비교 불일치, Candidate13 11 PASS/2 FAIL, ProducerClose Export01을 보존한다. additive reader 교정과 후속 export는 시험 재실행이 아니다. SaveRoot13 **13/39**(source110 manifest `a68cd9c06dd1f32e0f9adfb085aee8fa1c9e6e706f5ef5bf6dc4cf00e09517e8`)와 ProducerClose **50/150**(source112 manifest `256401fab80446fe042ab6f2fb8eab8f1d7b56ed5a58294d795b6aeb08dc1430`)는 별도 선택이며 위 변경 사례를 선택하지 않는다. 수를 합산·반복하거나 원래109와 현재의 guard/uploader 차이를 통합 FULL PASS로 상속하지 않는다.

**NOT TESTED:** Baseline8 causal RED는 기존 [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/control/SELECTION.json)에 있으나 start/evidence/Main 결과가 없는 역사 공백이다. Main이 이번 이미 수용된 수정의 소스 종결 선행 조건에서 명시적으로 제외했다. baseline exact pins는 독립 검토에 보존하며 RED를 관측했다고 주장하지 않는다.

**UNPROVEN:** real lease-bearing ACKED-orphan 동기화/marker 실패의 통합 재시작. M3 #257은 mocked forwarding, materializer/lane은 로컬 lease를 seed하고 검증·전송을 double로 대체한다. N6는 lease 없이 receipt `{}`인 역사 ACKED/marker0 row를 seed했으며 현재 전송 claim은 marker1만 선택한다. matched-file timeout/disappearance/decode/CSV 예외별 사례, native Tk F3/close/reopen, 두 GUI same-store 복구, 실제 원격 lease/backend ACK, hardware power-loss 및 통합 FULL은 별도 공백이다. 이번 FULL/build/install은 **NOT TESTED**다.

다음 요구는 종결된 candidate와 실제 provider·설정·입력/관측을 동결해 기존 N/I same-store key/member/hash/lease, native F3/정상 종료·중단/재개·EOF, producer transport/receipt, 두 GUI writer 제외·pending 복구 및 backend/FULL을 각 해당 범위에서 검증하는 것이다. 기존 Machine anchor·DNS/TLS/authority와 grants, Linux40/Label25 및 남은 준비 조건은 아래 ProducerClose 절과 LM-B04/05/06/07/10을 따른다. 이 소스 종결은 실행·배포 권한을 추가하지 않고 Ready **0/6**을 유지한다. 중앙 Q08 갱신은 Main 소유다.

<a id="producer-close-preparation"></a>
<a id="producer-close-evidence"></a>
### ProducerClose · lifecycle 수신 계약의 한정 회귀 수용

2026-09-08 [소스·VM packet 준비](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md)는 uploader와 회귀 테스트를 교정하고 별도 `vm-packet02`를 작성했다. 기존 SaveRoot13 controls의 source/provider/native handle·dual EOF·phase/JUnit·보존 기준을 재사용하되 새 source manifest **Label112-ProducerClose-20260908**(109개 기존 guest copy + uploader/두 테스트 모듈 3개 delta), 새 guest `C:\Qualification\vm-test-g6\label-producer-close-07`, `ProducerClose/pc50`만 선택한다. 기존 Label110 snapshot과 source pins는 과거 증거로 보존하며 새 코드에 그대로 적용하지 않는다.

독립/Main 소스·packet 검토 후 Main이 **실제 50 collected/50 PASS(신규 38 + 기존 12), 150 contiguous ordered setup/call/teardown 모두 PASS, failure/error/skip 0**을 수용했다. pytest stdout은 `50 passed in 3.29s`다. canonical close-only/reopen-close/scan-close 및 replay ACK·retention, identity/hash/byte·observation·오류/격리·business 대체·HTTP commit·runtime fence 거부와 기존 COMPLETE/명령 구별/rotation의 동결 receipt·로컬 session double 회귀다. 실제 서버나 native GUI/F3/close 실행이 아니며 Label25/Linux40 설치 admission을 대신하지 않는다. 이번 소스 종결은 기존 원본의 정적 대조만 수행했고 동일 50개를 재실행하지 않았다. 준비/독립 검토 보고의 당시 NOT TESTED와 Main 원본의 수용 대기 문구는 이력으로 보존하며, 후속 Main 수용과 [커밋·정확한 범위](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md)를 별도로 연결한다.

[실제 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-ProducerClose-READBACK.json)은 **2,641,188 bytes / SHA256 `a922aed7a84255770a3193be5e1a9378828d0b06344a24ae8770c647aa58d91d`**다. [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/control/SELECTION.json)과 [소스 manifest](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/control/SOURCE-MANIFEST.json)의 원래 pins를 변경하지 않는다.

| 근거 축 | 정확한 수용 범위 |
| --- | --- |
| 소스 pin | `direct_sync_push.py` 107,609 bytes / SHA256 `eebb80c832821cdac1d07829f7673557da6ba994720e24b1425a5f3fcf71ae00`; `tests/test_direct_sync_push.py` 86,502 bytes / `0defe8aeaf3a23555fb8f6e3143860c866be728612f3101d0164a39c9fc8194f`. 테스트 당시 working bytes를 유지하며 Git 저장 표현은 종결 근거에 별도 기록 |
| 환경·provider | VM01 `8d9fd433-02bb-4609-93fa-f992ef8a1838` / `KMTech-GenericPC-01`, 기존 `label-b1-f3-01\venv\Scripts\python.exe`, x64 Python 3.12.10·pytest 9.0.2. Label22의 22 versions/19 imports 및 원래 desktop·source/provider closure 일치 |
| 실제 Main 대조 | native 36784 birth `19:07:44.7865471Z` → 자연 종료0 `19:09:29.3768049Z`(2026-09-07 UTC), retained handle·dual EOF **803/0 bytes**. guest 8,503·host 17 불일치0, task Ready/result0/XML 일치, active owned0, evidenceComplete/candidateReadyForMainDisposition=true |
| 자식·stderr | controller2832·dependency2516·pytest5276 모두 자연 종료0 및 stdout/stderr 길이 대조. 각각 0/0·0/187·101/238 bytes이며 dependency의 정확한 기존 tkcalendar SyntaxWarning 1회와 pytest debug 경로 안내 두 줄을 보존 |

초기 [Export01 FAILED](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-NATIVE-PRODUCERCLOSE-EXPORT-01.json)는 PID49544가 `19:02:00.4082666Z`에 자연 종료1한 이력이며, controller의 자연 종료0 `19:02:56.0121977Z`보다 앞선 관측이다. 원본 stderr의 observe-probe RemoteException과 실패를 보존한다. 이후 [Export02](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-NATIVE-PRODUCERCLOSE-EXPORT-02.json)는 PID34512 birth `19:06:13.9159115Z` → 자연 종료0 `19:06:59.0967975Z`, dual EOF 6583/0 bytes로 안정된 17파일을 반출했고 별도 Main 수용 및 이번 호스트 재해시에서 불일치0이었다. 두 번째 export는 시험 재실행이 아니며 초기 실패를 지우거나 PASS로 바꾸지 않는다.

별도 5파일 E: `fixture-overlay`는 **PREP ONLY / NOT TESTED**이며 N_CLOSE/FINAL_CLOSE raw observation과 실제 업로드 lifecycle counts 비교, PHS2 조회/F3 lease 발급 설명, guest evidence를 쓸 때 `guestFilesWritten=true`를 반영했다. 기존 D controls/docs는 변경하지 않았다. 이 overlay만으로 native 실행은 준비되지 않는다. Main이 실제 Machine anchor·기존 DNS/TLS/authority·두 pending operation grant·Linux40/Label25 provider·새 source binding과 Stage/Start/Export/Main 및 native input/F3/normal-close/interruption/reopen/EOF·producer metadata capture/transport를 연결해야 한다. 실제 backend/security grant는 수행하지 않았다. N/I의 같은 store/key/member/hash/lease와 기존 실패·미실행을 유지하며 Ready **0/6**, FULL/설치/통합은 **NOT TESTED**다.

<a id="saveroot13-evidence"></a>
### SaveRoot13 · 실제 저장 경로 회귀의 한정 수용

Main은 **2026-09-07 13:09:40Z**에 [실제 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/MAIN-SaveRoot13-READBACK.json)을 수용했다. 2,556,398 bytes, SHA256 `5de446ed86984329e99c31709873efb88b998eecc0d72b836a380d048da450c2`다. [이번 독립 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-actual-source-close/REVIEW-AND-UPDATE.md)는 기존 원시 JUnit·hook trace·로그·process journal·해시를 읽었으며 새 VM 관측이나 실행은 하지 않았다. packet 준비·독립/Main 검토가 끝난 뒤의 실제 실행 결과이며 준비 보고의 당시 NOT TESTED 기록은 보존한다.

| 근거 축 | 입증된 정확한 범위 |
| --- | --- |
| 소스·환경 | HEAD `3f535c97086129862e5c4699f0d4d0b3b69152a7`의 dirty 소스. app SHA256 `a205c587c3ca0a518f80410729a4e6d28bb9c267282d21aa2957c1cb780bece5`, guard `5a9bf7a5d235896c0160c192cc80f6696cf28d2af37d7807b94eb700f7485c97`, test `160dd2166b4cd04db1c01104b502de108e2b3c01619004f0c96cefacbe3078b2`. VM01 `8d9fd433-02bb-4609-93fa-f992ef8a1838`, host E:에 있는 guest C:의 `C:\Qualification\vm-test-g6\label-save-root-05\source` 110개 파일. 기존 `label-b1-f3-01\venv\Scripts\python.exe`의 x64 Python 3.12.10·pytest 9.0.2, 22 versions/19 imports와 provider closure 일치 |
| 실제 선택·결과 | [SELECTION](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/control/SELECTION.json)의 순서대로 **13 collected / 13 PASS, stdout 1.00s, 39 contiguous setup/call/teardown 모두 PASS**, JUnit failure/error/skip 0. custom 우선 1, missing/empty/whitespace/null × default/env의 실제 writer resolver 대조 8, conflicting env의 native 중복 callback 제외·재진입 1, 기존 config/default 1·key 정규화 1·native 소유/해제 1 |
| 종료·원본 보존 | controller 1688·dependency 3400·pytest 3392의 PID/birth·자연 종료 0·stream 길이 일치. Main native 33844는 birth `13:05:50.1943144Z` → 자연 종료 0 `13:07:30.9324149Z`, retained handle·dual EOF 791/0 bytes. 안정된 export 17개와 Main의 guest 8,356개/host 17개 모두 일치, source/provider closure·지정 desktop 일치, active owned 0, task Ready/result 0/XML 일치, evidenceComplete=true |
| stderr | pytest 226 bytes는 정확히 두 debug 경로 안내 줄, dependency 187 bytes는 pinned tkcalendar warning 1회, controller stderr 0 bytes. 다른 stderr를 무시하거나 warning을 억제한 결과가 아님 |

**PROVEN은 안정된 settings에서 기존 custom 우선 선택·nullable fallback·동일 lexical 저장 경로의 native callback 제외/해제 범위다.** Tk를 구성하거나 두 process의 실제 GUI writer·파일 쓰기·중단 복구를 실행한 시험은 아니다. source/source 및 packaged/source의 중복 writer 제외, 같은 C의 pending identity 복구, onboarding/relay A/C의 enqueue 이전 CSV 발견, template/alias/settings race는 [LM-B10](BACKLOG.md#lm-b10)에 남는다. VM01 Label 예약은 Main이 해제했고 이 문서는 다음 실행 권한을 부여하지 않는다. `baselinePassClaim=false`, `fullExecuted=false`, `qualification=false`이며 generator/build/FULL/최종 설치 자격·제품 Ready를 입증하지 않는다.

기존 Label Modules3 **313 PASS / 939 ordered phase PASS**의 수탁 수용과 원래 reader의 실패는 [README 증거 경계](README.md#evidence-scope)·[중앙 준비도](../../../Program_Spec_Hub/READINESS.md)에 보존한다. 이 숫자를 새 guard 또는 V01–V08 전체 통과로 바꾸지 않는다. 향후 근거에는 수행 시각, exact 소스·테스트·artifact·provider, 환경/설정·입력/관측, 원본 경로·실패/미실행·적용 한계를 남긴다.
