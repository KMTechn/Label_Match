# 포장 지원 설치·제거 수명주기

요구 기준은 w9setupprep sub-109/119/122/124, 본보기는 검사 앱 3f96bd2다. 자동 적용의 엄격한 결속은 유지하고 지원 제거의 출구에는 control 기록을 전제하지 않는다.

## 설치·등록

자동 GUI 시작의 onboarding은 transition JSON의 뜻이 같은 다시 쓰기를 허용한다. 관측 변경/진행 중 phase는 writer 허가 밖에서 update lock 아래 한 번 다시 읽고, 미해결이면 현재 판의 기존 ordinary/trial 허가를 재검증하여 UNKNOWN으로 등록을 미룬다. 자동 경합은 재설치 안내 없이 GUI 데이터 guard로 이어지며 수동 설치·등록의 판정과 거절은 그대로다.

설치는 첫/제거 상태 또는 정상 설치 상태에서만 진행한다. 정상은 고정 current owner A, 실제 active A/B, 보호 helper로 검증한 두 stock, 일치하는 pointer/안정 phase, 실제 소유 startup task와 재관측 결과로 판정한다. C는 `versions/<C>`에 배치하고 실제 B를 expected-from으로 다시 확인한 뒤 보호 포인터만 전환한다. 고정 owner A와 키·설정은 보존한다. 중간/손상/불명확 상태는 자료·등록·대기 전송을 보존하고 한국어로 지원 제거 후 재설치를 안내한다.

제거 뒤 current에 파일·directory·reparse 등 항목이 남으면 설치가 이를 덮지 않으며 경로를 알려 멈춘다. 남긴 미상 version/packet은 제거 표식과 보존 경로를 이용해 식별하며 새 C 목적지의 충돌은 별도로 거절한다. 기존 옛 포장 위 첫 등록의 정확한 code/data 부재 판정과 배포 위임 경계를 유지한다.

managed 등록은 actual active code를 사용한다. 새 자료·설정·direct-sync·profile·ledger 및 직접 등록 도구의 출력 경로는 current/versions/update-control/removal-evidence/removed.json과 모든 깊이에서 겹칠 수 없다. 기본 사용자 onboarding 자료는 LOCALAPPDATA/KMTech/Label_Match/data이며 standalone 등록의 ProgramData 자료와 DirectSync 기본 경로는 코드 바깥이다. 사용자 제거는 자기 Run/relay 정지만 수행하고 machine control을 해석하지 않으며 신원·profile·원장·대기 전송을 보존한다.

사용자 제거의 보고와 persistence는 active A/B/C가 아니라 고정 current owner에 결속한다. 재등록은 건강한 actual active를 재확인하고 owner의 canonical manifest·runtime·installer·bootstrap inventory를 확인한다. 정상 제거의 PASS_DATA_PRESERVED·Run/task/relay 부재 및 정확한 stop request ID/SHA가 일치하면 그 표식만 해제한다. 보호 credential handoff와 pinned EXACT_CLONE_RUNTIME_CONFLICT receipt는 기존 검증을 유지하며 owner stock에 결속한다. receipt가 지정되었으나 틀리면 정상 제거로 우회하지 않는다. 부재/UNKNOWN/다른 표식·다른 root는 보존하고, 해제 직전에도 표식 ID/SHA를 다시 확인한다.

## 내용으로 증명한 제거

지원 제거는 현재·판·packet의 readable 목록이 선언한 상대 경로와 SHA가 같은 일반 파일만 개별 이동한다. current/version/packet 디렉터리 통째 이동은 금지한다. 읽을 수 없는 목록·이름만 같은 다른 내용·미상 파일·reparse는 보존하며 하위 reparse를 따라가지 않는다. incoming packet은 manifest의 artifact SHA로만 증명하고 해시가 없는 signature는 보존한다.

목록 메타데이터는 원문 SHA를 기록하고 가장 마지막에 직전 대조 후 이동한다. 선언 파일의 SHA를 읽을 수 없으면 미확정이므로 제거 미완을 알리고 목록과 완료 표식을 유지한다. 이동 전/후에는 SHA와 원 경로 부재를 확인한다. 제거 도중 중단 뒤 다시 실행하면 같은 규칙으로 수렴하며 제거한 code/task를 보상으로 되살리지 않는다.

자기 파일 이동으로 비운 완전히 빈 부모만 비재귀 정리하고 필요한 ReadOnly만 해제한다. 정리 거절은 EMPTY_OWNED_PARENT_RETAINED/retained_paths에 기록한다. 코드 처리가 끝났다면 REMOVED이며 current 잔재가 새 설치와 충돌하면 안내로 멈춘다. 기존 자료·등록·credential·업무 원장·전송 대기는 소유 파일로 분류하지 않는다. 알려진 control 파일은 내용과 관계없이 복구 증거로 개별 격리하고 directory/미상 control 항목은 보존한다.

포장 standalone 지원 제거는 기존 상승 권한과 보호 부모·quiescence를 확인하고 설치된 helper/control을 읽어 실행하지 않는다. canonical caller가 preloaded 위임을 제공하면 기존 정확한 위임을 유지한다. 검사 앱의 keeper/release-owner 자동 제외는 포장에 해당 consumer가 없으므로 추가하지 않는다.

실제 startup task 이름은 root 소문자 + LF + operator SID의 SHA256 앞 24자리다. 정상 API metadata의 SYSTEM/service/highest·한 action·한 SID logon trigger·고정 protected helper 명령과 일치한 작업만 증거를 남기고 중지·삭제·부재 확인한다. 역사적 relay task는 기존 검증 함수를 유지한다. process 관측 불가 또는 제품 process 생존은 제거를 멈춘다. 모든 phase/손상 control에서 제거 출구와 재설치 경계를 시험한다.

## 증거와 남은 수용

headless 독립 clone과 실제 NTFS 파일/잠금/정션 함수 조각만 검증한다. scheduler/CIM/ACL/credential/GUI/relay 기동은 seam을 주입한다. 버전·환경·수치·원 FAILED는 sub-128 REPORT.md와 로그에 보존한다. 규칙 구현과 실제 설치 성공을 구별한다. 동결 뒤 LM 고유 exact 선언·inventory/fence/companion/source digest 갱신, 독립 검토, stock/전체 및 실제 Windows 수용은 중앙 OPEN이다.

후속 sub-130 source 회귀는 REPORT.md와 실제 XML에 연결한다. archive 기반 후보는 clean committed HEAD만 사용하며 DIRTY working tree 결과와 다른 HEAD의 archive 결과를 한 판으로 집계하지 않는다. 선언 대기는 사용자 제거/URL 교정의 GREEN과 분리하며 기대값을 선언에 맞춰 약화하지 않는다.
