# 포장 지원 설치·제거 수명주기

요구 기준은 w9setupprep sub-109/119/122/124, 본보기는 검사 앱 3f96bd2다. 자동 적용의 엄격한 결속은 유지하고 지원 제거의 출구에는 control 기록을 전제하지 않는다.

## 설치·등록

설치는 첫/제거 상태 또는 정상 설치 상태에서만 진행한다. 정상은 고정 current owner A, 실제 active A/B, 보호 helper로 검증한 두 stock, 일치하는 pointer/안정 phase, 실제 소유 startup task와 재관측 결과로 판정한다. C는 `versions/<C>`에 배치하고 실제 B를 expected-from으로 다시 확인한 뒤 보호 포인터만 전환한다. 고정 owner A와 키·설정은 보존한다. 중간/손상/불명확 상태는 자료·등록·대기 전송을 보존하고 한국어로 지원 제거 후 재설치를 안내한다.

제거 뒤 current에 파일·directory·reparse 등 항목이 남으면 설치가 이를 덮지 않으며 경로를 알려 멈춘다. 남긴 미상 version/packet은 제거 표식과 보존 경로를 이용해 식별하며 새 C 목적지의 충돌은 별도로 거절한다. 기존 옛 포장 위 첫 등록의 정확한 code/data 부재 판정과 배포 위임 경계를 유지한다.

managed 등록은 actual active code를 사용한다. 새 자료·설정·direct-sync·profile·ledger 및 직접 등록 도구의 출력 경로는 current/versions/update-control/removal-evidence/removed.json과 모든 깊이에서 겹칠 수 없다. 기본 사용자 onboarding 자료는 LOCALAPPDATA/KMTech/Label_Match/data이며 standalone 등록의 ProgramData 자료와 DirectSync 기본 경로는 코드 바깥이다. 사용자 제거는 자기 Run/relay 정지만 수행하고 machine control을 해석하지 않으며 신원·profile·원장·대기 전송을 보존한다.

기계 지원 제거의 파일 내용 증명/수렴은 다음 제거 단위에서 완성한다. 이 단위는 정상 설치와 사용자 제거/등록 경계를 구현한다.

## 증거와 남은 수용

headless 독립 clone과 실제 NTFS 파일/잠금/정션 함수 조각만 검증한다. scheduler/CIM/ACL/credential/GUI/relay 기동은 seam을 주입한다. 버전·환경·수치·원 FAILED는 sub-128 REPORT.md와 로그에 보존한다. 규칙 구현과 실제 설치 성공을 구별한다. 동결 뒤 LM 고유 exact 선언·inventory/fence/companion/source digest 갱신, 독립 검토, stock/전체 및 실제 Windows 수용은 중앙 OPEN이다.
