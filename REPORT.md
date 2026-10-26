REJECT · P1 0 · P2 3 · P3 3

범위 256ca67b..cdd5c5f7(2 커밋)와 그 diff 가 부르는 코드만 읽음. 시험·빌드 실행 0. T = cdd5c5f7 의 INSTALL_THIS_PC.ps1.

## 맞는 것
- 파일·control·marker·바로가기: 옮긴 것은 evidence 에서 되옮김, 지운 것은 백업 복사, 같은 해시면 안 씀, 다른 바이트면 덮지 않고 충돌(T:2387-2406). 자료 폴더는 백업·복원 대상 밖.
- 신원 검사(변조·다른 manifest·미등록 코드): 계획(T:2461)이 작업 삭제보다 앞으로 와 거부가 변경 0 에서 남.
- 되돌림 실패: rollback.json(ROLLBACK_FAILED)·안내·exit 1(T:2434-2440, :59).
- Run: 공개 제거는 읽기만(T:2079) → 되돌릴 쓰기·재쓰기 0, 기존 성공 계약 그대로.
- 핀: INSTALL_THIS_PC f7ad·fence 7952·writer ps1 c30c·probe 16aa·인벤토리 4a3a 를 head 바이트로 재계산해 일치, 옛 값 0.

## P2
1. 옛 legacy 작업 보상이 자기가 끈 상태를 충돌로 판정. T:1890 이 legacy=$false·enabled 없이 등록 → Remove-OwnedLegacyTask 가 Disable(T:1763) 뒤 120초 대기·변경 검사에서 실패하면 복원은 XML(Enabled) 차이로 REMOVAL_RESTORE_TASK_CONFLICT(T:2422). 작업은 꺼진 채 남고 ROLLBACK_FAILED. T:2423 legacy 가지는 죽은 코드. 옛 설치 정리 길에서 만날 수 있음.
2. 공식 legacy relay 보상이 변경 전에 등록(T:1868). relay 실행 중이면 Disable 은 아무것도 안 바꾸고 STOP_UNPROVEN(T:1675) → Restore 도 같은 정지 검사(T:1705)에서 실패 → 변경 0 인데 rollback.json ROLLBACK_FAILED·「설치 손상」. enabled 같음 확인이 먼저여야 함.
3. 작업 보상 uncovered. 새 시험은 -TaskFolder $null, entry 시험 legacy 장면은 성공만. RegisterTask(…,0,sddl)+Xml 문자열 완전 일치(T:2420-2424)는 기존 관용(T:1246 Register-ScheduledTask -Xml, T:1214 정의 비교)과 달라 SYSTEM 작업 재등록·읽기 일치가 증명 안 됨. 작업 삭제 뒤 실패 장면 시험 0.

## P3
1. 남의 것 메타: 외부 바로가기 부모(Public Desktop·Common Programs)가 saved.directories 에 들어가 실패마다 시각·속성을 다시 씀(T:2356, :2427). 'KMTech 포장.lnk' 은 검증(T:2482) 전 등록(T:2464) → 남의 파일이 그 사이 바뀌면 거짓 충돌, 지워지면 재생성.
2. 정확성 틈: 링크 없이 빈 관리용 폴더만 지운 경우(T:157) 복원 0. 작업자 바로가기 기록의 restored 표시(tools/label_worker_shortcuts.ps1:469) 보상 0(재시도엔 무해).
3. .restore-preimage 전체 사본이 성공 뒤에도 남고 Save(T:2468, try 밖) 실패 때 부분 사본 남음 → evidence 2배·재시도 누적. 잠긴 미변경 파일도 T:2392 예외로 손상 판정, tests/test_removal_content_ownership.py:313 은 원인 사슬 어디든 허용으로 약해짐.
