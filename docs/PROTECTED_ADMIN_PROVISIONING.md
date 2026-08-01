# Label Match 보호 관리자 프로비저닝

이 절차는 Python 소스나 별도 Python 설치 없이 릴리스 폴더만으로 PC별 오프라인 관리자 검증기를 설치한다. 릴리스 폴더의 다음 세 파일을 함께 사용한다.

- `Label_Match_Protected_Admin_Install.exe`
- `PROVISION_PROTECTED_ADMIN_ACL.ps1`
- `PROTECTED_ADMIN_PROVISIONING.md`

## 보안 계약

- 보호 관리자 코드는 명령행 인자, 환경 변수, 응답 파일, 설정 파일, PowerShell transcript 또는 운영 로그로 전달하지 않는다.
- 설치 EXE가 콘솔에서 두 번 숨김 입력을 요청하며, 일치한 값만 메모리에서 verifier로 변환한다.
- 기본 profile은 `C:\ProgramData\KMTech\Label_Match\protected\protected_admin.json`이다. 원문 코드는 저장되지 않는다.
- ACL은 `SYSTEM`과 로컬 `Administrators`에 Full Control, 지정한 단일 작업자 Windows 계정에 Read 권한만 허용한다. 그룹이나 광범위 principal은 허용하지 않는다.
- 설치기는 ACL 적용 후 exact readback을 검증한다. 교체 실패 시 이전 유효 profile을 복원하고, 복원을 증명할 수 없으면 새 profile을 사용할 수 없도록 무효화한다.

## TEST1 dry-run

압축을 푼 릴리스 폴더에서 다음을 실행한다. dry-run은 코드를 묻지 않고 profile을 생성하거나 변경하지 않는다.

```powershell
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File .\PROVISION_PROTECTED_ADMIN_ACL.ps1 -DryRun
```

`protected_admin_provision=PASS mode=dry-run`이 출력되어야 한다.

## 최초 설치

1. Label Match를 실행할 정확한 Windows 사용자 계정을 확인한다. 예: `LINE-PC01\operator`.
2. 관리자 권한 PowerShell을 열고 릴리스 폴더로 이동한다.
3. 다음 명령을 실행한다. 명령에는 보호 관리자 코드를 넣지 않는다.

```powershell
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File .\PROVISION_PROTECTED_ADMIN_ACL.ps1 -ReaderPrincipal 'LINE-PC01\operator'
```

4. EXE가 요청할 때 코드를 두 번 직접 입력한다. 콘솔 transcript, 화면 녹화, 키 입력 자동화는 사용하지 않는다.
5. `protected_admin_provision=PASS mode=installed`를 확인한 뒤 일반 작업자 계정으로 Label Match의 관리자 인증을 현장 검증한다.

## 의도적 재프로비저닝

기존 profile을 교체할 승인된 변경 창에서만 `-Replace`를 추가한다. PC 간 profile 복사, 기존 profile 편집, 공유 계정 또는 그룹 principal 지정은 금지한다.
