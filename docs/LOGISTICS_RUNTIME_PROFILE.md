# Label Match 중앙 물류 PC 프로필

중앙 포장 원장을 필수로 운영하는 PC는 공통 machine profile v1을 설치한다. 기본 위치는
`%ProgramData%\KMTech\Logistics\runtime-profile.json`이다. JSON에는 토큰을 저장하지 않고
`bearer_token_ref=dpapi:secrets/bearer-token.dpapi`만 기록한다. 토큰은 Windows
machine-scope DPAPI blob이며, 설치 폴더 ACL은 SYSTEM/Administrators 전체 권한과 지정
포장 작업 계정 읽기 권한만 남긴다.

관리자 PowerShell 예시:

```powershell
$secureToken = Read-Host 'PC 전용 bearer token' -AsSecureString
$tokenPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
try {
  $env:KM_LOGISTICS_INSTALL_BEARER_TOKEN = `
    [Runtime.InteropServices.Marshal]::PtrToStringBSTR($tokenPtr)
  .\KMTech_Logistics_Profile_Install.exe --base-url https://worker.example.com `
    --authority-scope PLANT-01 --authority-epoch 7 --plane-epoch 3 `
    --device-id LABEL-PC-01 --source-host-id LABEL-PC-01 `
    --reader-principal 'KMTECH\packaging-operator' --dry-run
  .\KMTech_Logistics_Profile_Install.exe --base-url https://worker.example.com `
    --authority-scope PLANT-01 --authority-epoch 7 --plane-epoch 3 `
    --device-id LABEL-PC-01 --source-host-id LABEL-PC-01 `
    --reader-principal 'KMTECH\packaging-operator'
} finally {
  Remove-Item Env:KM_LOGISTICS_INSTALL_BEARER_TOKEN -ErrorAction SilentlyContinue
  [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPtr)
}
[Environment]::SetEnvironmentVariable(
  'KM_LOGISTICS_PROFILE_PATH',
  'C:\ProgramData\KMTech\Logistics\runtime-profile.json',
  'Machine'
)
[Environment]::SetEnvironmentVariable('KM_LOGISTICS_REQUIRED', '1', 'Machine')
.\KMTech_Logistics_Profile_Check.exe
```

`KM_LOGISTICS_PROFILE_PATH`와 `KM_LOGISTICS_REQUIRED`는 반드시 `/M` Machine 값으로
같이 설치한다. 둘 중 하나라도 Machine에 있으면 동명 process 값은 사용하지 않는다.
토큰을 명령줄, JSON, 로그, report에 넣지 않는다. 회전은 `--replace`로 명시한다.

`KM_LOGISTICS_REQUIRED=1`에서는 프로필·HTTPS·authority·인증 capability를 Tk 시작 전에
검증한다. 하나라도 실패하면 포장을 시작하지 않는다. 실행 도중 중앙 client가 없더라도
`LEGACY_DIRECT_SYNC_ONLY` 또는 수기 완료로 내려가지 않고
`AUTHORITATIVE_LOGISTICS_REQUIRED`로 차단한다. 기존 앱별 환경변수는 비필수 모드에서만
호환된다.

## 10~30대 전환 순서

1. 서버의 scope/epoch와 PC별 token, `device_id`, `source_host_id`, 승인 작업 계정을
   확정한다. PC 식별자는 중복시키지 않는다.
2. 1대에서 dry-run, 실제 설치, Check, 포장 1건과 sealed 이적 제품 교체 1건을 확인한다.
3. 2~3대에서 동시 포장을 수행하고 같은 sealed 이적 또는 같은 교체 donor를 경쟁시킨다.
   중앙 CAS에서 한 요청만 승인되고 나머지는 stale/충돌로 차단되어야 한다.
4. 5대 단위로 배포하고 토큰이 아닌 PC ID, scope/epoch, Check 결과만 증적으로 남긴다.
5. 전체 전환 뒤 음수 재고, 중복 active owner, 구 seal 재사용, idempotency receipt 누락,
   package 이후 교체 승인 건수가 모두 0인지 확인한다.

토큰이나 epoch를 회전할 때만 `--replace`를 사용한다. 교체 뒤 Check가 실패하면 앱을
시작하지 않는다. DPAPI 파일 삭제는 승인된 복귀 절차의 마지막 단계다.
