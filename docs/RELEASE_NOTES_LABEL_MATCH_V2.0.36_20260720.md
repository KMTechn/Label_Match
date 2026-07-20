# Label_Match v2.0.36 릴리스 안전 메모

## GitHub Release와 비공개 업데이트 피드 분리

- release workflow는 기본적으로 테스트, 릴리스 신원 검증, Authenticode 서명, 결정적 archive 검증과 GitHub Release 자산 게시만 수행합니다.
- 비공개 update manifest 생성·Ed25519 서명·업로드는 저장소 변수 `ENABLE_PRIVATE_UPDATE_FEED_PUBLISH`가 정확히 `true`일 때만 실행합니다. 변수가 없거나 `false`이면 기존 private feed 관련 변수가 남아 있어도 실행하지 않습니다.
- opt-in이 없으면 패키지의 updater provider는 `github`로 고정됩니다. opt-in 시에는 기존 private manifest URL·공개키, HTTPS artifact URL과 허용 호스트, rollout 범위, signing key, HTTPS upload URL·token·origin IP, archive·checksum 검사를 모두 그대로 적용합니다.
- `true`, `false`, unset 이외의 값은 오타나 대소문자 혼동을 허용하지 않고 workflow를 실패시킵니다.

이 opt-in은 게시 승인을 대신하지 않습니다. 운영 복구와 피드 무결성 검증, 별도 변경 승인 및 현장 게이트가 모두 끝난 경우에만 `true`로 전환해야 합니다. signed annotated tag, self-hosted signing runner, release identity와 Authenticode 요구 조건은 변경되지 않습니다.
