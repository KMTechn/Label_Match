# Source

User request, 2026-06-27:

> 지금 프로그램과 관련해서 실제로 프로그램 실행부터 실제로 처음 작업하는 사용자의 페르소나로 테스트 코드를 쓰는게 아니라 UI하나씩 써보면서 워크플로우 따라가는데 문제가 없는지 실검증이 필요해 그리고 그 과정을 캡쳐해서 검수 꼼꼼하게 하고 수정할건 수정하고 최종 워크플로우 과정을 이미지 위주로 상세 정리해서 ouline에 올려야하고 먼저 계획을 만들어보자
>
> $loop로 진행해

## Working Goal

Run a real-user UI walkthrough of the Label_Match packaging-room program from startup through representative operator workflows, capture the process, inspect the UI/UX and resulting data carefully, fix meaningful issues found, and update the existing Outline document with an image-heavy final workflow guide.

## Success Conditions

- Use the actual application UI, not only test code, as a first-time operator persona.
- Exercise the workflow from program launch through normal scanning, button flows, error flows, cancellation/recovery, and history lookup.
- Capture screenshots for each representative step and inspect them for operator clarity and broken UI.
- Verify local data/log output corresponds to the UI actions.
- Include security-oriented malicious input checks where they are meaningful for an operator PC.
- Fix meaningful product/UI/data issues found during the walkthrough.
- Update the existing Outline document, not a new document:
  - https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1
- Keep evidence and notes in the repository/run artifacts.
- Do not commit unless the user later gives an explicit `$loop 커밋` instruction.

## Constraints

- Preserve existing user/unrelated dirty work.
- Existing Outline document must remain the primary target.
- Captures should support non-expert operator documentation.
- Prefer real UI operation and screenshot evidence; automated code-only assertions are insufficient by themselves.
- Avoid destructive cleanup of production-like data without a scoped test marker and explicit evidence.
