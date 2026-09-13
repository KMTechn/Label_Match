# Carrier 입력·품목 조회의 현행 계약 (W5-B2 / LM-B)

기준은 `a6253d1`의 호출자 동작이다. compact carrier는 제품 barcode parser나 표시용 parser가 아니며, 이 경계는 새 정책·설정·schema를 활성화하지 않는다. 기록 벡터와 고정 기준 함수는 [동등성 시험](../../tests/test_carrier_identity_port.py), [기준 fixture](../../tests/fixtures/carrier_identity_baseline.py)에 있다.

| 경로 | 허용·정규화·반환 | 거부·오류 |
|---|---|---|
| 스캔 decode | `str(raw or '').strip()`; 빈값·`\|` 포함·길이 ≤20은 그대로. 나머지는 URL-safe 문자 치환·padding 보완 후 permissive Base64→UTF-8, 결과에 `\|`와 `=`가 모두 있을 때만 채택 | Base64/UTF-8 실패는 원 text. 길이 >20인 비ASCII 단독 text의 Base64 `ValueError`는 전파. bytes도 `str(bytes)`로 처리하며 별도 bytes decode 없음 |
| compact 스캔 | decode 후 정확히 `PHS,SRC,ITG,CLC,LBL,HSH` 6필드. key strip+uppercase, value strip. PHS=`2`, SRC 대소문자 허용→`KMTECH_INPUT_TAG`, HSH 16 ASCII hex→소문자. ITG/CLC/LBL은 case 보존. dict 반환 | `ValueError`: 아래 표의 영어 문구. 추가·순서 변경·빈값·중복·field 내 여러 `=` 거부 |
| workflow raw compact | 위 6필드 결과와 정규화는 동일. 입력 `str(raw or '').strip()`만, Base64 decode 없음 | `PHSLabelWorkflowError`, code=`PHS2_FORMAT_INVALID`, retryable=False, details={}; 아래 한국어 문구 |
| legacy CLC/SPC/PHS | decode 후 `=` 없는 조각 무시, 첫 `=`만 분리, key uppercase, 중복은 마지막 값. CLC/SPC/PHS 모두 truthy면 전체 dict 반환. 순서·추가 필드 허용 | 필수값 부재는 None. decoder 예외는 전파 |
| legacy 중앙/INSPECTION | SRC 중앙이면 CLC→ITEM→ITEM_CODE fallback, SPC 없을 때 ITEM_NAME→ITEM→code. CLC=INSPECTION이면 ITEM→ITEM_CODE, SPC/ PHS 기본값, QT 없고 QTY 있으면 QT 보완. 명시 빈 SPC는 `setdefault`로 대체하지 않음 | code/phase/필수값 부재는 None. compact admission의 대체가 아님 |
| legacy 생산일 `6D` | 대소문자 무관 `<gs>`→ASCII GS, 각 field의 case-sensitive `6D` 접두사 뒤 8자리 digit를 `%Y%m%d`로 검증; 첫 날짜를 `%Y-%m-%d` 문자열로 반환. 길이/숫자 조건 미충족은 다음 field 검색 | 날짜 없음은 None. 잘못된 달력 날짜면 이후 field를 보지 않고 caller가 `생산 날짜 추출 오류: {예외}` 출력 후 None |
| Item Code view/lookup | CSV row의 정확한 `Item Code` key로 dict 생성, 중복 key는 마지막 row. 조회는 정확한 dict key, 원 row/default 객체를 그대로 반환. trim/case/NFKC/substring 변환 없음 | 누락은 caller별 None/빈 dict/테스트 품목 default. 필수 열 누락 KeyError는 기존 loader가 처리. 인증 snapshot·CSV I/O·cache gate는 caller 소유 |
| legacy QA sample | raw sample의 빈값/중복부터 거부, 그 뒤 최대 3개. 후속 기존 barcode canonicalization 유지 | `PackageLogisticsError`: `sample_barcodes must be non-empty and unique`, `legacy packaging QA samples cannot exceed three barcodes` |
| source/receipt exact membership | INHERIT_ALL은 source bundle/ITG/hint가 필요하고 exact rescan을 섞지 않음. EXACT_RESCAN은 canonical 결과가 비어 있지 않고 raw 개수와 같아야 함. sample은 membership이 아님 | source unit↔barcode·count/hash·version 및 receipt 명령 identity 검사는 계속 물류 계층 소유. carrier parse 결과만으로 membership을 승인하지 않음 |

| compact 거부 조건 (검사 순서) | 스캔 ValueError 문구 | workflow 문구 |
|---|---|---|
| 6필드 아님 | PHS2 must contain exactly six canonical fields | PHS2는 여섯 개의 표준 필드여야 합니다. |
| field의 `=` 개수 ≠1 | PHS2 field syntax is invalid | PHS2 필드 형식이 올바르지 않습니다. |
| 빈 key/value 또는 중복 | PHS2 fields must be non-empty and unique | PHS2 필드는 비어 있거나 중복될 수 없습니다. |
| 순서 다름 | PHS2 fields must be ordered PHS,SRC,ITG,CLC,LBL,HSH | PHS2 필드 순서가 표준과 다릅니다. |
| PHS/SRC 불일치 | only central KMTECH_INPUT_TAG PHS=2 is accepted | 중앙 KMTECH_INPUT_TAG PHS2 형식이 아닙니다. |
| HSH 길이/문자 오류 | PHS2 HSH must be a 16-character hexadecimal prefix | 중앙 KMTECH_INPUT_TAG PHS2 형식이 아닙니다. |

기록 벡터는 정상·순서·필드 syntax·HSH 길이/대소문자·SRC·legacy·인코딩 경계·빈/None·날짜를 포함한다. 생성 시험은 전체 720개 필드 순열의 raw/encoded 입력, 고정 seed의 필드 변이·legacy·날짜를 기준 함수와 비교하며 반환값·예외 타입/문구/코드·진단 출력을 함께 대조한다. 이 시험은 장비·실물·서버 수용을 뜻하지 않는다.

## 호출 경계

| 기존 호출 지점 | `carrier_identity_port` 함수 | caller가 계속 소유하는 책임 |
|---|---|---|
| 앱 `_label_match_decode_possible_base64_label` | `decode_carrier_scan` | 기존 helper 이름과 표시 소비자 유지 |
| 앱 `_label_match_parse_compact_phs2` | `parse_compact_carrier` | 중앙 admission·source 조회·복구 |
| 앱 `_label_match_parse_new_format_fields` / `_parse_new_format_label` | `parse_legacy_fields` | 명시 legacy workflow 선택 |
| workflow `parse_compact_phs2` | `parse_raw_compact_carrier` | 기존 `PHSLabelWorkflowError` 타입을 명시 인자로 공급; receipt·서명·활성화 검증 |
| 앱 `_extract_production_date` | `parse_legacy_production_date` | 진단 출력·None fallback |
| 앱 `_load_items_data` | `item_catalog_view` | 설정/경로·파일·인증 snapshot·CSV decode·예외 UI |
| 앱 입력·표시·시뮬레이션의 `items_data.get` | `item_lookup` | 기존 default와 표시·입력 순서 |
| `PackageCommandDraft.build` QA 검사 | `legacy_qa_sample_error` | 같은 위치에서 기존 `PackageLogisticsError` 발생, 그 뒤 canonicalization |
| 같은 build의 source/exact 검사 | `package_membership_error` | mode 검사와 원 source/receipt exact membership 검증 |

raw workflow parser는 스캔 parser와 오류 생성 방식을 통합하지 않는다. 표시용 `_label_match_display_fields`는 계속 표시만 담당한다. `item_catalog_sync.py`·QR payload·template·크기·hash/HMAC·내구 저장/성공/ACK 순서는 이 추출의 변경 대상이 아니다.
