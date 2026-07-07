---
name: crawl-price-sites
description: "다나와/컴퓨존/견적왕 크롤러 개발·수정·디버깅 스킬. 크롤러 버그, HTML 파싱 오류, 광고 필터, 새 사이트 추가, 가격 파싱 실패, 셀렉터 변경, BeautifulSoup 파싱 등 크롤링 관련 모든 작업 시 반드시 이 스킬을 사용."
---

# 크롤러 개발 스킬

다나와, 컴퓨존, 견적왕 크롤러를 개발하고 수정하는 지침.

## 프로젝트 구조

```
src/crawlers/
├── base.py          — BaseCrawler 추상 클래스, DEFAULT_HEADERS
├── danawa.py        — 다나와 크롤러 (pcode 기반)
├── compuzone.py     — 컴퓨존 크롤러
├── pc_estimate.py   — 견적왕 크롤러
└── parser_utils.py  — parse_korean_price, validate_price 공용 유틸
```

## 핵심 패턴

### 1. BaseCrawler 상속

모든 크롤러는 `src/crawlers/base.py`의 `BaseCrawler`를 상속한다. 새 크롤러 작성 전 base.py를 반드시 읽어 인터페이스를 확인한다.

### 2. 광고 필터링 (다나와)

다나와는 실제 상품과 광고가 같은 리스트에 섞여 있다:
- `productItem{숫자}` → 실제 상품 (크롤링 대상)
- `adReaderProductItem*`, `adPointProductItem*` → 광고 (무조건 제외)

`_is_real_product()` 함수를 항상 유지한다. 필터 로직 수정 시 이 규칙을 깨뜨리지 않는다.

### 3. RawCrawledPrice DTO

모든 크롤러의 출력은 `src/common/models.py`의 `RawCrawledPrice` frozen dataclass다:
- 새 필드 추가 전 pipeline-agent에게 알린다 (MySQL UNIQUE 키 영향)
- 가격은 정수(원 단위)로 반환

### 4. 가격 파싱

`parser_utils.parse_korean_price()`를 사용한다. 직접 파싱 로직을 작성하지 않는다.
`validate_price()`로 유효 범위(0 < 가격 < 10,000,000) 확인 후 적재한다.

### 5. WATCHLIST 기반 동적 크롤링

크롤링 대상은 코드에 하드코딩하지 않는다. MySQL stg_watchlist 테이블에서 동적으로 로드한다.

## 작업 절차

1. 기존 크롤러 파일과 `base.py` 읽기
2. 설계 제시 → 사용자 확인 → 구현
3. 단계별 구현 (셀렉터 → 파싱 → 필터 → DTO 변환 순서)
4. 각 단계 완료 후 실제 동작 확인
5. 광고 필터 로직이 유지되는지 최종 확인

## 디버깅 체크리스트

사이트 구조 변경으로 파싱이 깨졌을 때:
- [ ] BeautifulSoup으로 실제 HTML 덤프 확인
- [ ] 대상 셀렉터가 현재 HTML에 존재하는지 확인
- [ ] 광고 필터 정규식이 새 HTML에서도 동작하는지 확인
- [ ] `parse_korean_price` 입력 문자열 형식 변경 여부 확인
