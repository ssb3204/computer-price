# 파이프라인 함수 흐름도

## 전체 파이프라인 (Step 1 ~ Step 6)

```mermaid
flowchart TD
    GHA([GitHub Actions<br/>00·05·10·15시 KST])
    RUN["run_pipeline.py<br/>main()"]

    subgraph S1["Step 1: 크롤링"]
        CAS["crawl_all_sites()"]
        DC["DanawaCrawler.crawl_raw()"]
        CC["CompuzoneCrawler.crawl_raw()"]
        PC["PCEstimateCrawler.crawl_raw()"]
        LW1["_load_watch_products()"]
        LW2["_load_watch_products()"]
        LW3["_load_watch_products()"]
    end

    subgraph S2["Step 2: Raw 적재"]
        LR["load_raw(all_raw)<br/>MERGE INSERT"]
    end

    subgraph S3["Step 3: 가공"]
        TS["transform_staging()<br/>Stream 소비 + MERGE"]
    end

    subgraph S35["Step 3.5: 품질검증"]
        QL["check_layer_consistency()<br/>손실률 10% 초과 체크"]
        QC["check_cross_site_prices()<br/>사이트 간 편차 20% 초과 체크"]
    end

    subgraph S4["Step 4: 변경 감지"]
        DET["detect_changes()<br/>LAG + MIN/MAX_EVER"]
    end

    subgraph S5["Step 5: Slack"]
        SF["send_slack_failures()<br/>크롤링 실패만 전송"]
    end

    subgraph S6["Step 6: 집계"]
        AA["aggregate_analytics()<br/>3개 MERGE 실행"]
    end

    WL[("STAGING.WATCHLIST")]
    RAW[("RAW.CRAWLED_PRICES")]
    STREAM{{"RAW.CRAWLED_PRICES_STREAM<br/>책갈피"}}
    TF[("RAW.TRANSFORM_FAILURES")]
    PROD[("STAGING.PRODUCTS")]
    PH[("STAGING.PRICE_HISTORY")]
    ALERTS[("STAGING.PRICE_ALERTS")]
    DAILY[("ANALYTICS.DAILY_PRICE_STATS")]
    WEEKLY[("ANALYTICS.WEEKLY_PRICE_STATS")]
    PSTATS[("ANALYTICS.PRODUCT_STATS")]
    SLACK[/"Slack Webhook"/]

    GHA --> RUN
    RUN --> CAS
    CAS --> DC & CC & PC
    DC --> LW1 --> WL
    CC --> LW2 --> WL
    PC --> LW3 --> WL
    DC & CC & PC -->|"list RawCrawledPrice 반환"| CAS

    CAS -->|"all_raw 리스트"| LR
    LR --> RAW
    RAW -.->|"INSERT 감지"| STREAM

    RUN --> TS
    STREAM -->|"새 행만 읽음"| TS
    TS -->|"성공"| PROD
    TS -->|"성공"| PH
    TS -->|"실패 이유 함께"| TF

    RUN --> QL & QC
    QL -.->|"읽기"| RAW
    QL -.->|"읽기"| PH
    QC -.->|"읽기"| PH
    QL -->|"이상 시"| SLACK
    QC -->|"이상 시"| SLACK

    RUN --> DET
    DET -.->|"LAG 비교"| PH
    DET -.->|"MIN/MAX_EVER 참조"| PSTATS
    DET --> ALERTS

    RUN --> SF
    SF -->|"실패 목록"| SLACK

    RUN --> AA
    AA -.->|"GROUP BY 집계"| PH
    AA --> DAILY & WEEKLY & PSTATS

    classDef raw fill:#dbeafe,stroke:#2563eb
    classDef staging fill:#fef3c7,stroke:#d97706
    classDef analytics fill:#dcfce7,stroke:#16a34a
    classDef external fill:#f3e8ff,stroke:#9333ea
    class RAW,STREAM,TF raw
    class WL,PROD,PH,ALERTS staging
    class DAILY,WEEKLY,PSTATS analytics
    class SLACK,GHA external
```

---

## detect_changes() 함수 내부 흐름

```mermaid
flowchart LR
    PH[("STAGING.PRICE_HISTORY<br/>모든 가격 이력")]
    PS[("ANALYTICS.PRODUCT_STATS<br/>역대 최저/최고가")]
    ALERTS[("STAGING.PRICE_ALERTS<br/>알림 결과")]

    LAG["LAG() 윈도우 함수<br/>PARTITION BY PRODUCT_ID<br/>ORDER BY CRAWLED_AT"]
    FILTER1{"rn = 1<br/>(상품별 최신 행만)"}
    FILTER2{"변동률<br/>1% ~ 70%"}
    FILTER3{"NOT EXISTS<br/>(중복 알림 제외)"}
    JUDGE{"alert_type 판정"}

    PH --> LAG
    LAG --> FILTER1
    FILTER1 --> FILTER2
    FILTER2 --> FILTER3
    PS -.->|"MIN_PRICE_EVER<br/>MAX_PRICE_EVER"| JUDGE
    FILTER3 --> JUDGE

    JUDGE -->|"new_price < MIN_EVER"| NL["NEW_LOW"]
    JUDGE -->|"new_price > MAX_EVER"| NH["NEW_HIGH"]
    JUDGE -->|"변동률 ≤ -5%"| PD["PRICE_DROP"]
    JUDGE -->|"변동률 ≥ +10%"| PSPK["PRICE_SPIKE"]

    NL & NH & PD & PSPK --> ALERTS

    classDef table fill:#fef3c7,stroke:#d97706
    classDef analytics fill:#dcfce7,stroke:#16a34a
    classDef judge fill:#fce7f3,stroke:#db2777
    class PH,ALERTS table
    class PS analytics
    class JUDGE,FILTER1,FILTER2,FILTER3 judge
```

---

## crawl_all_sites() 함수 내부 흐름

```mermaid
flowchart TD
    START["crawl_all_sites(settings)"]

    LOOP{"for crawler_cls in<br/>(Danawa, Compuzone, PCEstimate)"}
    CONN["MySQL 연결 생성<br/>(크롤러마다 새로)"]
    MAKE["crawler_cls(conn=conn)"]
    CLOSE["연결 종료"]

    CALL["crawler.crawl_raw()"]
    LWP["_load_watch_products()<br/>WATCHLIST에서 활성 상품 조회"]
    FETCH["_fetch_with_retry(url)<br/>HTTP 요청 + 재시도"]
    PARSE["BeautifulSoup 파싱<br/>_is_real_product / _extract_*"]
    RAW_OBJ["RawCrawledPrice 생성"]

    OK["all_raw.extend(raw_prices)"]
    FAIL["crawl_failures.append(...)"]

    RET["return (all_raw, crawl_failures)"]

    WL[("stg_watchlist")]
    SITE[/"다나와/컴퓨존/견적왕<br/>웹사이트"/]

    START --> LOOP
    LOOP --> CONN --> MAKE --> CALL
    CALL --> LWP
    LWP -.-> WL
    LWP --> FETCH
    FETCH -.-> SITE
    FETCH --> PARSE
    PARSE --> RAW_OBJ
    RAW_OBJ --> CALL
    CALL -->|"성공"| OK
    CALL -->|"예외 발생 or 0건"| FAIL
    OK --> CLOSE
    FAIL --> CLOSE
    CLOSE --> LOOP
    LOOP -->|"3개 끝"| RET
```

> 커넥션을 크롤러 하나마다 열고 닫는 이유: 커넥션의 유일한 사용처는 `_load_watch_products()`
> 뿐이라, 하나를 3개 크롤러에 걸쳐 재사용하면 앞선 사이트가 무응답으로 수 분간 붙들려 있는
> 동안 유휴 커넥션이 끊겨 뒤 사이트가 `(2006) MySQL server has gone away`로 죽는다.
> (2026-07-28 장애)

---

## 테이블별 읽기/쓰기 매트릭스

| 함수 | 읽기 | 쓰기 |
|---|---|---|
| `crawl_all_sites()` | `STAGING.WATCHLIST` | — |
| `load_raw()` | — | `RAW.CRAWLED_PRICES` |
| `transform_staging()` | `RAW.CRAWLED_PRICES_STREAM` | `STAGING.PRODUCTS`<br/>`STAGING.PRICE_HISTORY`<br/>`RAW.TRANSFORM_FAILURES` |
| `check_layer_consistency()` | `RAW.CRAWLED_PRICES`<br/>`STAGING.PRICE_HISTORY`<br/>`ANALYTICS.PRODUCT_STATS` | (Slack만) |
| `check_cross_site_prices()` | `STAGING.PRICE_HISTORY`<br/>`STAGING.PRODUCTS` | (Slack만) |
| `detect_changes()` | `STAGING.PRICE_HISTORY`<br/>`ANALYTICS.PRODUCT_STATS` | `STAGING.PRICE_ALERTS` |
| `send_slack_failures()` | (`crawl_failures` 변수) | (Slack만) |
| `aggregate_analytics()` | `STAGING.PRICE_HISTORY` | `ANALYTICS.DAILY_PRICE_STATS`<br/>`ANALYTICS.WEEKLY_PRICE_STATS`<br/>`ANALYTICS.PRODUCT_STATS` |

---

## 색상 범례

- 🟦 **파랑** — RAW 스키마 (원본 데이터)
- 🟨 **노랑** — STAGING 스키마 (가공 데이터)
- 🟩 **초록** — ANALYTICS 스키마 (집계 데이터)
- 🟪 **보라** — 외부 시스템 (GitHub Actions, Slack, 웹사이트)
