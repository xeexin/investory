# 실시간 시세 SSE 구독 남용 측정 기록

`StockStreamAbuseTest`를 **수정 전 코드**와 **수정 후 코드**에 똑같이 실행한 기록입니다.

- 측정일: 2026-11-22
- 환경: Apple M3 (8코어), 8GB, macOS 27.2, JDK 21.0.2 (Temurin), Gradle 8.14.2
- KIS 연결: mock. 실제 KIS처럼 **세션당 41건**을 넘는 구독은 거부하고, 거부돼도 서버에는 예외가 오지 않습니다
  (실제 `KisWebSocketClient.handleJson`도 KIS 오류 응답을 로그로만 남깁니다).

## 점검 결과 (수정 전)

| 항목 | 상태 | 근거 |
|---|---|---|
| 비로그인 SSE 접속 | 가능 | `SecurityConfig`: `GET /stock/**` → `permitAll()` |
| 한 사용자의 종목 수 제한 | 없음 | 서비스에 클라이언트 구분 자체가 없음 |
| 한 사용자의 연결 수 제한 | 없음 | 〃 |
| 서버 전체 KIS 종목 수 제한 | 없음 | 41건을 넘으면 KIS가 거부하지만, 서버는 모르고 연결을 유지 |

## 결과

| 시나리오 | 지표 | 수정 전 | 수정 후 |
|---|---|---|---|
| `manyStocks`: 한 IP가 서로 다른 종목 300개 구독 | 수락된 연결 | 300 | **10** (290건 429) |
| | KIS에 보낸 구독 요청 | 301 | **11** |
| | KIS가 거부한 구독 (= 시세가 안 오는 종목) | **260** | 0 |
| | 이후 다른 사용자가 새 종목을 열었을 때 시세 수신 | **못 받음** (연결은 성공) | 받음 |
| `manyStreams`: 한 IP가 같은 종목에 연결 200개 | 수락된 연결 | 200 | **10** (190건 429) |
| | 모두 닫은 뒤 다시 연결 가능 (슬롯 누수 없음) | 가능 | 가능 |
| `concurrentNewStocks`: 200명이 동시에 서로 다른 새 종목 구독, 200회 반복 | KIS 한도를 넘긴 회차 | **200 / 200** | 0 / 200 |
| | 한 회차의 최대 KIS 구독 수 | 200 | 40 |

핵심은 `manyStocks`의 마지막 줄입니다. 수정 전에는 한 사람이 KIS 등록 한도를 다 쓰면,
**그 뒤에 들어온 정상 사용자는 연결은 되는데 시세가 영영 오지 않습니다.** 오류도 나지 않아 원인을 알기 어렵습니다.
수정 후에는 한 IP가 최대 10개까지만 열 수 있고, 서버 전체 종목 수가 40개를 넘으면 새 종목 요청을 503으로 바로 거절합니다.

## 무엇을 바꿨나

- `StockWebSocketService`
  - `MAX_STREAMS_PER_CLIENT = 10`: IP별 동시 SSE 연결 수. `ConcurrentHashMap.compute`로 확인과 증가를 한 번에 처리
  - `MAX_KIS_SUBSCRIPTIONS = 40`: 서버 전체 KIS 종목 수. 종목별 락은 서로 다르므로 전역 한도는 `Semaphore`로 셈
  - 연결이 어떤 경로(timeout, completion, error, 팬아웃 실패)로 끊겨도 슬롯은 한 번만 반환
    (`removeEmitterOnly`가 실제로 제거했을 때만 반환)
  - 클라이언트 카운터는 0이 되면 지워서, IP가 많아져도 맵이 계속 커지지 않음
- `StockStreamController`: `request.getRemoteAddr()`를 클라이언트 키로 전달. `X-Forwarded-For`는 위조할 수 있어서 쓰지 않음
- `SubscriptionLimitException`: 연결 수 초과는 429, KIS 한도 소진은 503

비로그인 접속은 **막지 않았습니다.** 종목 시세 화면은 공개 페이지이고, 브라우저 `EventSource`는
`Authorization` 헤더를 보낼 수 없어서 인증을 걸면 현재 프론트엔드가 동작하지 않습니다.
대신 인증과 상관없이 IP 단위로 제한합니다.

## 비교 대상

- **수정 전:** 커밋 `f90faeb`. 테스트가 `register(clientKey, stockId, emitter)`를 호출할 수 있도록
  `patch_before.py`로 clientKey를 무시하는 오버로드 하나만 추가했습니다. 동작은 바꾸지 않았습니다.
- **수정 후:** 이 커밋.

## 재현 방법

```sh
# 수정 후 (현재 코드)
sh ./gradlew test --tests '*StockStreamAbuseTest' --rerun-tasks -q
grep -ho '\[RESULT\][^<]*' build/test-results/test/TEST-*StockStreamAbuseTest.xml

# 수정 전
git worktree add --detach /tmp/before f90faeb
cp src/test/java/com/project/stock/investory/stockInfo/service/StockStreamAbuseTest.java \
   /tmp/before/src/test/java/com/project/stock/investory/stockInfo/service/
python3 docs/sse-abuse-test/patch_before.py /tmp/before
(cd /tmp/before && sh ./gradlew test --tests '*StockStreamAbuseTest' --rerun-tasks -q)
```

## 원본 출력

수정 전:
```
[RESULT] scenario=manyStocks attempts=300 accepted=300 rejected=0 kisRequested=301 kisActive=41 kisRefused=260 victimAccepted=true victimGetsData=false
[RESULT] scenario=manyStreams attempts=200 accepted=200 rejected=0 reopenAfterClose=true
[RESULT] scenario=concurrentNewStocks runs=200 failedRuns=200 maxKisRequested=200
```

수정 후:
```
[RESULT] scenario=manyStocks attempts=300 accepted=10 rejected=290 kisRequested=11 kisActive=11 kisRefused=0 victimAccepted=true victimGetsData=true
[RESULT] scenario=manyStreams attempts=200 accepted=10 rejected=190 reopenAfterClose=true
[RESULT] scenario=concurrentNewStocks runs=200 failedRuns=0 maxKisRequested=40
```
