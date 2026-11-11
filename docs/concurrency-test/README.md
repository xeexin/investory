# 실시간 시세 구독 동시성 테스트 측정 기록

`StockWebSocketServiceConcurrencyTest`를 **수정 전 코드**와 **수정 후 코드**에 각각 5회 실행한 기록입니다.
한 번 실행할 때마다 각 시나리오를 1,000회 반복하고, 실패한 회차 수를 셉니다.

- 측정일: 2026-11-11
- 환경: Apple M3 (8코어), 8GB, macOS 27.2.1, JDK 21.0.2 (Temurin), Gradle 8.14.2
- KIS 연결: mock (네트워크·DB 미사용)

## 결과 요약: 마지막 구독자 퇴장 vs 신규 입장 경합 (`lastLeaveVsJoin`)

| 실행 | 수정 전 실패 회차 / 1,000 | 수정 후 실패 회차 / 1,000 |
|---|---|---|
| 1 | 40 | 0 |
| 2 | 12 | 0 |
| 3 | 6 | 0 |
| 4 | 7 | 0 |
| 5 | 107 | 0 |
| **중앙값** | **12** | **0** |
| 범위 | 6 ~ 107 | 0 |
| 합계 | 172 / 5,000 | 0 / 5,000 |

수정 전 실패 172회의 유형 (한 회차에 여러 유형이 함께 나올 수 있음):

- 새 구독자가 목록에서 빠짐: 39회
- 외부(KIS) 구독 상태와 구독자 목록이 어긋남: 133회
- 경합 중 예외 발생: 15회. 등록 직후 다른 스레드가 목록을 지워 `NullPointerException`이 난 경우

경합 테스트라서 실행할 때마다 실패 수가 크게 달라집니다. 그래서 단일 측정값이 아니라 중앙값과 범위를 함께 기록했습니다.

## 다른 시나리오

| 시나리오 | 수정 전 | 수정 후 |
|---|---|---|
| `fanOut`: 팬아웃 중 구독자 추가·삭제 | 5회 모두 0 | 5회 모두 0 |
| `independence`: A 종목 락이 B 종목 구독을 막지 않음 | 0 (B가 막힌 회차) | 0 |

- `fanOut`은 수정 전에도 이미 `CopyOnWriteArrayList`를 쓰고 있어서 차이가 없습니다.
- `independence`의 대조군 `aNotBlockedRuns`는 수정 전 1,000입니다. 수정 전 코드는 구독 경로에서 락을 잡지 않았기 때문입니다.

## 비교 대상

- **수정 전:** 커밋 `e355bcb`. 테스트가 메서드를 호출할 수 있도록 `patch_before.py`로 다음 두 가지만 적용했고, 동작은 바꾸지 않았습니다.
  - 구독 블록을 락 없는 `register()`로 추출
  - `removeEmitterOnly()`, `lockOf()`의 접근 제한자를 package-private으로 변경
- **수정 후:** 커밋 `a4fe3b9` 이후 코드. 구독 경로를 해지 경로와 같은 종목별 `ReentrantLock` 안으로 옮겼습니다.

## 파일

- `before.csv`, `after.csv`: 실행별 결과 (`phase,run,timestamp,scenario,repeats,failed_runs,detail`)
- `before.log`, `after.log`: 테스트가 출력한 원본 `[RESULT]` 줄
- `patch_before.py`: 수정 전 코드에 적용한 최소 패치
- `measure.sh`: 반복 실행·수집 스크립트

## 재현 방법

```sh
# 수정 후 (현재 코드)
sh docs/concurrency-test/measure.sh after . /tmp/out 5

# 수정 전
git worktree add --detach ../investory-before e355bcb
cp src/test/java/com/project/stock/investory/stockInfo/service/StockWebSocketServiceConcurrencyTest.java \
   ../investory-before/src/test/java/com/project/stock/investory/stockInfo/service/
python3 docs/concurrency-test/patch_before.py \
   ../investory-before/src/main/java/com/project/stock/investory/stockInfo/service/StockWebSocketService.java
sh docs/concurrency-test/measure.sh before ../investory-before /tmp/out 5
```
