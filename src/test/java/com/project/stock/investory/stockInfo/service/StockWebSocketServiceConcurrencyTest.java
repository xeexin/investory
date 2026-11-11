package com.project.stock.investory.stockInfo.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.project.stock.investory.stockInfo.dto.RealTimeTradeDTO;
import com.project.stock.investory.stockInfo.repository.StockRepository;
import com.project.stock.investory.stockInfo.websocket.KisWebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * StockWebSocketService 동시성 테스트.
 *
 * - KIS WebSocket 클라이언트는 Mockito mock 으로 대체한다 (실제 네트워크 사용 안 함).
 *   mock 은 queueSubscribe / queueUnsubscribe 호출을 "외부(KIS) 구독 상태" Set 에 반영한다.
 * - SSE 연결은 실제로 아무것도 보내지 않는 {@link NoopEmitter} 로 대체한다.
 * - 각 시나리오를 {@value #REPEAT}회 반복하고 실패 회차 수를 센다. 결과는 "[RESULT]" 줄로 출력된다.
 */
class StockWebSocketServiceConcurrencyTest {

    private static final int REPEAT = 1_000;
    private static final String STOCK = "005930";

    /** 시나리오 1 설정 */
    private static final int BASE_SUBSCRIBERS = 50;
    private static final int CHURN_THREADS = 4;
    private static final int CHURN_CYCLES = 500;

    private ExecutorService pool;

    @BeforeAll
    static void quietLogs() {
        // 서비스의 구독/해지 info 로그가 수십만 줄 찍히지 않도록 테스트 중에는 WARN 이상만 출력
        ((Logger) LoggerFactory.getLogger("com.project.stock")).setLevel(Level.WARN);
    }

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(CHURN_THREADS + 1);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 1. 팬아웃 중 목록 변경                                       */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("팬아웃 중 같은 종목에 구독/해지가 반복돼도 예외가 나지 않는다")
    void fanOutWhileSubscribersChange() throws Exception {
        RealTimeTradeDTO dto = new RealTimeTradeDTO(STOCK, "80000", "10", "1.2%", "1000", "093000");

        int failedRuns = 0;          // 예외가 1건 이상 난 회차
        long totalExceptions = 0;    // 전체 예외 건수
        int sizeMismatchRuns = 0;    // 끝난 뒤 구독자 수가 50명이 아닌 회차
        long totalFanOuts = 0;

        for (int run = 0; run < REPEAT; run++) {
            Set<String> external = ConcurrentHashMap.newKeySet();
            StockWebSocketService service = newService(external);
            for (int i = 0; i < BASE_SUBSCRIBERS; i++) {
                service.register(STOCK, new NoopEmitter());
            }

            CountDownLatch ready = new CountDownLatch(CHURN_THREADS + 1);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch churnDone = new CountDownLatch(CHURN_THREADS);
            AtomicInteger exceptions = new AtomicInteger();
            AtomicInteger fanOuts = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();

            // 팬아웃 스레드: 구독/해지 스레드가 모두 끝날 때까지 fanOut 반복
            futures.add(pool.submit(() -> {
                ready.countDown();
                await(start);
                while (churnDone.getCount() > 0) {
                    try {
                        service.fanOut(STOCK, dto);
                        fanOuts.incrementAndGet();
                    } catch (Throwable t) {
                        exceptions.incrementAndGet();
                    }
                }
            }));

            // 구독/해지 스레드: 같은 종목에 새 구독자 등록 → 해지를 반복
            for (int t = 0; t < CHURN_THREADS; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    await(start);
                    try {
                        for (int c = 0; c < CHURN_CYCLES; c++) {
                            SseEmitter e = new NoopEmitter();
                            try {
                                service.register(STOCK, e);
                                service.removeEmitterOnly(STOCK, e);
                            } catch (Throwable ex) {
                                exceptions.incrementAndGet();
                            }
                        }
                    } finally {
                        churnDone.countDown();
                    }
                }));
            }

            ready.await();
            start.countDown();          // 모든 스레드 동시 출발
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }

            if (exceptions.get() > 0) failedRuns++;
            totalExceptions += exceptions.get();
            totalFanOuts += fanOuts.get();
            List<SseEmitter> list = emitters(service).get(STOCK);
            if (list == null || list.size() != BASE_SUBSCRIBERS) sizeMismatchRuns++;
        }

        System.out.printf("[RESULT] scenario=fanOut runs=%d failedRuns=%d totalExceptions=%d sizeMismatchRuns=%d avgFanOutsPerRun=%d%n",
                REPEAT, failedRuns, totalExceptions, sizeMismatchRuns, totalFanOuts / REPEAT);

        assertEquals(0, failedRuns, "팬아웃 중 예외가 발생한 회차 수");
        assertEquals(0, sizeMismatchRuns, "끝난 뒤 구독자 수가 50명이 아닌 회차 수");
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 2. 마지막 퇴장과 신규 입장 경합                              */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("마지막 구독자 퇴장과 신규 입장이 겹쳐도 새 구독자가 남고 외부 구독 상태와 목록이 일치한다")
    void lastLeaveRacesWithNewJoin() throws Exception {
        int failedRuns = 0;
        int missingNewSubscriber = 0;   // 새 구독자가 목록에 없음
        int externalMismatch = 0;       // 외부(KIS) 구독 상태 ≠ 목록 상태
        int exceptionRuns = 0;          // 퇴장·입장 스레드 중 예외가 난 회차

        for (int run = 0; run < REPEAT; run++) {
            Set<String> external = ConcurrentHashMap.newKeySet();
            StockWebSocketService service = newService(external);

            SseEmitter last = new NoopEmitter();
            SseEmitter newcomer = new NoopEmitter();
            service.register(STOCK, last);   // 구독자 1명, 외부 구독 중

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            Future<?> leave = pool.submit(() -> {
                ready.countDown();
                await(start);
                service.removeEmitterOnly(STOCK, last);
            });
            Future<?> join = pool.submit(() -> {
                ready.countDown();
                await(start);
                service.register(STOCK, newcomer);
            });

            ready.await();
            start.countDown();
            // 경합 중 예외(예: 등록 직후 목록이 지워져 NPE)도 실패 회차로 센다 — 중단하지 않고 집계를 끝까지 낸다
            boolean threw = false;
            for (Future<?> f : List.of(leave, join)) {
                try {
                    f.get(5, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    threw = true;
                }
            }

            List<SseEmitter> list = emitters(service).get(STOCK);
            boolean listHasSubscribers = list != null && !list.isEmpty();
            boolean newcomerPresent = list != null && list.contains(newcomer);
            boolean consistent = external.contains(STOCK) == listHasSubscribers
                    && subscribed(service).contains(STOCK) == external.contains(STOCK);

            if (threw) exceptionRuns++;
            if (!newcomerPresent) missingNewSubscriber++;
            if (!consistent) externalMismatch++;
            if (threw || !newcomerPresent || !consistent) failedRuns++;
        }

        System.out.printf("[RESULT] scenario=lastLeaveVsJoin runs=%d failedRuns=%d missingNewSubscriber=%d externalMismatch=%d exceptionRuns=%d%n",
                REPEAT, failedRuns, missingNewSubscriber, externalMismatch, exceptionRuns);

        assertEquals(0, failedRuns, "새 구독자 누락 또는 외부 구독 상태 불일치가 발생한 회차 수");
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 3. 종목별 독립성                                            */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("A 종목 락을 잡고 있어도 B 종목 구독은 기다리지 않고 끝난다")
    void otherStockDoesNotWait() throws Exception {
        int bBlockedRuns = 0;     // B 구독이 시간 안에 끝나지 않은 회차 (실패)
        int aNotBlockedRuns = 0;  // 대조군: A 구독이 A 락을 무시하고 끝난 회차
        long maxBMicros = 0;

        for (int run = 0; run < REPEAT; run++) {
            StockWebSocketService service = newService(ConcurrentHashMap.newKeySet());
            ReentrantLock lockA = service.lockOf("A");

            lockA.lock();             // 테스트 스레드가 A 종목 락을 쥔 상태
            Future<?> aJoin;
            try {
                long t0 = System.nanoTime();
                Future<?> bJoin = pool.submit(() -> service.register("B", new NoopEmitter()));
                try {
                    bJoin.get(1, TimeUnit.SECONDS);
                    maxBMicros = Math.max(maxBMicros, (System.nanoTime() - t0) / 1_000);
                } catch (TimeoutException e) {
                    bBlockedRuns++;
                }

                // 대조군: 같은 A 종목 구독은 락 때문에 기다려야 정상
                aJoin = pool.submit(() -> service.register("A", new NoopEmitter()));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (!lockA.hasQueuedThreads() && !aJoin.isDone() && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                if (aJoin.isDone()) aNotBlockedRuns++;
            } finally {
                lockA.unlock();
            }
            aJoin.get(5, TimeUnit.SECONDS);
        }

        System.out.printf("[RESULT] scenario=independence runs=%d failedRuns=%d aNotBlockedRuns=%d maxBMicros=%d%n",
                REPEAT, bBlockedRuns, aNotBlockedRuns, maxBMicros);

        assertEquals(0, bBlockedRuns, "A 락 때문에 B 구독이 기다린 회차 수");
        assertEquals(0, aNotBlockedRuns, "대조군: A 구독이 A 락에 막히지 않은 회차 수 (락이 실제로 걸리는지 확인)");
    }

    /* ------------------------------------------------------------------ */
    /* 헬퍼                                                                 */
    /* ------------------------------------------------------------------ */

    /** KIS 클라이언트 mock: 구독/해지 호출을 external Set 에 반영 */
    private static StockWebSocketService newService(Set<String> external) {
        KisWebSocketClient kis = mock(KisWebSocketClient.class, withSettings().stubOnly());
        doAnswer(inv -> external.add(inv.getArgument(0)))
                .when(kis).queueSubscribe(anyString(), any());
        doAnswer(inv -> external.remove(inv.getArgument(0)))
                .when(kis).queueUnsubscribe(anyString());
        return new StockWebSocketService(kis, mock(StockRepository.class));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<SseEmitter>> emitters(StockWebSocketService service) {
        return (Map<String, List<SseEmitter>>) ReflectionTestUtils.getField(service, "emitters");
    }

    @SuppressWarnings("unchecked")
    private static Set<String> subscribed(StockWebSocketService service) {
        return (Set<String>) ReflectionTestUtils.getField(service, "subscribed");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 네트워크 없이 send 를 무시하는 SSE 연결 */
    private static class NoopEmitter extends SseEmitter {
        @Override
        public void send(SseEventBuilder builder) {
            // no-op
        }
    }
}
