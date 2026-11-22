package com.project.stock.investory.stockInfo.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.project.stock.investory.stockInfo.repository.StockRepository;
import com.project.stock.investory.stockInfo.websocket.KisWebSocketClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * 실시간 시세 SSE 구독 남용 시나리오 측정.
 *
 * <p>수정 전/후 코드에 같은 테스트를 돌려 숫자를 비교하기 위한 측정용 테스트다.
 * 결과는 "[RESULT]" 줄로 출력되며, 어느 쪽 코드에서도 실패(assert)하지 않는다.
 *
 * <p>KIS 는 mock 으로 대체한다. 실제 KIS 처럼 세션당 {@value #KIS_REAL_LIMIT}건을 넘는 구독은 거부하고,
 * 거부해도 서버에는 예외가 오지 않는다(실제 코드도 KIS 오류 응답을 로그로만 남긴다).
 * 즉 거부된 종목의 구독자는 "연결은 됐는데 시세가 안 오는" 상태가 된다.
 */
class StockStreamAbuseTest {

    /** KIS 실시간 등록 한도 (세션당) */
    private static final int KIS_REAL_LIMIT = 41;

    private static final String ATTACKER = "10.0.0.66";
    private static final String VICTIM = "10.0.0.7";

    @BeforeAll
    static void quietLogs() {
        ((Logger) LoggerFactory.getLogger("com.project.stock")).setLevel(Level.ERROR);
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 1. 한 클라이언트가 서로 다른 종목 300개를 구독                    */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("한 클라이언트가 종목 300개를 구독한 뒤, 다른 사용자가 새 종목 시세를 받을 수 있는가")
    void oneClientSubscribesManyStocks() {
        FakeKis kis = new FakeKis();
        StockWebSocketService service = kis.service();

        int[] r = subscribeAll(service, ATTACKER, 300, i -> String.format("A%05d", i));

        // 공격 이후 다른 사용자가 처음 보는 종목을 연다
        boolean victimAccepted = tryRegister(service, VICTIM, "VICTIM");
        boolean victimGetsData = kis.active.contains("VICTIM");

        System.out.printf("[RESULT] scenario=manyStocks attempts=300 accepted=%d rejected=%d "
                        + "kisRequested=%d kisActive=%d kisRefused=%d victimAccepted=%b victimGetsData=%b%n",
                r[0], r[1], kis.requested.get(), kis.active.size(), kis.refused.get(),
                victimAccepted, victimGetsData);
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 2. 한 클라이언트가 같은 종목에 연결 200개                        */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("한 클라이언트가 같은 종목에 SSE 연결을 200개 연다")
    void oneClientOpensManyStreams() {
        FakeKis kis = new FakeKis();
        StockWebSocketService service = kis.service();

        List<SseEmitter> opened = new ArrayList<>();
        int accepted = 0, rejected = 0;
        for (int i = 0; i < 200; i++) {
            SseEmitter e = new NoopEmitter();
            if (tryRegister(service, ATTACKER, "005930", e)) {
                accepted++;
                opened.add(e);
            } else {
                rejected++;
            }
        }

        // 연결을 모두 닫으면 슬롯이 돌아와 다시 열 수 있어야 한다 (슬롯 누수 확인)
        opened.forEach(e -> service.removeEmitterOnly("005930", e));
        boolean reopenAfterClose = tryRegister(service, ATTACKER, "005930");

        System.out.printf("[RESULT] scenario=manyStreams attempts=200 accepted=%d rejected=%d "
                + "reopenAfterClose=%b%n", accepted, rejected, reopenAfterClose);
    }

    /* ------------------------------------------------------------------ */
    /* 시나리오 3. 여러 클라이언트가 동시에 서로 다른 새 종목 구독 (경합)          */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("200명이 동시에 서로 다른 새 종목을 구독해도 KIS 구독 수가 한도를 넘지 않는가")
    void concurrentNewStocksRespectKisLimit() throws Exception {
        final int repeat = 200;
        final int clients = 200;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        int overLimitRuns = 0;
        int maxActive = 0;
        try {
            for (int run = 0; run < repeat; run++) {
                FakeKis kis = new FakeKis();
                StockWebSocketService service = kis.service();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> fs = new ArrayList<>();
                for (int c = 0; c < clients; c++) {
                    final int id = c;
                    fs.add(pool.submit(() -> {
                        start.await();
                        tryRegister(service, "10.1.0." + id, String.format("S%05d", id));
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> f : fs) f.get();

                maxActive = Math.max(maxActive, kis.requested.get());
                if (kis.refused.get() > 0) overLimitRuns++;
            }
        } finally {
            pool.shutdownNow();
        }
        System.out.printf("[RESULT] scenario=concurrentNewStocks runs=%d failedRuns=%d maxKisRequested=%d%n",
                repeat, overLimitRuns, maxActive);
    }

    /* ------------------------------------------------------------------ */
    /* helpers                                                             */
    /* ------------------------------------------------------------------ */

    private interface Namer { String name(int i); }

    private static int[] subscribeAll(StockWebSocketService service, String client, int n, Namer namer) {
        int accepted = 0, rejected = 0;
        for (int i = 0; i < n; i++) {
            if (tryRegister(service, client, namer.name(i))) accepted++;
            else rejected++;
        }
        return new int[]{accepted, rejected};
    }

    private static boolean tryRegister(StockWebSocketService service, String client, String stock) {
        return tryRegister(service, client, stock, new NoopEmitter());
    }

    /** 한도 초과 예외(수정 후에만 존재)를 이름으로 의존하지 않도록 RuntimeException 으로 받는다 */
    private static boolean tryRegister(StockWebSocketService service, String client, String stock, SseEmitter e) {
        try {
            service.register(client, stock, e);
            return true;
        } catch (RuntimeException ex) {
            return false;
        }
    }

    /** 세션당 41건 한도를 흉내 내는 KIS mock */
    private static class FakeKis {
        final Set<String> active = ConcurrentHashMap.newKeySet();
        final AtomicInteger requested = new AtomicInteger();
        final AtomicInteger refused = new AtomicInteger();

        @SuppressWarnings("unchecked")
        StockWebSocketService service() {
            KisWebSocketClient kis = mock(KisWebSocketClient.class);
            doAnswer(inv -> {
                requested.incrementAndGet();
                synchronized (active) {
                    if (active.size() >= KIS_REAL_LIMIT) {
                        refused.incrementAndGet();   // KIS: MAX SUBSCRIBE OVER (서버는 로그만 남김)
                    } else {
                        active.add(inv.getArgument(0));
                    }
                }
                return null;
            }).when(kis).queueSubscribe(anyString(), any(java.util.function.Consumer.class));
            doAnswer(inv -> {
                requested.decrementAndGet();
                synchronized (active) {
                    active.remove(inv.<String>getArgument(0));
                }
                return null;
            }).when(kis).queueUnsubscribe(anyString());
            return new StockWebSocketService(kis, mock(StockRepository.class));
        }
    }

    private static class NoopEmitter extends SseEmitter {
        @Override
        public void send(SseEventBuilder builder) {
        }
    }
}
