package com.project.stock.investory.stockInfo.websocket;

import jakarta.websocket.CloseReason;
import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * KisWebSocketClient 의 구독 상태 복구 / 전송 순서 검증.
 * 실제 KIS 서버 대신 Session·RemoteEndpoint 를 mock 으로 대체한다.
 */
class KisWebSocketClientTest {

    private static final String STOCK = "005930";

    private KisWebSocketClient client;

    @BeforeEach
    void setUp() {
        client = new KisWebSocketClient();  // @PostConstruct(init) 는 호출하지 않음 → 실제 연결 없음
        ReflectionTestUtils.setField(client, "approvalKey", "test-key");
    }

    @AfterEach
    void tearDown() {
        client.destroy();
    }

    private static Session openSession(RemoteEndpoint.Basic remote) {
        Session s = mock(Session.class);
        when(s.isOpen()).thenReturn(true);
        when(s.getBasicRemote()).thenReturn(remote);
        return s;
    }

    private static String subscribeOf(String stockId) {
        return argThat((String msg) -> msg != null && isType(msg, "1") && msg.contains(stockId));
    }

    private static String unsubscribeOf(String stockId) {
        return argThat((String msg) -> msg != null && isType(msg, "2") && msg.contains(stockId));
    }

    @Test
    void 구독_전송_실패시_구독상태를_유지하고_재연결_후_재구독한다() throws Exception {
        // 1) 첫 세션: sendText 가 실패하는 끊어진 연결
        RemoteEndpoint.Basic brokenRemote = mock(RemoteEndpoint.Basic.class);
        doThrow(new IOException("broken pipe")).when(brokenRemote).sendText(anyString());
        Session broken = openSession(brokenRemote);
        client.onOpen(broken);

        client.queueSubscribe(STOCK, dto -> { });

        // 전송 실패 → 세션을 닫아 재연결을 유도해야 한다
        verify(broken, timeout(2_000)).close(any(CloseReason.class));

        // 서비스와 클라이언트의 구독 상태가 어긋나지 않아야 한다 (예전 코드는 여기서 제거됨)
        @SuppressWarnings("unchecked")
        java.util.Set<String> subscribed =
                (java.util.Set<String>) ReflectionTestUtils.getField(client, "subscribed");
        assertThat(subscribed).contains(STOCK);

        // 2) 재연결된 세션: onOpen 에서 기존 구독이 복원되어야 한다
        RemoteEndpoint.Basic healthyRemote = mock(RemoteEndpoint.Basic.class);
        client.onOpen(openSession(healthyRemote));

        verify(healthyRemote, timeout(2_000)).sendText(subscribeOf(STOCK));
    }

    @Test
    void 재연결_재구독은_sender_스레드에서_실행되어_해지된_종목이_다시_구독되지_않는다() throws Exception {
        String keep = "005930";
        String leave = "000660";

        // 초기 연결에서 두 종목 구독
        RemoteEndpoint.Basic firstRemote = mock(RemoteEndpoint.Basic.class);
        client.onOpen(openSession(firstRemote));
        client.queueSubscribe(keep, dto -> { });
        client.queueSubscribe(leave, dto -> { });
        verify(firstRemote, timeout(2_000)).sendText(subscribeOf(keep));
        verify(firstRemote, timeout(2_000)).sendText(subscribeOf(leave));

        // 재연결 세션: 전송 메시지와 스레드를 기록
        List<String> messages = new CopyOnWriteArrayList<>();
        List<String> threads = new CopyOnWriteArrayList<>();
        CountDownLatch unsubSent = new CountDownLatch(1);
        RemoteEndpoint.Basic secondRemote = mock(RemoteEndpoint.Basic.class);
        doAnswer(inv -> {
            String msg = inv.getArgument(0);
            messages.add(msg);
            threads.add(Thread.currentThread().getName());
            if (isType(msg, "2") && msg.contains(leave)) unsubSent.countDown();
            return null;
        }).when(secondRemote).sendText(anyString());

        client.onOpen(openSession(secondRemote));   // 재구독 예약
        client.queueUnsubscribe(leave);              // 재연결 직후 마지막 구독자 이탈

        assertThat(unsubSent.await(2, TimeUnit.SECONDS)).isTrue();
        verify(secondRemote, timeout(2_000)).sendText(subscribeOf(keep));
        Thread.sleep(200);  // 뒤늦은 전송이 있는지 확인할 여유

        // 콜백 스레드가 아닌 단일 sender 스레드에서만 전송되어야 순서가 보장된다
        assertThat(threads).containsOnly("kis-ws-sender");

        // 해지된 종목의 마지막 메시지는 반드시 unsubscribe 여야 한다 (해지 뒤 재구독 금지)
        List<String> leaveMsgs = messages.stream().filter(m -> m.contains(leave)).toList();
        assertThat(isType(leaveMsgs.get(leaveMsgs.size() - 1), "2")).isTrue();
    }

    private static boolean isType(String msg, String trType) {
        return msg.matches(".*\"tr_type\":\\s*\"" + trType + "\".*");
    }

    private static String anyString() {
        return org.mockito.ArgumentMatchers.anyString();
    }
}
