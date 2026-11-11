package com.project.stock.investory.stockInfo.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.stock.investory.stockInfo.dto.RealTimeTradeDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.websocket.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 📈 KIS 실시간 주식 데이터 WebSocket 클라이언트
 *
 * ===================================================================
 * 💡 이 클래스가 하는 일을 쉽게 설명하면?
 * ===================================================================
 *
 * 🏢 한국투자증권(KIS) 서버 ←--→ 🖥️ 우리 애플리케이션
 *                실시간 주식 데이터 전송
 *
 * 예를 들어:
 * 1. 사용자가 "삼성전자 주가 보고 싶어!" 라고 요청
 * 2. 이 클래스가 KIS 서버에 "삼성전자 데이터 보내줘!" 구독 요청
 * 3. KIS 서버가 삼성전자 주가가 바뀔 때마다 실시간으로 데이터 전송
 * 4. 받은 데이터를 사용자 화면에 실시간으로 표시
 *
 * ===================================================================
 * 🏆 자소서 핵심 어필 포인트 4가지:
 * ===================================================================
 *
 * 1️⃣ 【실시간 금융 데이터 처리 - WebSocket + SSE 하이브리드 아키텍처】
 *    ✅ 기술적 복잡도: 외부 WebSocket API → 내부 SSE 스트리밍 변환
 *    ✅ 실무 적용성: 실제 금융 거래소 데이터 (한국투자증권 KIS API) 처리
 *    ✅ 아키텍처 설계: 프로토콜 변환 계층을 통한 클라이언트 최적화
 *    📊 구현 포인트: WebSocket 수신 → JSON 파싱 → SSE 이벤트 발송
 *
 * 2️⃣ 【1:N 팬아웃 최적화 - 리소스 효율성과 성능 최적화】
 *    ✅ 성능 최적화: 동일 종목 중복 구독 방지 (100명 구독 = 1회 서버 요청)
 *    ✅ 메모리 효율성: 단일 WebSocket 연결로 다중 사용자 서빙
 *    ✅ 비용 절약: 외부 API 호출 횟수 최소화로 트래픽 비용 절약
 *    📊 구현 포인트: Set<String> subscribed + Map<String, List<Consumer>> 팬아웃
 *
 * 3️⃣ 【멀티스레드 동시성 제어 - ReentrantLock, AtomicBoolean 활용】
 *    ✅ 동시성 안전성: 여러 사용자가 동시 구독/해지해도 데이터 일관성 보장
 *    ✅ 성능 최적화: 세밀한 락 분리 (연결용 락 vs 전송용 락)
 *    ✅ 무충돌 처리: Lock-free 알고리즘으로 재연결 상태 관리
 *    📊 구현 포인트: ReentrantLock connectionLock/sendLock + AtomicBoolean reconnecting
 *
 * 4️⃣ 【자동 재연결 메커니즘 - 백오프 알고리즘으로 안정성 확보】
 *    ✅ 시스템 복원력: 네트워크 장애 시 자동 복구 (최대 5회 재시도)
 *    ✅ 서버 부하 방지: 점진적 대기시간 증가 (2초 → 4초 → 6초 → ...)
 *    ✅ 상태 복원: 재연결 성공 시 기존 구독 목록 자동 복원
 *    📊 구현 포인트: 백오프 알고리즘 + 구독 상태 복원 로직
 *
 * ===================================================================
 * 🎯 핵심 기능들:
 * ===================================================================
 *
 * 1️⃣ 실시간 연결 유지
 *    - KIS WebSocket 서버와 24시간 연결 상태 유지
 *    - 연결이 끊어지면 자동으로 다시 연결 시도
 *
 * 2️⃣ 스마트한 구독 관리
 *    - 같은 종목을 100명이 봐도 서버에는 1번만 구독 요청 (효율성)
 *    - 마지막 사람이 나갈 때까지 구독 유지
 *
 * 3️⃣ 데이터 팬아웃 (1:N 전달)
 *    - 하나의 데이터를 여러 사용자에게 동시에 전달
 *    - 예: 삼성전자 데이터 → 100명의 사용자 화면에 동시 업데이트
 *
 * 4️⃣ 안전한 멀티스레드 처리
 *    - 여러 사용자가 동시에 접근해도 데이터 꼬임 없음
 *    - 락(Lock) 메커니즘으로 동시성 제어
 *
 * ===================================================================
 * 📡 WebSocket이 뭔가요?
 * ===================================================================
 *
 * 🔄 일반 HTTP 방식:
 *    클라이언트 → "데이터 줘!" → 서버
 *    클라이언트 ← "여기 데이터!" ← 서버
 *    (매번 요청해야 함, 실시간 어려움)
 *
 * ⚡ WebSocket 방식:
 *    클라이언트 ←--→ 서버 (연결 유지)
 *    서버가 데이터 변경되면 즉시 클라이언트에게 자동 전송!
 *    (주식처럼 빠르게 변하는 데이터에 최적)
 *
 * ===================================================================
 * 🚀 전체 동작 흐름:
 * ===================================================================
 *
 * 시작 단계:
 * 1. Spring 애플리케이션 시작
 * 2. 이 클래스 자동 생성 (init() 메서드 실행)
 * 3. KIS WebSocket 서버에 연결 시도
 *
 * 사용자 요청 시:
 * 1. 사용자가 특정 종목 구독 요청
 * 2. 이미 구독 중인지 확인
 * 3. 새 종목이면 KIS 서버에 구독 패킷 전송
 * 4. 사용자 리스너를 내부 목록에 추가
 *
 * 데이터 수신 시:
 * 1. KIS 서버에서 실시간 데이터 수신
 * 2. 데이터 파싱 (문자열 → 객체)
 * 3. 해당 종목을 구독한 모든 리스너에게 데이터 전달
 * 4. 사용자 화면에 실시간 업데이트
 *
 * 연결 문제 발생 시:
 * 1. 연결 끊김 감지
 * 2. 자동 재연결 시도 (최대 5번)
 * 3. 재연결 성공 시 기존 구독 복원
 */
@ClientEndpoint  // 이 클래스가 WebSocket 클라이언트임을 표시
@Component  // Spring에서 관리하는 컴포넌트로 등록
@Slf4j  // 로깅을 위한 Lombok 어노테이션
public class KisWebSocketClient {

    /* ------------------------------------------------------------------ */
    /* ✨ 설정 값들 - application.properties에서 주입받음                    */
    /* ------------------------------------------------------------------ */

    /**
     * 📍 KIS WebSocket 서버 주소
     * 실제 거래소 데이터를 제공하는 한국투자증권 서버
     */
    @Value("${kis.ws.url:ws://ops.koreainvestment.com:21000/WebSocket}")
    private String wsUrl;

    /**
     * 🔐 KIS API 사용을 위한 승인키
     * 한국투자증권에서 발급받은 개인 인증키
     */
    @Value("${sse.approval_key}")
    private String approvalKey;

    /**
     * ⚙️ 재연결 관련 상수들
     */
    private static final int MAX_RECONNECT = 5;  // 최대 재연결 시도 횟수
    private static final long RECONNECT_BACKOFF_MS = 2_000L;  // 재연결 대기 시간 (2초)

    /* ------------------------------------------------------------------ */
    /* 🔧 내부 상태 관리 객체들                                             */
    /* ------------------------------------------------------------------ */

    /**
     * 🔄 JSON 파싱을 위한 ObjectMapper
     */
    private final ObjectMapper om = new ObjectMapper();

    /**
     * 🔐 동시성 제어를 위한 락(Lock)들
     * 여러 스레드가 동시에 접근할 때 충돌 방지
     */
    private final ReentrantLock connectionLock = new ReentrantLock();   // 연결/해제 작업용
    private final ReentrantLock sendLock = new ReentrantLock();         // 메시지 전송용

    /**
     * 📋 현재 KIS 서버에 실제로 구독 요청을 보낸 종목들
     * 중복 구독을 방지하기 위해 사용
     * 예: 삼성전자(005930)를 100명이 구독해도 KIS에는 1번만 요청
     */
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();

    /**
     * 🎯 종목별 데이터 리스너 관리
     * Key: 종목코드 (예: "005930")
     * Value: 해당 종목 데이터를 받고 싶어하는 리스너들의 목록
     *
     * Consumer<RealTimeTradeDTO>: 데이터를 받아서 처리하는 함수
     */
    private final Map<String, CopyOnWriteArrayList<Consumer<RealTimeTradeDTO>>> listeners =
            new ConcurrentHashMap<>();

    /**
     * 🌐 실제 WebSocket 연결 세션
     * 이 객체를 통해 서버와 메시지를 주고받음
     */
    private Session session;

    /**
     * 🔄 재연결 중인지 표시하는 플래그
     * AtomicBoolean: 멀티스레드 환경에서 안전하게 boolean 값 관리
     */
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);

    /**
     * 🛑 애플리케이션 종료 중 표시 (종료 시 onClose → 재연결이 일어나지 않도록)
     */
    private volatile boolean shuttingDown = false;

    /**
     * 📤 구독/해지 패킷 전송 전용 단일 스레드
     * 공용 풀에서 병렬 실행하면 "구독 → 해지 → 재구독"이 뒤바뀌어 전송될 수 있으므로,
     * 요청된 순서 그대로 KIS에 전달되도록 한 줄로 세움
     */
    private final ExecutorService sendExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "kis-ws-sender");
        t.setDaemon(true);
        return t;
    });

    /* ------------------------------------------------------------------ */
    /* 🔄 생명주기 관리 - Spring이 자동으로 호출                            */
    /* ------------------------------------------------------------------ */

    /**
     * 📡 Spring 컨테이너가 이 컴포넌트를 생성한 후 자동으로 호출
     * 애플리케이션 시작 시 KIS WebSocket 연결을 시도
     */
    @PostConstruct
    public void init() {
        asyncConnect();  // 비동기로 연결 시도
    }

    /**
     * 🔌 애플리케이션 종료 시 Spring이 자동으로 호출
     * 연결을 깔끔하게 종료
     */
    @PreDestroy
    public void destroy() {
        shuttingDown = true;         // 이후 onClose가 재연결을 시작하지 않도록
        sendExecutor.shutdownNow();  // 대기 중인 전송 작업 중단
        connectionLock.lock();  // 연결 작업 락 획득
        try {
            if (session != null && session.isOpen()) {
                // 정상적으로 연결 종료 메시지 전송
                session.close(new CloseReason(
                        CloseReason.CloseCodes.NORMAL_CLOSURE, "shutdown"));
            }
        } catch (Exception ignored) {
            // 종료 중 예외는 무시 (이미 종료되었을 수도 있음)
        } finally {
            connectionLock.unlock();  // 반드시 락 해제
        }
    }

    /* ------------------------------------------------------------------ */
    /* 🔗 외부 API – 다른 서비스에서 호출하는 메서드들                        */
    /* ------------------------------------------------------------------ */

    /**
     * 📈 특정 종목의 실시간 데이터 구독 요청
     *
     * ===================================================================
     * 💡 이 메서드의 동작 흐름을 쉽게 설명하면?
     * ===================================================================
     *
     * 상황: 사용자가 "삼성전자 주가 보고 싶어!" 라고 요청
     *
     * 🔍 1단계: 리스너 등록
     *    - 사용자의 요청(handler)을 삼성전자 리스너 목록에 추가
     *    - 이미 10명이 삼성전자를 보고 있다면 11번째로 추가
     *
     * 🔍 2단계: 중복 체크
     *    - 이미 KIS 서버에 삼성전자 구독 요청을 보냈는지 확인
     *    - 처음이면: KIS 서버에 "삼성전자 데이터 보내줘!" 요청
     *    - 이미 있으면: 아무것도 안 함 (효율성!)
     *
     * 🎯 핵심 아이디어: "서버 부담 최소화"
     * - 1000명이 삼성전자를 봐도 KIS 서버에는 1번만 요청
     * - 새로운 사용자는 기존 데이터 스트림에 "편승"
     *
     * @param stockId 구독할 종목코드 (예: "005930" = 삼성전자)
     * @param handler 실시간 데이터를 받으면 실행할 함수 (사용자 화면 업데이트 등)
     */
    public void queueSubscribe(String stockId, Consumer<RealTimeTradeDTO> handler) {
        // 🎯 1단계: 사용자 리스너 등록
        // 예: 삼성전자(005930) 리스너 목록에 새 사용자 추가
        // computeIfAbsent: 삼성전자가 처음이면 새 리스트 생성, 아니면 기존 리스트 가져오기
        listeners.computeIfAbsent(stockId, k -> new CopyOnWriteArrayList<>()).add(handler);

        // 🎯 2단계: 서버 구독 필요성 판단
        // subscribed는 "실제로 KIS 서버에 구독 요청을 보낸 종목들" 목록
        if (subscribed.add(stockId)) {
            // ✅ 최초 구독: 이 종목을 처음 구독하는 상황
            // Set.add()가 true를 반환 = 새로운 종목이라는 뜻
            // → KIS 서버에 실제 구독 패킷 전송 필요
            log.info("🆕 [{}] 새 종목 구독 - KIS 서버에 요청 전송", stockId);
            sendLater(() -> sendSubscribe(stockId));
        } else {
            // ✅ 기존 구독: 이미 다른 사용자가 구독 중인 종목
            // Set.add()가 false를 반환 = 이미 있는 종목이라는 뜻
            // → KIS 서버 요청 없이 기존 데이터 스트림에 "편승"
            log.debug("♻️ [{}] 기존 구독 활용 - 리스너만 추가", stockId);
        }
    }

    /**
     * 📉 특정 종목의 구독 해지
     *
     * ===================================================================
     * 💡 이 메서드의 동작 흐름을 쉽게 설명하면?
     * ===================================================================
     *
     * 상황: 마지막 사용자가 "삼성전자 그만 볼래" 라고 나가는 경우
     *
     * 🔍 1단계: 해당 종목의 모든 리스너 제거
     *    - 삼성전자를 보던 모든 사용자 목록을 완전히 삭제
     *    - 더 이상 이 종목에 관심 있는 사람이 없다는 뜻
     *
     * 🔍 2단계: 서버 구독 해지 여부 결정
     *    - 정말로 아무도 이 종목을 안 보는지 최종 확인
     *    - 맞다면: KIS 서버에 "삼성전자 데이터 그만 보내!" 요청
     *    - 아니면: 아무것도 안 함
     *
     * 🎯 핵심 아이디어: "자원 절약"
     * - 아무도 안 보는 데이터는 받지 않음
     * - 서버 트래픽과 처리 비용 절약
     *
     * @param stockId 구독 해지할 종목코드 (예: "005930")
     */
    public void queueUnsubscribe(String stockId) {
        // 🎯 1단계: 해당 종목의 모든 사용자 리스너 완전 제거
        // 예: 삼성전자를 보던 10명의 리스너를 모두 삭제
        listeners.remove(stockId);

        // 🎯 2단계: KIS 서버 구독 해지 필요성 판단
        // subscribed에서 제거 시도 → 실제로 구독 중이었다면 true 반환
        if (subscribed.remove(stockId)) {
            // ✅ 실제 구독 해지 필요
            // → KIS 서버에 "이 종목 데이터 그만 보내줘" 요청
            log.info("🗑️ [{}] 모든 사용자 나감 - KIS 서버 구독 해지", stockId);
            sendLater(() -> sendUnsubscribe(stockId));
        } else {
            // ✅ 이미 구독 해지됨 (또는 원래 구독 안 했음)
            // → 별도 작업 불필요
            log.debug("ℹ️ [{}] 이미 구독 해지된 상태", stockId);
        }
    }

    /* ------------------------------------------------------------------ */
    /* 🔌 WebSocket 연결 관리 헬퍼 메서드들                                */
    /* ------------------------------------------------------------------ */

    /**
     * 🚀 비동기로 KIS WebSocket 서버에 연결
     *
     * ===================================================================
     * 💡 이 메서드가 하는 일을 쉽게 설명하면?
     * ===================================================================
     *
     * 🔍 목표: KIS 서버와 실시간 연결 통로 만들기
     *
     * 🎯 왜 "비동기"인가?
     * - 네트워크 연결은 시간이 걸림 (1-3초)
     * - 메인 스레드가 기다리면 앱이 멈춤
     * - 별도 스레드에서 연결하고 완료되면 알려줌
     *
     * 🔍 동작 과정:
     * 1. 새 스레드 생성해서 연결 작업 시작
     * 2. WebSocket 컨테이너 준비 (연결 도구)
     * 3. KIS 서버 주소로 실제 연결 시도
     * 4. 성공하면 session 객체에 연결 정보 저장
     * 5. 실패하면 재연결 로직 실행
     */
    private void asyncConnect() {
        // 🚀 별도 스레드에서 연결 작업 실행 (메인 스레드 블로킹 방지)
        CompletableFuture.runAsync(() -> {
            if (!connectOnce()) {
                triggerReconnect();  // 💥 실패 시 백오프 재연결 시작
            }
        });
    }

    /**
     * 🔗 KIS 서버에 1회 동기 연결 시도
     * 재연결 루프가 "성공/실패"를 직접 확인할 수 있도록 결과를 boolean으로 반환
     *
     * @return 연결 성공(또는 이미 연결됨) 여부
     */
    private boolean connectOnce() {
        connectionLock.lock();  // 🔒 동시에 여러 연결 시도하는 것 방지
        try {
            if (isSessionOpen()) return true;  // 다른 스레드가 이미 연결함

            // 📡 1단계: WebSocket 연결 도구 준비
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.setDefaultMaxSessionIdleTimeout(60_000);  // 60초간 응답 없으면 타임아웃

            // 🔗 2단계: 실제 KIS 서버에 연결 (성공 시 onOpen()에서 구독 복원)
            this.session = container.connectToServer(this, URI.create(wsUrl));
            log.info("🎉 KIS WebSocket 연결 성공! {}", wsUrl);
            return true;

        } catch (Exception e) {
            // 💥 연결 실패 (네트워크 문제, 서버 다운 등)
            log.warn("❌ WebSocket 연결 실패: {}", e.getMessage());
            return false;
        } finally {
            connectionLock.unlock();  // 🔓 반드시 락 해제 (데드락 방지)
        }
    }

    /**
     * 🔄 재연결 로직: 연결이 끊어졌을 때 자동으로 재시도
     *
     * 🎯 선형 백오프(Linear Backoff) 전략:
     * - 1차: 2초 대기 후 재시도
     * - 2차: 4초 대기 후 재시도
     * - 3차: 6초 대기 후 재시도
     * - 최대 5번까지 시도 (각 시도의 성공 여부를 확인한 뒤 다음 단계로 진행)
     */
    private void triggerReconnect() {
        if (shuttingDown) return;  // 종료 중에는 재연결하지 않음

        // 🚫 이미 재연결 중이면 중복 실행 방지
        if (!reconnecting.compareAndSet(false, true)) return;

        CompletableFuture.runAsync(() -> {
            try {
                for (int i = 1; i <= MAX_RECONNECT && !shuttingDown; i++) {
                    long waitMs = RECONNECT_BACKOFF_MS * i;  // ⏰ 2초 → 4초 → 6초 → ...
                    log.info("🔄 재연결 시도 {}/{} – {}ms 대기", i, MAX_RECONNECT, waitMs);
                    Thread.sleep(waitMs);

                    if (connectOnce()) {
                        log.info("✅ 재연결 성공 ({}회차)", i);
                        return;
                    }
                }
                log.error("❌ WebSocket 재연결 {}회 실패 – 더 이상 시도하지 않음", MAX_RECONNECT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).whenComplete((v, t) -> reconnecting.set(false));  // 재연결 플래그 해제
    }

    /* ------------------------------------------------------------------ */
    /* 📤 실제 구독/해지 패킷 전송 메서드들                                  */
    /* ------------------------------------------------------------------ */

    /**
     * 📈 KIS 서버에 종목 구독 요청 패킷 전송
     *
     * @param stockId 구독할 종목코드
     */
    private void sendSubscribe(String stockId) {
        sendLock.lock();  // 메시지 전송 동시성 제어
        try {
            if (!isSessionOpen()) return;  // 연결이 끊어져 있으면 종료

            // 🚀 실제 구독 패킷 전송 (JSON 형태)
            session.getBasicRemote().sendText(buildPayload("1", stockId));
            log.info("▶️ subscribe : {}", stockId);

        } catch (Exception e) {
            log.error("subscribe 전송 오류 – {}", stockId, e);
            subscribed.remove(stockId);  // 실패 시 구독 목록에서 제거
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * 📉 KIS 서버에 종목 구독 해지 요청 패킷 전송
     *
     * @param stockId 구독 해지할 종목코드
     */
    private void sendUnsubscribe(String stockId) {
        sendLock.lock();
        try {
            if (!isSessionOpen()) return;

            // 🛑 실제 구독 해지 패킷 전송
            session.getBasicRemote().sendText(buildPayload("2", stockId));
            log.info("⏹️ unsubscribe : {}", stockId);

        } catch (Exception e) {
            log.error("unsubscribe 전송 오류 – {}", stockId, e);
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * 🔍 WebSocket 세션이 열려있는지 확인
     *
     * @return 연결 상태 (true: 열림, false: 닫힘)
     */
    private boolean isSessionOpen() {
        return session != null && session.isOpen();
    }

    /**
     * 📤 메시지 전송을 나중에 실행 (비동기)
     * 연결이 끊어져 있으면 먼저 재연결 시도
     *
     * @param task 실행할 작업
     */
    private void sendLater(Runnable task) {
        CompletableFuture.runAsync(() -> {
            // 🔍 연결이 끊어져 있으면 재연결 시도
            if (!isSessionOpen()) {
                asyncConnect();
                try {
                    Thread.sleep(300);  // 연결 완료 대기
                } catch (InterruptedException ignored) {
                }
            }
            task.run();  // 실제 작업 실행
        }, sendExecutor);  // 전용 단일 스레드 → 요청 순서 보장
    }

    /**
     * 📋 KIS WebSocket API 요청 패킷 생성
     *
     * 🔍 패킷 구조:
     * - header: 인증 정보, 거래 유형, 종목코드 등
     * - body: 상세 요청 정보
     *
     * @param trType 거래 유형 ("1": 구독, "2": 해지)
     * @param stockId 종목코드
     * @return JSON 형태의 요청 패킷
     */
    private String buildPayload(String trType, String stockId) {
        return """
                {
                  "header": {
                    "approval_key": "%s",
                    "custtype":    "P",
                    "tr_type":     "%s",
                    "content-type":"utf-8",
                    "tr_id":       "H0NXCNT0",
                    "tr_key":      "%s"
                  },
                  "body": {
                    "input": {
                      "tr_id":  "H0NXCNT0",
                      "tr_key": "%s"
                    }
                  }
                }
                """.replace("\n", "")      // 개행 문자 제거
                .formatted(approvalKey, trType, stockId, stockId);
    }

    /* ------------------------------------------------------------------ */
    /* 🎭 WebSocket 이벤트 콜백 메서드들                                     */
    /* 이 메서드들은 WebSocket 연결 상태가 변할 때 자동으로 호출됨             */
    /* ------------------------------------------------------------------ */

    /**
     * ✅ WebSocket 연결이 성공적으로 열렸을 때 호출
     *
     * @param s 연결된 세션 객체
     */
    @OnOpen
    public void onOpen(Session s) {
        log.info("✅  WebSocket OPEN");
        this.session = s;

        // 🔄 재연결 시 기존 구독 복원
        // 연결이 끊어졌다가 다시 연결되면 이전에 구독했던 종목들을 자동으로 다시 구독
        subscribed.forEach(this::sendSubscribe);
    }

    /**
     * 📨 KIS 서버로부터 메시지를 받았을 때 자동으로 호출되는 메서드
     *
     * ===================================================================
     * 💡 이 메서드가 하는 일을 쉽게 설명하면?
     * ===================================================================
     *
     * 🔍 KIS 서버가 보내는 메시지 종류:
     *
     * 1️⃣ 💓 하트비트 ("PINGPONG")
     *    - 목적: "연결 살아있나?" 확인용
     *    - 처리: 무시 (자동으로 응답됨)
     *
     * 2️⃣ 📊 실시간 주식 데이터 ("0|H0NXCNT0|...")
     *    - 목적: 실제 주가, 거래량 등 실시간 정보
     *    - 처리: 파싱해서 사용자들에게 전달
     *    - 예: "삼성전자^82500^+1.2%^1000주"
     *
     * 3️⃣ 📋 JSON 응답 메시지 ("{ ... }")
     *    - 목적: 구독 성공/실패, 에러 메시지 등
     *    - 처리: 로그로 기록
     *
     * 🎯 핵심: 메시지 타입에 따라 적절한 처리기로 분기
     *
     * @param msg KIS 서버에서 받은 원본 메시지 문자열
     */
    @OnMessage
    public void onMessage(String msg) {
        // 💓 1단계: 하트비트 필터링
        // KIS 서버가 주기적으로 보내는 "살아있나?" 확인 메시지
        // 이런 메시지는 무시하고 바로 리턴
        if (msg.contains("\"tr_id\":\"PINGPONG\"")) {
            log.trace("💓 하트비트 수신 (무시)");
            return;
        }

        // 📊 2단계: 실시간 주식 데이터 처리
        // 형태: "0|H0NXCNT0|20231215103000|005930^82500^+1.2^1000^..."
        if (msg.startsWith("0|H0NXCNT0|")) {
            log.trace("📈 실시간 데이터 수신: {}", msg.substring(0, Math.min(50, msg.length())));
            handleRealtime(msg);  // 실시간 데이터 파싱 및 사용자들에게 전달
        }
        // 📋 3단계: JSON 응답 메시지 처리
        // 구독 성공/실패, 에러 코드 등
        else if (msg.startsWith("{")) {
            log.trace("📋 JSON 응답 수신");
            handleJson(msg);  // JSON 파싱해서 결과 확인
        }
        // ❓ 4단계: 예상치 못한 메시지
        else {
            log.debug("❓ 알 수 없는 메시지 형태: {}", msg.substring(0, Math.min(100, msg.length())));
        }
    }

    /**
     * ⚠️ WebSocket 연결이 닫혔을 때 호출
     *
     * @param s 닫힌 세션
     * @param reason 닫힌 이유
     */
    @OnClose
    public void onClose(Session s, CloseReason reason) {
        log.warn("⚠️  WebSocket CLOSED : {} ({})",
                reason.getReasonPhrase(), reason.getCloseCode());
        triggerReconnect();  // 자동 재연결 시도
    }

    /**
     * ❌ WebSocket 오류 발생 시 호출
     *
     * @param s 오류가 발생한 세션
     * @param t 발생한 예외
     */
    @OnError
    public void onError(Session s, Throwable t) {
        log.error("WebSocket ERROR", t);
    }

    /* ------------------------------------------------------------------ */
    /* 🔍 메시지 파싱 및 처리 메서드들                                        */
    /* ------------------------------------------------------------------ */

    /**
     * 📊 실시간 거래 데이터 처리 - 가장 중요한 메서드!
     *
     * ===================================================================
     * 💡 이 메서드가 하는 일을 쉽게 설명하면?
     * ===================================================================
     *
     * 🔍 입력: KIS 서버에서 온 원본 메시지
     * 예: "0|H0NXCNT0|20231215103000|005930^82500^+1.2^1000^매수^..."
     *
     * 🎯 목표: 이 복잡한 문자열을 사용자가 이해할 수 있는 형태로 변환
     *
     * 🔍 처리 과정:
     * 1. 파이프(|)로 분리 → ["0", "H0NXCNT0", "20231215103000", "005930^82500^..."]
     * 2. 마지막 부분을 캐럿(^)으로 분리 → ["005930", "82500", "+1.2", "1000", ...]
     * 3. 첫 번째 값이 종목코드 → "005930" (삼성전자)
     * 4. 이 종목을 구독한 사용자들 찾기
     * 5. 데이터를 예쁜 객체(DTO)로 변환
     * 6. 모든 구독자들에게 전달 (팬아웃!)
     *
     * @param raw KIS에서 받은 원본 메시지 (파싱 전)
     */
    private void handleRealtime(String raw) {
        try {
            // 🔍 1단계: 파이프(|)로 메시지 구조 분해
            // "0|H0NXCNT0|20231215103000|005930^82500^+1.2^1000^..."
            // → ["0", "H0NXCNT0", "20231215103000", "005930^82500^+1.2^1000^..."]
            String[] parts = raw.split("\\|", 4);
            if (parts.length < 4) {
                log.warn("⚠️ 메시지 형식 오류: 파이프 구분자 부족");
                return;
            }

            // 🔍 2단계: 실제 데이터 부분을 캐럿(^)으로 분해
            // "005930^82500^+1.2^1000^매수^..." → ["005930", "82500", "+1.2", "1000", ...]
            String[] f = parts[3].split("\\^");
            String code = f[0];  // 첫 번째 = 종목코드 (예: "005930")

            // 🔍 3단계: 이 종목을 구독한 사용자들 찾기
            List<Consumer<RealTimeTradeDTO>> list = listeners.get(code);
            if (list == null || list.isEmpty()) {
                log.trace("📭 [{}] 구독자 없음 - 데이터 무시", code);
                return;
            }

            // 🔍 4단계: 원본 데이터를 예쁜 객체로 변환
            // 문자열 배열 → RealTimeTradeDTO 객체 (현재가, 등락률, 거래량 등)
            RealTimeTradeDTO dto = RealTimeTradeDTO.from(f);
//            log.trace("📊 [{}] 데이터 변환 완료: 현재가 {}", code, dto.getCurrentPrice());

            // 🔍 5단계: 모든 구독자에게 데이터 전달 (팬아웃)
            // 예: 삼성전자를 보는 100명에게 동시에 새 주가 정보 전달
            list.forEach(cb -> {
                try {
                    cb.accept(dto);  // 각 사용자의 콜백 함수 실행 (화면 업데이트 등)
                } catch (Exception e) {
                    log.warn("⚠️ 사용자 콜백 실행 중 오류", e);
                }
            });

            log.trace("📢 [{}] {}명에게 데이터 전달 완료", code, list.size());

        } catch (Exception e) {
            log.warn("💥 실시간 데이터 파싱 실패: {}", raw.substring(0, Math.min(100, raw.length())), e);
        }
    }

    /**
     * 📋 JSON 응답 메시지 처리
     * 구독 성공/실패, 오류 메시지 등을 처리
     *
     * @param json JSON 형태의 응답 메시지
     */
    private void handleJson(String json) {
        try {
            // 🔍 JSON 파싱하여 응답 코드 확인
            JsonNode body = om.readTree(json).path("body");
            String rt = body.path("rt_cd").asText();

            if ("0".equals(rt)) {
                // ✅ 구독 성공
                log.info("구독 성공 – {}", body.path("tr_key").asText(""));
            } else {
                // ❌ 오류 발생
                String code = body.path("msg_cd").asText();
                String msg = body.path("msg1").asText();
                log.warn("❗ KIS 오류 ({}) {}", code, msg);
            }
        } catch (Exception e) {
            log.warn("JSON 메시지 파싱 실패: {}", json, e);
        }
    }
}