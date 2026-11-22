package com.project.stock.investory.stockInfo.exception;

import com.project.stock.investory.exception.BusinessException;
import org.springframework.http.HttpStatus;

/**
 * 실시간 시세 SSE 구독 한도 초과.
 *
 * <ul>
 *   <li>{@link #tooManyStreams}: 한 클라이언트가 동시에 열 수 있는 SSE 연결 수 초과 → 429</li>
 *   <li>{@link #kisCapacityFull}: KIS 세션에 등록할 수 있는 종목 수(서버 전체 공유 자원) 소진 → 503</li>
 * </ul>
 */
public class SubscriptionLimitException extends BusinessException {

    private SubscriptionLimitException(String message, HttpStatus status) {
        super(message, status);
    }

    public static SubscriptionLimitException tooManyStreams(int limit) {
        return new SubscriptionLimitException(
                "동시에 열 수 있는 실시간 시세 연결은 " + limit + "개까지입니다.", HttpStatus.TOO_MANY_REQUESTS);
    }

    public static SubscriptionLimitException kisCapacityFull() {
        return new SubscriptionLimitException(
                "실시간 시세 구독 가능한 종목 수를 초과했습니다. 잠시 후 다시 시도해 주세요.", HttpStatus.SERVICE_UNAVAILABLE);
    }
}
