"""수정 전 코드(StockWebSocketService)에 측정용 오버로드만 추가한다. 동작은 바꾸지 않는다.

usage: python3 patch_before.py <project_dir>
"""
import sys

p = sys.argv[1] + '/src/main/java/com/project/stock/investory/stockInfo/service/StockWebSocketService.java'
s = open(p, encoding='utf-8').read()
anchor = "    void register(String stockId, SseEmitter emitter) {"
assert s.count(anchor) == 1, "register(stockId, emitter) not found"
s = s.replace(anchor, """    /** [측정용 패치] 수정 전 코드는 클라이언트를 구분하지 않으므로 clientKey 를 무시한다 */
    void register(String clientKey, String stockId, SseEmitter emitter) {
        register(stockId, emitter);
    }

""" + anchor)
open(p, 'w', encoding='utf-8').write(s)
