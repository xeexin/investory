"""Expose pre-fix StockWebSocketService to the concurrency test without changing behavior.

- Moves the existing (unlocked) subscribe block into a package-private register().
- Makes removeEmitterOnly() package-private and adds lockOf() returning the same per-stock lock.
"""
import sys

path = sys.argv[1]
s = open(path, encoding="utf-8").read()


def sub(old, new):
    global s
    assert s.count(old) == 1, f"expected exactly one match for: {old[:60]!r}"
    s = s.replace(old, new)


start = s.index("        emitters.computeIfAbsent(stockId, k -> new CopyOnWriteArrayList<>()).add(emitter);")
end_marker = '            log.debug("[{}] 이미 subscribe 중", stockId);\n        }\n'
end = s.index(end_marker, start) + len(end_marker)
block = s[start:end]
s = s[:start] + "        register(stockId, emitter);\n" + s[end:]

register = (
    "    // [측정용] 수정 전 동작 그대로: 락 없이 등록 → 최초 구독 판단\n"
    "    void register(String stockId, SseEmitter emitter) {\n"
    + block
    + "    }\n\n"
    "    ReentrantLock lockOf(String stockId) {\n"
    "        return locks.computeIfAbsent(stockId, k -> new ReentrantLock());\n"
    "    }\n\n"
)
sub("    private void removeEmitterOnly(String stockId, SseEmitter emitter) {",
    register + "    void removeEmitterOnly(String stockId, SseEmitter emitter) {")

open(path, "w", encoding="utf-8").write(s)
print("patched", path)
