"""
Single-file Python project: lowlatency_trading_singlefile.py
Purpose: a minimal, well-documented toolkit demonstrating low-latency trading system primitives
(components only).

This file includes:
- low-latency primitives (Lock-free-ish ring buffer wrapper using collections.deque)
- high-resolution timestamping helpers
- async I/O skeleton for market-data consumer and order gateway (abstracted)
- risk checks and simple strategy hook
- latency measurement and warm-up mode
- a small 'demo' mode (simulated feed) so reviewers can run it locally without real exchange credentials

"""

from __future__ import annotations
import asyncio
import collections
import time
import typing as t
import random

# ----------------------------- Utilities ------------------------------------

def now_ns() -> int:
    """High-resolution timestamp in nanoseconds."""
    return time.perf_counter_ns()


class LatencyMonitor:
    """Collect simple latency stats (ns) — rolling window."""

    def __init__(self, window: int = 1024):
        self.window = max(16, window)
        self.samples = collections.deque(maxlen=self.window)

    def add(self, ns: int) -> None:
        self.samples.append(ns)

    def stats(self) -> dict:
        if not self.samples:
            return {"count": 0}
        s = list(self.samples)
        s_sorted = sorted(s)
        return {
            "count": len(s),
            "min_ns": min(s),
            "max_ns": max(s),
            "p50_ns": s_sorted[len(s_sorted) // 2],
            "p95_ns": s_sorted[int(len(s_sorted) * 0.95) - 1],
            "mean_ns": sum(s) // len(s),
        }


# ----------------------------- Ring Buffer ----------------------------------

class RingBuffer:
    """Ring buffer based on collections.deque."""

    def __init__(self, size: int = 4096):
        self._dq = collections.deque(maxlen=size)

    def put(self, item) -> None:
        self._dq.append(item)

    def get_batch(self, max_items: int = 64) -> list:
        out = []
        for _ in range(min(max_items, len(self._dq))):
            out.append(self._dq.popleft())
        return out

    def __len__(self) -> int:
        return len(self._dq)


# ----------------------------- Market Data Types ----------------------------

class MarketEvent(t.TypedDict):
    ts_ns: int
    symbol: str
    bid: float
    ask: float
    seq: int


# ----------------------------- Abstract Adapters ----------------------------

class OrderGateway:
    """Abstract OrderGateway — replace with exchange-specific connector."""

    async def send_order(self, symbol: str, size: float, side: str, price: float | None = None) -> dict:
        raise NotImplementedError


class DemoOrderGateway(OrderGateway):
    """Simulated low-latency gateway."""

    def __init__(self, latency_ns_mean: int = 50_000):
        self.latency_ns_mean = latency_ns_mean

    async def send_order(self, symbol: str, size: float, side: str, price: float | None = None) -> dict:
        await asyncio.sleep(self.latency_ns_mean / 1e9)
        ack = {
            "status": "ACK",
            "symbol": symbol,
            "size": size,
            "side": side,
            "price": price,
            "ts_ns": now_ns(),
        }
        print(f"[gateway] sent {side} {size}@{price} -> {ack['status']}")
        return ack


# ----------------------------- Risk Manager ---------------------------------

class RiskManager:
    """Simple risk manager with position limits."""

    def __init__(self, position_limit: float = 1_000_000.0):
        self.position_limit = position_limit
        self.positions: dict[str, float] = {}

    def allowed(self, symbol: str, size: float) -> bool:
        pos = self.positions.get(symbol, 0.0)
        return abs(pos + size) <= self.position_limit

    def apply_fill(self, symbol: str, size: float) -> None:
        self.positions[symbol] = self.positions.get(symbol, 0.0) + size


# ----------------------------- Strategy Hook --------------------------------

async def simple_strategy(feed: RingBuffer, gateway: OrderGateway, risk: RiskManager, monitor: LatencyMonitor):
    """A toy strategy: place order if bid/ask spread > threshold."""
    spread_threshold = 0.5
    while True:
        batch = feed.get_batch(16)
        for event in batch:
            spread = event["ask"] - event["bid"]
            if spread > spread_threshold and risk.allowed(event["symbol"], 1):
                t0 = now_ns()
                ack = await gateway.send_order(event["symbol"], 1, "BUY", event["ask"])
                monitor.add(now_ns() - t0)
                risk.apply_fill(event["symbol"], 1)
        await asyncio.sleep(0)  # yield control


# ----------------------------- Demo Producer --------------------------------

async def demo_feed_producer(feed: RingBuffer, symbol: str = "XYZ"):
    """Simulate fast tick generation."""
    seq = 0
    while True:
        seq += 1
        mid = 100 + random.uniform(-1, 1)
        event: MarketEvent = {
            "ts_ns": now_ns(),
            "symbol": symbol,
            "bid": mid - 0.25,
            "ask": mid + 0.25 + random.uniform(0, 1),
            "seq": seq,
        }
        feed.put(event)
        await asyncio.sleep(0.001)  # ~1kHz


# ----------------------------- Main Runner ----------------------------------

async def main_demo():
    feed = RingBuffer(4096)
    gateway = DemoOrderGateway()
    risk = RiskManager()
    monitor = LatencyMonitor(512)

    await asyncio.gather(
        demo_feed_producer(feed),
        simple_strategy(feed, gateway, risk, monitor),
        stats_printer(monitor),
    )


async def stats_printer(monitor: LatencyMonitor):
    while True:
        await asyncio.sleep(2)
        print("[latency]", monitor.stats())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="Run local demo mode")
    args = parser.parse_args()

    if args.demo:
        asyncio.run(main_demo())
    else:
        print("This is a low-latency trading primitives demo. Run with --demo to test locally.")
