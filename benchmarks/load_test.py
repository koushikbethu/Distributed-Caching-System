import argparse
import asyncio
import httpx
import json
import logging
import random
import string
import time
from collections import defaultdict
from statistics import quantiles

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Metrics:
    def __init__(self):
        self.read_latencies = []
        self.write_latencies = []
        self.read_success = 0
        self.write_success = 0
        self.read_404 = 0
        self.errors = 0
        self.start_time = 0
        self.end_time = 0

    @property
    def total_operations(self):
        return len(self.read_latencies) + len(self.write_latencies) + self.errors

    @property
    def duration(self):
        return self.end_time - self.start_time

    @property
    def total_throughput(self):
        if self.duration > 0:
            return self.total_operations / self.duration
        return 0

    @property
    def read_throughput(self):
        if self.duration > 0:
            return (len(self.read_latencies)) / self.duration
        return 0

    @property
    def write_throughput(self):
        if self.duration > 0:
            return (len(self.write_latencies)) / self.duration
        return 0

    @property
    def success_rate(self):
        total = self.total_operations
        if total > 0:
            return ((self.read_success + self.write_success + self.read_404) / total) * 100
        return 0

    @property
    def cache_hit_rate(self):
        total_reads = self.read_success + self.read_404
        if total_reads > 0:
            return (self.read_success / total_reads) * 100
        return 0
        
    def get_percentiles(self, latencies):
        if not latencies:
            return [0.0, 0.0, 0.0, 0.0, 0.0]
        l = sorted(latencies)
        try:
            return [
                self.percentile(l, 50),
                self.percentile(l, 90),
                self.percentile(l, 95),
                self.percentile(l, 99),
                self.percentile(l, 99.9)
            ]
        except Exception:
            return [0.0, 0.0, 0.0, 0.0, 0.0]
            
    def percentile(self, data, p):
        if not data:
            return 0.0
        k = (len(data) - 1) * (p / 100.0)
        f = int(k)
        c = f + 1
        if f == k or c >= len(data):
            return data[f] * 1000.0
        return (data[f] + (data[c] - data[f]) * (k - f)) * 1000.0

class LoadTester:
    def __init__(self, args):
        self.nodes = args.nodes.split(",")
        self.clients = args.clients
        self.operations = args.operations
        self.key_space = args.key_space
        self.read_ratio = args.read_ratio
        self.value_size = args.value_size
        self.ttl = args.ttl
        self.warmup_ops = args.warmup
        self.metrics = Metrics()
        self.shutdown_event = asyncio.Event()
        self.client = None
        
    async def _make_request(self, op_type, key, value=None):
        node = random.choice(self.nodes)
        start_time = time.perf_counter()
        
        try:
            if op_type == "GET":
                response = await self.client.get(f"{node}/cache/{key}")
                latency = time.perf_counter() - start_time
                if response.status_code == 200:
                    return ("GET", "SUCCESS", latency)
                elif response.status_code == 404:
                    return ("GET", "404", latency)
                else:
                    return ("GET", "ERROR", latency)
            elif op_type == "SET":
                payload = {"value": value}
                if self.ttl:
                    payload["ttl"] = self.ttl
                response = await self.client.put(f"{node}/cache/{key}", json=payload)
                latency = time.perf_counter() - start_time
                if response.status_code in (200, 201):
                    return ("SET", "SUCCESS", latency)
                else:
                    return ("SET", "ERROR", latency)
        except Exception:
            return (op_type, "ERROR", time.perf_counter() - start_time)

    async def _worker(self, queue, is_warmup=False):
        while not self.shutdown_event.is_set():
            try:
                task = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
            op_type, key, value = task
            result_type, status, latency = await self._make_request(op_type, key, value)
            
            if not is_warmup:
                if status == "ERROR":
                    self.metrics.errors += 1
                elif result_type == "GET":
                    self.metrics.read_latencies.append(latency)
                    if status == "SUCCESS":
                        self.metrics.read_success += 1
                    elif status == "404":
                        self.metrics.read_404 += 1
                elif result_type == "SET":
                    self.metrics.write_latencies.append(latency)
                    if status == "SUCCESS":
                        self.metrics.write_success += 1

            queue.task_done()

    def generate_random_string(self, length):
        return ''.join(random.choices(string.ascii_letters, k=length))

    async def _run_phase(self, num_ops, is_warmup=False):
        queue = asyncio.Queue()
        
        value_pool = [self.generate_random_string(self.value_size) for _ in range(100)]
        
        for _ in range(num_ops):
            key = f"key:{random.randint(0, self.key_space - 1)}"
            if is_warmup:
                op = "SET"
                value = random.choice(value_pool)
            else:
                op = "GET" if random.random() < self.read_ratio else "SET"
                value = random.choice(value_pool) if op == "SET" else None
                
            queue.put_nowait((op, key, value))

        workers = []
        for _ in range(self.clients):
            worker = asyncio.create_task(self._worker(queue, is_warmup))
            workers.append(worker)

        total_tasks = queue.qsize()
        
        async def monitor():
            while not queue.empty() and not self.shutdown_event.is_set():
                remaining = queue.qsize()
                completed = total_tasks - remaining
                if total_tasks > 0 and not is_warmup:
                    progress = (completed / total_tasks) * 100
                    if completed % max(1, (total_tasks // 10)) == 0:
                        logger.info(f"Progress: {progress:.1f}% ({completed}/{total_tasks})")
                await asyncio.sleep(1.0)

        monitor_task = asyncio.create_task(monitor())
        await queue.join()
        monitor_task.cancel()
        
        for w in workers:
            w.cancel()

    async def run(self):
        limits = httpx.Limits(max_connections=self.clients, max_keepalive_connections=self.clients)
        transport = httpx.AsyncHTTPTransport(retries=1)
        async with httpx.AsyncClient(limits=limits, transport=transport, timeout=10.0) as client:
            self.client = client
            try:
                if self.warmup_ops > 0:
                    logger.info(f"Starting warmup phase ({self.warmup_ops} ops)...")
                    await self._run_phase(self.warmup_ops, is_warmup=True)
                    logger.info("Warmup phase completed.")
                
                logger.info(f"Starting load test phase ({self.operations} ops)...")
                self.metrics.start_time = time.perf_counter()
                await self._run_phase(self.operations, is_warmup=False)
                self.metrics.end_time = time.perf_counter()
                logger.info("Load test phase completed.")
                
                self.report()
            except asyncio.CancelledError:
                logger.info("Test interrupted.")
            finally:
                self.shutdown_event.set()

    def report(self):
        m = self.metrics
        all_latencies = m.read_latencies + m.write_latencies
        
        overall_p = m.get_percentiles(all_latencies)
        read_p = m.get_percentiles(m.read_latencies)
        write_p = m.get_percentiles(m.write_latencies)

        border = "═" * 62
        print(f"\n╔{border}╗")
        print(f"║{'Distributed Cache Load Test Results'.center(62)}║")
        print(f"╠{border}╣")
        print(f"║ Configuration                                                ║")
        print(f"║   Nodes:        {str(len(self.nodes)).ljust(45)}║")
        print(f"║   Clients:      {str(self.clients).ljust(45)}║")
        print(f"║   Operations:   {str(self.operations).ljust(45)}║")
        print(f"║   Read Ratio:   {f'{self.read_ratio * 100:.0f}%'.ljust(45)}║")
        print(f"║   Key Space:    {str(self.key_space).ljust(45)}║")
        print(f"╠{border}╣")
        print(f"║ Throughput                                                   ║")
        print(f"║   Total:        {f'{m.total_throughput:,.0f} ops/sec'.ljust(45)}║")
        print(f"║   Reads:        {f'{m.read_throughput:,.0f} ops/sec'.ljust(45)}║")
        print(f"║   Writes:       {f'{m.write_throughput:,.0f} ops/sec'.ljust(45)}║")
        print(f"╠{border}╣")
        print(f"║ Latency (ms)     p50    p90    p95    p99    p999            ║")
        print(f"║   Overall:       {f'{overall_p[0]:.1f}'.ljust(6)}{f'{overall_p[1]:.1f}'.ljust(7)}{f'{overall_p[2]:.1f}'.ljust(7)}{f'{overall_p[3]:.1f}'.ljust(7)}{f'{overall_p[4]:.1f}'.ljust(15)}║")
        print(f"║   Reads:         {f'{read_p[0]:.1f}'.ljust(6)}{f'{read_p[1]:.1f}'.ljust(7)}{f'{read_p[2]:.1f}'.ljust(7)}{f'{read_p[3]:.1f}'.ljust(7)}{f'{read_p[4]:.1f}'.ljust(15)}║")
        print(f"║   Writes:        {f'{write_p[0]:.1f}'.ljust(6)}{f'{write_p[1]:.1f}'.ljust(7)}{f'{write_p[2]:.1f}'.ljust(7)}{f'{write_p[3]:.1f}'.ljust(7)}{f'{write_p[4]:.1f}'.ljust(15)}║")
        print(f"╠{border}╣")
        print(f"║ Reliability                                                  ║")
        print(f"║   Success Rate:  {f'{m.success_rate:.1f}%'.ljust(44)}║")
        print(f"║   Cache Hit Rate: {f'{m.cache_hit_rate:.1f}%'.ljust(43)}║")
        print(f"║   Errors:        {str(m.errors).ljust(44)}║")
        print(f"╚{border}╝\n")

        results = {
            "configuration": {
                "nodes": len(self.nodes),
                "clients": self.clients,
                "operations": self.operations,
                "read_ratio": self.read_ratio,
                "key_space": self.key_space
            },
            "throughput": {
                "total_ops_sec": m.total_throughput,
                "reads_ops_sec": m.read_throughput,
                "writes_ops_sec": m.write_throughput
            },
            "latency_ms": {
                "overall": {"p50": overall_p[0], "p90": overall_p[1], "p95": overall_p[2], "p99": overall_p[3], "p999": overall_p[4]},
                "reads": {"p50": read_p[0], "p90": read_p[1], "p95": read_p[2], "p99": read_p[3], "p999": read_p[4]},
                "writes": {"p50": write_p[0], "p90": write_p[1], "p95": write_p[2], "p99": write_p[3], "p999": write_p[4]}
            },
            "reliability": {
                "success_rate_percent": m.success_rate,
                "cache_hit_rate_percent": m.cache_hit_rate,
                "errors": m.errors
            }
        }

        with open("load_test_results.json", "w") as f:
            json.dump(results, f, indent=2)
        logger.info("Results saved to load_test_results.json")

def main():
    parser = argparse.ArgumentParser(description="Distributed Cache Load Tester")
    parser.add_argument("--nodes", type=str, default="http://localhost:8001,http://localhost:8002,http://localhost:8003", help="Comma-separated node URLs")
    parser.add_argument("--clients", type=int, default=50, help="Number of concurrent clients")
    parser.add_argument("--operations", type=int, default=10000, help="Total operations to perform")
    parser.add_argument("--key-space", type=int, default=1000, help="Size of the key space")
    parser.add_argument("--read-ratio", type=float, default=0.8, help="Ratio of reads to total ops")
    parser.add_argument("--value-size", type=int, default=100, help="Size of values in bytes")
    parser.add_argument("--ttl", type=int, default=60, help="TTL for SET operations in seconds")
    parser.add_argument("--warmup", type=int, default=1000, help="Number of warmup operations")
    
    args = parser.parse_args()
    tester = LoadTester(args)
    
    loop = asyncio.get_event_loop()
    
    def shutdown():
        logger.info("Shutdown signal received...")
        tester.shutdown_event.set()
        for task in asyncio.all_tasks(loop):
            task.cancel()
            
    try:
        loop.run_until_complete(tester.run())
    except KeyboardInterrupt:
        shutdown()
    finally:
        # Give cancelled tasks a moment to cleanup
        try:
            loop.run_until_complete(asyncio.sleep(0.1))
        except:
            pass

if __name__ == "__main__":
    main()
