import asyncio
import signal
import structlog
import uvicorn
from src.config import Settings
from src.core.store import CacheStore
from src.metrics.prometheus import CacheMetrics, default_metrics
from src.cluster.coordinator import ClusterCoordinator
from src.api.rest import create_app

logger = structlog.get_logger()

def setup_logging(level: str = "INFO") -> None:
    """Configure structlog for JSON structured logging."""
    import logging
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        cache_logger_on_first_use=True,
    )

async def run_node() -> None:
    """Boot and run a single cache node."""
    settings = Settings()
    setup_logging(settings.log_level)
    
    logger.info("node_starting", node_id=settings.node_id, 
                rest_port=settings.rest_port, grpc_port=settings.grpc_port)
    
    # Create components
    metrics = default_metrics
    store = CacheStore(
        max_keys=settings.max_keys,
        eviction_policy=settings.eviction_policy,
        default_ttl=settings.default_ttl,
        reaper_interval=settings.reaper_interval_seconds
    )
    store.start()
    
    coordinator = ClusterCoordinator(settings, store, metrics)
    app = create_app(coordinator, settings, metrics)
    
    # Start cluster coordination
    await coordinator.start()
    
    # Configure uvicorn
    config = uvicorn.Config(
        app, host=settings.host, port=settings.rest_port,
        log_level=settings.log_level.lower(),
        access_log=False  # We use our own logging middleware
    )
    server = uvicorn.Server(config)
    
    # Handle graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.info("shutdown_signal_received")
        shutdown_event.set()
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler
    
    logger.info("node_started", node_id=settings.node_id,
                rest_url=f"http://{settings.host}:{settings.rest_port}",
                eviction_policy=settings.eviction_policy,
                max_keys=settings.max_keys)
    
    # Run server
    try:
        await server.serve()
    finally:
        logger.info("node_shutting_down", node_id=settings.node_id)
        await coordinator.stop()
        store.stop()
        logger.info("node_stopped", node_id=settings.node_id)

def main():
    """Entry point."""
    asyncio.run(run_node())

if __name__ == "__main__":
    main()
