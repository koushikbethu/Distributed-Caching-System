import time
import uuid
import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger()

class RequestIdMiddleware(BaseHTTPMiddleware):
    """Inject a unique request ID into each request for tracing."""
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

class LoggingMiddleware(BaseHTTPMiddleware):
    """Log all requests with structured JSON logging."""
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Skip logging for health and metrics
        if request.url.path in ("/health", "/metrics"):
            return await call_next(request)
            
        response = await call_next(request)
        
        duration_ms = (time.time() - start_time) * 1000
        request_id = getattr(request.state, "request_id", "unknown")
        
        logger.info(
            "http_request",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2)
        )
        
        return response

class MetricsMiddleware(BaseHTTPMiddleware):
    """Record request metrics for Prometheus."""
    
    def __init__(self, app, metrics):
        super().__init__(app)
        self.metrics = metrics
        
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time
        
        self.metrics.record_request(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code,
            duration=duration
        )
        
        return response
