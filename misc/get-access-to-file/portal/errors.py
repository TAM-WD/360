class ServiceError(Exception):
    def __init__(self, code, message, status=503, *, upstream_status=None, upstream_service=None):
        self.code = code
        self.message = message
        self.status = status
        self.upstream_status = upstream_status
        self.upstream_service = upstream_service
        super().__init__(message)
