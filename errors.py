class ConfigurationError(Exception):
    def __init__(self, message=''):
        super().__init__(message)


class ExporterError(Exception):
    def __init__(self, message=''):
        super().__init__(message)


class SchedulingError(Exception):
    def __init__(self, message=''):
        super().__init__(message)