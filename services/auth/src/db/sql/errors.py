class DatabaseUnavailableError(Exception):
    """База данных недоступна."""

    def __init__(self, message: str = 'Database is unavailable'):
        self.message = message
        super().__init__(self.message)
