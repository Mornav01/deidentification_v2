class RaiseException(Exception):
    def __init__(self, message="Exception Raised"):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"RaiseException: {self.message}"