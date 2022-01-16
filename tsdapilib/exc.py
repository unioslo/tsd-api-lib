
# content verifier

class ContentVerificationError(Exception):
    # base exception
    pass

class ContentVerificationReadError(ContentVerificationError):
    # raised if we read too many bytes of a file.
    pass

class ContentVerificationMissingReferenceError(ContentVerificationError):
    # raised when a reference does not resolve to a path
    pass

class ContentVerificationReferenceTypeError(ContentVerificationError):
    # raised when finding a file, but expecting a directory
    # and vice versa.
    pass

class ContentVerificationMissingHashError(ContentVerificationError):
    # raise during incremental processing, if there is no
    # state for th givn reference, i.e. start() was not called.
    pass
