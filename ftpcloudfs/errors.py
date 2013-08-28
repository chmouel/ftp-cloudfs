"""
Errors for ObjectStorageFS
"""

class IOSError(OSError, IOError):
    """
    Subclass of OSError and IOError.

    This is needed because pyftpdlib catches either OSError, or
    IOError depending on which operation it is performing, which is
    perfectly correct, but makes our life more difficult.

    However our operations don't map to simple functions, and have
    common infrastructure.  These common infrastructure functions can
    be called from either context and so don't know which error to
    raise.

    Using this combined type everywhere fixes the problem at very
    small cost (multiple inheritance!).
    """

