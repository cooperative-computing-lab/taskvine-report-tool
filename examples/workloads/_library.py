"""Functions for the library workloads, importable on workers by name."""


def square(x):
    return x * x


def concat(a, b):
    return f"{a}-{b}"


def slow(x):
    import time
    time.sleep(0.2)
    return x


def kill_library_once(marker, x):
    # In fork mode a call runs in a child of the library process.  The first call to see no marker kills the
    # library, which the worker reports and the Manager replaces.
    import os
    import signal
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getppid(), signal.SIGKILL)
    return x


# Workers cannot import this module, so its functions travel by value.
import sys as _sys
import cloudpickle as _cloudpickle

_cloudpickle.register_pickle_by_value(_sys.modules[__name__])
