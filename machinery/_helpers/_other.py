def add_error(catcher, error):
    """
    Adds an error to an error_catcher.

    This means if it's callable we call it with the error and if it's a ``list``
    or ``set`` we add the error to it.
    """
    if callable(catcher):
        catcher(error)
    elif type(catcher) is list:
        catcher.append(error)
    elif type(catcher) is set:
        catcher.add(error)
