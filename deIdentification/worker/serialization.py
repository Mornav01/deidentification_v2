import jsonpickle


try:
    import jsonpickle.ext.numpy as jsonpickle_numpy

    jsonpickle_numpy.register_handlers()
except ImportError:
    pass


def serialize(obj):
    """Serialize object to a json string.

    Args:
        obj: Object to serialize

    Returns:
        str: Serialized object
    """
    return jsonpickle.encode(obj)


def deserialize(serialized_obj):
    """Deserialize string to object.

    Args:
        serialized_obj (str): serialized by serialize().

    Returns:
        object: Deserialized object
    """
    return jsonpickle.decode(serialized_obj)


def flatten(obj):
    """Flatten object to a json complaint dict.

    Args:
        obj: Object to flatten

    Returns:
        dict: flattened object
    """
    p = jsonpickle.pickler.Pickler()
    return p.flatten(obj)


def restore(flattened_obj):
    """Restore flattened object to object.

    Args:
        flattened_obj (str): flattened by flatten().

    Returns:
        object: Restore object
    """
    p = jsonpickle.unpickler.Unpickler()
    return p.restore(flattened_obj)
