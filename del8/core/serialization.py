"""TODO: Add docs."""
import inspect
import json
from pydoc import locate


_LOCATE_CACHE = {}


def _cached_locate(path):
    if path not in _LOCATE_CACHE:
        _LOCATE_CACHE[path] = locate(path)
    return _LOCATE_CACHE[path]


class SerializationType(object):
    CLASS_OBJECT = "CLASS_OBJECT"
    DATA_CLASS = "DATA_CLASS"


def serialize_class(klass):
    return {
        "__type__": SerializationType.CLASS_OBJECT,
        "module": klass.__module__,
        "name": klass.__name__,
    }


def deserialize_class(serialized_class):
    # TODO: Probably allow some (user-defined) mapping/transformations as that
    # could improve backwards compatibility and make this less fragile.
    module = serialized_class["module"]
    name = serialized_class["name"]
    path = f"{module}.{name}"
    # return locate(path)
    return _cached_locate(path)


def _process_attrs(klass, serialized_attrs):
    shared = {}
    klass_only = {}
    serialized_only = {}

    for attr in klass.data_class_attr_names:
        if attr in serialized_attrs:
            shared[attr] = serialized_attrs[attr]
        else:
            klass_only[attr] = None

    for attr in serialized_attrs.keys():
        if attr not in klass.data_class_attr_names:
            serialized_only[attr] = serialized_attrs[attr]

    return shared, klass_only, serialized_only


def _deserialize_object_hook(dikt):
    if "__type__" in dikt:
        dtype = dikt["__type__"]
        if dtype == SerializationType.CLASS_OBJECT:
            return deserialize_class(dikt)
        elif dtype == SerializationType.DATA_CLASS:
            klass = dikt["__class__"]
            attrs = dikt["attributes"]
            shared, klass_only, serialized_only = _process_attrs(klass, attrs)
            del klass_only, serialized_only
            # TODO: We'll probably want some settings for what to do when the *_only
            # are non-empty. There might be some global/class/instance level settings
            # that might also be saved. There could also be some back-compatability
            # logis defined somewhere on a per-class basis.
            return klass(**shared)
        else:
            raise ValueError(f"Unrecognized serialization type {dikt['__type__']}.")

    return dikt


def deserialize(str_or_json, **kwargs):
    if isinstance(str_or_json, str):
        s = str_or_json
    else:
        # Here we assume that the input is a serialized JSON object.
        # NOTE: This is probably a more efficient way of doing this, but
        # given our use-cases, this probably shouldn't be an issue.
        s = json.dumps(str_or_json)
    return json.loads(s, object_hook=_deserialize_object_hook, **kwargs)


def _serialize_json_handler(obj):
    if hasattr(obj, "as_json"):
        return obj.as_json()
    elif inspect.isclass(obj):
        return serialize_class(obj)
    else:
        raise TypeError(
            f"Object of type {type(obj)} with value of {repr(obj)} is not JSON serializable."
        )


def serialize(obj, **kwargs):
    return json.dumps(obj, default=_serialize_json_handler, sort_keys=True, **kwargs)


def to_pretty_json(obj):
    # Probably not the right place for this, but what'cha gonna do?
    root_jason = json.loads(serialize(obj))

    def inner(jason):
        if isinstance(jason, dict):
            if "__type__" in jason:
                dtype = jason["__type__"]
                if dtype == SerializationType.CLASS_OBJECT:
                    module = jason["module"]
                    name = jason["name"]
                    return f"{module}.{name}"
                elif dtype == SerializationType.DATA_CLASS:
                    jason = jason["attributes"]
            return {k: inner(v) for k, v in jason.items()}
        elif isinstance(jason, list):
            return [inner(v) for v in jason]
        else:
            return jason

    return inner(root_jason)
