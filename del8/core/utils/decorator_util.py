"""Utilities for decorators."""
import inspect
import functools
import types


###############################################################################


def _to_signature(signature):
    # Lets us pass in things such as a function instead of a signature.
    if callable(signature):
        return inspect.signature(signature)
    else:
        return signature


###############################################################################


def wraps_class(wrapped):
    """Version of `wraps` that does what I need it to do.

    Usually needed for my version of serialization and maybe
    dependency injection to work.

    Note that `wrapped` can be a function as well as a class. However,
    `wrapper` should be a class.
    """

    def dec(wrapper):
        wrapper.__name__ = wrapped.__name__
        wrapper.__module__ = wrapped.__module__
        wrapper.__qualname__ = wrapped.__qualname__
        return wrapper

    return dec


def signature_with_prepended_arg(fn, arg_name):
    # NOTE: This function is not a decorator itself.
    sig = inspect.signature(fn)
    params = (
        inspect.Parameter(arg_name, inspect.Parameter.POSITIONAL_OR_KEYWORD),
    ) + tuple(sig.parameters.values())
    sig = sig.replace(parameters=params)
    return sig


def fn_to_method(fn, method_name=None):
    # NOTE: This function is not a decorator itself.

    def method(self, *args, **kwargs):
        return fn(*args, **kwargs)

    method.__signature__ = signature_with_prepended_arg(fn, "self")

    if method_name:
        method.__name__ = method_name

    return method


def fn_to_cls(method_name="__call__"):
    def dec(fn):
        @wraps_class(fn)
        class Functor(object):
            pass

        method = fn_to_method(fn, method_name)
        setattr(Functor, method_name, method)

        return Functor

    return dec


def always_as_kwargs(signature=None, skip_first=False):
    # This lets us pass a function instead of a signature.
    signature = _to_signature(signature)

    def dec(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            all_kwargs = to_kwargs_only(signature, args, kwargs, skip_first=skip_first)
            return fn(**all_kwargs)

        return inner

    return dec


def to_kwargs_only(signature, args, kwargs, skip_first=False):
    # NOTE: This function is not a decorator itself.
    signature = _to_signature(signature)
    var_names = [p.name for p in signature.parameters.values()]
    if skip_first:
        # Typically used in cases such as unbound methods where
        # the first argument is going to be self.
        var_names = var_names[1:]
    all_kwargs = {}
    all_kwargs.update(kwargs)
    all_kwargs.update(zip(var_names, args))
    return all_kwargs


def get_missing_parameters(signature, kwargs, ignore_params=()):
    # NOTE: This function is not a decorator itself.
    #
    # Intended to be used when we are using @always_as_kwargs,
    # so all of the parameters will be in the kwargs.
    #
    # Does not do anything if there are parameters in kwargs
    # that are not in the signature.
    parameters = _to_signature(signature).parameters
    missing_keys = set(parameters.keys()) - set(kwargs.keys()) - set(ignore_params)
    return [param for param in parameters.values() if param.name in missing_keys]


def get_missing_method_parameters(signature, kwargs, ignore_params=()):
    # Same as `get_missing_parameters`, but ignores the "self" param by default.
    ignore_params = ("self",) + tuple(ignore_params)
    return get_missing_parameters(signature, kwargs, ignore_params)


def get_initializer_parameters(cls):
    # NOTE: This function is not a decorator itself.
    if cls.__init__ == object.__init__:
        # Classes that don't overide __init__ will have object.__init__, which
        # has (*args, **kwargs). However, they are initialized as just cls(), so
        # we return no parameters.
        return []
    params = inspect.signature(cls.__init__).parameters.values()
    # Ignore the self parameter.
    return list(params)[1:]


def is_public_method(attr_name, attr_value):
    if not callable(attr_value):
        return False
    elif attr_name.startswith("_"):
        return False
    return True


def get_user_defined_public_methods(cls, superclass=object):
    # Public means that the method name does not start with
    # an underscore. User-defined means that the method is not
    # present on the `superclass` parameter passed in.
    #
    # Class and static methods are not included in what this
    # function returns.
    # NOTE: I'm not sure how it will handle static methods.
    #
    # It will also only work for methods defined directly on the
    # class. Inherited ones won't work, even if `superclass` is
    # set to object.
    #
    # NOTE: I'm not sure how this function is/should going to handle
    # stuff like properties or getters/setters or anything else fancy.
    superclassdict = superclass.__dict__
    ret = {}
    for key, value in dict(cls.__dict__).items():
        if key.startswith("_"):
            continue
        elif key in superclassdict:
            continue
        elif not callable(value):
            continue
        ret[key] = value
    return ret
