"""TODO: Add title."""
import contextlib
import functools
from typing import Any, Dict, Sequence
import inspect

from ..utils import decorator_util as dec_util
from ..utils import type_util
from . import scopes


_fn_to_exec_cls = dec_util.fn_to_cls("call")


def _ensure_exec_cls(cls_or_fn):
    if inspect.isclass(cls_or_fn):
        return cls_or_fn
    else:
        # Assume we are dealing with a function.
        return _fn_to_exec_cls(cls_or_fn)


def _get_user_defined_public_methods(cls):
    return {
        k: v
        for k, v in dec_util.get_user_defined_public_methods(cls).items()
        if k != "call"
    }


class _ExecutableABC(object):
    """Used purely for the purposes of the `is_executable_instance` function."""


def _create_default_binding_specs(default_bindings):
    if not default_bindings:
        return ()
    binding_specs = []
    for name, binding in default_bindings.items():
        if type_util.islambda(binding):
            binding = binding()
            if is_executable_instance(binding):
                raise TypeError(
                    "Please do not use lambdas to inject executable instances. "
                    "The lambdas were intended only to get around circular dependency "
                    "issues when injecting classes."
                )
        binding_specs.append(scopes.ArgNameBindingSpec(name, binding))
    # Make immutable.
    return tuple(binding_specs)


def _get_default_binding_specs(cls, default_bindings):
    # Ensures that the binding specs are (lazily) initialized, and makes sure
    # that the initialization only happens once per class.
    if cls._default_binding_specs is None:
        cls._default_binding_specs = _create_default_binding_specs(default_bindings)
    return cls._default_binding_specs


def _wrap_public_method(fn, scope=None, default_bindings=None, skip_first=True):
    if fn == object.__init__:
        # Classes that don't overide __init__ will have object.__init__. It has
        # has (*args, **kwargs), which messes up our processing. Since it should
        # not be called with any arguments, we skip the the processing.
        return fn

    @functools.wraps(fn)
    def inner(self, *args, **kwargs):
        kwargs = dec_util.to_kwargs_only(fn, args, kwargs, skip_first=skip_first)
        del args

        default_binding_specs = _get_default_binding_specs(
            self.__class__, default_bindings
        )

        if scope:
            # NOTE: Right now we assume `scope` is a string. In the future,
            # we may wish to supply other styles of scopes.
            outer_scope = scopes.name_scope(scope)
        else:
            outer_scope = contextlib.suppress()

        with outer_scope, scopes.default_binding_specs_scope(default_binding_specs):
            missing_params = dec_util.get_missing_method_parameters(fn, kwargs)
            added_kwargs = {}
            for p in missing_params:
                with scopes.name_scope(p.name):
                    # Note that the inject_from_parameter call will raise an exception
                    # if it cannot find a binding for a non-optional parameter.
                    injected = scopes.inject_from_parameter(p)
                    if not p.name.startswith("_") and is_executable_instance(injected):
                        injected = injected()
                    added_kwargs[p.name] = injected

            return fn(self, **kwargs, **added_kwargs)

    return inner


def executable(
    *,
    apt_get_packages: Sequence[str] = (),
    pip_packages: Sequence[str] = (),
    # To get around potentially circular dependency issues, users can
    # provide a binding in the form of a zero-argument lambda function
    # that returns the binding. Note that it MUST be a lambda function;
    # regular functions will not be lazily evaluated and just be bound.
    #
    # NOTE: This appears fairly clean but was done with little research
    # on the issue. It is a good idea to look into other solutions to
    # the circular dependency issue.
    default_bindings: Dict[str, Any] = None,
):
    if default_bindings is None:
        default_bindings = {}

    def dec(cls_or_fn):
        cls = _ensure_exec_cls(cls_or_fn)

        @dec_util.wraps_class(cls)
        class Executable(cls, _ExecutableABC):
            _apt_get_packages = tuple(apt_get_packages)
            _pip_packages = tuple(pip_packages)
            _default_bindings = default_bindings

            # NOTE: This only needed due to @data_class having a dependency on pinject.
            # We only use a single function unrelated to anything specific to pinject.
            # Once we remove that, we remove this.
            _pip_packages += ("pinject",)

            # Will be filled in at the first wrapped instance call. We do this
            # to get around circular dependency issues.
            # NOTE: See if we actually need to do this.
            _default_binding_specs = None

            __init__ = _wrap_public_method(
                cls.__init__, default_bindings=default_bindings
            )
            __call__ = _wrap_public_method(cls.call, default_bindings=default_bindings)

        # TODO: Have a similar wrapper for public class and maybe static
        # methods. They are completely ignored right now. (Will be present
        # but no injection and scoping.)
        methods = _get_user_defined_public_methods(cls)
        for name, method in methods.items():
            setattr(
                Executable,
                name,
                _wrap_public_method(
                    method, scope=name, default_bindings=default_bindings
                ),
            )

        return Executable

    return dec


def is_executable_instance(instance):
    return isinstance(instance, _ExecutableABC)


def is_executable_class(klass):
    return issubclass(klass, _ExecutableABC)
