"""TODO: Add title."""
import contextlib
import inspect
import itertools

from .. import data_class
from ..utils import decorator_util as dec_util


Parameter = inspect.Parameter


###############################################################################


class BindingNotFoundException(Exception):
    pass


###############################################################################


class InjectionContext(object):
    """Singleton containing global state."""

    def __init__(self):
        self._scope_stack = []

        self._default_binding_specs_stack = []

        # The bottom of this stack will be the "global" bindings
        # and won't be popped off until we are finished with a run.
        #
        # Note that "global" here just means not directly attached
        # to a scope; it can still contain specifications that dictate
        # in which scopes it does (not) apply.
        self._binding_specs_stack = []

    def push_scope(self, scope):
        self._scope_stack.append(scope)
        if isinstance(scope, BindingScope):
            self._binding_specs_stack.append(scope.get_binding_specs())

    def pop_scope(self):
        scope = self._scope_stack.pop()
        if isinstance(scope, BindingScope) and scope.is_my_binding_specs(
            self._binding_specs_stack[-1]
        ):
            self._binding_specs_stack.pop()
        return scope

    @contextlib.contextmanager
    def scope(self, scope):
        self.push_scope(scope)
        try:
            yield scope
        finally:
            self.pop_scope()

    def push_default_binding_specs(self, specs):
        self._default_binding_specs_stack.append(specs)

    def pop_default_binding_specs(self):
        return self._default_binding_specs_stack.pop()

    @contextlib.contextmanager
    def default_binding_specs_scope(self, specs):
        self.push_default_binding_specs(specs)
        try:
            yield specs
        finally:
            self.pop_default_binding_specs()

    def get_binding_specs_by_precedence(self):
        # Note that position in the stack has the opposite effect on
        # precedence for regular and default binding specs.
        return itertools.chain(
            reversed(self._binding_specs_stack), self._default_binding_specs_stack
        )

    def inject_from_parameter(self, parameter: Parameter):
        # Right now, we inject parameters purely based on their name. In the
        # future, we may which to take annotations into account.
        try:
            # Try to inject the parameter. This means that bound arguments will
            # take higher precedence than default arguments.
            return self.inject_from_str(parameter.name)
        except BindingNotFoundException as e:
            if parameter.default != Parameter.empty:
                # Return the default if the parameter has one and we were not able
                # to find a binding.
                return parameter.default
            #
            #
            #
            print(self._default_binding_specs_stack)
            #
            #
            #
            raise e

    def inject_from_str(self, key_str):
        # We use underscores to indicate stuff elsewhere. Ignore them
        # while injecting.
        if key_str.startswith("_"):
            key_str = key_str[1:]

        # The order of traversal dictates precedence.
        for specs in self.get_binding_specs_by_precedence():
            for spec in specs:
                if spec.matches_str(key_str):
                    return self.inject_from_binding(spec.get_binding())
        raise BindingNotFoundException(f"No binding found for key string {key_str}.")

    def inject_from_binding(self, binding):
        if inspect.isclass(binding):
            return self.inject_from_cls(binding)
        else:
            # Assume that we have an instance bound, so just inject it.
            return binding

    def inject_from_cls(self, cls):
        # See if we have any bindings for the class directly. The order of
        # traversal dictates precedence.
        for specs in self.get_binding_specs_by_precedence():
            for spec in specs:
                if spec.matches_cls(cls):
                    return self.inject_from_binding(spec.get_binding())

        # Now try to inject the arguments to the class's initializer.
        #
        # NOTE: We may want to append the exceptions from each parameter to
        # a list and raise a BindingNotFoundException after the loop that
        # contains all the information on the bindings we are missing.
        kwargs = {
            p.name: self.inject_from_parameter(p)
            for p in dec_util.get_initializer_parameters(cls)
        }

        return cls(**kwargs)


# Despite not starting with an underscore, do not access this
# variable from outside this file.
CONTEXT = InjectionContext()
# Don't accidentally set stuff on the class instead of the
# singleton instance.
del InjectionContext

inject_from_parameter = CONTEXT.inject_from_parameter
inject_from_cls = CONTEXT.inject_from_cls
###############################################################################


class Scope(object):
    """Abstract base class."""

    pass


class StringScope(Scope):
    def __init__(self, scope_str):
        self.scope_str = scope_str


class ExecutableScope(Scope):
    # TODO: Figure out exactly how (and if) I want to use this.
    pass


class BindingScope(Scope):
    # TODO: Figure out exactly how I want this to work.
    def __init__(self, binding_specs):
        self._binding_specs = binding_specs

    def get_binding_specs(self):
        return self._binding_specs

    def is_my_binding_specs(self, binding_specs):
        return binding_specs is self._binding_specs


###############################################################################


class BindingSpec(object):
    """Abstract base class."""

    def matches_str(self, key_str):
        return False

    def matches_cls(self, cls):
        return False

    # def matches_full_str_scope(self, full_str_scope):
    #     return False

    def get_binding(self):
        return self.binding


@data_class.data_class()
class ArgNameBindingSpec(BindingSpec):
    """Binding spec based on the name of a function/initializer argument."""

    def __init__(self, name, binding):
        self.name = name
        # NOTE: For now, a binding will just be a class or instance. Later, we
        # might want to support stuff like macros.
        self.binding = binding

    def matches_str(self, key_str):
        return key_str == self.name


###############################################################################


def name_scope(name: str):
    # We use underscores to indicate stuff elsewhere. Ignore them
    # while injecting.
    if name.startswith("_"):
        name = name[1:]

    return CONTEXT.scope(StringScope(name))


def binding_scope(binding_spec):
    # binding_spec can be either a BindingSpec or sequence of them.
    if isinstance(binding_spec, BindingSpec):
        binding_spec = [binding_spec]
    # Ensure immutability of the sequence.
    binding_spec = tuple(binding_spec)
    return CONTEXT.scope(BindingScope(binding_spec))


def binding_by_name_scope(name, binding):
    binding_spec = ArgNameBindingSpec(name, binding)
    return binding_scope(binding_spec)


def binding_by_name_scopes(name_binding_pairs):
    binding_specs = []
    for name, binding in name_binding_pairs:
        binding_specs.append(ArgNameBindingSpec(name, binding))
    return binding_scope(binding_specs)


def default_binding_specs_scope(binding_specs):
    # NOTE: Probably should not be used for any reason except for the
    # the default bindings on executable classes.
    return CONTEXT.default_binding_specs_scope(binding_specs)


###############################################################################


@contextlib.contextmanager
def multiple(*scopes):
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(s) for s in scopes]
