"""TODO: Add title."""
import contextlib
import functools
import inspect

from typing import Sequence, TypeVar, Generic

# NOTE: I think this is only thing we are importing from pinject. We
# can probably write our own version so we need fewer dependencies.
from pinject import copy_args_to_public_fields

from del8.core import serialization
from del8.core.utils import decorator_util as dec_util
from del8.core.utils import type_util


SerializationType = serialization.SerializationType

T = TypeVar("T")

BANNED_ATTRIBUTE_NAMES = frozenset({"uuid", "group_uuid", "exp_uuid", "run_uuid"})


class _DataClassABC(object):
    """Used purely for the purposes of the `is_data_instance` function."""


def data_class():
    def dec(cls):

        attributes = [
            p
            for p in dec_util.get_initializer_parameters(cls)
            if not p.name.startswith("_")
        ]
        attributes_names = [p.name for p in attributes]

        for name in attributes_names:
            # TODO: I'll probably want to change RunKey.
            if cls.__name__ != "RunKey" and name in BANNED_ATTRIBUTE_NAMES:
                raise TypeError(
                    f"Data classes cannot have an attribute with name {name}."
                )

        @dec_util.wraps_class(cls)
        class DataClass(cls, _DataClassABC):
            data_class_attrs = attributes
            data_class_attr_names = attributes_names
            _data_class_refs = None

            @property
            def data_class_refs(self):
                kls = self.__class__
                if kls._data_class_refs is None:
                    kls._data_class_refs = _creates_refs_dict(kls.data_class_attrs)
                return kls._data_class_refs

            __init__ = copy_args_to_public_fields(cls.__init__)

            def as_json(self):
                return {
                    "__class__": serialization.serialize_class(self.__class__),
                    "__type__": SerializationType.DATA_CLASS,
                    "attributes": {
                        a: getattr(self, a) for a in self.data_class_attr_names
                    },
                }

            def copy(self, **attr_overrides):
                # Volatile state (attributes beginning with an underscore) is not copied.
                # The copy is also shallow.
                for key in attr_overrides.keys():
                    if key.startswith("_"):
                        raise TypeError(
                            f"Keyword argument {key} should represent an attribute "
                            "but starts with an underscore."
                        )
                kwargs = {a: getattr(self, a) for a in self.data_class_attr_names}
                kwargs.update(**attr_overrides)
                return self.__class__(**kwargs)

            def __eq__(self, other):
                if not other or self.__class__ != other.__class__:
                    return False

                for name in self.data_class_attr_names:
                    # NOTE: The Nones are there mostly out of paranoia.
                    if getattr(self, name, None) != getattr(other, name, None):
                        return False

                return True

            def __hash__(self):
                # NOTE: Technically a hash. Not a good one though.
                # NOTE: I'm not really sure how this plays with mutable data classes,
                # which probably are not going to be supported in the first place.
                return 0

        return DataClass

    return dec


def is_data_instance(instance):
    return isinstance(instance, _DataClassABC)


###############################################################################


class Ref(Generic[T]):
    pass


def is_ref_name(name):
    return name.endswith("_uuid")


def get_dereferenced_name(name):
    if name.endswith("_uuid"):
        return name[: -len("_uuid")]
    else:
        raise TypeError(f"Invalid reference name {name}.")


def is_ref(annotation):
    if annotation.__class__ is Ref:
        return True
    elif getattr(annotation, "__origin__", None) is Ref:
        return True
    return False


def get_referenced(ref):
    if not is_ref(ref):
        raise TypeError("The argument `ref` must be a Ref[T]")
    # By how Ref is defined, it must have exactly zero or one args in order to exist.

    # Non-parameterized (i.e., `param: Ref`)
    if ref.__class__ is Ref:
        return None

    # Parameterized (i.e., `param: Ref[SomeClass]`)
    (referenced,) = type_util.get_args(ref)
    if type_util.islambda(referenced):
        referenced = referenced()
    return referenced


def _creates_refs_dict(parameters):
    # TODO: Handle collections of refs.
    refs = {}
    for p in parameters:
        if not is_ref(p.annotation):
            continue
        elif not is_ref_name(p.name):
            raise TypeError(
                f'Reference attribute {p.name} must have a name ending with "_uuid".'
            )
        refs[p.name] = get_referenced(p.annotation)
    return refs
