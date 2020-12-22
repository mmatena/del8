"""TODO: Add title."""
import contextlib
import functools
from typing import Sequence
import inspect

# NOTE: I think this is only thing we are importing from pinject. We
# can probably write our own version so we need fewer dependencies.
from pinject import copy_args_to_public_fields

from .utils import decorator_util as dec_util
from . import serialization


SerializationType = serialization.SerializationType


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

        @dec_util.wraps_class(cls)
        class DataClass(cls, _DataClassABC):
            data_class_attrs = attributes
            data_class_attr_names = attributes_names

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
                        raise ValueError(
                            f"Keyword argument {key} should represent an attribute "
                            "but starts with an underscore."
                        )
                kwargs = {a: getattr(self, a) for a in self.data_class_attr_names}
                kwargs.update(**attr_overrides)
                return self.__class__(**kwargs)

        return DataClass

    return dec


def is_data_instance(instance):
    return isinstance(instance, _DataClassABC)
