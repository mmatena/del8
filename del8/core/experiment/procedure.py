"""TODO: Add title.

NOTE: Not doing procedures in the first pass.
"""
# import abc

# from ..utils import decorator_util as dec_util


# class _ProcedureABC(abc.ABC):
#     def __init__(self, params):
#         # NOTE: Subclasses should probably not change the signature of
#         # the __init__ method. Adding some more initialization in it
#         # though is probably fine.
#         self.params = params

#     @classmethod
#     def from_params(cls, params):
#         return cls(params)

#     @abc.abstractmethod
#     def create_execution_item(self):
#         # NOTE: This is where we'll handle setting up the injection config
#         # and maybe some kwargs for the executable class.
#         raise NotImplementedError


# def procedure(
#     *,
#     # Must be a @data_class.
#     params_cls,
#     # Must be a @executable.
#     executable_cls,
#     name=None,
#     description=None,
# ):
#     def dec(cls):
#         @dec_util.wraps_class(cls)
#         class Procedure(cls, _ProcedureABC):
#             pass

#         return Procedure

#     return dec
