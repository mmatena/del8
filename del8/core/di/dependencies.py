"""TODO: Add title."""
import abc

from . import executable
from ..utils import type_util


def _extract_exe_cls(binding):
    if type_util.islambda(binding):
        # We assume that a lambda default binding is used
        # purely for getting around circular dependency issues
        # when injecting classes.
        binding = binding()
        assert not executable.is_executable_instance(binding)

    if executable.is_executable_instance(binding):
        return binding.__class__
    elif executable.is_executable_class(binding):
        return binding
    return None


def _get_dep_children(exe_cls):
    children = []
    for binding in exe_cls._default_bindings.values():
        exe_cls = _extract_exe_cls(binding)
        if exe_cls:
            children.append(exe_cls)
    return children


def get_all_executables_classes_in_graph(executable_classes, binding_specs):
    # NOTE: As long as all executable classes are injected, I think
    # this should provide a superset of the classes that are actually
    # used.
    if not type_util.isiterable(executable_classes):
        executable_classes = [executable_classes]

    # First add the classes we know are in the graph without
    # having to do any traversal.
    executable_classes = set(executable_classes)
    for spec in binding_specs:
        binding = spec.get_binding()
        exe_cls = _extract_exe_cls(binding)
        if exe_cls:
            executable_classes.add(exe_cls)

    # Do a DFS over the graph consisting of dependencies we know.
    visited = set()
    to_traverse = list(executable_classes)
    while to_traverse:
        cls = to_traverse.pop()
        if cls not in visited:
            to_traverse.extend(_get_dep_children(cls))
        visited.add(cls)

    return visited


def get_all_apt_get_packages(all_executable_classes):
    apt_get_packages = set()
    for exe_cls in all_executable_classes:
        assert executable.is_executable_class(exe_cls), "Expecting @executable."
        apt_get_packages.add_all(exe_cls._apt_get_packages)
    return sorted(apt_get_packages)


def get_all_pip_packages(all_executable_classes):
    pip_packages = set()
    for exe_cls in all_executable_classes:
        assert executable.is_executable_class(exe_cls), "Expecting @executable."
        pip_packages.add_all(exe_cls._pip_packages)
    return sorted(pip_packages)
