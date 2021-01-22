"""TODO: Add title."""
import re
import collections


def get_leaf_layers(model_or_layer):
    # NOTE: Shared layers will be repeated in the output.
    try:
        layers = model_or_layer.layers
    except AttributeError:
        return [model_or_layer]

    leaf_layers = []
    for layer in layers:
        leaf_layers.extend(get_leaf_layers(layer))
    return leaf_layers


def _extract_variable_name(name):
    # NOTE: Only supports variables names without the namescope attached.
    assert "/" not in name
    return re.match(r"^(.+):", name).group(1)


def get_layer_path_to_variables(model_or_variables, ignored_prefix_length=0):
    if isinstance(model_or_variables, (list, tuple)):
        variables = model_or_variables
    else:
        variables = model_or_variables.variables

    ret = collections.defaultdict(dict)
    for var in variables:
        name = var.name

        *layer_path, var_name = name.split("/")
        # layer_name = layer_path[-1]
        layer_path = "/".join(layer_path[ignored_prefix_length:])
        var_name = _extract_variable_name(var_name)

        ret[layer_path][var_name] = var

    return dict(ret)
