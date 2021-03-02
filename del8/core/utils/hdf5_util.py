"""TODO: Add title."""
import h5py

import numpy as np
import tensorflow as tf

_LIST_GROUP_NAME = "__list__"


def save_variables_to_hdf5(variables, filepath):
    with h5py.File(filepath, "w") as f:
        ls = f.create_group(_LIST_GROUP_NAME)
        ls.attrs["length"] = len(variables)
        for i, v in enumerate(variables):
            val = v.numpy()
            ds = ls.create_dataset(str(i), val.shape, dtype=val.dtype)
            # NOTE: Code modified from a section of tf source code here.
            if not val.shape:
                # scalar
                ds[()] = val
            else:
                ds[:] = val
            name = v.name
            if name.endswith(":0"):
                name = name[: -len(":0")]
            ds.attrs["name"] = name
            ds.attrs["trainable"] = v.trainable


def load_variables_from_hdf5(filepath, trainable=None):
    with h5py.File(filepath, "r") as f:
        if _LIST_GROUP_NAME not in f or len(f.keys()) > 1:
            # TODO: Support other nested structures for both writing and reading.
            raise ValueError(
                "Restoring variables from a hdf5 requires the hdf5 only to contain a list."
            )
        ls = f[_LIST_GROUP_NAME]

        variables = []
        for i in range(ls.attrs["length"]):
            ds = ls[str(i)]
            tr = trainable
            if trainable is None:
                tr = ds.attrs["trainable"]
            var = tf.Variable(ds, name=ds.attrs["name"], trainable=tr)
            variables.append(var)
        return variables


def save_np_dict_to_hdf5(np_dict, filepath):
    with h5py.File(filepath, "w") as f:
        for key, val in np_dict.items():
            # Do this to handle scalars.
            val = np.array(val)
            ds = f.create_dataset(key, val.shape, dtype=val.dtype)
            # NOTE: Code modified from a section of tf source code here.
            if not val.shape:
                # scalar
                ds[()] = val
            else:
                ds[:] = val


def load_np_dict_from_hdf5(filepath):
    ret = {}
    with h5py.File(filepath, "r") as f:
        for key in f.keys():
            ret[key] = np.array(f[key])
    return ret
