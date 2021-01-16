"""Meant to speed up and simplify processing of data."""
import abc
import collections
import itertools

from del8.core.storage.storage import RunState as StorageRunState
from del8.core.experiment import runs


class Data(object):
    _SINGLE_KEYS = {"uuid"}

    def __init__(
        self, base_data=None, parent_data=None, items=(), blobs=(), run_states=()
    ):
        self._base_data = self if base_data is None else base_data
        self._parent_data = parent_data

        self._items = tuple(items)
        self._blobs = tuple(blobs)
        self._run_states = tuple(run_states)

        self._datums = {
            "items": self._items,
            "blobs": self._blobs,
            "run_states": self._run_states,
        }

        self._indices = {
            "items": {},
            "blobs": {},
            "run_states": {},
        }

    @classmethod
    def create_base(cls, items=(), blobs=(), run_states=()):
        data = cls(items=items, blobs=blobs, run_states=run_states)
        for datum in itertools.chain(items, blobs, run_states):
            datum.set_base_data(data)
        return data

    ############################################

    def _get_index(self, data_name, key):
        indices = self._indices[data_name]
        if key not in indices:
            multiple = key not in self._SINGLE_KEYS
            indices[key] = self._create_index(
                self._datums[data_name], key, multiple=multiple
            )
        return indices[key]

    def _create_index(self, datums, key, multiple=True):
        index = collections.defaultdict(list)
        for datum in datums:
            if key == "CLASS":
                # NOTE: You can only use CLASS for items.
                datum_key = datum._item.__class__
            else:
                datum_key = getattr(datum, key)
            index[datum_key].append(datum)

        if multiple:
            return _MultiIndex({k: tuple(v) for k, v in index.items()})
        else:
            ret = _SingleIndex()
            for k, v in index.items():
                if len(v) > 1:
                    raise ValueError(
                        f"Found {len(index)} items with {key} of {k} "
                        "in index with multiple=False."
                    )
                ret[k] = v
            return ret

    ############################################

    def _create_subdata(self, **kwargs):
        # TODO: Think about when and how we can efficiently copy/share indices.
        return Data(base_data=self._base_data, parent_data=self, **kwargs)

    ############################################

    def _get_items_index(self, key):
        return self._get_index("items", key)

    def _get_blobs_index(self, key):
        return self._get_index("blobs", key)

    def _get_run_states_index(self, key):
        return self._get_index("run_states", key)

    ############################################

    def get_item_by_uuid(self, uuid):
        index = self._get_items_index("uuid")
        return index[uuid]

    def get_blob_by_uuid(self, uuid):
        index = self._get_blobs_index("uuid")
        return index[uuid]

    ############################################

    def get_run_data(self, run_uuid):
        items_index = self._get_items_index("run_uuid")
        blobs_index = self._get_blobs_index("run_uuid")
        run_states_index = self._get_run_states_index("run_uuid")
        return self._create_subdata(
            items=items_index[run_uuid],
            blobs=blobs_index[run_uuid],
            run_states=run_states_index[run_uuid],
        )

    def get_items_by_class(self, cls):
        index = self._get_items_index("CLASS")
        return index[cls] if cls in index else []

    def get_single_item_by_class(self, cls):
        items = self.get_items_by_class(cls)
        assert len(items) == 1
        return items[0]

    ############################################

    def get_finished_runs_ids(self, group_uuid=None, experiment_uuid=None):
        ret = []
        for rs in self._run_states:
            if rs.state != StorageRunState.FINISHED:
                continue
            elif group_uuid is not None and rs.group_uuid != group_uuid:
                continue
            elif experiment_uuid is not None and rs.exp_uuid != experiment_uuid:
                continue
            ret.append(rs.run_uuid)
        return ret


###############################################################################


class _SingleIndex(dict):
    pass


class _MultiIndex(dict):
    def __missing__(self, key):
        return ()


###############################################################################


class DatumAbc(abc.ABC):
    def __init__(self, base_data=None, group_uuid=None, exp_uuid=None, run_uuid=None):
        self._base_data = base_data

        self._group_uuid = group_uuid
        self._exp_uuid = exp_uuid
        self._run_uuid = run_uuid

    def set_base_data(self, base_data):
        self._base_data = base_data

    @property
    def group_uuid(self):
        return self._group_uuid

    @property
    def exp_uuid(self):
        return self._exp_uuid

    @property
    def run_uuid(self):
        return self._run_uuid


class Item(DatumAbc):
    def __init__(self, uuid, item, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._uuid = uuid
        self._item = item

    @property
    def uuid(self):
        return self._uuid

    def __getattr__(self, name):
        # TODO: Handle collections of refs.
        ref_name = f"{name}_uuid"
        if ref_name in self._item.data_class_refs:
            referenced_uuid = getattr(self._item, ref_name)
            return self._base_data.get_item_by_uuid(referenced_uuid)

        return getattr(self._item, name)


class Blob(DatumAbc):
    def __init__(self, uuid, blob_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._uuid = uuid
        self._blob_name = blob_name

    @property
    def uuid(self):
        return self._uuid

    @property
    def blob_name(self):
        return self._blob_name


class RunState(DatumAbc):
    def __init__(self, state, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = state

    @property
    def state(self):
        return self._state
