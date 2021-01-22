"""TODO: Add title."""
from del8.core.di import executable


@executable.executable()
def common_prebatch_processer(
    dataset,
    dataset_skip=None,
    num_examples=None,
    shuffle=False,
    repeat=False,
    shuffle_buffer_size=1000,
):
    ds = dataset
    if dataset_skip is not None:
        ds = ds.skip(dataset_skip)
    if num_examples is not None and num_examples >= 0:
        ds = ds.take(num_examples).cache()
    if repeat:
        ds = ds.repeat()
    if shuffle:
        ds = ds.shuffle(shuffle_buffer_size)
    return ds


@executable.executable()
def batcher(dataset, batch_size):
    return dataset.batch(batch_size)
