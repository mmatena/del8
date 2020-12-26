# """TODO: Add title."""
# from del8.core import data_class
# from del8.core.di import executable

# from del8.core.experiment import experiment

# from del8.core.storage.storage import RunState

# from . import eval_execs


# @data_class.data_class()
# class EvaluationExperiment(object):
#     def __init__(self, training_exp_uuid, evaluation_exp_uuid):
#         pass


# # NOTE: I might want to turn this into some sort of decorator.
# def create_evaluation_experiment(
#     uuid,
#     training_exp,
#     bindings=(),
# ):
#     def create_varying_params(eval_exp):
#         varying_params = []

#         run_uuids = training_exp.retrieve_run_uuids(RunState.FINISHED)
#         for run_uuid in run_uuids:
#             ckpt_summary = training_exp.retrieve_checkpoints_summary(run_uuid)

#         return varying_params
