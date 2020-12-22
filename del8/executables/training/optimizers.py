"""TODO: Add title."""
import tensorflow as tf

from del8.core.di import executable


# NOTE: It's probably a good idea to "protect" some parameters so that they
# can't be overriden without referencing the class itself. For example, it
# seems easy to accidently override the betas and epsilon here. The alternative
# is to prefix parameter names with "adam_" defensively, which I don't like.
@executable.executable()
def adam_optimizer(learning_rate=1e-3, beta_1=0.9, beta_2=0.999, epsilon=1e-07):
    # These are the default hyperparameters from keras.
    return tf.keras.optimizers.Adam(
        learning_rate, beta_1=beta_1, beta_2=beta_2, epsilon=epsilon
    )
