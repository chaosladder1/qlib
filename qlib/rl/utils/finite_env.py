# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
This is to support finite env in vector env.
See https://github.com/thu-ml/tianshou/issues/322 for details.
"""

from __future__ import annotations

import copy
import warnings
from contextlib import contextmanager
from typing import Any, Callable, cast, Dict, Generator, List, Optional, Set, Tuple, Type, Union

import gym
import numpy as np
from tianshou.env import BaseVectorEnv, DummyVectorEnv, ShmemVectorEnv, SubprocVectorEnv

from qlib.typehint import Literal
from .log import LogWriter

__all__ = [
    "generate_nan_observation",
    "check_nan_observation",
    "FiniteVectorEnv",
    "FiniteDummyVectorEnv",
    "FiniteSubprocVectorEnv",
    "FiniteShmemVectorEnv",
    "FiniteEnvType",
    "vectorize_env",
]

FiniteEnvType = Literal["dummy", "subproc", "shmem"]
T = Union[dict, list, tuple, np.ndarray]


def fill_invalid(obj: int | float | bool | T) -> T:
    if isinstance(obj, (int, float, bool)):
        return fill_invalid(np.array(obj))
    if hasattr(obj, "dtype"):
        if isinstance(obj, np.ndarray):
            if np.issubdtype(obj.dtype, np.floating):
                return np.full_like(obj, np.nan)
            return np.full_like(obj, np.iinfo(obj.dtype).max)
        # dealing with corner cases that numpy number is not supported by tianshou's sharray
        return fill_invalid(np.array(obj))
    elif isinstance(obj, dict):
        return {k: fill_invalid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fill_invalid(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(fill_invalid(v) for v in obj)
    raise ValueError(f"Unsupported value to fill with invalid: {obj}")


def is_invalid(arr: int | float | bool | T) -> bool:
    if isinstance(arr, np.ndarray):
        if np.issubdtype(arr.dtype, np.floating):
            return np.isnan(arr).all()
        return cast(bool, cast(np.ndarray, np.iinfo(arr.dtype).max == arr).all())
    if isinstance(arr, dict):
        return all(is_invalid(o) for o in arr.values())
    if isinstance(arr, (list, tuple)):
        return all(is_invalid(o) for o in arr)
    if isinstance(arr, (int, float, bool, np.number)):
        return is_invalid(np.array(arr))
    return True


def generate_nan_observation(obs_space: gym.Space) -> Any:
    """The NaN observation that indicates the environment receives no seed.

    We assume that obs is complex and there must be something like float.
    Otherwise this logic doesn't work.
    """

    sample = obs_space.sample()
    sample = fill_invalid(sample)
    return sample


def check_nan_observation(obs: Any) -> bool:
    """Check whether obs is generated by :func:`generate_nan_observation`."""
    return is_invalid(obs)


class FiniteVectorEnv(BaseVectorEnv):
    """To allow the paralleled env workers consume a single DataQueue until it's exhausted.

    See `tianshou issue #322 <https://github.com/thu-ml/tianshou/issues/322>`_.

    The requirement is to make every possible seed (stored in :class:`qlib.rl.utils.DataQueue` in our case)
    consumed by exactly one environment. This is not possible by tianshou's native VectorEnv and Collector,
    because tianshou is unaware of this "exactly one" constraint, and might launch extra workers.

    Consider a corner case, where concurrency is 2, but there is only one seed in DataQueue.
    The reset of two workers must be both called according to the logic in collect.
    The returned results of two workers are collected, regardless of what they are.
    The problem is, one of the reset result must be invalid, or repeated,
    because there's only one need in queue, and collector isn't aware of such situation.

    Luckily, we can hack the vector env, and make a protocol between single env and vector env.
    The single environment (should be :class:`qlib.rl.utils.EnvWrapper` in our case) is responsible for
    reading from queue, and generate a special observation when the queue is exhausted. The special obs
    is called "nan observation", because simply using none causes problems in shared-memory vector env.
    :class:`FiniteVectorEnv` then read the observations from all workers, and select those non-nan
    observation. It also maintains an ``_alive_env_ids`` to track which workers should never be
    called again. When also the environments are exhausted, it will raise StopIteration exception.

    The usage of this vector env in collector are two parts:

    1. If the data queue is finite (usually when inference), collector should collect "infinity" number of
       episodes, until the vector env exhausts by itself.
    2. If the data queue is infinite (usually in training), collector can set number of episodes / steps.
       In this case, data would be randomly ordered, and some repetitions wouldn't matter.

    One extra function of this vector env is that it has a logger that explicitly collects logs
    from child workers. See :class:`qlib.rl.utils.LogWriter`.
    """

    _logger: list[LogWriter]

    def __init__(
        self, logger: LogWriter | list[LogWriter] | None, env_fns: list[Callable[..., gym.Env]], **kwargs: Any
    ) -> None:
        super().__init__(env_fns, **kwargs)

        if isinstance(logger, list):
            self._logger = logger
        elif isinstance(logger, LogWriter):
            self._logger = [logger]
        else:
            self._logger = []
        self._alive_env_ids: Set[int] = set()
        self._reset_alive_envs()
        self._default_obs = self._default_info = self._default_rew = None
        self._zombie = False

        self._collector_guarded: bool = False

    def _reset_alive_envs(self) -> None:
        if not self._alive_env_ids:
            # starting or running out
            self._alive_env_ids = set(range(self.env_num))

    # to workaround with tianshou's buffer and batch
    def _set_default_obs(self, obs: Any) -> None:
        if obs is not None and self._default_obs is None:
            self._default_obs = copy.deepcopy(obs)

    def _set_default_info(self, info: Any) -> None:
        if info is not None and self._default_info is None:
            self._default_info = copy.deepcopy(info)

    def _set_default_rew(self, rew: Any) -> None:
        if rew is not None and self._default_rew is None:
            self._default_rew = copy.deepcopy(rew)

    def _get_default_obs(self) -> Any:
        return copy.deepcopy(self._default_obs)

    def _get_default_info(self) -> Any:
        return copy.deepcopy(self._default_info)

    def _get_default_rew(self) -> Any:
        return copy.deepcopy(self._default_rew)

    # END

    @staticmethod
    def _postproc_env_obs(obs: Any) -> Optional[Any]:
        # reserved for shmem vector env to restore empty observation
        if obs is None or check_nan_observation(obs):
            return None
        return obs

    @contextmanager
    def collector_guard(self) -> Generator[FiniteVectorEnv, None, None]:
        """Guard the collector. Recommended to guard every collect.

        This guard is for two purposes.

        1. Catch and ignore the StopIteration exception, which is the stopping signal
           thrown by FiniteEnv to let tianshou know that ``collector.collect()`` should exit.
        2. Notify the loggers that the collect is ready / done what it's ready / done.

        Examples
        --------
        >>> with finite_env.collector_guard():
        ...     collector.collect(n_episode=INF)
        """
        self._collector_guarded = True

        for logger in self._logger:
            logger.on_env_all_ready()

        try:
            yield self
        except StopIteration:
            pass
        finally:
            self._collector_guarded = False

        # At last trigger the loggers
        for logger in self._logger:
            logger.on_env_all_done()

    def reset(
        self,
        id: int | List[int] | np.ndarray | None = None,
    ) -> np.ndarray:
        assert not self._zombie

        # Check whether it's guarded by collector_guard()
        if not self._collector_guarded:
            warnings.warn(
                "Collector is not guarded by FiniteEnv. "
                "This may cause unexpected problems, like unexpected StopIteration exception, "
                "or missing logs.",
                RuntimeWarning,
            )

        wrapped_id = self._wrap_id(id)
        self._reset_alive_envs()

        # ask super to reset alive envs and remap to current index
        request_id = [i for i in wrapped_id if i in self._alive_env_ids]
        obs = [None] * len(wrapped_id)
        id2idx = {i: k for k, i in enumerate(wrapped_id)}
        if request_id:
            for i, o in zip(request_id, super().reset(request_id)):
                obs[id2idx[i]] = self._postproc_env_obs(o)

        for i, o in zip(wrapped_id, obs):
            if o is None and i in self._alive_env_ids:
                self._alive_env_ids.remove(i)

        # logging
        for i, o in zip(wrapped_id, obs):
            if i in self._alive_env_ids:
                for logger in self._logger:
                    logger.on_env_reset(i, obs)

        # fill empty observation with default(fake) observation
        for o in obs:
            self._set_default_obs(o)
        for i, o in enumerate(obs):
            if o is None:
                obs[i] = self._get_default_obs()

        if not self._alive_env_ids:
            # comment this line so that the env becomes indispensable
            # self.reset()
            self._zombie = True
            raise StopIteration

        return np.stack(obs)

    def step(
        self,
        action: np.ndarray,
        id: int | List[int] | np.ndarray | None = None,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        assert not self._zombie
        wrapped_id = self._wrap_id(id)
        id2idx = {i: k for k, i in enumerate(wrapped_id)}
        request_id = list(filter(lambda i: i in self._alive_env_ids, wrapped_id))
        result = [[None, None, False, None] for _ in range(len(wrapped_id))]

        # ask super to step alive envs and remap to current index
        if request_id:
            valid_act = np.stack([action[id2idx[i]] for i in request_id])
            for i, r in zip(request_id, zip(*super().step(valid_act, request_id))):
                result[id2idx[i]] = list(r)
                result[id2idx[i]][0] = self._postproc_env_obs(result[id2idx[i]][0])

        # logging
        for i, r in zip(wrapped_id, result):
            if i in self._alive_env_ids:
                for logger in self._logger:
                    logger.on_env_step(i, *r)

        # fill empty observation/info with default(fake)
        for _, r, ___, i in result:
            self._set_default_info(i)
            self._set_default_rew(r)
        for i, r in enumerate(result):
            if r[0] is None:
                result[i][0] = self._get_default_obs()
            if r[1] is None:
                result[i][1] = self._get_default_rew()
            if r[3] is None:
                result[i][3] = self._get_default_info()

        ret = list(map(np.stack, zip(*result)))
        return cast(Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray], ret)


class FiniteDummyVectorEnv(FiniteVectorEnv, DummyVectorEnv):
    pass


class FiniteSubprocVectorEnv(FiniteVectorEnv, SubprocVectorEnv):
    pass


class FiniteShmemVectorEnv(FiniteVectorEnv, ShmemVectorEnv):
    pass


def vectorize_env(
    env_factory: Callable[..., gym.Env],
    env_type: FiniteEnvType,
    concurrency: int,
    logger: LogWriter | List[LogWriter],
) -> FiniteVectorEnv:
    """Helper function to create a vector env. Can be used to replace usual VectorEnv.

    For example, once you wrote: ::

        DummyVectorEnv([lambda: gym.make(task) for _ in range(env_num)])

    Now you can replace it with: ::

        finite_env_factory(lambda: gym.make(task), "dummy", env_num, my_logger)

    By doing such replacement, you have two additional features enabled (compared to normal VectorEnv):

    1. The vector env will check for NaN observation and kill the worker when its found.
       See :class:`FiniteVectorEnv` for why we need this.
    2. A logger to explicit collect logs from environment workers.

    Parameters
    ----------
    env_factory
        Callable to instantiate one single ``gym.Env``.
        All concurrent workers will have the same ``env_factory``.
    env_type
        dummy or subproc or shmem. Corresponding to
        `parallelism in tianshou <https://tianshou.readthedocs.io/en/master/api/tianshou.env.html#vectorenv>`_.
    concurrency
        Concurrent environment workers.
    logger
        Log writers.

    Warnings
    --------
    Please do not use lambda expression here for ``env_factory`` as it may create incorrectly-shared instances.

    Don't do: ::

        vectorize_env(lambda: EnvWrapper(...), ...)

    Please do: ::

        def env_factory(): ...
        vectorize_env(env_factory, ...)
    """
    env_type_cls_mapping: Dict[str, Type[FiniteVectorEnv]] = {
        "dummy": FiniteDummyVectorEnv,
        "subproc": FiniteSubprocVectorEnv,
        "shmem": FiniteShmemVectorEnv,
    }

    finite_env_cls = env_type_cls_mapping[env_type]

    return finite_env_cls(logger, [env_factory for _ in range(concurrency)])
