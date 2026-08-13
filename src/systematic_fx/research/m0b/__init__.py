"""Fail-closed staged real-data adapter for a bounded CME 6E M0b slice."""

from systematic_fx.research.m0b.adapter import build_real_slice, verify_real_slice
from systematic_fx.research.m0b.config import RealSliceConfig, load_real_slice_config
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageStore,
    FirstPassageStoreSpec,
    build_first_passage_store,
    load_first_passage_store,
)
from systematic_fx.research.m0b.materialize import (
    load_materialized_real_slice,
    materialize_real_slice,
)
from systematic_fx.research.m0b.model import RealSliceBuild, RealSliceError
from systematic_fx.research.m0b.runner import (
    ClaimedWorkerCycleResult,
    M0bControlPlaneReplayError,
    M0bRunnerError,
    M0bRuntimeCodeIdentityError,
    run_claimed_worker_cycle,
)
from systematic_fx.research.m0b.store_config import (
    FirstPassageStoreConfig,
    load_first_passage_store_config,
)
from systematic_fx.research.m0b.worker import (
    CandidateJob,
    CandidateWorkArtifact,
    CandidateWorkSpec,
    NumericAdmissionRules,
    VolatilityBarrierSpec,
    WorkerAttempt,
    load_candidate_work_artifact,
    load_candidate_work_manifest,
    publish_candidate_work_manifest,
    publish_signal_artifact,
    run_bounded_daemon_cycle,
    run_candidate_work,
)
from systematic_fx.research.m0b.worker_db import (
    M0bCheckpointPublicationError,
    M0bTerminalPublicationError,
    PostgresWorkerObserver,
)

__all__ = [
    "CandidateJob",
    "CandidateWorkArtifact",
    "CandidateWorkSpec",
    "ClaimedWorkerCycleResult",
    "FirstPassageStore",
    "FirstPassageStoreConfig",
    "FirstPassageStoreSpec",
    "M0bCheckpointPublicationError",
    "M0bControlPlaneReplayError",
    "M0bRunnerError",
    "M0bRuntimeCodeIdentityError",
    "M0bTerminalPublicationError",
    "NumericAdmissionRules",
    "PostgresWorkerObserver",
    "RealSliceBuild",
    "RealSliceConfig",
    "RealSliceError",
    "VolatilityBarrierSpec",
    "WorkerAttempt",
    "build_first_passage_store",
    "build_real_slice",
    "load_candidate_work_artifact",
    "load_candidate_work_manifest",
    "load_first_passage_store",
    "load_first_passage_store_config",
    "load_materialized_real_slice",
    "load_real_slice_config",
    "materialize_real_slice",
    "publish_candidate_work_manifest",
    "publish_signal_artifact",
    "run_bounded_daemon_cycle",
    "run_candidate_work",
    "run_claimed_worker_cycle",
    "verify_real_slice",
]
