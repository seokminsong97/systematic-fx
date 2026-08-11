from fractions import Fraction

from systematic_fx.research.bar_state_config import (
    BAR_STATE_ECONOMIC_MULTIPLIERS,
    BAR_STATE_EXECUTION_SCENARIOS,
    BAR_STATE_LABEL_BOUNDARY_EVENT_ORDERING,
    BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS,
    BAR_STATE_LABEL_SIMULTANEOUS_POLICY,
    MORPHOLOGY_FEATURE_IDS,
    STATE_FEATURE_IDS,
    frozen_discovery_selection_policy,
    frozen_entry_policy,
    frozen_label_policy,
    frozen_model_policy,
)
from systematic_fx.research.bar_state_features import (
    MORPHOLOGY_FEATURE_NAMES,
    STATE_FEATURE_NAMES,
)
from systematic_fx.research.bar_state_labels import LABEL_HORIZON_ACTIVE_DAYS
from systematic_fx.research.bar_state_model import (
    BAR_STATE_V2_MODEL_HYPERPARAMETERS,
    BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
    FROZEN_MODEL_HYPERPARAMETERS,
)
from systematic_fx.research.bar_state_portfolio import (
    DEFAULT_STATE_EXECUTION_SCENARIOS,
    STATE_VOLATILITY_MULTIPLIERS,
    StateExecutionScenario,
)
from systematic_fx.research.bar_state_selection import (
    BH_FAMILY_SIZE,
    BOOTSTRAP_LCB_ORDER_INDEX,
    BOOTSTRAP_RANDOM_SEED,
    BOOTSTRAP_REPLICATES,
)


def test_core_feature_label_and_grid_constants_match_config_contract() -> None:
    assert MORPHOLOGY_FEATURE_NAMES == MORPHOLOGY_FEATURE_IDS
    assert STATE_FEATURE_NAMES == STATE_FEATURE_IDS
    assert LABEL_HORIZON_ACTIVE_DAYS == BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS == 20
    assert BAR_STATE_LABEL_SIMULTANEOUS_POLICY == "CENSORED_WITH_AMBIGUITY_COUNT"
    assert BAR_STATE_LABEL_BOUNDARY_EVENT_ORDERING == (
        "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED"
    )
    assert frozen_label_policy()["boundary_event_ordering"] == (
        BAR_STATE_LABEL_BOUNDARY_EVENT_ORDERING
    )
    assert STATE_VOLATILITY_MULTIPLIERS == tuple(
        Fraction(item.numerator, item.denominator) for item in BAR_STATE_ECONOMIC_MULTIPLIERS
    )
    assert (
        tuple(StateExecutionScenario.from_spec(item) for item in BAR_STATE_EXECUTION_SCENARIOS)
        == DEFAULT_STATE_EXECUTION_SCENARIOS
    )
    assert frozen_entry_policy()["successor_policy"] == (
        "IMMEDIATE_CHRONOLOGICAL_OBSERVED_SIGNAL_BAR_WITHIN_SAME_OUTCOME_SPAN"
    )


def test_actual_sklearn_kwargs_match_deprecation_free_config_contract() -> None:
    arguments = frozen_model_policy()["sklearn_arguments"]
    assert arguments == {
        "C_decimal": "0.1",
        "class_weight": "balanced",
        "fit_intercept": True,
        "l1_ratio_decimal": "0.5",
        "max_iter": 5_000,
        "random_state": 20_260_809,
        "solver": "saga",
        "tol_decimal": "0.00000001",
    }
    assert FROZEN_MODEL_HYPERPARAMETERS.as_dict()["penalty"] == (
        "OMITTED_SKLEARN_1_9_L1_RATIO_IMPLIES_ELASTICNET"
    )
    assert FROZEN_MODEL_HYPERPARAMETERS.as_dict()["n_jobs"] == ("OMITTED_SKLEARN_1_9_NO_EFFECT")


def test_v2a_sklearn_kwargs_change_only_the_campaign_owned_iteration_cap() -> None:
    predecessor = frozen_model_policy(max_iter=5_000)
    successor = frozen_model_policy(max_iter=50_000)
    predecessor_arguments = predecessor["sklearn_arguments"]
    successor_arguments = successor["sklearn_arguments"]

    assert predecessor_arguments.pop("max_iter") == 5_000
    assert successor_arguments.pop("max_iter") == 50_000
    assert successor == predecessor
    assert BAR_STATE_V2_MODEL_HYPERPARAMETERS is FROZEN_MODEL_HYPERPARAMETERS
    assert BAR_STATE_V2A_MODEL_HYPERPARAMETERS.max_iter == 50_000


def test_selection_family_and_bootstrap_constants_match_config_contract() -> None:
    policy = frozen_discovery_selection_policy()
    assert policy["multiplicity"]["family_size"] == BH_FAMILY_SIZE == 804
    assert policy["bootstrap"]["replicates"] == BOOTSTRAP_REPLICATES == 10_000
    assert policy["bootstrap"]["random_seed"] == BOOTSTRAP_RANDOM_SEED == 20_260_809
    assert policy["bootstrap"]["lower_bound_order_statistic"] == (
        "ONE_INDEXED_500_OF_10000_UNCENTERED"
    )
    assert BOOTSTRAP_LCB_ORDER_INDEX == 499
    assert policy["barrier_stability"]["post_bh_minimum_component_cells"] == 9
    assert policy["ranking"]["finalist_rank_order"].endswith("CANDIDATE_KEY_ASC")
