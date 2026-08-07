import unittest
from unittest.mock import MagicMock, call, patch

import psycopg

from systematic_fx.db.pattern_registry import (
    PatternRegistryDatabaseError,
    PatternRegistryDriftError,
)
from systematic_fx.db.pattern_registry import (
    _translate_psycopg_errors as translate_pattern_errors,
)
from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.db.research_registry import ResearchRegistryError
from systematic_fx.db.research_registry import (
    _translate_psycopg_errors as translate_research_errors,
)
from systematic_fx.db.run_registry import RunRegistryDatabaseError
from systematic_fx.db.run_registry import (
    _translate_psycopg_errors as translate_run_errors,
)
from systematic_fx.db.screening_feature_registry import (
    ScreeningFeatureRegistryDatabaseError,
)
from systematic_fx.db.screening_feature_registry import (
    _translate_psycopg_errors as translate_screening_feature_errors,
)


class PostgreSQLSerializationRetryTest(unittest.TestCase):
    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_succeeds_after_three_serialization_conflicts(self, sleep: MagicMock) -> None:
        calls = 0

        def operation() -> str:
            nonlocal calls
            calls += 1
            if calls < 4:
                raise psycopg.errors.SerializationFailure("concurrent update")
            return "completed"

        self.assertEqual(retry_serialization_failures(operation), "completed")
        self.assertEqual(calls, 4)
        sleep.assert_has_calls([call(0.01), call(0.05), call(0.2)])

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_exhaustion_reraises_raw_serialization_failure(self, sleep: MagicMock) -> None:
        calls = 0

        def operation() -> None:
            nonlocal calls
            calls += 1
            raise psycopg.errors.SerializationFailure("still conflicting")

        with self.assertRaises(psycopg.errors.SerializationFailure):
            retry_serialization_failures(operation)

        self.assertEqual(calls, 4)
        sleep.assert_has_calls([call(0.01), call(0.05), call(0.2)])

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_non_serialization_database_error_is_not_retried(self, sleep: MagicMock) -> None:
        calls = 0

        def operation() -> None:
            nonlocal calls
            calls += 1
            raise psycopg.OperationalError("connection lost")

        with self.assertRaises(psycopg.OperationalError):
            retry_serialization_failures(operation)

        self.assertEqual(calls, 1)
        sleep.assert_not_called()

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_research_boundary_translates_only_after_retry_exhaustion(
        self,
        sleep: MagicMock,
    ) -> None:
        calls = 0

        @translate_research_errors("test operation")
        def operation() -> None:
            nonlocal calls
            calls += 1
            raise psycopg.errors.SerializationFailure("concurrent update")

        with self.assertRaisesRegex(
            ResearchRegistryError,
            "PostgreSQL test operation failed",
        ) as raised:
            operation()

        self.assertEqual(calls, 4)
        self.assertIsInstance(raised.exception.__cause__, psycopg.errors.SerializationFailure)
        sleep.assert_has_calls([call(0.01), call(0.05), call(0.2)])

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_pattern_boundary_translates_only_after_retry_exhaustion(
        self,
        sleep: MagicMock,
    ) -> None:
        calls = 0

        @translate_pattern_errors("test operation")
        def operation() -> None:
            nonlocal calls
            calls += 1
            raise psycopg.errors.SerializationFailure("concurrent update")

        with self.assertRaisesRegex(
            PatternRegistryDatabaseError,
            "PostgreSQL test operation failed",
        ) as raised:
            operation()

        self.assertEqual(calls, 4)
        self.assertIsInstance(raised.exception.__cause__, psycopg.errors.SerializationFailure)
        sleep.assert_has_calls([call(0.01), call(0.05), call(0.2)])

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_pattern_domain_error_is_not_retried(self, sleep: MagicMock) -> None:
        calls = 0
        expected = PatternRegistryDriftError("immutable drift")

        @translate_pattern_errors("test operation")
        def operation() -> None:
            nonlocal calls
            calls += 1
            raise expected

        with self.assertRaises(PatternRegistryDriftError) as raised:
            operation()

        self.assertIs(raised.exception, expected)
        self.assertEqual(calls, 1)
        sleep.assert_not_called()

    @patch("systematic_fx.db.postgres_retry.time.sleep")
    def test_remaining_phase1a_registry_boundaries_retry_then_translate(
        self,
        sleep: MagicMock,
    ) -> None:
        boundaries = (
            (translate_screening_feature_errors, ScreeningFeatureRegistryDatabaseError),
            (translate_run_errors, RunRegistryDatabaseError),
        )
        for translate, expected_error in boundaries:
            calls = 0

            @translate("test operation")
            def operation() -> None:
                nonlocal calls
                calls += 1
                raise psycopg.errors.SerializationFailure("concurrent update")

            with (
                self.subTest(error=expected_error.__name__),
                self.assertRaises(expected_error) as raised,
            ):
                operation()

            self.assertEqual(calls, 4)
            self.assertIsInstance(
                raised.exception.__cause__,
                psycopg.errors.SerializationFailure,
            )

        self.assertEqual(
            sleep.call_args_list,
            [call(0.01), call(0.05), call(0.2)] * len(boundaries),
        )


if __name__ == "__main__":
    unittest.main()
