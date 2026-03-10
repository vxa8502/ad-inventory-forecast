"""CLI entrypoint for model training, forecasting, and evaluation pipeline.

Usage:
    python -m scripts.run_model_pipeline [--dry-run] [--fold FOLD] [--model MODEL]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from config import settings
from config.settings import validate_horizon_alignment
from src.cli import add_common_args, configure_logging_from_args, require_project_id
from src.logging_config import setup_logging
from src.pipeline_utils import (
    PIPELINE_EXCEPTIONS,
    SEPARATOR_WIDTH,
    execute_sql_step,
    get_base_params,
    print_footer,
    print_header,
    print_validation_results,
)
from src.validators import (
    validate_forecast_coverage,
    validate_metrics_completeness,
    validate_no_infinite_metrics,
)

setup_logging()
logger = logging.getLogger(__name__)


def _should_run_model(model_name: str, model_filter: str | None) -> bool:
    """Check if model should run given the current filter."""
    return model_filter is None or model_filter == model_name


def get_fold_params(fold: dict) -> dict[str, str]:
    """Build parameter dictionary for a specific fold."""
    params = get_base_params()
    params.update({
        "fold_name": fold["name"],
        "train_start": fold["train_start"],
        "train_end": fold["train_end"],
        "test_start": fold["test_start"],
        "test_end": fold["test_end"],
        "horizon": str(settings.FORECAST_HORIZON),
        "confidence_level": str(settings.CONFIDENCE_LEVEL),
    })
    return params


def _train_arima_models(
    sql_dir: Path,
    params: dict[str, str],
    dry_run: bool,
    model_filter: str | None,
) -> float:
    """Train ARIMA_PLUS and ARIMA_PLUS_XREG models for a fold."""
    total_cost = 0.0

    if _should_run_model("arima_plus", model_filter):
        total_cost += execute_sql_step(
            "Training ARIMA_PLUS",
            sql_dir / "05_model" / "train_arima_plus.sql",
            params,
            dry_run,
            indent=2,
        )

    if _should_run_model("arima_plus_xreg", model_filter):
        total_cost += execute_sql_step(
            "Training ARIMA_PLUS_XREG",
            sql_dir / "05_model" / "train_arima_plus_xreg.sql",
            params,
            dry_run,
            indent=2,
        )

    return total_cost


def _generate_forecasts(
    sql_dir: Path,
    params: dict[str, str],
    dry_run: bool,
    model_filter: str | None,
) -> float:
    """Generate forecasts for all models in a fold."""
    total_cost = 0.0

    # TimesFM forecasts (requires BigQuery Enterprise edition)
    if _should_run_model("timesfm_2_5", model_filter):
        if not settings.TIMESFM_ENABLED:
            print("  [SKIP] TimesFM 2.5 disabled (TIMESFM_ENABLED=false)")
            print("         Requires BigQuery Enterprise. Set TIMESFM_ENABLED=true to enable.")
        else:
            total_cost += execute_sql_step(
                "Forecasting with TimesFM 2.5",
                sql_dir / "05_model" / "forecast_timesfm.sql",
                params,
                dry_run,
                indent=2,
            )

    # ARIMA_PLUS forecasts
    if _should_run_model("arima_plus", model_filter):
        total_cost += execute_sql_step(
            "Generating ARIMA_PLUS forecasts",
            sql_dir / "06_evaluate" / "generate_forecasts.sql",
            params,
            dry_run,
            indent=2,
        )

    # ARIMA_PLUS_XREG forecasts (requires future features)
    if _should_run_model("arima_plus_xreg", model_filter):
        total_cost += execute_sql_step(
            "Building future features",
            sql_dir / "06_evaluate" / "build_future_features.sql",
            params,
            dry_run,
            indent=2,
        )
        total_cost += execute_sql_step(
            "Generating ARIMA_PLUS_XREG forecasts",
            sql_dir / "06_evaluate" / "generate_forecasts_xreg.sql",
            params,
            dry_run,
            indent=2,
        )

    return total_cost


def _detect_anomalies(
    sql_dir: Path,
    params: dict[str, str],
    dry_run: bool,
) -> float:
    """Detect anomalies in training data using ARIMA_PLUS model."""
    return execute_sql_step(
        "Detecting anomalies",
        sql_dir / "06_evaluate" / "detect_anomalies.sql",
        params,
        dry_run,
        indent=2,
    )


def _evaluate_forecasts(
    sql_dir: Path,
    params: dict[str, str],
    dry_run: bool,
) -> float:
    """Calculate metrics and compare models for a fold."""
    total_cost = 0.0

    total_cost += execute_sql_step(
        "Calculating metrics",
        sql_dir / "06_evaluate" / "calculate_metrics.sql",
        params,
        dry_run,
        indent=2,
    )
    total_cost += execute_sql_step(
        "Comparing models",
        sql_dir / "06_evaluate" / "compare_models.sql",
        params,
        dry_run,
        indent=2,
    )
    total_cost += execute_sql_step(
        "Calculating business impact",
        sql_dir / "06_evaluate" / "calculate_business_impact.sql",
        params,
        dry_run,
        indent=2,
    )

    return total_cost


def _aggregate_cross_fold(
    sql_dir: Path,
    params: dict[str, str],
    dry_run: bool,
) -> float:
    """Aggregate metrics across all folds for headline numbers."""
    return execute_sql_step(
        "Aggregating cross-fold metrics",
        sql_dir / "06_evaluate" / "aggregate_cross_fold.sql",
        params,
        dry_run,
        indent=2,
    )


def _run_validations(fold_filter: str | None) -> None:
    """Execute model validation checks and print results."""
    try:
        results = []
        folds = [fold_filter] if fold_filter else [f["name"] for f in settings.FOLD_CONFIGS]

        for fold_name in folds:
            results.append(validate_forecast_coverage(fold_name))
            results.append(validate_metrics_completeness(fold_name))
            results.append(validate_no_infinite_metrics(fold_name))

        print_validation_results(results, "model validations")
    except ValueError as e:
        print(f"Validation config error: {e}")
        logger.error("Invalid fold configuration: %s", e)
    except PIPELINE_EXCEPTIONS as e:
        print(f"Validation failed: {e}")
        logger.exception("Validation error: %s", e)


def _create_model_tables(sql_dir: Path, params: dict[str, str], dry_run: bool) -> float:
    """Create model output tables.

    Always executes (even in dry-run) since CREATE TABLE IF NOT EXISTS
    is free and idempotent, and tables must exist for cost estimation.
    """
    return execute_sql_step(
        "Creating model tables",
        sql_dir / "01_schema" / "create_model_tables.sql",
        params,
        dry_run=False,  # Always create tables
    )


def run_model_pipeline(
    dry_run: bool = False,
    fold_filter: str | None = None,
    model_filter: str | None = None,
) -> None:
    """Execute the model training and evaluation pipeline.

    Args:
        dry_run: If True, estimate costs without executing queries.
        fold_filter: Run only this fold (e.g., 'fold_1').
        model_filter: Run only this model (e.g., 'arima_plus').
    """
    validate_horizon_alignment()

    sql_dir = settings.PROJECT_ROOT / "sql"
    total_cost = 0.0

    # Filter folds if specified
    folds = settings.FOLD_CONFIGS
    if fold_filter:
        folds = [f for f in folds if f["name"] == fold_filter]
        if not folds:
            print(f"Error: Unknown fold '{fold_filter}'")
            print(f"Available folds: {[f['name'] for f in settings.FOLD_CONFIGS]}")
            sys.exit(1)

    # Validate model filter
    if model_filter and model_filter not in settings.MODEL_NAMES:
        print(f"Error: Unknown model '{model_filter}'")
        print(f"Available models: {settings.MODEL_NAMES}")
        sys.exit(1)

    # Build header extras
    extras = {}
    if fold_filter:
        extras["Fold"] = fold_filter
    if model_filter:
        extras["Model"] = model_filter

    print_header("Ad Inventory Forecast - Model Pipeline", dry_run, **extras)
    logger.info(
        "Starting model pipeline in %s mode (fold=%s, model=%s)",
        "dry-run" if dry_run else "execution",
        fold_filter or "all",
        model_filter or "all",
    )

    # Create model tables (once)
    base_params = get_base_params()
    total_cost += _create_model_tables(sql_dir, base_params, dry_run)

    # Process each fold
    for fold in folds:
        print(f"\n[{fold['name'].upper()}] Train: {fold['train_start']} to {fold['train_end']}")
        print(f"[{fold['name'].upper()}] Test: {fold['test_start']} to {fold['test_end']}\n")

        params = get_fold_params(fold)

        # Train models
        total_cost += _train_arima_models(sql_dir, params, dry_run, model_filter)

        # Detect anomalies (runs on ARIMA_PLUS model, requires training first)
        if _should_run_model("arima_plus", model_filter):
            total_cost += _detect_anomalies(sql_dir, params, dry_run)

        # Generate forecasts
        total_cost += _generate_forecasts(sql_dir, params, dry_run, model_filter)

        # Evaluate (only if running all models)
        # When --model is specified, skip evaluation since metrics require all models
        if model_filter is None:
            total_cost += _evaluate_forecasts(sql_dir, params, dry_run)
        else:
            print(f"\n  [NOTE] Skipping evaluation (--model {model_filter} specified)")
            print("         Run without --model to calculate metrics for all models.")

    # Aggregate across folds (only when all folds and all models processed)
    if fold_filter is None and model_filter is None:
        print("\n[CROSS-FOLD] Aggregating headline metrics...\n")
        total_cost += _aggregate_cross_fold(sql_dir, base_params, dry_run)

    print(f"\n{'.' * SEPARATOR_WIDTH}")

    if dry_run:
        print(f"Total estimated cost: ${total_cost:.4f}")
        logger.info("Dry-run complete. Total estimated cost: $%.4f", total_cost)
    else:
        _run_validations(fold_filter)

    print_footer()


def main() -> None:
    """CLI entrypoint with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Run the model training and evaluation pipeline"
    )
    add_common_args(parser)
    parser.add_argument(
        "--fold",
        type=str,
        choices=[f["name"] for f in settings.FOLD_CONFIGS],
        help="Run only this fold",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=settings.MODEL_NAMES,
        help="Run only this model",
    )

    args = parser.parse_args()
    configure_logging_from_args(args)
    require_project_id()

    run_model_pipeline(
        dry_run=args.dry_run,
        fold_filter=args.fold,
        model_filter=args.model,
    )


if __name__ == "__main__":
    main()
