"""
Validation utilities for Util inputs.
"""

from datetime import datetime


def validate_zip_code(zip_code: str) -> str:
    """
    Validate a U.S. ZIP code as a 5-digit string.
    """
    zip_code = str(zip_code).strip()

    if len(zip_code) != 5 or not zip_code.isdigit():
        raise ValueError("zip_code must be a 5-digit string")

    return zip_code


def validate_compute_hours(compute_hours_required: int) -> int:
    """
    Validate compute hours as a positive integer.
    """
    if not isinstance(compute_hours_required, int):
        raise ValueError("compute_hours_required must be an integer")

    if compute_hours_required <= 0:
        raise ValueError("compute_hours_required must be positive")

    return compute_hours_required


def validate_objective(objective: str) -> str:
    """
    Validate optimization objective.
    """
    valid_objectives = {"cost", "carbon", "balanced"}

    objective = str(objective).strip().lower()

    if objective not in valid_objectives:
        raise ValueError(f"objective must be one of {valid_objectives}")

    return objective


def validate_objective_weights(
    carbon_weight: float,
    price_weight: float,
) -> tuple[float, float]:
    """
    Validate objective weights as non-negative values that sum to 1.0.
    """
    try:
        carbon_weight = float(carbon_weight)
        price_weight = float(price_weight)
    except (TypeError, ValueError) as exc:
        raise ValueError("objective weights must be numeric") from exc

    if carbon_weight < 0 or price_weight < 0:
        raise ValueError("objective weights must be non-negative")

    total_weight = carbon_weight + price_weight
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError("objective weights must sum to 1.0")

    return carbon_weight, price_weight


def validate_machine_watts(machine_watts: int) -> int:
    """
    Validate machine wattage as a positive number.
    """
    if not isinstance(machine_watts, (int, float)):
        raise ValueError("machine_watts must be numeric")

    if machine_watts <= 0:
        raise ValueError("machine_watts must be positive")

    return int(machine_watts)


def validate_deadline(deadline):
    """
    Validate deadline as either a datetime object or ISO datetime string.
    """

    if deadline is None:
        return None

    # If Streamlit provides a datetime object
    if isinstance(deadline, datetime):
        return deadline

    # If provided as a string (notebook tests etc.)
    if isinstance(deadline, str):
        try:
            return datetime.fromisoformat(deadline)
        except ValueError as exc:
            raise ValueError(
                "deadline must be a valid ISO datetime string "
                "for example '2026-03-13 17:00'"
            ) from exc

    raise ValueError(
        "deadline must be a datetime object or ISO datetime string"
    )
