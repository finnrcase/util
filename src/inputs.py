from dataclasses import dataclass

from src.validators import (
    validate_compute_hours,
    validate_deadline,
    validate_machine_watts,
    validate_objective,
    validate_objective_weights,
    validate_zip_code,
)


@dataclass
class WorkloadInput:
    zip_code: str
    compute_hours_required: int
    deadline: str
    objective: str
    machine_watts: int
    carbon_weight: float = 0.5
    price_weight: float = 0.5

    def __post_init__(self):
        self.zip_code = validate_zip_code(self.zip_code)
        self.compute_hours_required = validate_compute_hours(self.compute_hours_required)
        self.deadline = validate_deadline(self.deadline)
        self.objective = validate_objective(self.objective)
        self.machine_watts = validate_machine_watts(self.machine_watts)
        self.carbon_weight, self.price_weight = validate_objective_weights(
            self.carbon_weight,
            self.price_weight,
        )
