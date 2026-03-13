from dataclasses import dataclass

from src.validators import (
    validate_compute_hours,
    validate_deadline,
    validate_machine_watts,
    validate_objective,
    validate_zip_code,
)


@dataclass
class WorkloadInput:
    zip_code: str
    compute_hours_required: int
    deadline: str
    objective: str
    machine_watts: int

    def __post_init__(self):
        self.zip_code = validate_zip_code(self.zip_code)
        self.compute_hours_required = validate_compute_hours(self.compute_hours_required)
        self.deadline = validate_deadline(self.deadline)
        self.objective = validate_objective(self.objective)
        self.machine_watts = validate_machine_watts(self.machine_watts)