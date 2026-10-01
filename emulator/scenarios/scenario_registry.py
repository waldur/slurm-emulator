"""Scenario registry for managing and visualizing test scenarios."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

# (user, node-hours, partition, joules) seeded by the regular_access_energy
# scenario: 100 Nh CPU at 500 W and 40 Nh GPU at 500 W + 4x300 W.
REGULAR_ACCESS_ENERGY_USERS: list[tuple[str, float, str, int]] = [
    ("alice", 100.0, "compute", 180_000_000),
    ("bob", 40.0, "gpu", 244_800_000),
]
REGULAR_ACCESS_ENERGY_JOULES = sum(j for _u, _n, _p, j in REGULAR_ACCESS_ENERGY_USERS)

# eessi_software_usage: the EESSI stack version and CPU target the jobs ran on.
EESSI_VERSION = "2023.06"
EESSI_CPU_TARGET = "x86_64/amd/zen3"
EESSI_ACCOUNT = "eessi_demo"


@dataclass(frozen=True)
class EessiJob:
    """One March 2024 job of the eessi_software_usage scenario."""

    user: str
    # Real EESSI 2023.06 modules; None = no module loaded (no AdminComment),
    # ``mysite-tool/1.0`` = a site-local module outside EESSI.
    modules: Optional[list[str]]
    cpus: int
    elapsed_hours: float
    energy: int
    end: str
    gpus: int = 0
    partition: str = "compute"
    state: str = "COMPLETED"
    cpu_efficiency: Optional[float] = None
    total_cpu_seconds: Optional[float] = None
    # CPU the job ran on, when it differs from the EESSI build it used.
    host_cpu: Optional[str] = None
    cpu_target: str = EESSI_CPU_TARGET

    @property
    def admin_comment(self) -> str:
        return eessi_admin_comment(self.modules, self.cpu_target, self.host_cpu)

    def inject_parameters(self) -> dict[str, Any]:
        """``POST /api/inject-jobs`` / ``inject_job`` fields for this job."""
        params: dict[str, Any] = {
            "account": EESSI_ACCOUNT,
            "user": self.user,
            "cpus": self.cpus,
            "gpus": self.gpus,
            "elapsed_hours": self.elapsed_hours,
            "partition": self.partition,
            "energy": self.energy,
            "admin_comment": self.admin_comment,
            "end": self.end,
            "state": self.state,
        }
        if self.cpu_efficiency is not None:
            params["cpu_efficiency"] = self.cpu_efficiency
        if self.total_cpu_seconds is not None:
            params["total_cpu_seconds"] = self.total_cpu_seconds
        return params


_GROMACS = "GROMACS/2024.1-foss-2023b"
_OPENFOAM = "OpenFOAM/11-foss-2023a"

EESSI_SOFTWARE_JOBS: list[EessiJob] = [
    # Built for zen3, ran on a zen4 host.
    EessiJob(
        "alice",
        [_GROMACS],
        32,
        6.0,
        5_412_600,
        "2024-03-04T14:00:00",
        cpu_efficiency=0.92,
        host_cpu="x86_64/amd/zen4",
    ),
    EessiJob(
        "alice",
        [_GROMACS, "Python/3.11.5-GCCcore-13.2.0"],
        16,
        3.0,
        1_361_700,
        "2024-03-06T10:30:00",
        cpu_efficiency=0.78,
    ),
    # Hit its time limit; TotalCPU spans more than a day of CPU time.
    EessiJob(
        "alice",
        ["LAMMPS/2Aug2023_update2-foss-2023a-kokkos"],
        64,
        12.0,
        21_744_000,
        "2024-03-12T22:00:00",
        state="TIMEOUT",
        cpu_efficiency=0.97,
    ),
    # No module loaded; sub-hour TotalCPU with milliseconds.
    EessiJob("alice", None, 2, 0.5, 56_400, "2024-03-13T09:15:00", total_cpu_seconds=1234.567),
    EessiJob(
        "bob",
        [_OPENFOAM],
        48,
        8.0,
        10_872_000,
        "2024-03-05T18:00:00",
        cpu_efficiency=0.88,
        host_cpu="x86_64/amd/zen3",
    ),
    EessiJob(
        "bob",
        [_OPENFOAM, "Python/3.11.3-GCCcore-12.3.0"],
        8,
        2.0,
        451_800,
        "2024-03-07T11:00:00",
        cpu_efficiency=0.61,
    ),
    EessiJob(
        "bob",
        ["QuantumESPRESSO/7.3.1-foss-2023a"],
        32,
        0.25,
        225_540,
        "2024-03-15T16:45:00",
        state="FAILED",
        cpu_efficiency=0.4,
    ),
    EessiJob(
        "bob",
        ["R/4.3.2-gfbf-2023a"],
        4,
        1.5,
        169_200,
        "2024-03-19T13:20:00",
        state="CANCELLED",
        cpu_efficiency=0.3,
    ),
    EessiJob(
        "carol",
        ["PyTorch/2.1.2-foss-2023a"],
        16,
        1.5,
        677_700,
        "2024-03-08T20:00:00",
        state="OUT_OF_MEMORY",
        cpu_efficiency=0.55,
    ),
    # GPU job: the CPUs mostly feed the GPUs.
    EessiJob(
        "carol",
        ["GROMACS/2024.4-foss-2023b-CUDA-12.4.0"],
        16,
        10.0,
        24_264_000,
        "2024-03-18T07:30:00",
        gpus=2,
        partition="gpu",
        cpu_efficiency=0.35,
    ),
    # Generic build on an Intel host.
    EessiJob(
        "carol",
        ["TensorFlow/2.13.0-foss-2023a"],
        8,
        3.0,
        676_800,
        "2024-03-22T15:00:00",
        cpu_efficiency=0.72,
        host_cpu="x86_64/intel/sapphirerapids",
        cpu_target="x86_64/generic",
    ),
    # Fully busy: no efficiency given.
    EessiJob("carol", ["mysite-tool/1.0"], 4, 2.0, 225_900, "2024-03-27T10:00:00"),
]


def eessi_admin_comment(
    modules: Optional[list[str]],
    cpu_target: str = EESSI_CPU_TARGET,
    host_cpu: Optional[str] = None,
) -> str:
    """AdminComment an Lmod load hook + slurmctld epilog would leave on a job.

    Compact JSON namespaced under ``eessi``; module names contain no
    ``|``, the separator of ``sacct -P``, nor a newline. ``host_cpu`` is
    added only when given. ``None`` (no module loaded) leaves the comment
    empty.
    """
    if modules is None:
        return ""
    eessi: dict[str, Any] = {"version": EESSI_VERSION, "cpu_target": cpu_target}
    if host_cpu is not None:
        eessi["host_cpu"] = host_cpu
    eessi["modules"] = modules
    return json.dumps({"eessi": eessi}, separators=(",", ":"))


class ScenarioType(Enum):
    """Types of scenarios."""

    PERIODIC_LIMITS = "periodic_limits"
    DECAY_TESTING = "decay_testing"
    QOS_MANAGEMENT = "qos_management"
    USAGE_PATTERNS = "usage_patterns"
    CONFIGURATION = "configuration"


class ActionType(Enum):
    """Types of actions in scenarios."""

    TIME_SET = "time_set"
    TIME_ADVANCE = "time_advance"
    USAGE_INJECT = "usage_inject"
    JOB_INJECT = "job_inject"
    ACCOUNT_CREATE = "account_create"
    ACCOUNT_DELETE = "account_delete"
    LIMITS_CALCULATE = "limits_calculate"
    QOS_CHECK = "qos_check"
    QOS_SET = "qos_set"
    CHECKPOINT = "checkpoint"
    VALIDATE = "validate"
    CONFIG_RELOAD = "config_reload"
    CLEANUP = "cleanup"


@dataclass
class ScenarioAction:
    """Individual action within a scenario."""

    type: ActionType
    description: str
    parameters: dict[str, Any] = field(default_factory=dict)
    expected_outcome: str = ""
    validation: dict[str, Any] = field(default_factory=dict)

    def get_cli_command(self) -> str:
        """Convert action to CLI command."""
        if self.type == ActionType.TIME_SET:
            return f"time set {self.parameters['time']}"
        if self.type == ActionType.TIME_ADVANCE:
            amount = self.parameters["amount"]
            unit = self.parameters["unit"]
            return f"time advance {amount} {unit}"
        if self.type == ActionType.USAGE_INJECT:
            user = self.parameters["user"]
            amount = self.parameters["amount"]
            account = self.parameters.get("account", "default")
            cmd = f"usage inject {user} {amount} {account}"
            if "energy" in self.parameters:
                cmd += f"  # energy={self.parameters['energy']} J"
            return cmd
        if self.type == ActionType.JOB_INJECT:
            # No interactive command; the control API takes the same fields.
            return f"# POST /api/inject-jobs {json.dumps({'jobs': [self.parameters]})}"
        if self.type == ActionType.ACCOUNT_CREATE:
            name = self.parameters["name"]
            desc = self.parameters.get("description", "Test Account")
            allocation = self.parameters.get("allocation", 1000)
            return f'account create {name} "{desc}" {allocation}'
        if self.type == ActionType.LIMITS_CALCULATE:
            account = self.parameters.get("account", "default")
            return f"limits calculate {account}"
        if self.type == ActionType.QOS_CHECK:
            account = self.parameters.get("account", "default")
            return f"qos check {account}"
        if self.type == ActionType.QOS_SET:
            account = self.parameters["account"]
            qos = self.parameters["qos"]
            return f"qos set {account} {qos}"
        if self.type == ActionType.CHECKPOINT:
            name = self.parameters["name"]
            return f"checkpoint create {name}"
        if self.type == ActionType.CONFIG_RELOAD:
            path = self.parameters["config_path"]
            return f"config reload {path}"
        if self.type == ActionType.ACCOUNT_DELETE:
            account = self.parameters["account"]
            return f"account delete {account}"
        if self.type == ActionType.CLEANUP:
            return f"# Cleanup: {self.description}"
        return f"# {self.description}"


@dataclass
class ScenarioStep:
    """A step in a scenario containing one or more actions."""

    name: str
    description: str
    actions: list[ScenarioAction] = field(default_factory=list)
    time_point: Optional[datetime] = None
    expected_state: dict[str, Any] = field(default_factory=dict)

    def add_action(self, action: ScenarioAction) -> None:
        """Add an action to this step."""
        self.actions.append(action)


@dataclass
class ScenarioDefinition:
    """Complete scenario definition with metadata."""

    name: str
    title: str
    description: str
    scenario_type: ScenarioType
    steps: list[ScenarioStep] = field(default_factory=list)

    # Metadata
    duration_estimate: str = "10-15 minutes"
    complexity: str = "intermediate"  # basic, intermediate, advanced
    prerequisites: list[str] = field(default_factory=list)
    learning_objectives: list[str] = field(default_factory=list)
    key_concepts: list[str] = field(default_factory=list)

    # Configuration requirements
    required_config: dict[str, Any] = field(default_factory=dict)
    recommended_config: Optional[str] = None

    def add_step(self, step: ScenarioStep) -> None:
        """Add a step to the scenario."""
        self.steps.append(step)

    def get_total_actions(self) -> int:
        """Get total number of actions across all steps."""
        return sum(len(step.actions) for step in self.steps)

    def get_summary(self) -> dict[str, Any]:
        """Get scenario summary."""
        return {
            "name": self.name,
            "title": self.title,
            "type": self.scenario_type.value,
            "description": self.description,
            "steps": len(self.steps),
            "total_actions": self.get_total_actions(),
            "duration": self.duration_estimate,
            "complexity": self.complexity,
            "key_concepts": self.key_concepts,
        }


class ScenarioRegistry:
    """Registry for managing all available scenarios."""

    def __init__(self) -> None:
        self.scenarios: dict[str, ScenarioDefinition] = {}
        self._register_built_in_scenarios()

    def register_scenario(self, scenario: ScenarioDefinition) -> None:
        """Register a scenario."""
        self.scenarios[scenario.name] = scenario

    def get_scenario(self, name: str) -> Optional[ScenarioDefinition]:
        """Get scenario by name."""
        return self.scenarios.get(name)

    def list_scenarios(self) -> list[ScenarioDefinition]:
        """List all registered scenarios."""
        return list(self.scenarios.values())

    def list_by_type(self, scenario_type: ScenarioType) -> list[ScenarioDefinition]:
        """List scenarios by type."""
        return [s for s in self.scenarios.values() if s.scenario_type == scenario_type]

    def search_scenarios(self, query: str) -> list[ScenarioDefinition]:
        """Search scenarios by name, title, or description."""
        query = query.lower()
        results = []
        for scenario in self.scenarios.values():
            if (
                query in scenario.name.lower()
                or query in scenario.title.lower()
                or query in scenario.description.lower()
                or any(query in concept.lower() for concept in scenario.key_concepts)
            ):
                results.append(scenario)
        return results

    def _register_built_in_scenarios(self) -> None:
        """Register built-in scenarios."""
        # Register the sequence scenario
        self._register_sequence_scenario()
        self._register_decay_comparison_scenario()
        self._register_qos_threshold_scenario()
        self._register_carryover_testing_scenario()
        self._register_configuration_comparison_scenario()
        self._register_regular_access_energy_scenario()
        self._register_eessi_software_usage_scenario()

        # Register new limits configuration scenarios
        self._register_limits_configuration_scenarios()

    def _register_sequence_scenario(self) -> None:
        """Register the complete sequence scenario from SLURM_PERIODIC_LIMITS_SEQUENCE.md."""
        scenario = ScenarioDefinition(
            name="sequence",
            title="Complete Periodic Limits Sequence",
            description="Full implementation of the SLURM_PERIODIC_LIMITS_SEQUENCE.md scenario demonstrating quarterly allocations, carryover logic, QoS management, and decay calculations.",
            scenario_type=ScenarioType.PERIODIC_LIMITS,
            duration_estimate="15-20 minutes",
            complexity="advanced",
            learning_objectives=[
                "Understand quarterly allocation management",
                "Learn carryover calculation with decay",
                "Experience QoS threshold behavior",
                "Observe grace period and hard limits",
                "See period transition automation",
            ],
            key_concepts=[
                "15-day decay half-life",
                "Quarterly period transitions",
                "Grace period (20% overconsumption)",
                "QoS switching (normal → slowdown → blocked)",
                "Carryover with decay factor",
                "Manual allocation adjustments",
                "Billing unit calculations",
            ],
            recommended_config="examples/slurm.conf",
        )

        # Step 1: Initial Setup
        step1 = ScenarioStep(
            name="initial_setup",
            description="Q1 2024 setup with 1000Nh quarterly allocation and 20% grace period",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create test account with 1000Nh allocation",
                parameters={
                    "name": "slurm_account_123",
                    "description": "Test Account",
                    "allocation": 1000,
                },
                expected_outcome="Account created with fairshare=333, GrpTRESMins=72000",
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Calculate initial periodic settings",
                parameters={"account": "slurm_account_123"},
                expected_outcome="Fairshare: 333, QoS threshold: 1200Nh, Grace limit: 1200Nh",
            )
        )
        scenario.add_step(step1)

        # Step 2: Q1 Usage
        step2 = ScenarioStep(
            name="q1_usage",
            description="Q1 usage simulation: 500Nh over 3 months",
            time_point=datetime(2024, 3, 31),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Month 1: 167Nh usage",
                parameters={"user": "user1", "amount": 100, "account": "slurm_account_123"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Month 1: 67Nh usage",
                parameters={"user": "user2", "amount": 67, "account": "slurm_account_123"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.TIME_ADVANCE,
                description="Advance to end of Q1",
                parameters={"amount": 2, "unit": "months"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Complete Q1 with 500Nh total",
                parameters={"user": "user1", "amount": 333, "account": "slurm_account_123"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Verify QoS remains normal",
                parameters={"account": "slurm_account_123"},
                expected_outcome="QoS: normal (usage < threshold)",
            )
        )
        scenario.add_step(step2)

        # Step 3: Q2 Transition
        step3 = ScenarioStep(
            name="q2_transition",
            description="Q2 transition with carryover calculation",
            time_point=datetime(2024, 4, 1),
        )
        step3.add_action(
            ScenarioAction(
                type=ActionType.TIME_SET,
                description="Jump to Q2 start",
                parameters={"time": "2024-04-01"},
            )
        )
        step3.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Calculate Q2 limits with carryover",
                parameters={"account": "slurm_account_123"},
                expected_outcome="Carryover: ~500Nh, New total: ~1500Nh, Threshold: ~1800Nh",
            )
        )
        scenario.add_step(step3)

        # Step 4: Q2 Heavy Usage
        step4 = ScenarioStep(
            name="q2_heavy_usage",
            description="Q2 heavy usage reaching and exceeding thresholds",
            time_point=datetime(2024, 5, 20),
        )
        step4.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Heavy usage: 1000Nh in Q2",
                parameters={"user": "user1", "amount": 1000, "account": "slurm_account_123"},
            )
        )
        step4.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Check QoS after threshold breach",
                parameters={"account": "slurm_account_123"},
                expected_outcome="QoS: slowdown (usage exceeded threshold)",
            )
        )
        step4.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Push to hard limit: +700Nh",
                parameters={"user": "user1", "amount": 700, "account": "slurm_account_123"},
            )
        )
        step4.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Check QoS after hard limit",
                parameters={"account": "slurm_account_123"},
                expected_outcome="QoS: blocked (hard limit exceeded)",
            )
        )
        scenario.add_step(step4)

        # Step 5: Q3 Transition with Decay
        step5 = ScenarioStep(
            name="q3_transition_decay",
            description="Q3 transition demonstrating 15-day decay factor",
            time_point=datetime(2024, 7, 1),
        )
        step5.add_action(
            ScenarioAction(
                type=ActionType.TIME_SET,
                description="Jump to Q3 start",
                parameters={"time": "2024-07-01"},
            )
        )
        step5.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Calculate Q3 limits with decay",
                parameters={"account": "slurm_account_123"},
                expected_outcome="Decay factor: ~0.0156, Effective previous: ~27Nh, New allocation: ~1027Nh",
            )
        )
        step5.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Verify QoS restored for new period",
                parameters={"account": "slurm_account_123"},
                expected_outcome="QoS: normal (new period reset)",
            )
        )
        scenario.add_step(step5)

        self.register_scenario(scenario)

    def _register_decay_comparison_scenario(self) -> None:
        """Register decay comparison scenario."""
        scenario = ScenarioDefinition(
            name="decay_comparison",
            title="Decay Half-Life Comparison",
            description="Compare different decay half-life settings (7 vs 15 days) to understand their impact on carryover calculations.",
            scenario_type=ScenarioType.DECAY_TESTING,
            duration_estimate="10 minutes",
            complexity="intermediate",
            learning_objectives=[
                "Understand decay half-life impact",
                "Compare different configurations",
                "See carryover calculation differences",
            ],
            key_concepts=[
                "Decay half-life formula: 2^(-days/half_life)",
                "Configuration impact on behavior",
                "Carryover calculation sensitivity",
            ],
        )

        step1 = ScenarioStep(
            name="setup_15day",
            description="Test with 15-day half-life (standard)",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.CONFIG_RELOAD,
                description="Load standard 15-day config",
                parameters={"config_path": "examples/slurm.conf"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create test account",
                parameters={"name": "decay_test_15", "allocation": 1000},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Use 800Nh in Q1",
                parameters={"user": "user_15day", "amount": 800, "account": "decay_test_15"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.TIME_ADVANCE,
                description="Advance to Q2",
                parameters={"amount": 3, "unit": "months"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check Q2 carryover with 15-day decay",
                parameters={"account": "decay_test_15"},
                expected_outcome="~12.5Nh effective usage, ~987.5Nh carryover",
            )
        )
        scenario.add_step(step1)

        step2 = ScenarioStep(
            name="setup_7day",
            description="Test with 7-day half-life (aggressive)",
            time_point=datetime(2024, 1, 1),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.TIME_SET,
                description="Reset to Q1 start",
                parameters={"time": "2024-01-01"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.CONFIG_RELOAD,
                description="Load aggressive 7-day config",
                parameters={"config_path": "examples/custom_slurm.conf"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create test account",
                parameters={"name": "decay_test_7", "allocation": 1000},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Use 800Nh in Q1",
                parameters={"user": "user_7day", "amount": 800, "account": "decay_test_7"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.TIME_ADVANCE,
                description="Advance to Q2",
                parameters={"amount": 3, "unit": "months"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check Q2 carryover with 7-day decay",
                parameters={"account": "decay_test_7"},
                expected_outcome="~0.1Nh effective usage, ~999.9Nh carryover",
            )
        )
        scenario.add_step(step2)

        self.register_scenario(scenario)

    def _register_qos_threshold_scenario(self) -> None:
        """Register QoS threshold testing scenario."""
        scenario = ScenarioDefinition(
            name="qos_thresholds",
            title="QoS Threshold Management",
            description="Test QoS transitions at different usage levels: normal → slowdown → blocked.",
            scenario_type=ScenarioType.QOS_MANAGEMENT,
            duration_estimate="8 minutes",
            complexity="basic",
            learning_objectives=[
                "Understand QoS threshold behavior",
                "See automatic QoS switching",
                "Learn grace period management",
            ],
            key_concepts=[
                "QoS levels: normal, slowdown, blocked",
                "Grace period (20% overconsumption)",
                "Automatic threshold enforcement",
                "Priority weight impact",
            ],
        )

        step1 = ScenarioStep(
            name="normal_usage",
            description="Normal usage within allocation",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create account with 1000Nh allocation",
                parameters={"name": "qos_test", "allocation": 1000},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Use 500Nh (50% of allocation)",
                parameters={"user": "user1", "amount": 500, "account": "qos_test"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Verify normal QoS",
                parameters={"account": "qos_test"},
                expected_outcome="QoS: normal, usage well below threshold",
            )
        )
        scenario.add_step(step1)

        step2 = ScenarioStep(
            name="threshold_breach",
            description="Breach QoS threshold (100% + grace)",
            time_point=datetime(2024, 1, 15),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Use additional 600Nh (total: 1100Nh = 110%)",
                parameters={"user": "user1", "amount": 600, "account": "qos_test"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Check QoS after threshold breach",
                parameters={"account": "qos_test"},
                expected_outcome="QoS: slowdown, usage exceeded threshold (1100 > 1000)",
            )
        )
        scenario.add_step(step2)

        step3 = ScenarioStep(
            name="hard_limit",
            description="Hit hard limit (grace exhausted)",
            time_point=datetime(2024, 1, 20),
        )
        step3.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Use additional 200Nh (total: 1300Nh)",
                parameters={"user": "user1", "amount": 200, "account": "qos_test"},
            )
        )
        step3.add_action(
            ScenarioAction(
                type=ActionType.QOS_CHECK,
                description="Check QoS at hard limit",
                parameters={"account": "qos_test"},
                expected_outcome="QoS: blocked, hard limit exceeded",
            )
        )
        scenario.add_step(step3)

        self.register_scenario(scenario)

    def _register_regular_access_energy_scenario(self) -> None:
        """One month of CPU + GPU usage with known energy for sreport demos.

        Seeds a project so that
        ``sreport cluster AccountUtilizationByUser start=2024-03-01
        end=2024-04-01 -T energy -t Seconds -P -n accounts=regular_access``
        reports exactly ``REGULAR_ACCESS_ENERGY_JOULES`` for the account,
        split per user as listed in ``REGULAR_ACCESS_ENERGY_USERS``.
        """
        scenario = ScenarioDefinition(
            name="regular_access_energy",
            title="Regular Access: Monthly Energy Report",
            description=(
                "Seed one month (March 2024) of CPU and GPU usage for a regular-access "
                "project with known energy so the aggregated sreport energy figure "
                "used for carbon-footprint reporting can be verified."
            ),
            scenario_type=ScenarioType.USAGE_PATTERNS,
            duration_estimate="3 minutes",
            complexity="basic",
            learning_objectives=[
                "See how energy is attached to usage records (power model vs seeded)",
                "Produce a monthly per-account / per-user energy aggregate with sreport",
                "Report a previous month on demand with the simulated clock",
            ],
            key_concepts=[
                "energy TRES in joules (sreport -T energy -t Seconds)",
                "Account total = sum of per-user rows",
                "Time-travel monthly windows",
            ],
        )

        step1 = ScenarioStep(
            name="seed_march",
            description="Seed March 2024 usage with known energy",
            time_point=datetime(2024, 3, 15),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create the regular-access project account",
                parameters={"name": "regular_access", "allocation": 5000},
            )
        )
        for user, node_hours, partition, joules in REGULAR_ACCESS_ENERGY_USERS:
            step1.add_action(
                ScenarioAction(
                    type=ActionType.USAGE_INJECT,
                    description=f"{user}: {node_hours}Nh on {partition} = {joules} J",
                    parameters={
                        "user": user,
                        "amount": node_hours,
                        "account": "regular_access",
                        "partition": partition,
                        "energy": joules,
                    },
                )
            )
        scenario.add_step(step1)

        step2 = ScenarioStep(
            name="report_previous_month",
            description="Move to April and report March",
            time_point=datetime(2024, 4, 2),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description=(
                    "sreport cluster AccountUtilizationByUser start=2024-03-01 "
                    "end=2024-04-01 -T energy -t Seconds -P -n accounts=regular_access"
                ),
                parameters={"account": "regular_access"},
                expected_outcome=(
                    f"account total {REGULAR_ACCESS_ENERGY_JOULES} J, one row per user"
                ),
                validation={"energy_joules": REGULAR_ACCESS_ENERGY_JOULES},
            )
        )
        scenario.add_step(step2)

        self.register_scenario(scenario)

    def _register_eessi_software_usage_scenario(self) -> None:
        """March 2024 jobs annotated with the EESSI modules they loaded.

        Each job carries its module list in AdminComment (JSON under an
        ``eessi`` key, some with the ``host_cpu`` they ran on), an exact
        energy figure, a final state (incl. TIMEOUT, FAILED, CANCELLED,
        OUT_OF_MEMORY) and a CPU efficiency behind TotalCPU, so
        ``sacct --format=JobID,Account,User,AllocTRES,Elapsed,ConsumedEnergyRaw,State,TotalCPU,AdminComment
        --parsable2 --noheader --allocations --allusers --starttime=2024-03-01
        --endtime=2024-04-01 --accounts=eessi_demo`` yields one row per job
        for per-software usage and energy reporting.
        """
        scenario = ScenarioDefinition(
            name="eessi_software_usage",
            title="EESSI: Per-Job Software and Energy Usage",
            description=(
                "Seed a month (March 2024) of completed jobs whose AdminComment "
                "lists the EESSI modules each job loaded, with known energy, so "
                "software usage can be attributed per job from sacct."
            ),
            scenario_type=ScenarioType.USAGE_PATTERNS,
            duration_estimate="3 minutes",
            complexity="basic",
            learning_objectives=[
                "Read a job's loaded modules from sacct AdminComment",
                "Attribute CPU-hours (AllocTRES cpu x Elapsed) and energy to software",
                "Handle jobs with no module or with a non-EESSI site module",
            ],
            key_concepts=[
                "AdminComment as an opaque per-job annotation",
                "One sacct row per job (--allocations)",
                "ConsumedEnergyRaw in joules",
            ],
        )

        step1 = ScenarioStep(
            name="seed_march_jobs",
            description="Record the March 2024 jobs with their module lists",
            time_point=datetime(2024, 4, 2),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create the EESSI demo project account",
                parameters={"name": EESSI_ACCOUNT, "allocation": 5000},
            )
        )
        for job in EESSI_SOFTWARE_JOBS:
            label = ", ".join(job.modules) if job.modules else "no modules"
            step1.add_action(
                ScenarioAction(
                    type=ActionType.JOB_INJECT,
                    description=(
                        f"{job.user}: {label} ({job.cpus} CPU, {job.gpus} GPU, "
                        f"{job.elapsed_hours}h, {job.state})"
                    ),
                    parameters=job.inject_parameters(),
                )
            )
        step1.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description=(
                    "sacct --format=JobID,Account,User,AllocTRES,Elapsed,"
                    "ConsumedEnergyRaw,State,TotalCPU,AdminComment --parsable2 --noheader "
                    "--allocations --allusers --starttime=2024-03-01 "
                    f"--endtime=2024-04-01 --accounts={EESSI_ACCOUNT}"
                ),
                parameters={"account": EESSI_ACCOUNT},
                expected_outcome=f"{len(EESSI_SOFTWARE_JOBS)} rows, one per job",
                validation={"jobs": len(EESSI_SOFTWARE_JOBS)},
            )
        )
        scenario.add_step(step1)

        self.register_scenario(scenario)

    def _register_carryover_testing_scenario(self) -> None:
        """Register carryover testing scenario."""
        scenario = ScenarioDefinition(
            name="carryover_test",
            title="Carryover Logic Validation",
            description="Test carryover calculations across different usage patterns and time periods.",
            scenario_type=ScenarioType.PERIODIC_LIMITS,
            duration_estimate="12 minutes",
            complexity="intermediate",
            learning_objectives=[
                "Understand carryover calculation logic",
                "See decay factor application",
                "Test different usage patterns",
            ],
            key_concepts=[
                "Unused allocation carryover",
                "Decay factor application",
                "Period transition logic",
            ],
        )

        step1 = ScenarioStep(
            name="light_usage_carryover",
            description="Light usage with significant carryover",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create account for carryover testing",
                parameters={"name": "carryover_light", "allocation": 1000},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Light usage: 200Nh (20%)",
                parameters={"user": "user1", "amount": 200, "account": "carryover_light"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.TIME_ADVANCE,
                description="Advance to next quarter",
                parameters={"amount": 3, "unit": "months"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check carryover from light usage",
                parameters={"account": "carryover_light"},
                expected_outcome="Large carryover: ~800Nh after decay",
            )
        )
        scenario.add_step(step1)

        step2 = ScenarioStep(
            name="heavy_usage_carryover",
            description="Heavy usage with minimal carryover",
            time_point=datetime(2024, 4, 1),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create account for heavy usage test",
                parameters={"name": "carryover_heavy", "allocation": 1000},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Heavy usage: 900Nh (90%)",
                parameters={"user": "user1", "amount": 900, "account": "carryover_heavy"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.TIME_ADVANCE,
                description="Advance to next quarter",
                parameters={"amount": 3, "unit": "months"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check carryover from heavy usage",
                parameters={"account": "carryover_heavy"},
                expected_outcome="Small carryover: ~100Nh after decay",
            )
        )
        scenario.add_step(step2)

        self.register_scenario(scenario)

    def _register_limits_configuration_scenarios(self) -> None:
        """Register scenarios demonstrating different limit configurations."""
        # Scenario 1: Traditional MaxTRESMins
        max_tres_scenario = ScenarioDefinition(
            name="traditional_max_tres_mins",
            title="Traditional MaxTRESMins Configuration",
            description="Example 1 from configuration plan: Traditional HPC setup using MaxTRESMins with raw TRES values and per-user time limits.",
            scenario_type=ScenarioType.CONFIGURATION,
            duration_estimate="8 minutes",
            complexity="basic",
            learning_objectives=[
                "Understand MaxTRESMins per-user limits",
                "See raw TRES value usage",
                "Learn traditional HPC allocation patterns",
            ],
            key_concepts=[
                "MaxTRESMins: per-user time limits",
                "Raw TRES values (no billing units)",
                "Individual user enforcement",
                "Traditional HPC resource management",
            ],
            required_config={"limit_type": "MaxTRESMins", "tres_billing_enabled": False},
        )

        step1 = ScenarioStep(
            name="setup_traditional",
            description="Configure traditional MaxTRESMins limits",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create traditional research account",
                parameters={
                    "name": "traditional_account",
                    "description": "Traditional HPC allocation",
                    "allocation": 1000,
                },
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Apply MaxTRESMins limits (43200 CPU-minutes per user)",
                parameters={
                    "account": "traditional_account",
                    "limit_type": "MaxTRESMins",
                    "cpu_limit": 43200,
                },
            )
        )
        max_tres_scenario.add_step(step1)

        step2 = ScenarioStep(
            name="usage_patterns",
            description="Simulate different user usage patterns",
            time_point=datetime(2024, 1, 15),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Researcher 1: Heavy usage (200Nh)",
                parameters={"user": "researcher1", "amount": 200, "account": "traditional_account"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Researcher 2: Light usage (50Nh)",
                parameters={"user": "researcher2", "amount": 50, "account": "traditional_account"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Check individual user limits enforcement",
                parameters={"account": "traditional_account", "check_type": "user_limits"},
            )
        )
        max_tres_scenario.add_step(step2)

        self.register_scenario(max_tres_scenario)

        # Scenario 2: Modern Billing Units
        billing_scenario = ScenarioDefinition(
            name="modern_billing_units",
            title="Modern Billing Units with GrpTRESMins",
            description="Example 2 from configuration plan: Cloud-style allocation using billing units with GrpTRESMins group limits.",
            scenario_type=ScenarioType.CONFIGURATION,
            duration_estimate="10 minutes",
            complexity="intermediate",
            learning_objectives=[
                "Understand billing unit conversion",
                "See GrpTRESMins group enforcement",
                "Learn modern cloud-style resource management",
            ],
            key_concepts=[
                "GrpTRESMins: group time limits",
                "Billing units: 64 CPU = 512GB = 4 GPU = 1 unit",
                "Group-level enforcement",
                "Modern cloud resource management",
            ],
            required_config={
                "limit_type": "GrpTRESMins",
                "tres_billing_enabled": True,
                "billing_weights": {"CPU": 0.015625, "Mem": 0.001953125, "GRES/gpu": 0.25},
            },
        )

        step1 = ScenarioStep(
            name="setup_billing",
            description="Configure billing-based GrpTRESMins",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create modern AI research account",
                parameters={
                    "name": "modern_billing_account",
                    "description": "AI research with billing units",
                    "allocation": 1000,
                },
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Apply GrpTRESMins with billing units (60000 billing-minutes)",
                parameters={
                    "account": "modern_billing_account",
                    "limit_type": "GrpTRESMins",
                    "billing_limit": 60000,
                },
            )
        )
        billing_scenario.add_step(step1)

        step2 = ScenarioStep(
            name="mixed_workloads",
            description="Simulate CPU and GPU workloads",
            time_point=datetime(2024, 1, 20),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Data scientist: CPU analysis (100Nh)",
                parameters={
                    "user": "data_scientist1",
                    "amount": 100,
                    "account": "modern_billing_account",
                },
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="ML engineer: GPU training (200 GPU-hours equiv)",
                parameters={
                    "user": "ml_engineer1",
                    "amount": 200,
                    "account": "modern_billing_account",
                },
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Check billing unit consumption and group limits",
                parameters={
                    "account": "modern_billing_account",
                    "check_type": "billing_consumption",
                },
            )
        )
        billing_scenario.add_step(step2)

        self.register_scenario(billing_scenario)

        # Scenario 3: Concurrent Resource Limits
        concurrent_scenario = ScenarioDefinition(
            name="concurrent_grp_tres",
            title="Concurrent Resource Limits with GrpTRES",
            description="Example 3 from configuration plan: Concurrent resource limits using GrpTRES for simultaneous job constraints.",
            scenario_type=ScenarioType.CONFIGURATION,
            duration_estimate="12 minutes",
            complexity="intermediate",
            learning_objectives=[
                "Understand concurrent resource limiting",
                "See GrpTRES simultaneous constraints",
                "Learn job scheduling with resource limits",
            ],
            key_concepts=[
                "GrpTRES: concurrent resource limits",
                "Simultaneous job constraints",
                "Resource allocation vs. time allocation",
                "Job scheduling impact",
            ],
            required_config={"limit_type": "GrpTRES", "tres_billing_enabled": False},
        )

        step1 = ScenarioStep(
            name="setup_concurrent",
            description="Configure concurrent GrpTRES limits",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create simulation group account",
                parameters={
                    "name": "concurrent_limits_account",
                    "description": "Simulation group with concurrent limits",
                    "allocation": 1000,
                },
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Apply GrpTRES limits (10 nodes, 640 CPUs, 8 GPUs concurrent)",
                parameters={
                    "account": "concurrent_limits_account",
                    "limit_type": "GrpTRES",
                    "node_limit": 10,
                    "cpu_limit": 640,
                    "gpu_limit": 8,
                },
            )
        )
        concurrent_scenario.add_step(step1)

        step2 = ScenarioStep(
            name="job_scheduling",
            description="Simulate concurrent job scheduling",
            time_point=datetime(2024, 1, 10),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Job 1: Large simulation (8 nodes, 512 CPUs)",
                parameters={
                    "job_request": {"nodes": 8, "cpus": 512},
                    "account": "concurrent_limits_account",
                },
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Job 2: GPU job (4 GPUs)",
                parameters={"job_request": {"gpus": 4}, "account": "concurrent_limits_account"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Job 3: Additional CPU job (2 nodes, 128 CPUs) - should be rejected",
                parameters={
                    "job_request": {"nodes": 2, "cpus": 128},
                    "account": "concurrent_limits_account",
                    "expected": "rejected",
                },
            )
        )
        concurrent_scenario.add_step(step2)

        self.register_scenario(concurrent_scenario)

        # Scenario 4: Mixed Limits Configuration
        mixed_scenario = ScenarioDefinition(
            name="mixed_limits_comprehensive",
            title="Comprehensive Mixed Limits Configuration",
            description="Advanced scenario combining GrpTRES, GrpTRESMins, and MaxTRESMins in a single multi-tier configuration.",
            scenario_type=ScenarioType.CONFIGURATION,
            duration_estimate="15 minutes",
            complexity="advanced",
            learning_objectives=[
                "Understand complex multi-limit configurations",
                "See interaction between different limit types",
                "Learn advanced resource management patterns",
            ],
            key_concepts=[
                "Multi-tier limit enforcement",
                "Limit type interactions",
                "Complex resource policies",
                "Enterprise resource management",
            ],
            required_config={
                "limit_types": ["GrpTRES", "GrpTRESMins", "MaxTRESMins"],
                "tres_billing_enabled": True,
            },
        )

        step1 = ScenarioStep(
            name="setup_mixed",
            description="Configure comprehensive mixed limits",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create enterprise multi-tier account",
                parameters={
                    "name": "mixed_limits_account",
                    "description": "Enterprise account with mixed limits",
                    "allocation": 2000,
                },
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Apply all limit types: GrpTRES (20 nodes), GrpTRESMins (120000), MaxTRESMins (86400)",
                parameters={"account": "mixed_limits_account", "all_limits": True},
            )
        )
        mixed_scenario.add_step(step1)

        step2 = ScenarioStep(
            name="limit_interactions",
            description="Test limit interactions and conflicts",
            time_point=datetime(2024, 2, 1),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Power user: Heavy usage (500Nh)",
                parameters={
                    "user": "power_user1",
                    "amount": 500,
                    "account": "mixed_limits_account",
                },
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Regular users: Moderate usage (200Nh each)",
                parameters={
                    "user": "regular_user1",
                    "amount": 200,
                    "account": "mixed_limits_account",
                },
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.VALIDATE,
                description="Check all limit types and conflicts",
                parameters={"account": "mixed_limits_account", "check_type": "all_limits"},
            )
        )
        mixed_scenario.add_step(step2)

        self.register_scenario(mixed_scenario)

    def _register_configuration_comparison_scenario(self) -> None:
        """Register configuration comparison scenario."""
        scenario = ScenarioDefinition(
            name="config_comparison",
            title="Configuration Impact Comparison",
            description="Compare behavior with different SLURM configurations to understand parameter impact.",
            scenario_type=ScenarioType.CONFIGURATION,
            duration_estimate="15 minutes",
            complexity="advanced",
            learning_objectives=[
                "Understand configuration parameter impact",
                "Compare different SLURM setups",
                "See billing weight effects",
            ],
            key_concepts=[
                "Configuration-driven behavior",
                "TRES billing weight impact",
                "Priority weight differences",
                "Decay half-life effects",
            ],
        )

        step1 = ScenarioStep(
            name="standard_config",
            description="Test with standard configuration",
            time_point=datetime(2024, 1, 1),
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.CONFIG_RELOAD,
                description="Load standard configuration",
                parameters={"config_path": "examples/slurm.conf"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create test account with standard config",
                parameters={"name": "config_standard", "allocation": 1000},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Add 500Nh usage",
                parameters={"user": "user1", "amount": 500, "account": "config_standard"},
            )
        )
        step1.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check limits with standard config",
                parameters={"account": "config_standard"},
            )
        )
        scenario.add_step(step1)

        step2 = ScenarioStep(
            name="custom_config",
            description="Test with custom configuration",
            time_point=datetime(2024, 1, 1),
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.TIME_SET,
                description="Reset time for comparison",
                parameters={"time": "2024-01-01"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.CONFIG_RELOAD,
                description="Load custom configuration",
                parameters={"config_path": "examples/custom_slurm.conf"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.ACCOUNT_CREATE,
                description="Create test account with custom config",
                parameters={"name": "config_custom", "allocation": 1000},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.USAGE_INJECT,
                description="Add 500Nh usage",
                parameters={"user": "user1", "amount": 500, "account": "config_custom"},
            )
        )
        step2.add_action(
            ScenarioAction(
                type=ActionType.LIMITS_CALCULATE,
                description="Check limits with custom config",
                parameters={"account": "config_custom"},
            )
        )
        scenario.add_step(step2)

        self.register_scenario(scenario)
