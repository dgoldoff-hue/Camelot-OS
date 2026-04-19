"""
pipeline.py — Camelot OS Multi-Bot Pipeline Executor

Executes sequential chains of bot actions where the output of each step
feeds as input to the next. Supports named pipelines, error handling,
retry logic, and step-level logging.

Usage:
    pipeline = Pipeline("lead_to_crm")
    pipeline.add_step("scout", "search_leads", {"region": "CT"})
    pipeline.add_step("scout", "enrich_lead", {})        # receives scout output
    pipeline.add_step("scout", "push_to_hubspot", {})    # receives enriched data
    results = pipeline.execute()

    # Or use named pipelines:
    results = run_pipeline("lead_to_crm", params={"region": "CT"})
"""

import asyncio
import importlib
import logging
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from bot_registry import BOTS, get_bot, validate_action

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStep:
    """
    A single unit of work within a pipeline.

    Attributes:
        bot_name:       Target bot identifier (e.g., "scout")
        action:         Bot capability to invoke (e.g., "search_leads")
        params:         Static parameters for this step
        step_id:        Auto-generated unique step ID
        step_name:      Human-readable label (defaults to bot.action)
        retry_count:    Number of retry attempts on failure (default 1)
        timeout:        Step timeout in seconds (None = use bot default)
        merge_input:    If True, merge upstream output into this step's params
        on_failure:     "abort" (default) | "skip" | "continue"
    """
    bot_name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    step_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    step_name: str = ""
    retry_count: int = 1
    timeout: Optional[int] = None
    merge_input: bool = True
    on_failure: str = "abort"

    def __post_init__(self):
        if not self.step_name:
            self.step_name = f"{self.bot_name}.{self.action}"


@dataclass
class StepResult:
    """Result of a single pipeline step execution."""
    step: PipelineStep
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    attempts: int = 0


@dataclass
class PipelineResult:
    """Aggregate result of a full pipeline execution."""
    pipeline_name: str
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: StepStatus = StepStatus.PENDING
    steps: List[StepResult] = field(default_factory=list)
    final_output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    @property
    def succeeded_steps(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.COMPLETED)

    @property
    def failed_steps(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.FAILED)

    def summary(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "duration_seconds": round(self.duration_seconds, 2),
            "steps_total": len(self.steps),
            "steps_succeeded": self.succeeded_steps,
            "steps_failed": self.failed_steps,
            "final_output": self.final_output,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Bot Executor — dispatches work to individual bots
# ---------------------------------------------------------------------------

class BotExecutor:
    """
    Responsible for invoking a single bot action.

    In production, this dispatches to:
      - HTTP API call if the bot has an api_endpoint
      - subprocess call if entry_point is defined (local execution)
      - A registered Python callable if loaded as a module

    The executor is designed to be swappable — replace _call_subprocess
    or _call_api with real implementations as bots are deployed.
    """

    # Registry of bot callable overrides (used for testing / direct module integration)
    _callable_registry: Dict[str, Callable] = {}

    @classmethod
    def register_callable(cls, bot_name: str, fn: Callable):
        """Register a Python callable to handle a bot's execution."""
        cls._callable_registry[bot_name] = fn
        logger.info("Registered callable for bot: %s", bot_name)

    def execute(
        self,
        bot_name: str,
        action: str,
        params: Dict[str, Any],
        timeout: Optional[int] = None,
    ) -> Any:
        """
        Execute a bot action with the given parameters.

        Returns the bot's output (dict, str, or any serializable object).
        Raises RuntimeError on bot execution failure.
        """
        bot_meta = get_bot(bot_name)
        if not bot_meta:
            raise RuntimeError(f"Unknown bot: '{bot_name}'")

        if not validate_action(bot_name, action):
            raise RuntimeError(
                f"Bot '{bot_name}' does not support action '{action}'. "
                f"Available: {bot_meta['capabilities']}"
            )

        effective_timeout = timeout or bot_meta.get("timeout_seconds", 60)

        logger.info(
            "Executing bot=%s action=%s params=%s timeout=%ds",
            bot_name, action, list(params.keys()), effective_timeout
        )

        # 1. Check for registered Python callable (test / direct integration)
        if bot_name in self._callable_registry:
            return self._callable_registry[bot_name](action=action, params=params)

        # 2. Check for HTTP API endpoint
        api_endpoint = bot_meta.get("api_endpoint")
        if api_endpoint:
            return self._call_api(
                endpoint=api_endpoint,
                action=action,
                params=params,
                timeout=effective_timeout,
            )

        # 3. Fall back to subprocess call via entry_point
        entry_point = bot_meta.get("entry_point")
        if entry_point:
            return self._call_subprocess(
                entry_point=entry_point,
                action=action,
                params=params,
                timeout=effective_timeout,
            )

        raise RuntimeError(
            f"Bot '{bot_name}' has no api_endpoint or entry_point configured."
        )

    def _call_api(
        self,
        endpoint: str,
        action: str,
        params: Dict[str, Any],
        timeout: int,
    ) -> Dict[str, Any]:
        """
        Call a bot via its HTTP REST API.

        POST {endpoint}/run
        Body: {"action": action, "params": params}
        """
        import urllib.request
        import json

        url = f"{endpoint.rstrip('/')}/run"
        payload = json.dumps({"action": action, "params": params}).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        logger.debug("Calling bot API: POST %s", url)

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                result = json.loads(body)
                logger.debug("Bot API response: %s", str(result)[:200])
                return result
        except Exception as exc:
            raise RuntimeError(f"Bot API call failed [{url}]: {exc}") from exc

    def _call_subprocess(
        self,
        entry_point: str,
        action: str,
        params: Dict[str, Any],
        timeout: int,
    ) -> Dict[str, Any]:
        """
        Call a bot by spawning it as a subprocess.

        Invokes: python {entry_point} --action {action} --params '{json}'
        Expects JSON output on stdout.
        """
        import json
        import sys

        cmd = [
            sys.executable,
            entry_point,
            "--action", action,
            "--params", json.dumps(params),
        ]

        logger.debug("Spawning subprocess: %s", " ".join(cmd))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"Bot subprocess exited with code {proc.returncode}: {proc.stderr[:500]}"
                )
            result = json.loads(proc.stdout.strip())
            return result
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Bot subprocess timed out after {timeout}s")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Bot returned non-JSON output: {proc.stdout[:200]}") from exc


# ---------------------------------------------------------------------------
# Pipeline Class
# ---------------------------------------------------------------------------

class Pipeline:
    """
    Multi-bot pipeline executor.

    Executes a sequence of bot actions in order, passing each step's
    output as merged input to the next step.

    Example:
        p = Pipeline("lead_to_crm")
        p.add_step("scout", "search_leads", {"region": "CT", "min_units": 50})
        p.add_step("scout", "enrich_lead", {})
        p.add_step("scout", "push_to_hubspot", {})
        result = p.execute()
        print(result.summary())
    """

    def __init__(
        self,
        name: str,
        executor: Optional[BotExecutor] = None,
        description: str = "",
    ):
        """
        Initialize a pipeline.

        Args:
            name:        Human-readable pipeline name (used in logs/results)
            executor:    BotExecutor instance (creates default if not provided)
            description: Optional description of what this pipeline does
        """
        self.name = name
        self.description = description
        self.executor = executor or BotExecutor()
        self._steps: List[PipelineStep] = []

    def add_step(
        self,
        bot_name: str,
        action: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        step_name: str = "",
        retry_count: int = 1,
        timeout: Optional[int] = None,
        merge_input: bool = True,
        on_failure: str = "abort",
    ) -> "Pipeline":
        """
        Append a step to the pipeline.

        Args:
            bot_name:    Target bot (e.g., "scout")
            action:      Bot capability (e.g., "search_leads")
            params:      Static parameters for this step
            step_name:   Optional human-readable label
            retry_count: Number of attempts on failure
            timeout:     Override bot default timeout
            merge_input: Whether to merge upstream output into params
            on_failure:  "abort" | "skip" | "continue"

        Returns:
            Self, for method chaining.
        """
        step = PipelineStep(
            bot_name=bot_name,
            action=action,
            params=params or {},
            step_name=step_name,
            retry_count=retry_count,
            timeout=timeout,
            merge_input=merge_input,
            on_failure=on_failure,
        )
        self._steps.append(step)
        logger.debug("Added step: %s", step.step_name)
        return self

    def execute(
        self,
        initial_params: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        """
        Execute all steps in sequence.

        Each step receives a merged context of:
          - Its own static params
          - The output from the previous step (if merge_input=True)
          - The initial_params passed to execute()

        Args:
            initial_params: Seed parameters injected into all steps.

        Returns:
            PipelineResult with per-step results and final output.
        """
        result = PipelineResult(
            pipeline_name=self.name,
            started_at=datetime.utcnow(),
        )

        if not self._steps:
            result.status = StepStatus.FAILED
            result.error = "Pipeline has no steps defined."
            result.completed_at = datetime.utcnow()
            logger.error("Pipeline '%s' has no steps.", self.name)
            return result

        logger.info(
            "Starting pipeline '%s' with %d steps",
            self.name, len(self._steps)
        )

        context: Dict[str, Any] = dict(initial_params or {})
        last_output: Any = None

        for i, step in enumerate(self._steps):
            logger.info(
                "Pipeline '%s' — step %d/%d: %s",
                self.name, i + 1, len(self._steps), step.step_name
            )

            step_result = StepResult(
                step=step,
                status=StepStatus.RUNNING,
                started_at=datetime.utcnow(),
            )

            # Build merged params for this step
            merged_params = dict(context)
            merged_params.update(step.params)  # Step's own params win
            if step.merge_input and last_output and isinstance(last_output, dict):
                # Previous step output fills gaps (doesn't override step params)
                for k, v in last_output.items():
                    if k not in merged_params:
                        merged_params[k] = v

            # Execute with retry logic
            success = False
            last_error: Optional[str] = None

            for attempt in range(1, step.retry_count + 1):
                step_result.attempts = attempt
                try:
                    t0 = time.monotonic()
                    output = self.executor.execute(
                        bot_name=step.bot_name,
                        action=step.action,
                        params=merged_params,
                        timeout=step.timeout,
                    )
                    step_result.duration_seconds = time.monotonic() - t0
                    step_result.output = output
                    step_result.status = StepStatus.COMPLETED
                    last_output = output
                    success = True
                    logger.info(
                        "Step '%s' completed in %.2fs (attempt %d)",
                        step.step_name, step_result.duration_seconds, attempt
                    )
                    break
                except Exception as exc:
                    last_error = str(exc)
                    logger.warning(
                        "Step '%s' attempt %d/%d failed: %s",
                        step.step_name, attempt, step.retry_count, last_error
                    )
                    if attempt < step.retry_count:
                        time.sleep(2 ** (attempt - 1))  # Exponential backoff

            if not success:
                step_result.status = StepStatus.FAILED
                step_result.error = last_error
                logger.error(
                    "Step '%s' failed after %d attempt(s): %s",
                    step.step_name, step.retry_count, last_error
                )

                if step.on_failure == "abort":
                    result.status = StepStatus.FAILED
                    result.error = (
                        f"Pipeline aborted at step '{step.step_name}': {last_error}"
                    )
                    step_result.completed_at = datetime.utcnow()
                    result.steps.append(step_result)
                    result.completed_at = datetime.utcnow()
                    logger.error("Pipeline '%s' aborted.", self.name)
                    return result

                elif step.on_failure == "skip":
                    step_result.status = StepStatus.SKIPPED
                    logger.info("Skipping failed step '%s' and continuing.", step.step_name)

                elif step.on_failure == "continue":
                    logger.info(
                        "Continuing past failed step '%s' (on_failure=continue).",
                        step.step_name
                    )

            step_result.completed_at = datetime.utcnow()
            result.steps.append(step_result)

        result.status = (
            StepStatus.COMPLETED
            if result.failed_steps == 0
            else StepStatus.FAILED
        )
        result.final_output = last_output
        result.completed_at = datetime.utcnow()

        logger.info(
            "Pipeline '%s' finished: %s (%d/%d steps succeeded) in %.2fs",
            self.name,
            result.status.value,
            result.succeeded_steps,
            len(result.steps),
            result.duration_seconds,
        )
        return result


# ---------------------------------------------------------------------------
# Named Pipeline Factory
# ---------------------------------------------------------------------------

def build_named_pipeline(
    pipeline_name: str,
    params: Optional[Dict[str, Any]] = None,
) -> Pipeline:
    """
    Build a pre-defined named pipeline.

    Args:
        pipeline_name: One of the standard Camelot OS pipelines.
        params:        Override/seed parameters for the pipeline.

    Returns:
        Configured Pipeline instance, ready to execute.

    Raises:
        ValueError: If pipeline_name is not recognized.
    """
    p = params or {}
    name = pipeline_name.lower().replace("-", "_")

    # ------------------------------------------------------------------
    # PIPELINE: lead_to_crm
    # Scout: search → enrich → push to HubSpot
    # ------------------------------------------------------------------
    if name == "lead_to_crm":
        pipeline = Pipeline(
            name="lead_to_crm",
            description="Search for PM leads, enrich with full data, push to HubSpot CRM",
        )
        pipeline.add_step(
            "scout", "search_leads",
            {"region": p.get("region"), "property_type": p.get("property_type", "multifamily")},
            step_name="Search for leads",
        )
        pipeline.add_step(
            "scout", "enrich_lead",
            {},
            step_name="Enrich top leads",
            merge_input=True,
        )
        pipeline.add_step(
            "scout", "push_to_hubspot",
            {},
            step_name="Push to HubSpot",
            merge_input=True,
            on_failure="continue",  # Don't abort if CRM push fails
        )
        return pipeline

    # ------------------------------------------------------------------
    # PIPELINE: property_audit
    # Compliance: HPD → DOB → LL97 → Report scorecard
    # ------------------------------------------------------------------
    elif name == "property_audit":
        pipeline = Pipeline(
            name="property_audit",
            description="Full compliance audit: HPD, DOB, LL97, then generate scorecard",
        )
        address = p.get("address")
        pipeline.add_step(
            "compliance", "check_hpd",
            {"address": address},
            step_name="Check HPD violations",
        )
        pipeline.add_step(
            "compliance", "check_dob",
            {"address": address},
            step_name="Check DOB permits",
            on_failure="continue",
        )
        pipeline.add_step(
            "compliance", "check_ll97",
            {"address": address},
            step_name="Check LL97 exposure",
            on_failure="continue",
        )
        pipeline.add_step(
            "report", "property_scorecard",
            {"address": address},
            step_name="Generate compliance scorecard",
            merge_input=True,
        )
        return pipeline

    # ------------------------------------------------------------------
    # PIPELINE: deal_outreach
    # Deal: prospect → battlecard → draft email → log to HubSpot
    # ------------------------------------------------------------------
    elif name == "deal_outreach":
        pipeline = Pipeline(
            name="deal_outreach",
            description="Research acquisition target, build battlecard, draft email, log to HubSpot",
        )
        company = p.get("company")
        pipeline.add_step(
            "deal", "research_target",
            {"company": company},
            step_name="Research target company",
        )
        pipeline.add_step(
            "deal", "build_battlecard",
            {"company": company},
            step_name="Build competitive battlecard",
            merge_input=True,
        )
        pipeline.add_step(
            "deal", "draft_email",
            {"company": company},
            step_name="Draft personalized outreach email",
            merge_input=True,
        )
        pipeline.add_step(
            "deal", "log_outreach",
            {"company": company},
            step_name="Log outreach to HubSpot",
            merge_input=True,
            on_failure="continue",
        )
        return pipeline

    # ------------------------------------------------------------------
    # PIPELINE: new_acquisition_dd
    # Scout enrich → Compliance full audit → Broker proforma → Report deal memo
    # ------------------------------------------------------------------
    elif name == "new_acquisition_dd":
        pipeline = Pipeline(
            name="new_acquisition_dd",
            description="Full due diligence: enrich lead, compliance audit, proforma, deal memo",
        )
        company = p.get("company")
        address = p.get("address")
        pipeline.add_step(
            "scout", "enrich_lead",
            {"company": company, "address": address},
            step_name="Enrich acquisition target",
        )
        pipeline.add_step(
            "compliance", "full_audit",
            {"address": address},
            step_name="Full compliance audit",
            merge_input=True,
            on_failure="continue",
        )
        pipeline.add_step(
            "broker", "build_proforma",
            {"company": company, "address": address},
            step_name="Build financial proforma",
            merge_input=True,
        )
        pipeline.add_step(
            "report", "deal_memo",
            {"company": company},
            step_name="Generate deal memo",
            merge_input=True,
        )
        return pipeline

    # ------------------------------------------------------------------
    # PIPELINE: weekly_ops_rhythm
    # Report KPIs → Scout daily leads → Compliance flagged violations
    # ------------------------------------------------------------------
    elif name == "weekly_ops_rhythm":
        pipeline = Pipeline(
            name="weekly_ops_rhythm",
            description="Weekly ops sequence: KPI report, new leads, open compliance flags",
        )
        pipeline.add_step(
            "report", "send_weekly_kpi",
            {},
            step_name="Send weekly KPI report",
            on_failure="continue",
        )
        pipeline.add_step(
            "scout", "search_leads",
            {"property_type": "multifamily"},
            step_name="Run daily Scout lead search",
            on_failure="continue",
            merge_input=False,
        )
        pipeline.add_step(
            "compliance", "track_remediation",
            {},
            step_name="Pull open compliance items",
            on_failure="continue",
            merge_input=False,
        )
        return pipeline

    # ------------------------------------------------------------------
    # PIPELINE: lease_audit
    # Index: extract lease data → flag expiring → report
    # ------------------------------------------------------------------
    elif name == "lease_audit":
        pipeline = Pipeline(
            name="lease_audit",
            description="Full lease audit: extract key terms, flag expiring, generate report",
        )
        pipeline.add_step(
            "index", "extract_lease_data",
            {"address": p.get("address")},
            step_name="Extract lease data",
        )
        pipeline.add_step(
            "index", "flag_expiring",
            {},
            step_name="Flag expiring leases",
            merge_input=True,
            on_failure="continue",
        )
        pipeline.add_step(
            "report", "occupancy_dashboard",
            {},
            step_name="Generate occupancy summary",
            merge_input=True,
        )
        return pipeline

    else:
        raise ValueError(
            f"Unknown pipeline: '{pipeline_name}'. "
            f"Available: lead_to_crm, property_audit, deal_outreach, "
            f"new_acquisition_dd, weekly_ops_rhythm, lease_audit"
        )


def run_pipeline(
    pipeline_name: str,
    params: Optional[Dict[str, Any]] = None,
) -> PipelineResult:
    """
    Build and execute a named pipeline in one call.

    Args:
        pipeline_name: Pipeline identifier (e.g., "lead_to_crm")
        params:        Seed parameters for the pipeline

    Returns:
        PipelineResult with execution summary and final output.
    """
    logger.info("Running named pipeline: '%s' with params: %s", pipeline_name, params)
    pipeline = build_named_pipeline(pipeline_name, params)
    return pipeline.execute(initial_params=params)


# ---------------------------------------------------------------------------
# Registry of available named pipelines (for API /pipelines endpoint)
# ---------------------------------------------------------------------------

NAMED_PIPELINES: Dict[str, Dict[str, Any]] = {
    "lead_to_crm": {
        "name": "lead_to_crm",
        "description": "Search for PM leads, enrich with full data, push to HubSpot CRM",
        "required_params": ["region"],
        "optional_params": ["property_type", "min_units"],
        "estimated_duration_seconds": 120,
        "bots_involved": ["scout"],
    },
    "property_audit": {
        "name": "property_audit",
        "description": "Full compliance audit: HPD, DOB, LL97, generate scorecard",
        "required_params": ["address"],
        "optional_params": [],
        "estimated_duration_seconds": 90,
        "bots_involved": ["compliance", "report"],
    },
    "deal_outreach": {
        "name": "deal_outreach",
        "description": "Research target, build battlecard, draft email, log to HubSpot",
        "required_params": ["company"],
        "optional_params": [],
        "estimated_duration_seconds": 150,
        "bots_involved": ["deal"],
    },
    "new_acquisition_dd": {
        "name": "new_acquisition_dd",
        "description": "Full DD: enrich lead, compliance audit, proforma, deal memo",
        "required_params": ["company"],
        "optional_params": ["address"],
        "estimated_duration_seconds": 240,
        "bots_involved": ["scout", "compliance", "broker", "report"],
    },
    "weekly_ops_rhythm": {
        "name": "weekly_ops_rhythm",
        "description": "Weekly ops: KPI report, new leads, open compliance flags",
        "required_params": [],
        "optional_params": [],
        "estimated_duration_seconds": 180,
        "bots_involved": ["report", "scout", "compliance"],
    },
    "lease_audit": {
        "name": "lease_audit",
        "description": "Lease audit: extract terms, flag expiring, generate occupancy report",
        "required_params": [],
        "optional_params": ["address"],
        "estimated_duration_seconds": 90,
        "bots_involved": ["index", "report"],
    },
}
