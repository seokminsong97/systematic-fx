#!/usr/bin/env python3
"""Model policy lock (PreToolUse hook).

Enforces `.ai/model-policy.json` — the machine-readable single source for
model policy (AI_WORKFLOW.md §14).

Scope: this hook blocks policy violations visible in call arguments and in
verifiable configuration. Session model identity, API availability, and
fallback behavior are covered by the behavioral contract and the audit log,
not by this hook.

Deny rules:
  - Agent/Task calls whose explicit `model` is outside the allowed set.
  - Agent/Task calls spawning codex-* agent types WITHOUT an explicit allowed
    model — their plugin frontmatter pins a default outside the policy.
  - Workflow scripts that set a disallowed agent model, or an effort other
    than the policy effort.
  - Bash invocations of the Codex CLI (`codex ...`) or its companion runtime
    (`codex-companion.mjs`) that carry per-call model/effort overrides.
  - Same invocations when `~/.codex/config.toml` cannot be read or its
    model / model_reasoning_effort drifts from policy (checked per call).
  - Fail closed: if the policy file is unreadable, every call that would
    select or execute a model is denied; plain calls stay allowed.

Every decision is appended as metadata-only JSONL to
`.ai/logs/hook-audit.jsonl` (git-ignored). This doubles as the runtime
evidence requested by AI_WORKFLOW.md §17 (which tool events Codex plugin
calls actually flow through).
"""
import datetime
import json
import os
import re
import sys

CODEX_DIRECT_RE = re.compile(r"(?:^|[|&;]\s*)codex\b")
CODEX_COMPANION_RE = re.compile(r"codex-companion\.mjs")
CODEX_DIRECT_OVERRIDE_RE = re.compile(
    r"(\s--model[\s=]|\s-m\s|-c\s*model|model_reasoning_effort\s*=)"
)
CODEX_COMPANION_OVERRIDE_RE = re.compile(r"\s--(model|effort)[\s=]")
WORKFLOW_MODEL_RE = re.compile(r"""model\s*:\s*['"]([A-Za-z0-9._-]+)['"]""")
WORKFLOW_EFFORT_RE = re.compile(r"""effort\s*:\s*['"]([A-Za-z0-9._-]+)['"]""")
CODEX_AGENT_TYPE_RE = re.compile(r"codex", re.IGNORECASE)


def project_root():
    env = os.environ.get("CLAUDE_PROJECT_DIR")
    if env:
        return env
    # <root>/.claude/hooks/model-lock.py -> <root>
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_policy():
    path = os.environ.get("MODEL_LOCK_POLICY") or os.path.join(
        project_root(), ".ai", "model-policy.json"
    )
    try:
        with open(path, encoding="utf-8") as f:
            policy = json.load(f)
        # Schema version gate: this hook supports schema 1.x only; an unknown
        # version counts as unreadable and fails closed (AI_WORKFLOW.md §14).
        if not str(policy.get("version", "")).startswith("1."):
            return None
        return policy
    except Exception:
        return None


def audit(tool, decision, rule, detail=""):
    """Metadata-only audit trail; never raise."""
    try:
        log_dir = os.path.join(project_root(), ".ai", "logs")
        os.makedirs(log_dir, exist_ok=True)
        entry = {
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "tool": tool,
            "decision": decision,
            "rule": rule,
        }
        if detail:
            entry["detail"] = detail
        with open(os.path.join(log_dir, "hook-audit.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def deny(tool, rule, reason, detail=""):
    audit(tool, "deny", rule, detail)
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": f"Model policy lock ({rule}): {reason}",
        }
    }))
    sys.exit(0)


def allow(tool, rule, detail=""):
    audit(tool, "allow", rule, detail)
    sys.exit(0)


def read_codex_config(policy):
    """Return (model, effort) from the Codex CLI config, or None if unreadable."""
    cfg_path = os.path.expanduser(
        policy.get("codex", {}).get("config_path", "~/.codex/config.toml")
    )
    try:
        model = effort = None
        with open(cfg_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                m = re.match(r'^model\s*=\s*"([^"]+)"', line)
                if m:
                    model = m.group(1)
                m = re.match(r'^model_reasoning_effort\s*=\s*"([^"]+)"', line)
                if m:
                    effort = m.group(1)
        return (model, effort)
    except Exception:
        return None


def check_agent(tool, ti, policy):
    model = ti.get("model")
    agent_type = ti.get("subagent_type") or ""
    is_codex_type = bool(CODEX_AGENT_TYPE_RE.search(agent_type))

    if policy is None:
        if model or is_codex_type:
            deny(tool, "fail-closed",
                 "policy file unreadable; cannot verify model-selecting call. "
                 "Restore .ai/model-policy.json.")
        allow(tool, "inherit-session", "no explicit model")

    allowed = set(policy.get("claude", {}).get("subagent_models", []))

    if is_codex_type and not model:
        deny(tool, "codex-wrapper-model",
             "codex agent types pin a non-allowed default model in plugin "
             f"frontmatter; pass an explicit model from {sorted(allowed)}.",
             agent_type)
    if model and model not in allowed:
        deny(tool, "subagent-model",
             f"model '{model}' is not allowed; allowed: {sorted(allowed)}. "
             "Omit 'model' to inherit the session model.", model)
    allow(tool, "subagent-ok" if model else "inherit-session", model or agent_type)


def check_workflow(tool, ti, policy):
    texts = []
    if isinstance(ti.get("script"), str):
        texts.append(ti["script"])
    script_path = ti.get("scriptPath")
    if isinstance(script_path, str) and os.path.isfile(script_path):
        try:
            with open(script_path, encoding="utf-8", errors="ignore") as f:
                texts.append(f.read())
        except OSError:
            pass
    blob = "\n".join(texts)
    models = WORKFLOW_MODEL_RE.findall(blob)
    efforts = WORKFLOW_EFFORT_RE.findall(blob)

    if policy is None:
        if models or efforts:
            deny(tool, "fail-closed",
                 "policy file unreadable; cannot verify model/effort options "
                 "in workflow script. Restore .ai/model-policy.json.")
        allow(tool, "no-model-opts")

    allowed = set(policy.get("claude", {}).get("subagent_models", []))
    required_effort = policy.get("claude", {}).get("effort")
    for m in models:
        if m not in allowed:
            deny(tool, "workflow-model",
                 f"script sets agent model '{m}'; allowed: {sorted(allowed)}.", m)
    for e in efforts:
        if required_effort and e != required_effort:
            deny(tool, "workflow-effort",
                 f"script sets effort '{e}'; policy requires "
                 f"'{required_effort}' (or omit to inherit).", e)
    allow(tool, "workflow-ok")


def check_bash(tool, ti, policy):
    cmd = ti.get("command") or ""
    direct = CODEX_DIRECT_RE.search(cmd)
    companion = CODEX_COMPANION_RE.search(cmd)
    if not (direct or companion):
        sys.exit(0)  # unrelated command; not audited to keep the log small

    kind = "codex-companion" if companion else "codex-direct"
    # Only flags AFTER the invocation token count as overrides; flags before it
    # (e.g. `grep -nE '--model' .../codex-companion.mjs`) are not an invocation.
    tail = cmd[(companion or direct).start():]

    if policy is None:
        deny(tool, "fail-closed",
             "policy file unreadable; cannot verify Codex invocation. "
             "Restore .ai/model-policy.json.", kind)

    overrides_allowed = policy.get("codex", {}).get("per_call_overrides", False)
    override_re = CODEX_COMPANION_OVERRIDE_RE if companion else CODEX_DIRECT_OVERRIDE_RE
    if not overrides_allowed and override_re.search(tail):
        deny(tool, "codex-override",
             "per-call Codex model/effort overrides are forbidden; "
             ".ai/model-policy.json is authoritative and ~/.codex/config.toml "
             "is the checked runtime setting that must match it.", kind)

    cfg = read_codex_config(policy)
    if cfg is None:
        deny(tool, "codex-config-unreadable",
             "cannot read the Codex config to verify the effective model "
             "(fail closed).", kind)
    want = policy.get("codex", {})
    got_model, got_effort = cfg
    observed = f"{kind} cfg={got_model}/{got_effort}"
    if got_model != want.get("model") or got_effort != want.get("model_reasoning_effort"):
        deny(tool, "codex-config-drift",
             f"config drift: policy requires {want.get('model')}/"
             f"{want.get('model_reasoning_effort')}, config has "
             f"{got_model}/{got_effort}. Align config or update policy "
             "through the contract-change process.", observed)
    allow(tool, f"{kind}-ok", observed)


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)
    tool = data.get("tool_name", "")
    ti = data.get("tool_input") or {}
    policy = load_policy()

    if tool in ("Agent", "Task"):
        check_agent(tool, ti, policy)
    elif tool == "Workflow":
        check_workflow(tool, ti, policy)
    elif tool == "Bash":
        check_bash(tool, ti, policy)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        # A hook bug must not brick the session; record and allow.
        audit("unknown", "allow", "hook-error")
        sys.exit(0)
