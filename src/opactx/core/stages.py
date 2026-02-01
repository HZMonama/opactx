BUILD_STAGES = [
    ("load_config", "Load config"),
    ("load_intent", "Load intent context"),
    ("fetch_sources", "Fetch sources"),
    ("normalize", "Normalize"),
    ("validate_schema", "Validate schema"),
    ("write_bundle", "Write bundle"),
]

VALIDATE_STAGES = [
    ("load_config", "Load config"),
    ("load_schema", "Load schema"),
    ("load_intent", "Load intent context"),
    ("resolve_plugins", "Resolve plugins"),
    ("schema_check", "Schema check"),
]

INIT_STAGES = [
    ("resolve_target", "Resolve target directory"),
    ("plan_scaffold", "Plan scaffold"),
    ("apply_scaffold", "Apply scaffold"),
]

INSPECT_STAGES = [
    ("open_bundle", "Open bundle"),
    ("read_manifest", "Read manifest"),
    ("read_data", "Read data"),
    ("summarize_context", "Summarize context"),
    ("extract_path", "Extract path"),
]

RUN_OPA_STAGES = [
    ("prepare_bundle", "Prepare bundle"),
    ("start_opa", "Start OPA"),
    ("stream_output", "Stream output"),
]


STAGE_ORDER = {
    "build": BUILD_STAGES,
    "validate": VALIDATE_STAGES,
    "init": INIT_STAGES,
    "inspect": INSPECT_STAGES,
    "run-opa": RUN_OPA_STAGES,
}


STAGE_LABELS = {
    command: {stage_id: label for stage_id, label in stages}
    for command, stages in STAGE_ORDER.items()
}
