"""Preview or reconcile explicitly managed repository rulesets using gh."""

import argparse
import difflib
import json
from pathlib import Path
import subprocess
import sys

FIELDS = {"name", "target", "enforcement", "conditions", "bypass_actors", "rules"}
DEFAULT_DIRECTORY = Path(__file__).resolve().parents[1] / "rulesets"


def canonical(value):
    # Rules, conditions, actors, and check lists are unordered in the API.
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return sorted((canonical(item) for item in value), key=json.dumps)
    return value


def render(value):
    return json.dumps(canonical(value), indent=2, sort_keys=True).splitlines(True)


def load_configs(directory):
    configs = []
    ids = set()
    paths = sorted(directory.glob("*.json"))
    if not paths:
        raise ValueError(f"No ruleset JSON files found in {directory}")
    for path in paths:
        config = json.loads(path.read_text())
        if not isinstance(config, dict) or set(config) != FIELDS | {"id"}:
            raise ValueError(f"{path}: expected exactly id and {sorted(FIELDS)}")
        ruleset_id = config.pop("id")
        if type(ruleset_id) is not int or ruleset_id <= 0 or ruleset_id in ids:
            raise ValueError(f"{path}: id must be a unique positive integer")
        ids.add(ruleset_id)
        if not isinstance(config["name"], str) or not config["name"].strip():
            raise ValueError(f"{path}: name must be a nonempty string")
        if config["target"] not in ("branch", "tag", "push"):
            raise ValueError(f"{path}: unsupported target")
        if config["enforcement"] not in ("active", "disabled", "evaluate"):
            raise ValueError(f"{path}: invalid enforcement")
        if not isinstance(config["conditions"], dict):
            raise ValueError(f"{path}: conditions must be an object")
        if not isinstance(config["bypass_actors"], list):
            raise ValueError(f"{path}: bypass_actors must be an array")
        if not isinstance(config["rules"], list) or not all(
            isinstance(rule, dict) and isinstance(rule.get("type"), str)
            for rule in config["rules"]
        ):
            raise ValueError(f"{path}: rules must be an array of typed objects")
        configs.append((ruleset_id, config))
    return configs


def api(endpoint, payload=None):
    command = ["gh", "api", "--hostname", "github.com", endpoint]
    if payload is not None:
        command += ["--method", "PUT", "--input", "-"]
    result = subprocess.run(
        command,
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return json.loads(result.stdout)


def remote_config(remote, repository, apply):
    if remote.get("source_type") != "Repository" or remote.get("source") != repository:
        raise ValueError("Refusing to manage a ruleset from a different source")
    missing = FIELDS - remote.keys()
    if missing == {"bypass_actors"} and not apply:
        print("Bypass actors are hidden by GitHub for this token; preview excludes them.")
    elif missing:
        raise ValueError(f"Incomplete ruleset response (permissions?): {sorted(missing)}")
    return {key: remote[key] for key in FIELDS if key in remote}


def sync(configs, repository, apply=False):
    changes = []
    # Read and plan every managed ruleset before issuing any writes.
    for ruleset_id, desired in configs:
        endpoint = f"repos/{repository}/rulesets/{ruleset_id}"
        current = remote_config(api(endpoint), repository, apply)
        visible_desired = {key: desired[key] for key in current}
        print(f"Ruleset {ruleset_id}: {desired['name']}")
        if canonical(current) == canonical(visible_desired):
            print("No visible changes." if "bypass_actors" not in current else "No changes.")
            continue
        print("".join(difflib.unified_diff(
            render(current), render(visible_desired), fromfile="GitHub", tofile="repository"
        )), end="")
        changes.append((endpoint, current, desired))
    if not apply:
        print(f"Dry run: {len(changes)} ruleset(s) would be updated. No writes performed.")
        return
    for endpoint, expected, desired in changes:
        # Avoid overwriting a concurrent manual edit between planning and writing.
        current = remote_config(api(endpoint), repository, True)
        if canonical(current) != canonical(expected):
            raise ValueError(f"{endpoint}: changed during planning; rerun to review the new diff")
        api(endpoint, desired)
        actual = remote_config(api(endpoint), repository, True)
        if canonical(actual) != canonical(desired):
            raise ValueError(f"{endpoint}: verification failed after update")
        print(f"Updated and verified {endpoint}.")
    print(f"Applied {len(changes)} ruleset update(s).")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="foyer-rs/foyer")
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Write and verify changes")
    mode.add_argument("--validate", action="store_true", help="Validate local files only")
    args = parser.parse_args()
    configs = load_configs(args.directory)
    if args.validate:
        print(f"Validated {len(configs)} ruleset configuration(s).")
    else:
        sync(configs, args.repo, args.apply)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
