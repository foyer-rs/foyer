# Repository rulesets

JSON files in this directory are the desired configuration for existing GitHub
repository rulesets. Each file contains the writable REST API fields plus an
`id` identifying the existing ruleset. The synchronizer updates that ID in place,
so renaming a ruleset does not create a duplicate. It never creates or deletes
rulesets, and deleting a file only stops managing that ruleset.

`default.json` was imported from `foyer-rs/foyer` ruleset `2665193`, including
the nine Rust 1.95.0 required checks that replaced Rust 1.91.0 during the MSRV
migration. The initial configuration preserves the live ruleset, including check
integration IDs and bypass actors. The first synchronization should be a no-op.
Future changes to required checks can be reviewed and applied through this pipeline.

## Authentication

Create a fine-grained personal access token with resource owner `foyer-rs`,
repository access limited to `foyer`, and repository **Administration: Read and
write**. Create an environment named `rulesets` in repository Settings, select
**Selected branches and tags**, and add a **branch** rule for `main` only (no tag
rule). Store the token as the environment secret `RULESETS_TOKEN`, not a repository
secret. If a repository secret with that name already exists, remove it after
moving the credential to the environment. An
organization may require approval before the token can access the repository.
Choose an expiration date and replace the secret before it expires.

The normal `GITHUB_TOKEN` cannot write rulesets. Pull requests use it only for
read-only previews. The `rulesets` environment branch policy prevents pull
request and non-main jobs from receiving `RULESETS_TOKEN`. GitHub may hide bypass
actors from read-only responses; the preview reports that limitation. Apply
requires the complete response, including bypass actors.

## Workflow

- Pull requests changing these files, the synchronizer, its tests, or the workflow
  validate the configuration, run tests, and show the live diff in the job log.
- Pushes to `main` affecting those paths automatically apply changes.
- The **Rulesets** workflow also supports manual dispatch on `main`. Leave
  `apply` false for a full preview, or set it true to reconcile drift.
- Sync jobs are serialized and always check out the latest `main`, including
  on re-runs. Writes are restricted to `foyer-rs/foyer` on `main`.
- Unchanged rulesets are skipped. Updates are followed by a read-back check.
  A detected edit between planning and writing aborts the update. GitHub does
  not provide a transaction across rulesets; earlier updates can remain applied
  if a later update fails. Fix the failure and rerun to converge.

Do not make this path-filtered workflow a required status check: it does not run
on unrelated pull requests. Review changes to the ruleset and privileged sync
code before merging. The environment restriction is configured in GitHub, outside
this workflow, so a branch cannot remove it by editing YAML. Anyone who can merge
code into `main` remains trusted to use the credential. For a separate trust
boundary, run reconciliation from an independently administered repository.

## Local use

Python 3 and an authenticated `gh` CLI are sufficient; no Python dependencies
are needed. Run from the repository root:

```sh
python3 .github/scripts/sync_rulesets.py --validate
python3 -m unittest discover -s .github/scripts -p 'test_rulesets.py'
python3 .github/scripts/sync_rulesets.py
python3 .github/scripts/sync_rulesets.py --apply
```

The default is a read-only preview. `--apply` uses the local `gh` authentication
(or `GH_TOKEN`) and requires repository administration permission. This can
bootstrap a required-check migration if stale checks block the initial merge.
Inspect the preview first. Once the workflow is merged, prefer it for updates.

To adopt another existing repository ruleset, export its ID and writable fields
into a new JSON file:

```sh
gh api repos/foyer-rs/foyer/rulesets/RULESET_ID \
  --jq '{id, name, target, enforcement, conditions, bypass_actors, rules}' \
  > .github/rulesets/another.json
```

Use administrative credentials for export so bypass actors are included. Local
validation checks the document shape; GitHub validates individual rule parameters
when applying. Unknown top-level fields are rejected rather than silently dropped.
To roll back a configuration change, revert its commit and merge the revert;
manual changes in GitHub are overwritten by the next reconciliation.

See [GitHub's ruleset API](https://docs.github.com/en/rest/repos/rules) and
[fine-grained token setup](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).
