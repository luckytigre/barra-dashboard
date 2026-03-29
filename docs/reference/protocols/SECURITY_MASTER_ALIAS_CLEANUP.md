# Security Master Alias Cleanup

This protocol is retired.

The physical `security_master` alias-cleanup entrypoint is no longer a supported
runtime or operator workflow:

```bash
python3 -m backend.scripts.cleanup_security_master_second_pass_aliases
```

The current script exists only as a retirement stub and exits with an error.

Historical procedure details are archived at:
- [SECURITY_MASTER_ALIAS_CLEANUP.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/archive/one-time-protocols/SECURITY_MASTER_ALIAS_CLEANUP.md)

Current rule:
- do not mutate physical `security_master` as an active maintenance workflow
- use registry/policy/taxonomy/source-observation maintenance plus `security_master_compat_current`
  for supported runtime behavior
