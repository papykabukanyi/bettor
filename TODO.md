# TODO - Fix duplicate pipeline runs and redundant HF API checks

- [ ] Add in-process idempotency/lock guard in `HFDirectPipeline.append_daily_results` to prevent duplicate same-day append execution.
- [ ] Reduce redundant Hugging Face `whoami` calls by caching resolved identity/model repo owner in `HFDirectPipeline`.
- [ ] Ensure active-cycle duplicate calls are minimized with explicit skip logging and safe behavior.
- [ ] Run focused verification (critical-path):
  - [ ] Validate Python syntax for edited files.
  - [ ] Smoke-test scheduler/job flow by checking relevant code paths.
- [ ] Summarize fixes and observed impact.
