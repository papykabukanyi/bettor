# Runs all 4 trading services (perps, stocks, crypto, options) as ONE
# process, for deployment as a Hugging Face Docker Space instead of 4
# separate Render services -- see combined_app.py's own docstring for the
# full architecture (werkzeug DispatcherMiddleware composing the 4
# existing, otherwise-unmodified Flask apps) and
# docs/RENDER_TO_HF_MIGRATION.md for the secrets checklist and staged
# cutover runbook.
#
# Mirrors hf_space_api/Dockerfile's own already-proven-working pattern
# (same base image, same non-root user convention) -- that Space is an
# unrelated project, but its Dockerfile is this repo's own precedent for
# "this shape of Docker Space actually works on HF."
FROM python:3.11-slim

# REAL, LIVE, CONFIRMED INCIDENT: this Space's own "Dev Mode" setting
# (HF's browser-VS-Code feature, toggled in Space Settings, independent
# of anything in this Dockerfile) wraps whatever image this Dockerfile
# produces in its OWN extra build stage that runs `git config --global
# user.name/user.email` -- and python:3.11-slim has no git installed,
# which took the ENTIRE merged Space (all 4 markets, not just one) down
# with BUILD_ERROR ("git: not found", exit code 127) on a plain code
# push that touched no infrastructure at all. Installing git here (in
# THIS image, which that wrapper stage builds FROM) is what actually
# fixes it -- toggling Dev Mode off in Space Settings is the other real
# fix and avoids the wrapper stage entirely, but this is the one fix
# that's actually version-controlled and can't silently regress if Dev
# Mode gets re-enabled later.
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# HF Docker Spaces' own conventional default app_port -- must match
# whatever this Space's README.md frontmatter sets app_port to.
ENV PORT=7860

# Set up a non-root user (HF Docker Spaces run containers as UID 1000 by
# convention -- see hf_space_api/Dockerfile's identical comment) before
# any COPY, so file ownership is correct from the start rather than
# needing a separate chown pass.
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH
WORKDIR $HOME/app

COPY --chown=user requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# The whole repo, not just src/ -- combined_app.py itself lives at the
# repo root alongside app_kalshi.py, and imports src/ on its own via
# sys.path (see that file), not via a working-directory assumption.
COPY --chown=user . .

# Local disk here is exactly as ephemeral as Render's own -- every path
# this app touches already self-creates via mkdir(parents=True,
# exist_ok=True) (server_common.py and every *_model.py/*_strategy.py),
# and everything durable already lives on Hugging Face. This directory
# only needs to exist for the FIRST write; nothing here needs to survive
# a container restart.
RUN mkdir -p data

EXPOSE 7860

# Still --workers 1 (exactly one Python process, so each market's own
# in-process APScheduler instance is created exactly once -- see
# combined_app.py's own docstring for why more workers would duplicate
# live order placement). --threads 8 (up from each individual service's
# own --threads 1 on Render) so a slow request in any one market can't
# block every market's web/API responsiveness at once now that they
# share a single process -- APScheduler's own background job execution
# runs on its own separate executor threads regardless, so this has no
# effect on trading-loop timing.
CMD ["gunicorn", "combined_app:application", "--bind", "0.0.0.0:7860", "--workers", "1", "--threads", "8", "--timeout", "300"]
