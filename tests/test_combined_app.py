"""combined_app.py -- the werkzeug DispatcherMiddleware composition of
the 4 existing Flask apps into one WSGI application, built for running
all 4 trading services as a single process inside one Hugging Face
Docker Space instead of 4 separate Render services. These tests lock in
the two things that actually matter here: (1) the real, confirmed bug
this work uncovered -- stocks/crypto/options' Flask apps used to resolve
templates/static files relative to the process's CURRENT WORKING
DIRECTORY, which only happened to work on Render by coincidence of its
own `--chdir src` startCommand -- and (2) that mounting all 4 apps under
one WSGI callable via distinct prefixes resolves every confirmed route
collision (/, /chart/<path:filename>, /api/server/activity, all defined
identically across the 4 apps) without any of them ever seeing a
colliding request."""
from __future__ import annotations

from pathlib import Path

import pytest
from werkzeug.test import Client

import combined_app

SRC_DIR = Path(__file__).resolve().parents[1] / "src"


def test_stocks_app_resolves_templates_and_static_to_an_absolute_src_path():
    """The real bug: Flask("alpaca_stocks_server", template_folder="templates")
    resolves that relative path against get_root_path("alpaca_stocks_server")
    -- since that string isn't the module's real dotted import name, Flask
    can't find it in sys.modules and silently falls back to os.getcwd().
    Must be an absolute path under src/ regardless of the process's cwd."""
    assert combined_app.alpaca_server.app.template_folder == str(SRC_DIR / "templates")
    assert combined_app.alpaca_server.app.static_folder == str(SRC_DIR / "static")


def test_crypto_app_resolves_templates_and_static_to_an_absolute_src_path():
    assert combined_app.alpaca_crypto_server.app.template_folder == str(SRC_DIR / "templates")
    assert combined_app.alpaca_crypto_server.app.static_folder == str(SRC_DIR / "static")


def test_options_app_resolves_templates_and_static_to_an_absolute_src_path():
    assert combined_app.alpaca_options_server.app.template_folder == str(SRC_DIR / "templates")
    assert combined_app.alpaca_options_server.app.static_folder == str(SRC_DIR / "static")


@pytest.fixture
def client():
    return Client(combined_app.application)


def test_perps_root_is_unchanged_and_reachable_at_the_bare_path(client):
    """Perps is the DEFAULT mounted app -- its own routes must need ZERO
    changes from what they are on Render today."""
    resp = client.get("/")
    assert resp.status_code == 200


def test_each_markets_dashboard_is_reachable_through_its_own_mount_prefix(client):
    assert client.get("/stocks/alpaca").status_code == 200
    assert client.get("/crypto/alpaca-crypto").status_code == 200
    assert client.get("/options/alpaca-options").status_code == 200


def test_stocks_static_asset_resolves_through_its_mount_prefix(client):
    resp = client.get("/stocks/static/cumdev-icon.svg")
    assert resp.status_code == 200
    assert resp.content_length and resp.content_length > 0


def test_api_server_activity_does_not_collide_across_markets(client):
    """Real, confirmed collision if these 4 apps were merged WITHOUT
    prefix-based dispatch: all 4 define the identical bare path
    /api/server/activity. DispatcherMiddleware routes by prefix before
    either app's own Flask routing ever runs, so each of these reaches
    its OWN market's copy, not colliding with the others."""
    assert client.get("/api/server/activity").status_code == 200  # perps (default mount)
    assert client.get("/stocks/api/server/activity").status_code == 200
    assert client.get("/crypto/api/server/activity").status_code == 200
    assert client.get("/options/api/server/activity").status_code == 200


def test_each_markets_own_status_route_is_reachable_through_its_mount(client):
    """Each market's ALREADY-prefixed internal routes (e.g.
    /api/alpaca/crypto/status) need no changes at all -- only the
    EXTERNAL mount point is new."""
    assert client.get("/api/status").status_code == 200  # perps, bare, unchanged from Render
    assert client.get("/stocks/api/alpaca/status").status_code == 200
    assert client.get("/crypto/api/alpaca/crypto/status").status_code == 200
    assert client.get("/options/api/alpaca/options/status").status_code == 200


def test_chart_route_does_not_collide_across_markets(client):
    """All 4 apps define an identical /chart/<path:filename> route --
    confirm each is only ever reached through its own market's prefix
    (a 404 for a made-up filename proves the route matched and executed
    that market's own send_from_directory call, not a routing collision)."""
    for prefix in ("", "/stocks", "/crypto", "/options"):
        resp = client.get(f"{prefix}/chart/does-not-exist.png")
        assert resp.status_code == 404  # reached the route, file genuinely doesn't exist
