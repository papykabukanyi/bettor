from __future__ import annotations

import json

from flask import Flask, jsonify

from modal_app import common

app = common.make_app("bettor-api-serve")
web_app = Flask(__name__)


@web_app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@web_app.route("/status")
def status():
    common.reload_volume()
    return jsonify(common.build_api_status())


@web_app.route("/predictions/today")
def predictions_today():
    common.reload_volume()
    return jsonify(common.predictions_for_label("today"))


@web_app.route("/predictions/tomorrow")
def predictions_tomorrow():
    common.reload_volume()
    return jsonify(common.predictions_for_label("tomorrow"))


@web_app.route("/model/stats")
def model_stats():
    common.reload_volume()
    return jsonify(
        {
            "ok": True,
            "updated_at": common.load_model_stats().get("trained_at", ""),
            "current_model": common.load_model_stats(),
            "history": common.load_training_history(),
        }
    )


@web_app.route("/polymarket/submissions")
def polymarket_submissions():
    common.reload_volume()
    return jsonify(common.load_submissions_payload())


@web_app.route("/polymarket/positions")
def polymarket_positions():
    common.reload_volume()
    return jsonify(common.load_positions_payload())


@app.function(
    image=common.image,
    volumes={common.REMOTE_VOLUME_MOUNT: common.volume},
    secrets=common.modal_secrets(),
    timeout=60 * 10,
)
@common.modal.wsgi_app()
def flask_app():
    return web_app


@app.local_entrypoint()
def main():
    print(json.dumps(common.build_api_status(), indent=2))
