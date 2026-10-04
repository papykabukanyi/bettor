

def test_exchange_bracket_calls_match_kalshis_api(monkeypatch):
    from data import kalshi_perps
    calls = []

    def fake(method, path, *, params=None, payload=None, auth=False):
        calls.append((method, path, payload))
        if method == "DELETE":
            raise ValueError("204 No Content")
        return {"id": "t1", "status": "active", "exit_triggers": [{"id": "t1"}], "fills": [{"fill_id": "f1"}]}

    monkeypatch.setattr(kalshi_perps, "_request_json", fake)
    assert kalshi_perps.set_cross_exit_bracket("KXBTCPERP", stop_loss_price=6.5346, take_profit_price=7.26)["id"] == "t1"
    assert calls[-1] == ("PUT", "/margin/cross/positions/KXBTCPERP/exit_trigger",
                         {"kind": "bracket", "stop_loss_price": "6.5346", "take_profit_price": "7.2600"})
    kalshi_perps.cancel_cross_exit_triggers("KXBTCPERP")  # 204: no body, no error
    assert calls[-1][:2] == ("DELETE", "/margin/cross/positions/KXBTCPERP/exit_trigger")
    assert kalshi_perps.get_cross_exit_triggers("KXBTCPERP") == [{"id": "t1"}]
    assert kalshi_perps.get_margin_fills(min_ts=1) == [{"fill_id": "f1"}]
