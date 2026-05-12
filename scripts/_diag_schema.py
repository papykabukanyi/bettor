"""DB schema + actual prediction data inspection."""
import os, sys
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")
from src.data.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        # Schema
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='predictions' ORDER BY ordinal_position")
        cols = cur.fetchall()
        print("PREDICTIONS COLUMNS:")
        for c in cols:
            print(f"  {c[0]}: {c[1]}")
        
        # Sample recent rows
        col_names = [c[0] for c in cols]
        cur.execute("SELECT * FROM predictions ORDER BY predicted_at DESC LIMIT 3")
        rows = cur.fetchall()
        print(f"\nSAMPLE ROWS ({len(rows)}):")
        for row in rows:
            d = dict(zip(col_names, row))
            for k, v in d.items():
                if v is not None and str(v).strip():
                    print(f"  {k}: {v}")
            print("---")

        # Also check value_bets / prop_history tables
        for tbl in ("value_bets", "prop_history"):
            try:
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}' ORDER BY ordinal_position")
                tcols = [r[0] for r in cur.fetchall()]
                print(f"\n{tbl.upper()} COLUMNS: {tcols}")
            except Exception as e:
                print(f"{tbl}: {e}")
