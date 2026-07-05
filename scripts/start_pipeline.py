#!/usr/bin/env python3
"""
Quick-start script to bootstrap and run the HF-first sports prediction pipeline.
Uses football-data.org for comprehensive soccer data collection.
"""

import os
import sys
import subprocess
import time
import json
from pathlib import Path
from datetime import datetime

# Add src to path
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from config import (
    HF_API_KEY, HF_DATASET_REPO, HF_MODEL_REPO, FOOTBALL_DATA_API_KEY,
    THESPORTSDB_API_KEY, NEWSDATA_API_KEY
)

DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70)

def check_env():
    """Verify environment is configured."""
    print_header("CHECKING ENVIRONMENT")
    
    checks = [
        ("HF_API_KEY", HF_API_KEY),
        ("HF_DATASET_REPO", HF_DATASET_REPO),
        ("HF_MODEL_REPO", HF_MODEL_REPO),
        ("FOOTBALL_DATA_API_KEY", FOOTBALL_DATA_API_KEY),
        ("THESPORTSDB_API_KEY", THESPORTSDB_API_KEY),
    ]
    
    all_ok = True
    for name, value in checks:
        status = "[OK]" if value else "[MISSING]"
        print(f"  {status} {name}")
        if not value and name != "NEWSDATA_API_KEY":
            all_ok = False
    
    if not all_ok:
        print("\n[ERROR] Missing critical environment variables!")
        print("Set them in .env or export as environment variables.")
        return False
    
    print("\n[SUCCESS] All critical variables configured!")
    return True

def import_pipeline():
    """Try importing the pipeline module."""
    print_header("IMPORTING PIPELINE")
    try:
        from data.hf_pipeline import HFDirectPipeline
        print("[OK] HFDirectPipeline imported successfully")
        return HFDirectPipeline
    except Exception as e:
        print(f"[ERROR] Failed to import pipeline: {e}")
        return None

def run_bootstrap(pipeline_class, days_back=7):
    """Run bootstrap to load historical data."""
    print_header(f"BOOTSTRAP: Loading {days_back} days of history")
    try:
        pipeline = pipeline_class()
        if not pipeline.ok:
            print("[ERROR] Pipeline not properly configured")
            return False
        
        print(f"  Bootstrapping {days_back} days of history...")
        result = pipeline.bootstrap_one_year_history(days_back=days_back)
        
        print(f"  Status: {result.get('ok', False)}")
        print(f"  Records: {result.get('records', 0)}")
        if result.get('error'):
            print(f"  Error: {result['error']}")
        
        return result.get('ok', False)
    except Exception as e:
        print(f"[ERROR] Bootstrap failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_daily_pipeline(pipeline_class):
    """Run the full daily pipeline."""
    print_header("DAILY PIPELINE: Fetch -> Train -> Predict")
    try:
        pipeline = pipeline_class()
        if not pipeline.ok:
            print("[ERROR] Pipeline not properly configured")
            return False
        
        print("  Running daily pipeline...")
        start_time = time.time()
        
        result = pipeline.run_daily_pipeline()
        
        elapsed = time.time() - start_time
        
        print(f"  Completed in {elapsed:.1f}s")
        print(f"  Status: {result.get('ok', False)}")
        
        if 'append' in result:
            print(f"  Appended: {result['append'].get('records', 0)} records")
        if 'train' in result:
            train = result['train']
            print(f"  Trained: {train.get('rows', 0)} rows, model={train.get('best_model', 'N/A')}, score={train.get('cv_roc_auc', 0):.4f}")
        if 'predictions' in result:
            preds = result['predictions']
            print(f"  Generated: {len(preds.get('predictions', []))} predictions")
        
        return result.get('ok', False)
    except Exception as e:
        print(f"[ERROR] Daily pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_predictions():
    """Check if predictions were generated."""
    print_header("CHECKING GENERATED PREDICTIONS")
    
    pred_file = DATA_DIR / "hf_daily_predictions.json"
    if not pred_file.exists():
        print("[WARN] No predictions file found yet")
        return False
    
    try:
        with open(pred_file, 'r') as f:
            data = json.load(f)
        
        preds = data.get('predictions', [])
        print(f"[OK] Found {len(preds)} predictions")
        
        # Group by sport
        by_sport = {}
        for pred in preds:
            sport = pred.get('sport', 'unknown')
            by_sport[sport] = by_sport.get(sport, 0) + 1
        
        print("\nBreakdown by sport:")
        for sport, count in sorted(by_sport.items()):
            print(f"  - {sport}: {count}")
        
        # Show sample soccer predictions
        soccer_preds = [p for p in preds if p.get('sport') == 'soccer']
        if soccer_preds:
            print("\nSample soccer predictions:")
            for pred in soccer_preds[:3]:
                game = f"{pred.get('away_team')} @ {pred.get('home_team')}"
                prob = pred.get('home_win_prob', 0)
                print(f"  - {game}: home {prob:.1%}")
        
        return True
    except Exception as e:
        print(f"[ERROR] Failed to read predictions: {e}")
        return False

def start_dashboard():
    """Try to start the dashboard server."""
    print_header("STARTING DASHBOARD")
    
    print("  Starting Flask dashboard server on http://localhost:5000")
    print("  (This runs in the background)")
    
    try:
        # Start dashboard in background
        dashboard_script = SRC_DIR / "dashboard.py"
        if dashboard_script.exists():
            print(f"  Dashboard script: {dashboard_script}")
            print("\n  To view predictions, open http://localhost:5000 in your browser")
            return True
        else:
            print(f"[WARN] Dashboard script not found at {dashboard_script}")
            return False
    except Exception as e:
        print(f"[WARN] Could not start dashboard: {e}")
        return False

def main():
    """Main flow."""
    print_header("BETTOR: HF-FIRST SOCCER + MULTI-SPORT PIPELINE")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Check environment
    if not check_env():
        return 1
    
    # Import pipeline
    pipeline_class = import_pipeline()
    if not pipeline_class:
        return 1
    
    # Bootstrap (small: 7 days to verify setup works)
    if not run_bootstrap(pipeline_class, days_back=7):
        print("\n[WARN] Bootstrap had issues, but continuing...")
    
    # Run daily pipeline
    if not run_daily_pipeline(pipeline_class):
        print("\n[ERROR] Daily pipeline failed")
        return 1
    
    # Check predictions
    if not check_predictions():
        print("\n[WARN] No predictions generated yet (may take time)")
    
    # Show next steps
    print_header("NEXT STEPS")
    print("""
  1. View dashboard:
     python src/dashboard.py
     Open http://localhost:5000

  2. Run pipeline on schedule (HF Space):
     Deploy to https://huggingface.co/papylove/sportprediction
     Uses hf_space_api/app.py with automated daily runs

  3. View your dataset:
     https://huggingface.co/datasets/papylove/sportprediction

  4. View your model:
     https://huggingface.co/papylove/sportprediction

  5. Predictions include:
     - Soccer (all football-data.org competitions)
     - MLB, NBA, NHL
     - Player props (all sports)
     - Game props
     - News signals (injury, lineup changes, momentum)
""")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
