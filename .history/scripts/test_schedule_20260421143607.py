import sys
sys.path.insert(0, 'src')
from data.mlb_fetcher import get_schedule_today
games = get_schedule_today()
print(f"{len(games)} games today")
for g in games[:3]:
    print(f"  {g['away_team']} @ {g['home_team']}  |  {g['away_starter']} vs {g['home_starter']}  [{g['status']}]")
