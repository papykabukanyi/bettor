import sys; sys.path.insert(0,'src')
import dashboard

tests = [
    {'stat_type':'strikeouts','sport':'mlb','era':3.5,'whip':1.2,'k9':9.1,'ip_per_start':5.8,'over_prob':0.63,'under_prob':0.37,'avg_per_game':5.9},
    {'stat_type':'home_runs','sport':'mlb','avg':0.285,'ops':0.901,'wrc_plus':138,'over_prob':0.59,'under_prob':0.41,'avg_per_game':0.14},
    {'stat_type':'rbi','sport':'mlb','avg':0.271,'ops':0.821,'wrc_plus':110,'over_prob':0.55,'under_prob':0.45,'avg_per_game':0.52},
    {'stat_type':'goals_scored','sport':'soccer','xg':0.42,'xa':0.18,'goals_pg':0.38,'assists_pg':0.16,'mp':24,'over_prob':0.71,'under_prob':0.29,'avg_per_game':0.38},
    {'stat_type':'cards','sport':'soccer','xg':0.08,'xa':0.05,'goals_pg':0.05,'assists_pg':0.04,'card_pg':0.28,'mp':19,'over_prob':0.64,'under_prob':0.36,'avg_per_game':0.28},
]
for t in tests:
    r = dashboard._build_prop_pick(t, '2026-04-21', '2026-04-22')
    print(f"{t['stat_type']:20} rate_label={r['rate_label']:12} ip_per_start={r['ip_per_start']} avg_pg={r['avg_per_game']}")
print('ALL OK')
