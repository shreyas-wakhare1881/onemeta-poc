import json
import sys
from statistics import mean

TRACE_PATH = sys.argv[1] if len(sys.argv) > 1 else "output/2026-07-27_18-26-12_test 7/session_trace.json"

with open(TRACE_PATH, 'r', encoding='utf-8') as f:
    trace = json.load(f)

session_start = trace.get('session', {}).get('start_time_epoch_ms', None)
events = trace.get('events', [])

sched_events = []
for ev in events:
    if ev.get('event') in ('AUDIO_PLAYBACK_SCHEDULED', 'AUDIO_SCHEDULED'):
        md = ev.get('metadata') or {}
        backlog = md.get('backlog_ms')
        qdb = md.get('queue_depth_before')
        if backlog is None and 'backlog_ms' not in md:
            # sometimes backlog recorded as "backlog_ms" or may be missing
            pass
        # only include events with numeric backlog and qdb
        if isinstance(backlog, (int, float)) and isinstance(qdb, (int, float)):
            t_epoch = ev.get('timestamp_epoch_ms')
            rel_sec = None
            if session_start is not None and t_epoch is not None:
                rel_sec = (t_epoch - session_start) / 1000.0
            sched_events.append((ev.get('event'), t_epoch, rel_sec, backlog, qdb, ev.get('correlation_id'), ev.get('event_id')))

if not sched_events:
    print('No scheduling events with backlog_ms and queue_depth_before found in trace.')
    sys.exit(0)

# Use AUDIO_PLAYBACK_SCHEDULED preferentially
playback_sched = [s for s in sched_events if s[0] == 'AUDIO_PLAYBACK_SCHEDULED']
use_events = playback_sched if playback_sched else sched_events

backlogs = [s[3] for s in use_events]
qdbs = [s[4] for s in use_events]

avg_backlog = mean(backlogs)
max_backlog = max(backlogs)
max_backlog_ev = max(use_events, key=lambda s: s[3])
avg_qdb = mean(qdbs)
max_qdb = max(qdbs)
max_qdb_ev = max(use_events, key=lambda s: s[4])

# Build 1s bins across session time (rel_sec). If rel_sec missing, use index-based bins
rel_times = [s[2] for s in use_events]
min_t = min([t for t in rel_times if t is not None]) if any(t is not None for t in rel_times) else 0
max_t = max([t for t in rel_times if t is not None]) if any(t is not None for t in rel_times) else len(use_events)

bin_size = 1.0
bins = {}
for ev in use_events:
    rel = ev[2]
    q = ev[4]
    if rel is None:
        # fallback: use sequential index as time
        idx = use_events.index(ev)
        b = idx
    else:
        b = int((rel - min_t) // bin_size)
    bins.setdefault(b, []).append(q)

# produce sorted timeline of bins with avg queue depth
timeline = []
for b in sorted(bins.keys()):
    start = min_t + b * bin_size
    timeline.append({'start_sec': round(start, 2), 'avg_queue_depth': round(mean(bins[b]), 3), 'count': len(bins[b])})

output = {
    'num_sched_events': len(use_events),
    'avg_backlog_ms': round(avg_backlog, 3),
    'max_backlog_ms': round(max_backlog, 3),
    'max_backlog_event': {
        'event_id': max_backlog_ev[6],
        'correlation_id': max_backlog_ev[5],
        'timestamp_epoch_ms': max_backlog_ev[1]
    },
    'avg_queue_depth_before': round(avg_qdb, 3),
    'max_queue_depth_before': int(max_qdb),
    'max_queue_depth_event': {
        'event_id': max_qdb_ev[6],
        'correlation_id': max_qdb_ev[5],
        'timestamp_epoch_ms': max_qdb_ev[1]
    },
    'timeline_bins': timeline[:120]
}

print(json.dumps(output, indent=2))
