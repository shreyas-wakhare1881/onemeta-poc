#!/usr/bin/env python3
import json, sys, math

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "output/2026-07-27_18-26-12_test 7/session_trace.json"
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    events = data.get('events', [])
    start_ms = data.get('session', {}).get('start_time_epoch_ms') or (events[0].get('timestamp_epoch_ms') if events else 0)

    backlog_list = []
    queue_list = []
    timeline = []  # (rel_sec, queue_before, correlation_id, ts)

    for e in events:
        ev = e.get('event')
        if ev in ('AUDIO_SCHEDULED', 'AUDIO_PLAYBACK_SCHEDULED'):
            md = e.get('metadata') or {}
            backlog = None
            for k, v in md.items():
                if 'backlog' in k.lower():
                    backlog = v
                    break
            if backlog is None:
                backlog = md.get('backlog_ms') or md.get('backlog')

            q = None
            for k, v in md.items():
                if 'queue' in k.lower() and 'before' in k.lower():
                    q = v
                    break
            if q is None:
                q = md.get('queue_depth_before') or md.get('queue_depth') or md.get('queue_before') or md.get('queue_depth_after')

            ts = e.get('timestamp_epoch_ms') or e.get('timestamp_monotonic_ns')

            if backlog is not None:
                try:
                    backlog_list.append(float(backlog))
                except Exception:
                    pass
            if q is not None and ts is not None:
                try:
                    qi = int(q)
                    rel_sec = (ts - start_ms) / 1000.0 if start_ms else 0.0
                    queue_list.append(qi)
                    timeline.append((rel_sec, qi, e.get('correlation_id'), ts))
                except Exception:
                    pass

    def avg(xs):
        return sum(xs) / len(xs) if xs else None

    out = {
        'count_backlog': len(backlog_list),
        'avg_backlog_ms': avg(backlog_list),
        'max_backlog_ms': max(backlog_list) if backlog_list else None,
        'count_queue': len(queue_list),
        'avg_queue_before': avg(queue_list),
        'max_queue_before': max(queue_list) if queue_list else None,
    }

    # bucket timeline per second (relative to session start)
    buckets = {}
    for rel_sec, qi, corr, ts in timeline:
        sec = int(math.floor(rel_sec))
        buckets.setdefault(sec, []).append(qi)
    buckets_list = sorted([(sec, sum(vals) / len(vals)) for sec, vals in buckets.items()], key=lambda x: x[0])
    out['timeline_per_second_avg_queue'] = buckets_list

    # top queue points
    top = sorted(timeline, key=lambda x: x[1], reverse=True)[:10]
    out['top_queue_points'] = [{'rel_sec': t[0], 'queue': t[1], 'correlation_id': t[2], 'timestamp_epoch_ms': t[3]} for t in top]

    print(json.dumps(out, indent=2))

if __name__ == '__main__':
    main()
