import json,sys,os
p=os.path.join('output','2026-07-27_18-26-12_test 7','session_trace.json')
if not os.path.exists(p):
    print('MISSING',p)
    sys.exit(2)
with open(p,'r',encoding='utf-8') as f:
    data=json.load(f)

events=data.get('events',[])
vals=[]
times=[]
for ev in events:
    meta=ev.get('metadata') or {}
    if 'audio_data_b64_len' in meta:
        try:
            b64len=int(meta.get('audio_data_b64_len'))
        except Exception:
            continue
        bytes_ = int(round(b64len * 3.0 / 4.0))
        samples = bytes_ // 2
        sample_rate = meta.get('sample_rate') or meta.get('sampleRate') or 24000
        try:
            sr = int(sample_rate)
        except:
            sr = 24000
        duration_ms = samples / sr * 1000.0
        ts = ev.get('timestamp_epoch_ms') or ev.get('timestamp') or ev.get('timestamp_epoch') or None
        if ts is None:
            ts = ev.get('timestamp_monotonic_ns')
            if ts:
                ts = ts/1e6
        vals.append({'b64len':b64len,'bytes':bytes_,'samples':samples,'duration_ms':duration_ms,'ts':ts})
        if ts:
            times.append(ts)

if not vals:
    print('NO_AUDIO_EVENTS_FOUND')
    sys.exit(0)

import statistics

durations=[v['duration_ms'] for v in vals]
min_d=min(durations)
max_d=max(durations)
avg_d=statistics.mean(durations)
med_d=statistics.median(durations)

intervals=[]
if len(times)>=2:
    st=[float(t) for t in times]
    for i in range(1,len(st)):
        intervals.append(st[i]-st[i-1])
if intervals:
    avg_int=statistics.mean(intervals)
    min_int=min(intervals)
    max_int=max(intervals)
else:
    avg_int=min_int=max_int=None

print('count_events',len(vals))
print(f'duration_ms -> avg {avg_d:.1f} median {med_d:.1f} min {min_d:.1f} max {max_d:.1f}')
if avg_int is not None:
    print(f'interval_ms -> avg {avg_int:.1f} min {min_int:.1f} max {max_int:.1f}')

print('\nsample events (first 10):')
for v in vals[:10]:
    print(v)
