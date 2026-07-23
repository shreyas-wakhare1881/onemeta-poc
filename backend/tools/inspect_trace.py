#!/usr/bin/env python3
import json
import sys
from collections import defaultdict


def summarize(trace_path, corr_ids):
    with open(trace_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    events = data.get("events", [])
    per = defaultdict(list)
    for ev in events:
        cid = ev.get("correlation_id", "")
        if cid:
            per[cid].append(ev)

    out = {}
    for cid in corr_ids:
        evs = per.get(cid, [])
        types = [e.get("event") for e in evs]
        comps = [e.get("component") for e in evs]
        has_published = any(e.get("event") == "AUDIO_PUBLISHED" for e in evs)
        has_packet_received = any(e.get("event") == "AUDIO_PACKET_RECEIVED" for e in evs)
        has_text_received = any(
            e.get("event") in ("TRANSLATED_TEXT_RECEIVED", "TEXT_PUBLISHED", "TEXT_PACKET_RECEIVED") for e in evs
        )
        has_gemini_final = any(
            e.get("event") in ("GEMINI_WS_FRAME_RECEIVED", "TRANSLATED_AUDIO_RECEIVED", "TRANSLATED_TEXT_RECEIVED") for e in evs
        )
        last_event = evs[-1] if evs else None
        pipeline_errors = [
            (e.get("metadata") or {}).get("message") or (e.get("metadata") or {}).get("exception")
            for e in evs
            if e.get("event") == "PIPELINE_ERROR"
        ]

        out[cid] = {
            "event_count": len(evs),
            "event_types": list(dict.fromkeys(types)),
            "components": list(dict.fromkeys(comps)),
            "has_gemini_final": bool(has_gemini_final),
            "has_published": bool(has_published),
            "has_packet_received": bool(has_packet_received),
            "has_text": bool(has_text_received),
            "last_event": last_event.get("event") if last_event else None,
            "last_event_epoch_ms": last_event.get("timestamp_epoch_ms") if last_event else None,
            "pipeline_errors": [p for p in pipeline_errors if p],
        }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    trace = sys.argv[1] if len(sys.argv) > 1 else "output/2026-07-23_17-28-02_Test1/session_trace.json"
    corr_ids = sys.argv[2:] if len(sys.argv) > 2 else [
        "corr-c525afc4",
        "corr-0367d4e0",
        "corr-919641c1",
        "corr-ac796ae6",
        "corr-3442ce83",
        "corr-b8a8b9d1",
        "corr-3cc7b945",
        "corr-a6d1a939",
    ]
    summarize(trace, corr_ids)
