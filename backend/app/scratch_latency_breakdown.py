import json

def analyze():
    log_path = "../output/gemini_debug.log"
    all_events = []
    
    with open(log_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            if "onemeta-demo" in line:
                try:
                    data = json.loads(line.strip())
                    all_events.append((i, data))
                except Exception:
                    pass

    if not all_events:
        print("No onemeta-demo events found.")
        return

    # Find the index of the last config event
    last_config_idx = -1
    for idx, (i, ev) in enumerate(all_events):
        if ev.get("action") == "sdk_config":
            last_config_idx = idx

    if last_config_idx == -1:
        events = all_events[-2000:]
    else:
        events = all_events[last_config_idx:]

    # Find the start time (first send_packet in this session)
    start_time = None
    for i, ev in events:
        if ev.get("action") == "send_packet":
            start_time = ev.get("time")
            break

    if not start_time:
        start_time = events[0][1].get("time")

    print(f"Latest session starts at line {events[0][0]}. start_time={start_time}")

    timeline = []
    for i, ev in events:
        action = ev.get("action")
        t = ev.get("time")
        rel_time = t - start_time
        
        if action == "send_packet":
            corr = ev.get("corr", "")
            timeline.append((rel_time, "send", f"packet (corr={corr})"))
        elif "server_content_summary" in ev:
            summary = ev.get("server_content_summary", {})
            text = summary.get("output_transcription")
            if text:
                timeline.append((rel_time, "recv_text", f"'{text}'"))
            parts = summary.get("model_turn_parts", [])
            for part in parts:
                inline_len = part.get("inline_len")
                if inline_len:
                    timeline.append((rel_time, "recv_audio", f"{inline_len} bytes"))

    # Write full timeline to a file in output directory
    output_file = "../output/timeline_analysis.txt"
    with open(output_file, "w", encoding="utf-8") as out:
        out.write("--- FULL CHRONOLOGICAL TIMELINE OF LATEST SESSION ---\n")
        idx = 0
        while idx < len(timeline):
            rel_time, action, detail = timeline[idx]
            if action == "send":
                send_count = 0
                start_t = rel_time
                end_t = rel_time
                corr_ids = set()
                while idx < len(timeline) and timeline[idx][1] == "send":
                    end_t = timeline[idx][0]
                    send_count += 1
                    det = timeline[idx][2]
                    corr = det.split("=")[-1][:-1]
                    if corr:
                        corr_ids.add(corr)
                    idx += 1
                corr_str = f" corr={list(corr_ids)}" if corr_ids else " silent"
                out.write(f"[{start_t:.4f}s - {end_t:.4f}s] SENT: {send_count} packets ({send_count*20}ms of audio){corr_str}\n")
            else:
                out.write(f"[{rel_time:.4f}s] {action.upper()}: {detail}\n")
                idx += 1
                
    print(f"Full timeline analysis written to {output_file}")

if __name__ == "__main__":
    analyze()
