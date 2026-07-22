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

    # Find the index of the last config event (where session starts)
    last_config_idx = -1
    for idx, (i, ev) in enumerate(all_events):
        if ev.get("action") == "sdk_config":
            last_config_idx = idx

    if last_config_idx == -1:
        # Fallback: take the last 1000 events
        events = all_events[-1000:]
        print("Warning: No sdk_config event found, taking last 1000 events.")
    else:
        events = all_events[last_config_idx:]
        print(f"Analyzing the latest session starting at event index {last_config_idx} (line {events[0][0]}).")

    start_time = None
    # Find the first send_packet in this session
    for i, ev in events:
        if ev.get("action") == "send_packet":
            start_time = ev.get("time")
            break

    if not start_time:
        start_time = events[0][1].get("time")

    print(f"Session start wall time: {start_time}")

    transcripts = []
    audio_responses = []
    packet_count = 0
    first_speech_packet_time = None
    last_send_time = None
    
    # We want to check the timing/spacing of packets
    packet_gaps = []

    for idx, (i, ev) in enumerate(events):
        action = ev.get("action")
        t = ev.get("time")
        rel_time = t - start_time if start_time else 0.0
        
        if action == "send_packet":
            packet_count += 1
            if last_send_time is not None:
                gap = (t - last_send_time) * 1000.0
                packet_gaps.append(gap)
            last_send_time = t
            
            corr = ev.get("corr", "")
            if corr and first_speech_packet_time is None:
                first_speech_packet_time = rel_time
                
        elif "server_content_summary" in ev:
            summary = ev.get("server_content_summary", {})
            text = summary.get("output_transcription")
            if text:
                transcripts.append((rel_time, text))
            parts = summary.get("model_turn_parts", [])
            for part in parts:
                inline_len = part.get("inline_len")
                if inline_len:
                    audio_responses.append((rel_time, inline_len))

    print(f"Total input packets sent in this session: {packet_count} ({packet_count * 20 / 1000.0:.2f} seconds of audio)")
    if first_speech_packet_time is not None:
        print(f"First speech packet sent at relative time: {first_speech_packet_time:.2f} s")
    else:
        print("No speech packets (with corr id) detected in this session.")

    # Analyze gaps
    if packet_gaps:
        avg_gap = sum(packet_gaps) / len(packet_gaps)
        max_gap = max(packet_gaps)
        min_gap = min(packet_gaps)
        print(f"Packet gaps: Average: {avg_gap:.2f}ms | Min: {min_gap:.2f}ms | Max: {max_gap:.2f}ms")
        
        # Let's count how many gaps are > 50ms (indicating irregular/buffered release)
        large_gaps = [g for g in packet_gaps if g > 50.0]
        print(f"Number of gaps > 50ms: {len(large_gaps)} (out of {len(packet_gaps)})")

    print("\n--- TRANSCRIPTS RECEIVED ---")
    for t_rel, txt in transcripts:
        print(f"[{t_rel:.2f}s] Text: '{txt}'")

    print("\n--- AUDIO CHUNKS RECEIVED ---")
    print(f"Total audio chunks received: {len(audio_responses)}")
    for t_rel, size in audio_responses[:30]:
        print(f"[{t_rel:.2f}s] Audio size: {size} bytes")

if __name__ == "__main__":
    analyze()
