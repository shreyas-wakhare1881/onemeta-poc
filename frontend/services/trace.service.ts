import { PipelineEvent, TraceEvent, PipelineTrace } from '../types/trace';

export class PipelineEventTracer {
  private events: TraceEvent[] = [];
  private seq = 0;
  private sessionId = '';
  private startTimeEpochMs = 0;
  private enabled = false;
  private traceLevel = 'basic';

  private consoleLogs: string[] = [];

  constructor() {
    if (typeof window !== 'undefined') {
      // Check environment variables
      const envTrace = process.env.NEXT_PUBLIC_ENABLE_PIPELINE_TRACE || (window as any).__ENABLE_PIPELINE_TRACE__;
      this.enabled = String(envTrace).toLowerCase() === 'true';
      this.traceLevel = (process.env.NEXT_PUBLIC_PIPELINE_TRACE_LEVEL || 'basic').toLowerCase();
      console.log(`[Tracer Service] Pipeline tracing enabled: ${this.enabled} (level: ${this.traceLevel})`);
      if (this.enabled) {
        this.captureConsole();
      }
    }
  }

  private captureConsole() {
    if (typeof window === 'undefined') return;
    const originalLog = console.log;
    const originalWarn = console.warn;
    const originalError = console.error;

    console.log = (...args: any[]) => {
      this.consoleLogs.push(`[info] ${args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ')}`);
      originalLog.apply(console, args);
    };
    console.warn = (...args: any[]) => {
      this.consoleLogs.push(`[warn] ${args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ')}`);
      originalWarn.apply(console, args);
    };
    console.error = (...args: any[]) => {
      this.consoleLogs.push(`[error] ${args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ')}`);
      originalError.apply(console, args);
    };
  }

  public isEnabled(): boolean {
    return this.enabled;
  }

  public getTraceLevel(): string {
    return this.traceLevel;
  }

  public startSession(sessionId: string) {
    if (!this.enabled) return;
    this.sessionId = sessionId;
    this.events = [];
    this.seq = 0;
    this.startTimeEpochMs = Date.now();
    this.logEvent(PipelineEvent.SESSION_STARTED, '');
    console.log(`[Tracer Service] Tracing session started: ${sessionId}`);
  }

  private determineComponent(event: PipelineEvent): string {
    if (event === PipelineEvent.SESSION_STARTED || event === PipelineEvent.SESSION_ENDED) {
      return 'session';
    }
    if (
      event === PipelineEvent.TEXT_PACKET_RECEIVED || 
      event === PipelineEvent.AUDIO_PACKET_RECEIVED || 
      event === PipelineEvent.REACT_RENDER_COMPLETED
    ) {
      return 'frontend';
    }
    if (
      event === PipelineEvent.PCM_DECODE_STARTED || 
      event === PipelineEvent.PCM_DECODE_COMPLETED || 
      event === PipelineEvent.AUDIO_SCHEDULED || 
      event === PipelineEvent.AUDIO_PLAYBACK_SCHEDULED
    ) {
      return 'pcm';
    }
    return 'unknown';
  }

  public logEvent(
    event: PipelineEvent,
    correlationId: string,
    metadata?: Record<string, any>,
    timestampEpochMsOverride?: number,
    timestampMonotonicNsOverride?: number
  ) {
    if (!this.enabled) return;
    this.seq++;

    const epochMs = timestampEpochMsOverride || Date.now();
    // performance.now() is monotonic. Convert milliseconds to nanoseconds.
    const monoNs = timestampMonotonicNsOverride !== undefined 
      ? timestampMonotonicNsOverride 
      : Math.round(performance.now() * 1_000_000);

    const component = this.determineComponent(event);

    this.events.push({
      seq: this.seq,
      event,
      component,
      // Stable event identity for cross-system tracing
      event_id: (typeof crypto !== 'undefined' && (crypto as any).randomUUID)
        ? `evt_${(crypto as any).randomUUID().replace(/-/g, '')}`
        : `evt_${Math.random().toString(36).slice(2, 10)}${Date.now().toString(36)}`,
      correlation_id: correlationId || '',
      timestamp_epoch_ms: epochMs,
      timestamp_monotonic_ns: monoNs,
      metadata: metadata || {}
    });

    if (this.traceLevel === 'verbose') {
      console.log(`[Tracer Service Log] ${event} (comp: ${component}) | corr=${correlationId} | seq=${this.seq}`);
    }
  }

  public endSession() {
    if (!this.enabled || this.events.length === 0) return;
    this.logEvent(PipelineEvent.SESSION_ENDED, '');
    console.log(`[Tracer Service] Session ended. Traces captured: ${this.events.length}`);
    this.uploadArtifacts();
  }

  private async uploadArtifacts() {
    try {
      const endEpochMs = this.events.length > 0 
        ? this.events[this.events.length - 1].timestamp_epoch_ms 
        : Date.now();

      const traceData: PipelineTrace = {
        trace_version: 1,
        session: {
          session_id: this.sessionId,
          start_time_epoch_ms: this.startTimeEpochMs,
          end_time_epoch_ms: endEpochMs
        },
        events: this.events
      };

      this.validateTrace(traceData);

      const base = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
      await fetch(`${base}/api/audio/session/upload_artifacts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          room_name: this.sessionId,
          frontend_trace: traceData,
          console_logs: this.consoleLogs
        })
      });
      console.log('[Tracer Service] Successfully uploaded session artifacts to backend.');
    } catch (e) {
      console.error('[Tracer Service] Failed to upload session artifacts:', e);
    }
  }

  private validateTrace(traceData: PipelineTrace): boolean {
    const errors: string[] = [];
    if (traceData.trace_version !== 1) {
      errors.push("Missing or invalid trace_version");
    }
    if (!traceData.session) {
      errors.push("session object is missing");
    } else {
      if (!traceData.session.session_id) {
        errors.push("session_id is missing");
      }
      if (!traceData.session.start_time_epoch_ms) {
        errors.push("start_time_epoch_ms is missing");
      }
      if (!traceData.session.end_time_epoch_ms) {
        errors.push("end_time_epoch_ms is missing");
      }
    }
    if (!Array.isArray(traceData.events)) {
      errors.push("events is missing or not an array");
    } else {
      let lastSeq = 0;
      const validEventNames = Object.values(PipelineEvent) as string[];
      traceData.events.forEach((ev, idx) => {
        if (!ev) {
          errors.push(`Event at index ${idx} is null/undefined`);
          return;
        }
        if (ev.seq === undefined) {
          errors.push(`Event at index ${idx} is missing seq`);
        } else {
          if (ev.seq <= lastSeq) {
            errors.push(`Event at index ${idx} seq is not strictly increasing (seq=${ev.seq}, last=${lastSeq})`);
          }
          lastSeq = ev.seq;
        }
        if (!ev.event) {
          errors.push(`Event at index ${idx} is missing event`);
        } else if (!validEventNames.includes(ev.event)) {
          errors.push(`Event at index ${idx} has invalid event name: ${ev.event}`);
        }
        if (ev.timestamp_epoch_ms === undefined) {
          errors.push(`Event at index ${idx} is missing timestamp_epoch_ms`);
        }
        if (ev.timestamp_monotonic_ns === undefined) {
          errors.push(`Event at index ${idx} is missing timestamp_monotonic_ns`);
        }
        if (!ev.event_id) {
          errors.push(`Event at index ${idx} is missing event_id`);
        }
        if (!ev.component) {
          errors.push(`Event at index ${idx} is missing component name`);
        }
        if (!ev.metadata || typeof ev.metadata !== 'object') {
          errors.push(`Event at index ${idx} is missing metadata object`);
        }
      });
    }

    if (errors.length > 0) {
      console.error(`[Tracer Service] Trace validation failed with ${errors.length} errors:\n`, errors.join("\n"));
      return false;
    }
    return true;
  }
}

export const tracer = new PipelineEventTracer();
