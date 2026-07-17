import React from 'react';

export interface TimelineEvent {
  label: string;
  time_ms: number;
}

interface TimelineVisualizerProps {
  lastTimelineEvents: TimelineEvent[];
}

export default function TimelineVisualizer({ lastTimelineEvents }: TimelineVisualizerProps) {
  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl flex flex-col h-[280px]">
      <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
        <span className="w-1.5 h-3 bg-indigo-500 rounded" />
        <span>Translation Latency Timeline</span>
      </h2>
      <div className="flex-1 flex flex-col justify-center space-y-3 pl-3 font-mono text-[11px]">
        {lastTimelineEvents.length > 0 ? (
          lastTimelineEvents.map((t, idx) => (
            <div key={idx} className="flex items-center space-x-3 animate-fadeIn">
              <div className="w-2 h-2 rounded-full bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.4)]" />
              <div className="w-14 font-bold text-indigo-400 text-right">{t.time_ms} ms</div>
              <div className="text-slate-350">{t.label}</div>
            </div>
          ))
        ) : (
          <p className="text-slate-600 italic pl-1">Awaiting timeline statistics from next processed chunk...</p>
        )}
      </div>
    </div>
  );
}
