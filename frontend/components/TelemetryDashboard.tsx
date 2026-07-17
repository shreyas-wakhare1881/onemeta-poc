import React from 'react';
import type { TelemetryUpdatePayload } from '../types/livekit';

interface TelemetryDashboardProps {
  telemetryData: TelemetryUpdatePayload | null;
}

export default function TelemetryDashboard({ telemetryData }: TelemetryDashboardProps) {
  const audio = telemetryData?.audio || ({} as any);
  const ai = telemetryData?.ai || ({} as any);

  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl">
      <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
        <span className="w-1.5 h-3 bg-indigo-500 rounded" />
        <span>Developer Metrics Dashboard</span>
      </h2>
      
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Frames Received</p>
          <p className="text-lg font-bold text-white font-mono mt-1">{audio.frames_received || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Speech Frames</p>
          <p className="text-lg font-bold text-white font-mono mt-1">{audio.frames_processed || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Inference Queue</p>
          <p className="text-lg font-bold text-white font-mono mt-1">{ai.inference_queue_depth || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Gemma Speed</p>
          <p className="text-lg font-bold text-indigo-400 font-mono mt-1">
            {Math.round(ai.tokens_per_second || 0)} t/s
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center mt-4">
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Chunks Completed</p>
          <p className="text-lg font-bold text-emerald-400 font-mono mt-1">{ai.successful_requests || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Failed Chunks</p>
          <p className="text-lg font-bold text-rose-400 font-mono mt-1">{ai.failed_requests || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Dropped Chunks</p>
          <p className="text-lg font-bold text-amber-500 font-mono mt-1">{ai.dropped_chunks || 0}</p>
        </div>
        <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
          <p className="text-[9px] font-bold text-slate-450 uppercase tracking-wider">Dropped Frames</p>
          <p className="text-lg font-bold text-rose-455 font-mono mt-1">{audio.dropped_frames || 0}</p>
        </div>
      </div>
    </div>
  );
}
