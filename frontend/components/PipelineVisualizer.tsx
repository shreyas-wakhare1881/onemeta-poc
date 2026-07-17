import React from 'react';

interface PipelineVisualizerProps {
  pipelineState: 'Idle' | 'Listening' | 'Speech Detected' | 'Chunk Processing' | 'Translating' | 'Playing Audio' | 'Completed' | 'Error';
  playbackState: 'idle' | 'playing';
  isConnected: boolean;
}

export default function PipelineVisualizer({ pipelineState, playbackState, isConnected }: PipelineVisualizerProps) {
  const isStateActive = (stages: string[]) => stages.includes(pipelineState);

  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl">
      <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-6 flex items-center space-x-2">
        <span className="w-1.5 h-3 bg-indigo-500 rounded" />
        <span>Pipeline Stage Visualizer</span>
      </h2>
      <div className="overflow-x-auto pb-2 scrollbar-none">
        <div className="flex items-center min-w-[700px] space-x-2 text-[10px] font-bold tracking-wider uppercase font-mono">
          
          {/* Mic */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            isConnected ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' : 'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Mic
          </div>
          <div className="text-slate-700">➔</div>

          {/* Frames */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            isConnected ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' : 'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Frames
          </div>
          <div className="text-slate-700">➔</div>

          {/* VAD */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Speech Detected' ? 'bg-amber-500/10 border-amber-500 text-amber-400 animate-pulse' :
            isConnected ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' : 'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            VAD
          </div>
          <div className="text-slate-700">➔</div>

          {/* Chunk */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Chunk Processing' ? 'bg-amber-500/10 border-amber-500 text-amber-400 animate-pulse' :
            !['Idle', 'Listening', 'Speech Detected'].includes(pipelineState) && isConnected ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Chunk
          </div>
          <div className="text-slate-700">➔</div>

          {/* AI Engine */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            isStateActive(['Chunk Processing', 'Translating']) ? 'bg-amber-500/10 border-amber-500 text-amber-400 animate-pulse' :
            !['Idle', 'Listening', 'Speech Detected'].includes(pipelineState) && isConnected ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            AI Engine
          </div>
          <div className="text-slate-700">➔</div>

          {/* Runtime */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Translating' ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400' :
            isStateActive(['Playing Audio', 'Completed']) ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Runtime
          </div>
          <div className="text-slate-700">➔</div>

          {/* Gemma */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Translating' ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400 animate-pulse' :
            isStateActive(['Playing Audio', 'Completed']) ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Gemma
          </div>
          <div className="text-slate-700">➔</div>

          {/* Translation */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Translating' ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400' :
            isStateActive(['Playing Audio', 'Completed']) ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Translation
          </div>
          <div className="text-slate-700">➔</div>

          {/* TTS */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            pipelineState === 'Playing Audio' ? 'bg-violet-500/10 border-violet-500 text-violet-400' :
            pipelineState === 'Completed' ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            TTS
          </div>
          <div className="text-slate-700">➔</div>

          {/* Playback */}
          <div className={`px-2 py-1.5 rounded-lg border transition ${
            playbackState === 'playing' ? 'bg-violet-500/10 border-violet-500 text-violet-400 animate-pulse' :
            'bg-slate-950 border-slate-850 text-slate-600'
          }`}>
            Playback
          </div>

        </div>
      </div>
    </div>
  );
}
