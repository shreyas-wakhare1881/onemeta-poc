import React from 'react';

export interface ChunkHistoryItem {
  chunk_id: string;
  sequence: number;
  duration: number;
  ttft: number;
  latency: number;
  status: 'Processing' | 'Translating' | 'Complete' | 'Failed';
  t_started: number;
}

interface ChunkHistoryListProps {
  chunkHistory: ChunkHistoryItem[];
}

export default function ChunkHistoryList({ chunkHistory }: ChunkHistoryListProps) {
  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl flex flex-col h-[280px]">
      <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
        <span className="w-1.5 h-3 bg-indigo-500 rounded" />
        <span>Chunk History</span>
      </h2>
      <div className="flex-1 overflow-y-auto space-y-2.5 font-mono text-[11px] leading-relaxed scrollbar-thin">
        {chunkHistory.length > 0 ? (
          [...chunkHistory].reverse().map((chunk) => (
            <div key={chunk.chunk_id} className="bg-slate-950/60 border border-slate-850 rounded-xl p-3 flex justify-between items-center animate-fadeIn">
              <div>
                <div className="font-bold text-slate-350">
                  Chunk #{chunk.sequence}
                </div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  ID: {chunk.chunk_id.slice(0, 8)}...
                </div>
              </div>
              <div className="text-right">
                <div className="flex space-x-3 text-[10px]">
                  <span>TTFT: <strong className="text-indigo-400">{chunk.ttft ? `${chunk.ttft}ms` : '--'}</strong></span>
                  <span>Total: <strong className="text-indigo-400">{chunk.latency ? `${chunk.latency}ms` : '--'}</strong></span>
                </div>
                <span className={`inline-block px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase mt-1 ${
                  chunk.status === 'Complete' ? 'bg-emerald-500/10 text-emerald-455 border border-emerald-500/20' :
                  chunk.status === 'Failed' ? 'bg-rose-500/10 text-rose-455 border border-rose-500/20' :
                  chunk.status === 'Translating' ? 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/20' :
                  'bg-slate-850 text-slate-500 border border-slate-800 animate-pulse'
                }`}>
                  {chunk.status}
                </span>
              </div>
            </div>
          ))
        ) : (
          <p className="text-slate-600 italic">No processed speech segments available.</p>
        )}
      </div>
    </div>
  );
}
