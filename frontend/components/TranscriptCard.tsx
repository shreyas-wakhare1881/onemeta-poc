import React from 'react';

interface TranscriptCardProps {
  englishTranscript: string;
  englishScrollRef: React.RefObject<HTMLDivElement | null>;
}

export default function TranscriptCard({ englishTranscript, englishScrollRef }: TranscriptCardProps) {
  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl flex flex-col h-[320px]">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-xs font-bold text-white uppercase tracking-wider flex items-center space-x-2">
          <span className="w-2 h-2 rounded-full bg-indigo-500 animate-pulse" />
          <span>User A (Speaker - English)</span>
        </h3>
        <span className="text-[10px] bg-indigo-500/10 text-indigo-400 px-2 py-0.5 rounded font-semibold font-mono uppercase border border-indigo-500/20">
          Local Preview
        </span>
      </div>
      <div 
        ref={englishScrollRef}
        className="flex-1 bg-slate-950/80 border border-slate-850 rounded-xl p-4 overflow-y-auto font-mono text-sm leading-relaxed text-slate-305 scrollbar-thin"
      >
        {englishTranscript ? (
          <p>{englishTranscript}</p>
        ) : (
          <p className="text-slate-600 italic">Waiting for local speech recognition preview...</p>
        )}
      </div>
      <div className="text-[10px] text-slate-500 mt-2 italic text-right">
        *Captured via Web Speech API locally as preview. Authoritative translation runs on backend.
      </div>
    </div>
  );
}
