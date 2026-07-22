import React from 'react';

interface TranslationCardProps {
  spanishTranslation: string;
  spanishScrollRef: React.RefObject<HTMLDivElement | null>;
  playbackState: 'idle' | 'playing';
}

export default function TranslationCard({ spanishTranslation, spanishScrollRef, playbackState }: TranslationCardProps) {
  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl flex flex-col h-[320px]">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-xs font-bold text-white uppercase tracking-wider flex items-center space-x-2">
          <span className={`w-2 h-2 rounded-full ${playbackState === 'playing' ? 'bg-violet-500 animate-ping' : 'bg-slate-600'}`} />
          <span>Spanish (Live Translation)</span>
        </h3>
        <span className={`text-[10px] px-2 py-0.5 rounded font-semibold font-mono uppercase border transition ${
          playbackState === 'playing' 
            ? 'bg-violet-500/10 text-violet-400 border-violet-500/20' 
            : 'bg-slate-800/20 text-slate-400 border-slate-800'
        }`}>
          {playbackState === 'playing' ? 'Playing' : 'Streaming'}
        </span>
      </div>
      <div 
        ref={spanishScrollRef}
        className="flex-1 bg-slate-950/80 border border-slate-850 rounded-xl p-4 overflow-y-auto font-mono text-sm leading-relaxed text-slate-300 scrollbar-thin"
      >
        {spanishTranslation ? (
          <p className="text-indigo-100">{spanishTranslation}</p>
        ) : (
          <p className="text-slate-600 italic">Waiting for streaming translations from Gemma...</p>
        )}
      </div>
    </div>
  );
}
