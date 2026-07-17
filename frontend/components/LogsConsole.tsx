import React from 'react';

interface LogsConsoleProps {
  devLogs: string[];
  logsScrollRef: React.RefObject<HTMLDivElement | null>;
}

export default function LogsConsole({ devLogs, logsScrollRef }: LogsConsoleProps) {
  return (
    <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl flex flex-col h-[240px]">
      <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-3 flex items-center space-x-2">
        <span className="w-1.5 h-3 bg-indigo-500 rounded" />
        <span>Developer Logs Console</span>
      </h2>
      <div 
        ref={logsScrollRef}
        className="flex-1 bg-slate-950/80 border border-slate-850 rounded-xl p-4 overflow-y-auto font-mono text-[11px] leading-relaxed text-indigo-350 scrollbar-thin"
      >
        {devLogs.length > 0 ? (
          devLogs.map((log, idx) => <p key={idx} className="border-b border-slate-900/40 py-0.5">{log}</p>)
        ) : (
          <p className="text-slate-600 italic">No console logs recorded.</p>
        )}
      </div>
    </div>
  );
}
