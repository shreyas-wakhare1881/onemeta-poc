import React from 'react';
import { RoomConnectionState } from '../types/livekit';

interface StatusCardProps {
  status: RoomConnectionState;
}

export default function StatusCard({ status }: StatusCardProps) {
  const getStatusColor = () => {
    switch (status) {
      case 'Connected':
        return 'bg-emerald-500 text-emerald-400 border-emerald-500/20';
      case 'Connecting':
        return 'bg-amber-500 text-amber-400 border-amber-500/20';
      case 'Disconnecting':
        return 'bg-orange-500 text-orange-400 border-orange-500/20';
      case 'Failed':
        return 'bg-red-500 text-red-400 border-red-500/20';
      default:
        return 'bg-slate-500 text-slate-400 border-slate-500/20';
    }
  };

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-md flex items-center justify-between">
      <div>
        <span className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1">
          Connection Status
        </span>
        <span id="connection-status-text" className="text-xl font-bold text-white">
          {status}
        </span>
      </div>
      <div className={`h-3 w-3 rounded-full ${getStatusColor().split(' ')[0]} animate-pulse shadow-lg`} />
    </div>
  );
}
