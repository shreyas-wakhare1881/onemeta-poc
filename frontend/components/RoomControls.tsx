import React, { useState } from 'react';
import { RoomConnectionState } from '../types/livekit';

interface RoomControlsProps {
  status: RoomConnectionState;
  error: string | null;
  onConnect: (roomName: string, identity: string) => void;
  onDisconnect: () => void;
}

export default function RoomControls({ status, error, onConnect, onDisconnect }: RoomControlsProps) {
  const [roomName, setRoomName] = useState<string>('onemeta-demo');
  const [identity, setIdentity] = useState<string>('User-A');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onConnect(roomName, identity);
  };

  const isConnecting = status === 'Connecting';
  const isConnected = status === 'Connected';
  const isDisconnecting = status === 'Disconnecting';
  const isDisabled = isConnecting || isConnected || isDisconnecting;

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-md">
      <h2 className="text-lg font-bold text-white mb-4">Connection Panel</h2>
      
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="room-name-input" className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
            Room Name
          </label>
          <input
            id="room-name-input"
            type="text"
            value={roomName}
            onChange={(e) => setRoomName(e.target.value)}
            disabled={isDisabled}
            placeholder="Enter room name..."
            className="w-full bg-slate-950 border border-slate-800 rounded-lg px-4 py-2.5 text-slate-200 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500 transition disabled:opacity-50"
            required
          />
        </div>

        <div>
          <label htmlFor="identity-input" className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
            Participant Identity
          </label>
          <input
            id="identity-input"
            type="text"
            value={identity}
            onChange={(e) => setIdentity(e.target.value)}
            disabled={isDisabled}
            placeholder="Enter identity..."
            className="w-full bg-slate-950 border border-slate-800 rounded-lg px-4 py-2.5 text-slate-200 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500 transition disabled:opacity-50"
            required
          />
        </div>

        {error && (
          <div className="bg-red-500/10 border border-red-500/20 text-red-400 text-xs rounded-lg p-3">
            <p className="font-mono break-all">{error}</p>
          </div>
        )}

        <div className="pt-2 flex gap-3">
          <button
            type="submit"
            id="join-room-btn"
            disabled={status !== 'Disconnected' && status !== 'Failed'}
            className="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-sm rounded-lg px-4 py-2.5 transition disabled:opacity-40 disabled:pointer-events-none"
          >
            {isConnecting ? 'Connecting...' : 'Join Room'}
          </button>

          <button
            type="button"
            id="disconnect-room-btn"
            onClick={onDisconnect}
            disabled={status !== 'Connected' && status !== 'Connecting'}
            className="flex-1 bg-rose-600 hover:bg-rose-500 text-white font-medium text-sm rounded-lg px-4 py-2.5 transition disabled:opacity-40 disabled:pointer-events-none"
          >
            {isDisconnecting ? 'Disconnecting...' : 'Disconnect'}
          </button>
        </div>
      </form>
    </div>
  );
}
