"use client";

import { useEffect, useState } from 'react';
import { getHealth } from '../services/api';
import RoomControls from '../components/RoomControls';
import StatusCard from '../components/StatusCard';
import ParticipantsGrid from '../components/ParticipantsGrid';
import MediaControls from '../components/MediaControls';
import { useLiveKit } from '../hooks/useLiveKit';

export default function Home() {
  const [backendStatus, setBackendStatus] = useState<string>('Checking...');
  const [backendConnected, setBackendConnected] = useState<boolean>(false);

  const {
    status,
    error,
    isCameraEnabled,
    isMicrophoneEnabled,
    localVideoTrack,
    remoteParticipants,
    connect,
    disconnect,
    toggleCamera,
    toggleMicrophone,
  } = useLiveKit();

  useEffect(() => {
    async function check() {
      try {
        const res = await getHealth();
        if (res && res.status === 'ok') {
          setBackendStatus('OK');
          setBackendConnected(true);
        } else {
          setBackendStatus('Unhealthy');
        }
      } catch (e) {
        setBackendStatus('Unavailable');
      }
    }
    check();
  }, []);

  const isConnected = status === 'Connected';

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100 flex flex-col justify-between font-sans">
      {/* Header */}
      <header className="border-b border-slate-800 bg-slate-900/50 px-6 py-4 flex items-center justify-between">
        <h1 className="text-xl font-bold text-white">OneMeta S2S POC</h1>
        <div className="flex items-center space-x-2 text-xs">
          <span className="text-slate-400">Backend:</span>
          <span className={`px-2 py-0.5 rounded font-semibold uppercase ${
            backendConnected ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'
          }`}>
            {backendConnected ? 'Connected' : backendStatus}
          </span>
        </div>
      </header>

      {/* Main composition container */}
      <div className="flex-1 max-w-4xl w-full mx-auto p-6 md:p-8 space-y-6 flex flex-col justify-center">
        <StatusCard status={status} />
        
        {!isConnected ? (
          <div className="max-w-md w-full mx-auto">
            <RoomControls 
              status={status} 
              error={error} 
              onConnect={connect} 
              onDisconnect={disconnect} 
            />
          </div>
        ) : (
          <div className="space-y-6 w-full">
            <ParticipantsGrid
              localTrack={localVideoTrack}
              localIdentity="You"
              remoteParticipants={remoteParticipants}
            />
            
            <MediaControls
              isCameraEnabled={isCameraEnabled}
              isMicrophoneEnabled={isMicrophoneEnabled}
              onToggleCamera={toggleCamera}
              onToggleMicrophone={toggleMicrophone}
              onLeaveRoom={disconnect}
              isConnected={isConnected}
            />
            
            {error && (
              <div className="bg-red-500/10 border border-red-500/20 text-red-400 text-xs rounded-lg p-3 max-w-md mx-auto">
                <p className="font-mono text-center">{error}</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Footer */}
      <footer className="border-t border-slate-900 py-4 px-6 text-center text-xs text-slate-500">
        OneMeta Speech-to-Speech Proof of Concept v0.1.0
      </footer>
    </main>
  );
}
