import React from 'react';

interface MediaControlsProps {
  isCameraEnabled: boolean;
  isMicrophoneEnabled: boolean;
  onToggleCamera: () => void;
  onToggleMicrophone: () => void;
  onLeaveRoom: () => void;
  isConnected: boolean;
}

export default function MediaControls({
  isCameraEnabled,
  isMicrophoneEnabled,
  onToggleCamera,
  onToggleMicrophone,
  onLeaveRoom,
  isConnected,
}: MediaControlsProps) {
  if (!isConnected) return null;

  return (
    <div className="flex items-center justify-center space-x-4 bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-md max-w-sm mx-auto">
      {/* Mic Button */}
      <button
        onClick={onToggleMicrophone}
        className={`px-4 py-2.5 rounded-lg font-medium text-xs transition ${
          isMicrophoneEnabled 
            ? 'bg-indigo-600 hover:bg-indigo-500 text-white' 
            : 'bg-slate-800 hover:bg-slate-700 text-slate-400'
        }`}
        title={isMicrophoneEnabled ? 'Mute Microphone' : 'Unmute Microphone'}
      >
        {isMicrophoneEnabled ? 'Mic On' : 'Mic Off'}
      </button>

      {/* Camera Button */}
      <button
        onClick={onToggleCamera}
        className={`px-4 py-2.5 rounded-lg font-medium text-xs transition ${
          isCameraEnabled 
            ? 'bg-indigo-600 hover:bg-indigo-500 text-white' 
            : 'bg-slate-800 hover:bg-slate-700 text-slate-400'
        }`}
        title={isCameraEnabled ? 'Disable Camera' : 'Enable Camera'}
      >
        {isCameraEnabled ? 'Cam On' : 'Cam Off'}
      </button>

      {/* Leave Button */}
      <button
        onClick={onLeaveRoom}
        className="px-4 py-2.5 rounded-lg bg-rose-600 hover:bg-rose-500 text-white font-medium text-xs transition"
        title="Leave Room"
      >
        Leave Room
      </button>
    </div>
  );
}
