import React, { useEffect, useRef } from 'react';
import type { ParticipantInfo } from '../types/livekit';

interface RemoteParticipantProps {
  participant: ParticipantInfo;
}

export default function RemoteParticipant({ participant }: RemoteParticipantProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const audioRef = useRef<HTMLAudioElement>(null);

  // Attach video track
  useEffect(() => {
    const el = videoRef.current;
    if (el && participant.videoTrack) {
      participant.videoTrack.attach(el);
    }
    return () => {
      if (el && participant.videoTrack) {
        participant.videoTrack.detach(el);
      }
    };
  }, [participant.videoTrack]);

  // Attach audio track
  useEffect(() => {
    const el = audioRef.current;
    if (el && participant.audioTrack) {
      participant.audioTrack.attach(el);
    }
    return () => {
      if (el && participant.audioTrack) {
        participant.audioTrack.detach(el);
      }
    };
  }, [participant.audioTrack]);

  const hasVideo = !!participant.videoTrack;

  return (
    <div className="relative bg-slate-900 border border-slate-800 rounded-xl overflow-hidden aspect-video flex items-center justify-center">
      {hasVideo ? (
        <video 
          ref={videoRef} 
          autoPlay 
          playsInline 
          className="w-full h-full object-cover"
        />
      ) : (
        <div className="text-center p-4">
          <div className="w-12 h-12 rounded-full bg-slate-800 flex items-center justify-center text-slate-400 font-bold mx-auto mb-2 uppercase">
            {participant.identity.slice(0, 2)}
          </div>
          <span className="text-xs text-slate-400 font-mono block">{participant.identity}</span>
          <span className="text-[10px] text-slate-500 block italic mt-1">(Camera Disabled)</span>
        </div>
      )}

      {/* Invisible audio element to play remote sound */}
      {participant.audioTrack && <audio ref={audioRef} autoPlay />}

      {/* Participant Identity Label Overlay */}
      <div className="absolute bottom-3 left-3 bg-black/60 px-2 py-1 rounded text-xs text-white font-mono">
        {participant.identity}
      </div>
    </div>
  );
}
