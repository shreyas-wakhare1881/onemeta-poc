import React, { useEffect, useRef } from 'react';
import type { LocalVideoTrack } from 'livekit-client';

interface LocalVideoProps {
  track: LocalVideoTrack | null;
  identity: string;
}

export default function LocalVideo({ track, identity }: LocalVideoProps) {
  const videoRef = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    const el = videoRef.current;
    if (el && track) {
      track.attach(el);
    }
    return () => {
      if (el && track) {
        track.detach(el);
      }
    };
  }, [track]);

  const hasVideo = !!track;

  return (
    <div className="relative bg-slate-900 border border-slate-800 rounded-xl overflow-hidden aspect-video flex items-center justify-center">
      {hasVideo ? (
        <video 
          ref={videoRef} 
          autoPlay 
          playsInline 
          muted
          className="w-full h-full object-cover transform -scale-x-100"
        />
      ) : (
        <div className="text-center p-4">
          <div className="w-12 h-12 rounded-full bg-slate-800 flex items-center justify-center text-slate-400 font-bold mx-auto mb-2 uppercase">
            {identity.slice(0, 2)}
          </div>
          <span className="text-xs text-slate-400 font-mono block">{identity} (You)</span>
          <span className="text-[10px] text-slate-500 block italic mt-1">(Camera Disabled)</span>
        </div>
      )}

      {/* Label Overlay */}
      <div className="absolute bottom-3 left-3 bg-indigo-600/80 px-2 py-1 rounded text-xs text-white font-mono">
        {identity} (You)
      </div>
    </div>
  );
}
