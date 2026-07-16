import React from 'react';
import type { LocalVideoTrack } from 'livekit-client';
import type { ParticipantInfo } from '../types/livekit';
import LocalVideo from './LocalVideo';
import RemoteParticipantComponent from './RemoteParticipant';

interface ParticipantsGridProps {
  localTrack: LocalVideoTrack | null;
  localIdentity: string;
  remoteParticipants: ParticipantInfo[];
}

export default function ParticipantsGrid({ localTrack, localIdentity, remoteParticipants }: ParticipantsGridProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full">
      {/* Local participant video */}
      <LocalVideo track={localTrack} identity={localIdentity} />

      {/* Remote participants video */}
      {remoteParticipants.map((participant) => (
        <RemoteParticipantComponent key={participant.sid} participant={participant} />
      ))}
    </div>
  );
}
