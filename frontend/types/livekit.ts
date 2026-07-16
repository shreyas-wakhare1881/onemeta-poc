import type { RemoteVideoTrack, RemoteAudioTrack } from 'livekit-client';

export type RoomConnectionState = 'Disconnected' | 'Connecting' | 'Connected' | 'Disconnecting' | 'Failed';

export interface ParticipantInfo {
  identity: string;
  sid: string;
  videoTrack: RemoteVideoTrack | null;
  audioTrack: RemoteAudioTrack | null;
}

export interface MediaDeviceState {
  isCameraEnabled: boolean;
  isMicrophoneEnabled: boolean;
  cameraError: string | null;
  microphoneError: string | null;
}
