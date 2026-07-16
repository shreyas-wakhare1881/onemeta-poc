import { useEffect, useState, useCallback, useRef } from 'react';
import { createRoom, connectRoom, disconnectRoom, stopLocalTracks } from '../services/livekit.service';
import { getLiveKitToken } from '../services/api';
import { 
  RoomEvent, 
  Room, 
  LocalVideoTrack, 
  LocalAudioTrack, 
  LocalTrackPublication, 
  RemoteVideoTrack, 
  RemoteAudioTrack,
  LocalTrack
} from 'livekit-client';
import type { RoomConnectionState, ParticipantInfo } from '../types/livekit';

export function useLiveKit() {
  const [status, setStatus] = useState<RoomConnectionState>('Disconnected');
  const [error, setError] = useState<string | null>(null);

  // Device status states
  const [isCameraEnabled, setIsCameraEnabled] = useState<boolean>(false);
  const [isMicrophoneEnabled, setIsMicrophoneEnabled] = useState<boolean>(false);

  // Local tracks
  const [localVideoTrack, setLocalVideoTrack] = useState<LocalVideoTrack | null>(null);
  const [localAudioTrack, setLocalAudioTrack] = useState<LocalAudioTrack | null>(null);

  // Mapped remote participant tracks list
  const [remoteParticipants, setRemoteParticipants] = useState<ParticipantInfo[]>([]);

  // Room instance ref
  const roomRef = useRef<Room | null>(null);

  // Connection State handler
  const handleStateChange = useCallback((state: string) => {
    if (state === 'connected') {
      setStatus('Connected');
    } else if (state === 'connecting' || state === 'reconnecting') {
      setStatus('Connecting');
    } else if (state === 'disconnected') {
      setStatus('Disconnected');
    }
  }, []);

  // Synchronizes the remote participants array in React state with SDK Room state
  const syncRemoteParticipants = useCallback(() => {
    if (!roomRef.current) {
      setRemoteParticipants([]);
      return;
    }

    const mapped: ParticipantInfo[] = Array.from(roomRef.current.remoteParticipants.values()).map((p) => {
      let videoTrack: RemoteVideoTrack | null = null;
      let audioTrack: RemoteAudioTrack | null = null;

      p.trackPublications.forEach((pub) => {
        if (pub.isSubscribed && pub.track) {
          if (pub.track.kind === 'video') {
            videoTrack = pub.track as RemoteVideoTrack;
          } else if (pub.track.kind === 'audio') {
            audioTrack = pub.track as RemoteAudioTrack;
          }
        }
      });

      return {
        identity: p.identity,
        sid: p.sid,
        videoTrack,
        audioTrack,
      };
    });

    setRemoteParticipants(mapped);
  }, []);

  // Handlers for local track publishing
  const handleLocalTrackPublished = useCallback((publication: LocalTrackPublication) => {
    if (publication.track?.kind === 'video') {
      setLocalVideoTrack(publication.track as LocalVideoTrack);
    } else if (publication.track?.kind === 'audio') {
      setLocalAudioTrack(publication.track as LocalAudioTrack);
    }
  }, []);

  const handleLocalTrackUnpublished = useCallback((publication: LocalTrackPublication) => {
    if (publication.track?.kind === 'video') {
      setLocalVideoTrack(null);
    } else if (publication.track?.kind === 'audio') {
      setLocalAudioTrack(null);
    }
  }, []);

  // Deterministic teardown and state reset helper
  const cleanupRoomInstance = useCallback(async () => {
    if (roomRef.current) {
      const room = roomRef.current;

      // 1. Remove all event listeners first
      room.off(RoomEvent.ConnectionStateChanged, handleStateChange);
      room.off(RoomEvent.ParticipantConnected, syncRemoteParticipants);
      room.off(RoomEvent.ParticipantDisconnected, syncRemoteParticipants);
      room.off(RoomEvent.TrackSubscribed, syncRemoteParticipants);
      room.off(RoomEvent.TrackUnsubscribed, syncRemoteParticipants);
      room.off(RoomEvent.LocalTrackPublished, handleLocalTrackPublished);
      room.off(RoomEvent.LocalTrackUnpublished, handleLocalTrackUnpublished);

      // 2. Stop and unpublish local tracks cleanly
      try {
        const localTracks = Array.from(room.localParticipant.trackPublications.values())
          .map(pub => pub.track)
          .filter(Boolean) as LocalTrack[];

        await stopLocalTracks(room, localTracks);
      } catch (trackErr) {
        console.error('Failed to unpublish and stop local tracks cleanly:', trackErr);
      }

      // 3. Disconnect from room
      try {
        await disconnectRoom(room);
      } catch (discErr) {
        console.error('Failed to disconnect room instance cleanly:', discErr);
      }

      // Trigger temporary stop endpoint for agent connection
      try {
        await fetch('http://127.0.0.1:8000/api/audio/agent/stop', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ room_name: room.name }),
        });
      } catch (err) {
        console.error('Failed to trigger backend agent teardown:', err);
      }

      roomRef.current = null;
    }

    // 4. Reset React states
    setIsCameraEnabled(false);
    setIsMicrophoneEnabled(false);
    setLocalVideoTrack(null);
    setLocalAudioTrack(null);
    setRemoteParticipants([]);
    setStatus('Disconnected');
  }, [
    handleStateChange,
    syncRemoteParticipants,
    handleLocalTrackPublished,
    handleLocalTrackUnpublished
  ]);

  const connect = useCallback(async (roomName: string, identity: string) => {
    if (!roomName.trim() || !identity.trim()) {
      setError('Room Name and Participant Identity are required.');
      setStatus('Failed');
      return;
    }

    setError(null);
    setStatus('Connecting');

    try {
      // Clean up previous room instance if still lingering to prevent duplicates
      await cleanupRoomInstance();

      // Retrieve connection token
      let res;
      try {
        res = await getLiveKitToken(roomName, identity);
      } catch (apiErr) {
        console.error('Token API generation failure:', apiErr);
        setError('Server authentication failed. Could not retrieve token.');
        setStatus('Failed');
        return;
      }

      // Initialize room and attach event listeners
      const room = createRoom();
      roomRef.current = room;

      room.on(RoomEvent.ConnectionStateChanged, handleStateChange);
      room.on(RoomEvent.ParticipantConnected, syncRemoteParticipants);
      room.on(RoomEvent.ParticipantDisconnected, syncRemoteParticipants);
      room.on(RoomEvent.TrackSubscribed, syncRemoteParticipants);
      room.on(RoomEvent.TrackUnsubscribed, syncRemoteParticipants);
      room.on(RoomEvent.LocalTrackPublished, handleLocalTrackPublished);
      room.on(RoomEvent.LocalTrackUnpublished, handleLocalTrackUnpublished);

      // Connect
      try {
        await connectRoom(room, res.url, res.token);
      } catch (connErr) {
        console.error('Server connection failure:', connErr);
        setError('Failed to connect to the room. Please check your internet connection or LiveKit URL.');
        setStatus('Failed');
        await cleanupRoomInstance();
        return;
      }

      setStatus('Connected');
      syncRemoteParticipants();

      // Trigger temporary start endpoint for agent connection
      try {
        await fetch('http://127.0.0.1:8000/api/audio/agent/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ room_name: roomName }),
        });
      } catch (err) {
        console.error('Failed to trigger backend agent startup:', err);
      }

      // Auto-enable devices on successful join
      try {
        await room.localParticipant.setCameraEnabled(true);
        setIsCameraEnabled(true);
      } catch (camErr: any) {
        console.warn('Auto-enable camera failed:', camErr);
        if (camErr?.name === 'NotAllowedError' || camErr?.message?.includes('Permission denied')) {
          setError('Camera permission was denied. Please allow camera access in browser settings.');
        } else {
          setError(`Camera access failed: ${camErr?.message || String(camErr)}`);
        }
      }

      try {
        await room.localParticipant.setMicrophoneEnabled(true);
        setIsMicrophoneEnabled(true);
      } catch (micErr: any) {
        console.warn('Auto-enable microphone failed:', micErr);
        if (micErr?.name === 'NotAllowedError' || micErr?.message?.includes('Permission denied')) {
          setError('Microphone permission was denied. Please allow microphone access in browser settings.');
        } else {
          setError(`Microphone access failed: ${micErr?.message || String(micErr)}`);
        }
      }

    } catch (err: any) {
      console.error('LiveKit Hook Connect encountered general error:', err);
      setError(`Connection failure: ${err?.message || String(err)}`);
      setStatus('Failed');
      await cleanupRoomInstance();
    }
  }, [
    cleanupRoomInstance,
    handleStateChange,
    syncRemoteParticipants,
    handleLocalTrackPublished,
    handleLocalTrackUnpublished
  ]);

  const disconnect = useCallback(async () => {
    setStatus('Disconnecting');
    await cleanupRoomInstance();
  }, [cleanupRoomInstance]);

  const toggleCamera = useCallback(async () => {
    if (!roomRef.current) return;
    const room = roomRef.current;
    const nextState = !isCameraEnabled;
    try {
      setError(null);
      await room.localParticipant.setCameraEnabled(nextState);
      setIsCameraEnabled(nextState);
    } catch (err: any) {
      console.error('Camera toggle failed:', err);
      if (err?.name === 'NotAllowedError' || err?.message?.includes('Permission denied')) {
        setError('Camera permission was denied. Please allow camera access in browser settings.');
      } else {
        setError(`Failed to toggle camera: ${err?.message || String(err)}`);
      }
    }
  }, [isCameraEnabled]);

  const toggleMicrophone = useCallback(async () => {
    if (!roomRef.current) return;
    const room = roomRef.current;
    const nextState = !isMicrophoneEnabled;
    try {
      setError(null);
      await room.localParticipant.setMicrophoneEnabled(nextState);
      setIsMicrophoneEnabled(nextState);
    } catch (err: any) {
      console.error('Microphone toggle failed:', err);
      if (err?.name === 'NotAllowedError' || err?.message?.includes('Permission denied')) {
        setError('Microphone permission was denied. Please allow microphone access in browser settings.');
      } else {
        setError(`Failed to toggle microphone: ${err?.message || String(err)}`);
      }
    }
  }, [isMicrophoneEnabled]);

  // Handle cleanup on unmount
  useEffect(() => {
    return () => {
      cleanupRoomInstance();
    };
  }, [cleanupRoomInstance]);

  return {
    status,
    error,
    isCameraEnabled,
    isMicrophoneEnabled,
    localVideoTrack,
    localAudioTrack,
    remoteParticipants,
    connect,
    disconnect,
    toggleCamera,
    toggleMicrophone,
  };
}
