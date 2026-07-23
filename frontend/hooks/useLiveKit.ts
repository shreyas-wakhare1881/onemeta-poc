import { useEffect, useState, useCallback, useRef } from 'react';
import { createRoom, connectRoom, disconnectRoom, stopLocalTracks } from '../services/livekit.service';
import { getLiveKitToken } from '../services/api';
import { tracer } from '../services/trace.service';
import { PipelineEvent } from '../types/trace';
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
import type { RoomConnectionState, ParticipantInfo, LiveKitAIEventPacket, TelemetryUpdatePayload } from '../types/livekit';

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

  // AI Events and Telemetry states
  const [aiEvents, setAiEvents] = useState<LiveKitAIEventPacket[]>([]);
  const [telemetryData, setTelemetryData] = useState<TelemetryUpdatePayload | null>(null);

  // Experiment: Audio packets received count ref
  const totalAudioEventsReceivedRef = useRef(0);

  // Room instance ref
  const roomRef = useRef<Room | null>(null);

  // Reconnection refs
  const reconnectingRef = useRef<boolean>(false);
  const reconnectAttemptRef = useRef<number>(0);
  const reconnectTimerRef = useRef<NodeJS.Timeout | null>(null);
  const connectionParamsRef = useRef<{ roomName: string; identity: string; loopback: boolean } | null>(null);
  const connectRef = useRef<((roomName: string, identity: string, loopback?: boolean, isReconnecting?: boolean) => Promise<void>) | null>(null);

  // Data packet received handler
  const handleDataReceived = useCallback((payload: Uint8Array, participant: unknown) => {
    const textDecoder = new TextDecoder();
    const dataStr = textDecoder.decode(payload);
    try {
      const parsed = JSON.parse(dataStr);
      
      // 1. Strict packet shape validation
      if (!parsed || typeof parsed !== 'object') {
        console.warn('Packet Validation Failure: Packet is not a valid JSON object. Discarding.');
        return;
      }
      
      const { id, version, type, payload: packetPayload } = parsed;
      
      if (!id || typeof id !== 'string') {
        console.warn('Packet Validation Failure: Missing or invalid "id" field. Discarding.');
        return;
      }
      
      if (version !== 1) {
        console.warn(`Packet Validation Failure: Unsupported protocol version (${version}). Discarding.`);
        return;
      }
      
      if (!type || typeof type !== 'string') {
        console.warn('Packet Validation Failure: Missing or invalid "type" field. Discarding.');
        return;
      }
      
      if (packetPayload === undefined) {
        console.warn('Packet Validation Failure: Missing packet "payload" content. Discarding.');
        return;
      }

      // 2. Validate known packet types
      const knownTypes = [
        'AIStartedEvent', 'AIPartialEvent', 'AICompletedEvent', 
        'TranslationFailedEvent', 'AIErrorEvent', 'TelemetryUpdate',
        'StreamingPartialTranslationEvent', 'StreamingTranslationAudioEvent',
        'StreamingTranslationCompletedEvent', 'StreamingRuntimeErrorEvent',
        'StreamingInputTranscriptionEvent', 'StreamingInputTranscriptionCompletedEvent'
      ];
      if (!knownTypes.includes(type)) {
        console.warn(`Packet Validation Failure: Unknown packet type "${type}". Discarding.`);
        return;
      }
      
      // 3. Dispatch to React state managers
      if (type === 'TelemetryUpdate') {
        setTelemetryData(packetPayload as TelemetryUpdatePayload);
      } else {
        const corrId = (packetPayload as any)?.correlation_id || '';
        if (type === 'StreamingPartialTranslationEvent') {
          const textPayload = packetPayload as any;
          tracer.logEvent(PipelineEvent.TEXT_PACKET_RECEIVED, corrId, {
            packet_id: id,
            text_delta: textPayload?.text_delta || '',
            text_length: (textPayload?.text_delta || '').length,
            cumulative_text: textPayload?.cumulative_text || ''
          });
        } else if (type === 'StreamingTranslationAudioEvent') {
          totalAudioEventsReceivedRef.current++;
          console.log(`[NET RECEIVE] Total Audio Chunks Received: ${totalAudioEventsReceivedRef.current}`);
          const audioPayload = packetPayload as any;
          const audioDataLen = audioPayload?.audio_data ? audioPayload.audio_data.length : 0;
          tracer.logEvent(PipelineEvent.AUDIO_PACKET_RECEIVED, corrId, {
            packet_id: id,
            chunk_index: totalAudioEventsReceivedRef.current,
            audio_data_b64_len: audioDataLen,
            mime_type: audioPayload?.mime_type || 'audio/pcm'
          });
        }
        setAiEvents((prev) => [...prev, parsed as LiveKitAIEventPacket]);
      }
    } catch (e) {
      console.error('Failed to parse received data packet:', e);
    }
  }, []);

  // Connection State handler
  const handleStateChange = useCallback((state: string) => {
    if (state === 'connected') {
      setStatus('Connected');
      reconnectAttemptRef.current = 0;
      reconnectingRef.current = false;
    } else if (state === 'connecting' || state === 'reconnecting') {
      setStatus('Connecting');
    } else if (state === 'disconnected') {
      setStatus('Disconnected');
      // If we did not disconnect manually, trigger auto-reconnect
      if (connectionParamsRef.current && !reconnectingRef.current) {
        reconnectingRef.current = true;
        const { roomName, identity, loopback } = connectionParamsRef.current;
        const attempt = reconnectAttemptRef.current + 1;
        if (attempt > 5) {
          setError('Failed to reconnect. Max connection attempts reached.');
          setStatus('Failed');
          reconnectingRef.current = false;
          return;
        }
        reconnectAttemptRef.current = attempt;
        const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
        console.log(`LiveKit connection lost. Reconnecting room "${roomName}" in ${delay}ms...`);
        setStatus('Connecting');
        
        if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = setTimeout(async () => {
          try {
            await cleanupRoomInstance(true);
            if (connectRef.current) {
              await connectRef.current(roomName, identity, loopback, true);
            }
            reconnectingRef.current = false;
          } catch (err) {
            console.error('Reconnection attempt failed, retrying...', err);
            reconnectingRef.current = false;
            handleStateChange('disconnected');
          }
        }, delay);
      }
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
  const cleanupRoomInstance = useCallback(async (isReconnecting = false) => {
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
      room.off(RoomEvent.DataReceived, handleDataReceived);

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

      // Trigger temporary stop endpoint for agent connection (only if NOT reconnecting)
      if (!isReconnecting) {
        // End tracing session and trigger trace download
        tracer.endSession();
        try {
          await fetch('http://127.0.0.1:8000/api/audio/agent/stop', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ room_name: room.name }),
          });
        } catch (err) {
          console.error('Failed to trigger backend agent teardown:', err);
        }
      }

      roomRef.current = null;
    }

    // 4. Reset React states
    setIsCameraEnabled(false);
    setIsMicrophoneEnabled(false);
    setLocalVideoTrack(null);
    setLocalAudioTrack(null);
    setRemoteParticipants([]);
    if (!isReconnecting) {
      setAiEvents([]);
      setTelemetryData(null);
      setStatus('Disconnected');
      totalAudioEventsReceivedRef.current = 0;
    } else {
      setStatus('Connecting');
    }
  }, [
    handleStateChange,
    syncRemoteParticipants,
    handleLocalTrackPublished,
    handleLocalTrackUnpublished,
    handleDataReceived
  ]);

  const connect = useCallback(async (roomName: string, identity: string, loopback = false, isReconnecting = false) => {
    if (!roomName.trim() || !identity.trim()) {
      setError('Room Name and Participant Identity are required.');
      setStatus('Failed');
      return;
    }

    if (!isReconnecting) {
      connectionParamsRef.current = { roomName, identity, loopback };
      reconnectAttemptRef.current = 0;
      setError(null);
    }
    
    setStatus('Connecting');

    try {
      // Clean up previous room instance if still lingering to prevent duplicates
      await cleanupRoomInstance(isReconnecting);

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
      room.on(RoomEvent.DataReceived, handleDataReceived);

      // Connect
      try {
        await connectRoom(room, res.url, res.token);
      } catch (connErr) {
        console.error('Server connection failure:', connErr);
        setError('Failed to connect to the room. Please check your internet connection or LiveKit URL.');
        setStatus('Failed');
        await cleanupRoomInstance(isReconnecting);
        return;
      }

      setStatus('Connected');
      syncRemoteParticipants();

      // Start tracing session
      tracer.startSession(roomName);

      // Trigger temporary start endpoint for agent connection (idempotent startup)
      try {
        await fetch('http://127.0.0.1:8000/api/audio/agent/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            room_name: roomName,
            loopback,
            source_participant_identity: identity
          }),
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
        setError(`Camera access failed: ${camErr?.message || String(camErr)}`);
      }

      try {
        await room.localParticipant.setMicrophoneEnabled(true);
        setIsMicrophoneEnabled(true);
      } catch (micErr: any) {
        console.warn('Auto-enable microphone failed:', micErr);
        setError(`Microphone access failed: ${micErr?.message || String(micErr)}`);
      }

    } catch (err: any) {
      console.error('LiveKit Hook Connect encountered general error:', err);
      setError(`Connection failure: ${err?.message || String(err)}`);
      setStatus('Failed');
      await cleanupRoomInstance(isReconnecting);
    }
  }, [
    cleanupRoomInstance,
    handleStateChange,
    syncRemoteParticipants,
    handleLocalTrackPublished,
    handleLocalTrackUnpublished,
    handleDataReceived
  ]);

  // Keep connectRef synced
  useEffect(() => {
    connectRef.current = connect;
  }, [connect]);

  const disconnect = useCallback(async () => {
    connectionParamsRef.current = null;
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    reconnectingRef.current = false;
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
      setError(`Failed to toggle camera: ${err?.message || String(err)}`);
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
      setError(`Failed to toggle microphone: ${err?.message || String(err)}`);
    }
  }, [isMicrophoneEnabled]);

  // Handle cleanup on unmount
  useEffect(() => {
    return () => {
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
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
    aiEvents,
    setAiEvents,
    telemetryData,
    connect,
    disconnect,
    toggleCamera,
    toggleMicrophone,
    totalAudioEventsReceivedRef,
  };
}
