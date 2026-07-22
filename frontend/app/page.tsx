"use client";

import { useEffect, useState, useRef, useCallback } from 'react';
import { useLiveKit } from '../hooks/useLiveKit';
import { pcmPlayer } from '../services/pcmPlayer.service';

// Subcomponent imports
import TranscriptCard from '../components/TranscriptCard';
import TranslationCard from '../components/TranslationCard';

export default function Home() {
  const [backendStatus, setBackendStatus] = useState<string>('Checking...');
  const [backendConnected, setBackendConnected] = useState<boolean>(false);

  // Connection config
  const [roomNameInput, setRoomNameInput] = useState<string>('onemeta-demo');
  const [identityInput, setIdentityInput] = useState<string>('User-A');

  // Experiment: Allow playing translation of own voice (local testing)
  const [hearOwnTranslation, setHearOwnTranslation] = useState<boolean>(true);

  // Audio / translation text state slices
  const [englishTranscript, setEnglishTranscript] = useState<string>('');
  const [spanishTranslation, setSpanishTranslation] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Pipeline execution state slices
  const [playbackState, setPlaybackState] = useState<'idle' | 'playing'>('idle');
  const [englishInterim, setEnglishInterim] = useState<string>('');

  const englishScrollRef = useRef<HTMLDivElement>(null);
  const spanishScrollRef = useRef<HTMLDivElement>(null);
  const recognitionRef = useRef<any>(null);
  const [isSpeechRecognitionActive, setIsSpeechRecognitionActive] = useState<boolean>(false);

  // Experiment: Audio packets played count ref
  const totalAudioEventsPlayedRef = useRef(0);
  const processedEventsCountRef = useRef(0);

  const {
    status,
    error,
    isMicrophoneEnabled,
    aiEvents,
    connect,
    disconnect,
    toggleMicrophone,
    totalAudioEventsReceivedRef,
  } = useLiveKit();

  const isConnected = status === 'Connected';
  const isConnecting = status === 'Connecting';

  // Add structured log to console helper
  const addLog = useCallback((message: string) => {
    const timeStr = new Date().toISOString().split('T')[1].slice(0, -1);
    console.log(`[${timeStr}] ${message}`);
  }, []);

  // Check Backend health on mount
  useEffect(() => {
    async function checkHealth() {
      try {
        const base = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
        const res = await fetch(`${base}/health`);
        if (res.ok) {
          const data = await res.json();
          if (data.status === 'ok') {
            setBackendStatus('OK');
            setBackendConnected(true);
            return;
          }
        }
        setBackendStatus('Unhealthy');
      } catch (e) {
        setBackendStatus('Offline');
        setBackendConnected(false);
      }
    }
    checkHealth();
    const interval = setInterval(checkHealth, 5000);
    return () => clearInterval(interval);
  }, []);

  // Setup local English Speech Recognition (Web Speech API ASR as local preview)
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
      if (SpeechRecognition) {
        const rec = new SpeechRecognition();
        rec.continuous = true;
        rec.interimResults = true;
        rec.lang = 'en-US';

        rec.onresult = (event: any) => {
          let interimTranscript = '';
          let newlyFinalized = '';

          for (let i = event.resultIndex; i < event.results.length; ++i) {
            const transcript = event.results[i][0].transcript;
            if (event.results[i].isFinal) {
              newlyFinalized += transcript;
            } else {
              interimTranscript += transcript;
            }
          }

          if (newlyFinalized) {
            setEnglishTranscript((prev) => {
              const base = prev.split(' ').slice(-30).join(' '); // Limit memory buffer size
              return (base + ' ' + newlyFinalized).trim();
            });
            setEnglishInterim('');
            addLog(`ASR Preview Input (Finalized): "${newlyFinalized}"`);
          } else if (interimTranscript) {
            setEnglishInterim(interimTranscript);
          }
        };

        rec.onerror = (e: any) => {
          console.error('Local Speech Recognition Error:', e);
        };

        rec.onend = () => {
          if (isSpeechRecognitionActive) {
            try {
              rec.start();
            } catch (err) {
              // Ignore restart conflicts
            }
          }
        };

        recognitionRef.current = rec;
      }
    }
  }, [isSpeechRecognitionActive, addLog]);

  // Register pcmPlayer callbacks
  useEffect(() => {
    pcmPlayer.onPlaybackStart = () => {
      setPlaybackState('playing');
      addLog('Audio Playback Started (Gemini Stream)');
    };
    pcmPlayer.onPlaybackEnd = () => {
      setPlaybackState('idle');
      addLog('Audio Playback Completed (Gemini Stream)');
    };
    return () => {
      pcmPlayer.stop();
    };
  }, [addLog]);

  // Sync speech recognition lifecycle with LiveKit room state
  useEffect(() => {
    if (status === 'Connected') {
      setIsSpeechRecognitionActive(true);
      setEnglishTranscript('');
      setEnglishInterim('');
      setSpanishTranslation('');
      totalAudioEventsPlayedRef.current = 0;
      addLog('Connection Established. Listening for active speech...');
      if (recognitionRef.current) {
        try {
          recognitionRef.current.start();
        } catch (e) {
          console.error('Failed to start SpeechRecognition:', e);
        }
      }
    } else {
      setIsSpeechRecognitionActive(false);
      if (recognitionRef.current) {
        try {
          recognitionRef.current.stop();
        } catch (e) {}
      }
    }
  }, [status, addLog]);

  // Process incoming AI pipeline events delivered from LiveKit Data channel
  useEffect(() => {
    const totalEvents = aiEvents.length;
    if (totalEvents === 0) {
      processedEventsCountRef.current = 0;
      return;
    }

    const previousCount = processedEventsCountRef.current;
    const newEvents = aiEvents.slice(previousCount);
    processedEventsCountRef.current = totalEvents;

    if (newEvents.length === 0) return;

    console.log(`[EFFECT TRIGGERED] Previous=${previousCount}, Current=${totalEvents}, New=${newEvents.length}`);
    console.table(newEvents.map(e => ({ type: e.type, timestamp: e.timestamp })));

    newEvents.forEach((event) => {
      if (event.type === 'AIStartedEvent') {
        addLog(`Chunk Received: ID: ${event.payload.chunk_id} | Sequence: ${event.payload.sequence_number}`);
      } 
      
      else if (event.type === 'AIPartialEvent') {
        const textDelta = event.payload.text_delta || '';
        if (textDelta) {
          setSpanishTranslation((prev) => {
            const words = prev.split(' ').slice(-30).join(' ');
            return (words + ' ' + textDelta).trim();
          });
        }
      } 
      
      else if (event.type === 'AICompletedEvent') {
        const fullText = event.payload.full_text || '';
        const metrics = event.metrics;
        const aiLatency = Math.round(metrics.total_ai_latency_ms || event.payload.duration_ms || 0);
        addLog(`Translation Completed: ID: ${event.payload.chunk_id} | Result: "${fullText}" (Latency: ${aiLatency}ms)`);
      } 
      
      else if (event.type === 'TranslationFailedEvent' || event.type === 'AIErrorEvent') {
        const errMsg = event.payload.error_message || 'Translation pipeline failure';
        setErrorMessage(errMsg);
        addLog(`Error Event: ID: ${event.payload.chunk_id} | ${errMsg}`);
      }
      
      else if (event.type === 'StreamingPartialTranslationEvent') {
        const textDelta = event.payload.text_delta || '';
        const cumulativeText = event.payload.cumulative_text || '';
        
        if (cumulativeText) {
          setSpanishTranslation(cumulativeText);
        } else if (textDelta) {
          setSpanishTranslation((prev) => (prev + ' ' + textDelta).trim());
        }
        
        addLog(`Streaming Spanish Transcript: "${textDelta}"`);
      }
      
      else if (event.type === 'StreamingTranslationAudioEvent') {
        totalAudioEventsPlayedRef.current++;
        console.log(`[UI PROCESS] Event Index: ${totalAudioEventsPlayedRef.current} | Net Received: ${totalAudioEventsReceivedRef?.current} | PCM playChunk() Called: ${pcmPlayer.playChunkCalledCount} | PCM Scheduled: ${pcmPlayer.playChunkScheduledCount} | PCM Playback Start Events: ${pcmPlayer.playbackStartEventCount} | PCM Playback End Events: ${pcmPlayer.playbackEndEventCount}`);
        const audioData = event.payload.audio_data || '';
        const participantIdentity = event.payload.participant_identity || '';
        // Echo Cancellation (Suggestion 3): 
        // Only play the translated audio if it was NOT originally spoken by the local participant, or if loopback is enabled.
        if (audioData && (hearOwnTranslation || participantIdentity !== identityInput)) {
          addLog(`StreamingTranslationAudioEvent received (${audioData.length} chars) from speaker ${participantIdentity}`);
          pcmPlayer.playChunk(audioData);
        } else if (audioData) {
          addLog(`Echo Cancellation: Ignored playing our own translation audio.`);
        }
      }
      
      else if (event.type === 'StreamingTranslationCompletedEvent') {
        const fullText = event.payload.full_text || '';
        if (fullText) {
          setSpanishTranslation(fullText);
        }
        addLog(`Translation Event Completed.`);
      }
      
      else if (event.type === 'StreamingRuntimeErrorEvent') {
        const errMsg = event.payload.error_message || 'Runtime streaming error';
        setErrorMessage(errMsg);
        addLog(`Gemini Streaming Error: ${errMsg}`);
      }
    });
  }, [aiEvents, addLog, identityInput, hearOwnTranslation]);

  // Auto-scroll scrollable areas
  useEffect(() => {
    if (englishScrollRef.current) {
      englishScrollRef.current.scrollTop = englishScrollRef.current.scrollHeight;
    }
  }, [englishTranscript, englishInterim]);

  useEffect(() => {
    if (spanishScrollRef.current) {
      spanishScrollRef.current.scrollTop = spanishScrollRef.current.scrollHeight;
    }
  }, [spanishTranslation]);

  // Connect handler
  const handleConnect = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!roomNameInput.trim() || !identityInput.trim()) return;
    setErrorMessage(null);
    try {
      await connect(roomNameInput, identityInput, hearOwnTranslation);
    } catch (err: any) {
      setErrorMessage(err.message || 'Room connection failed.');
    }
  };

  // Disconnect handler
  const handleDisconnect = async () => {
    setPlaybackState('idle');
    pcmPlayer.stop();
    await disconnect();
  };



  return (
    <main className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans selection:bg-indigo-500/30 selection:text-indigo-200">
      
      {/* Top Header */}
      <header className="border-b border-slate-900 bg-slate-950/80 backdrop-blur-md sticky top-0 z-40 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="w-3 h-3 rounded-full bg-indigo-500 shadow-[0_0_12px_rgba(99,102,241,0.5)] animate-pulse" />
          <h1 className="text-lg font-bold tracking-tight bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent">
            OneMeta Speech-to-Speech POC
          </h1>
        </div>
        
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2 text-xs">
            <span className="text-slate-500">Backend:</span>
            <span className={`px-2 py-0.5 rounded font-mono font-semibold uppercase ${
              backendConnected ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
            }`}>
              {backendStatus}
            </span>
          </div>
        </div>
      </header>

      {/* Main Container Grid */}
      <div className="flex-1 p-6 max-w-7xl w-full mx-auto grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Left Hand side configuration and status cards */}
        <div className="lg:col-span-1 space-y-6">
          
          {/* Connection card */}
          <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl">
            <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
              <span className="w-1.5 h-3 bg-indigo-500 rounded" />
              <span>Control Panel</span>
            </h2>
            
            {!isConnected ? (
              <form onSubmit={handleConnect} className="space-y-4">
                <div>
                  <label htmlFor="room-name-input" className="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1.5">
                    Room Name
                  </label>
                  <input
                    id="room-name-input"
                    type="text"
                    value={roomNameInput}
                    onChange={(e) => setRoomNameInput(e.target.value)}
                    disabled={isConnecting}
                    className="w-full bg-slate-950/80 border border-slate-880 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 rounded-xl px-4 py-2.5 text-slate-200 text-sm focus:outline-none transition disabled:opacity-50"
                    required
                  />
                </div>
                <div>
                  <label htmlFor="identity-input" className="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1.5">
                    Participant Identity
                  </label>
                  <input
                    id="identity-input"
                    type="text"
                    value={identityInput}
                    onChange={(e) => setIdentityInput(e.target.value)}
                    disabled={isConnecting}
                    className="w-full bg-slate-950/80 border border-slate-880 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 rounded-xl px-4 py-2.5 text-slate-200 text-sm focus:outline-none transition disabled:opacity-50"
                    required
                  />
                </div>
                <div className="flex items-center space-x-2 py-1">
                  <input
                    id="hear-own-translation"
                    type="checkbox"
                    checked={hearOwnTranslation}
                    onChange={(e) => setHearOwnTranslation(e.target.checked)}
                    className="rounded bg-slate-950 border-slate-800 text-indigo-650 focus:ring-indigo-500 focus:ring-offset-slate-900 cursor-pointer"
                  />
                  <label htmlFor="hear-own-translation" className="text-[11px] text-slate-400 font-semibold cursor-pointer select-none">
                    Hear Own Translation (Local Loopback Test)
                  </label>
                </div>
                <button
                  type="submit"
                  disabled={isConnecting || !backendConnected}
                  className="w-full bg-indigo-600 hover:bg-indigo-500 active:bg-indigo-700 disabled:bg-slate-800 text-white font-semibold text-sm rounded-xl py-3 transition shadow-lg shadow-indigo-600/20 disabled:shadow-none"
                >
                  {isConnecting ? 'Connecting...' : 'Start Session'}
                </button>
              </form>
            ) : (
              <div className="space-y-4">
                <div className="bg-indigo-950/20 border border-indigo-500/20 rounded-xl p-4 text-xs space-y-2">
                  <div className="flex justify-between">
                    <span className="text-slate-400">Connected Room:</span>
                    <span className="font-mono text-indigo-400 font-bold">{roomNameInput}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">User Identity:</span>
                    <span className="font-mono text-indigo-400 font-bold">{identityInput}</span>
                  </div>
                  <div className="flex items-center justify-between pt-2 mt-1 border-t border-indigo-500/10">
                    <span className="text-slate-400">Local Loopback (Hear Own Voice):</span>
                    <input
                      type="checkbox"
                      checked={hearOwnTranslation}
                      onChange={(e) => setHearOwnTranslation(e.target.checked)}
                      className="rounded bg-slate-950 border-slate-800 text-indigo-650 focus:ring-indigo-500 focus:ring-offset-slate-900 cursor-pointer"
                    />
                  </div>
                </div>

                <div className="flex space-x-3">
                  <button
                    onClick={toggleMicrophone}
                    className={`flex-1 flex items-center justify-center space-x-2 border rounded-xl py-2.5 text-xs font-semibold transition ${
                      isMicrophoneEnabled 
                        ? 'bg-slate-850 hover:bg-slate-800 border-slate-700 text-slate-350' 
                        : 'bg-rose-500/10 hover:bg-rose-500/20 border-rose-500/30 text-rose-450'
                    }`}
                  >
                    <span>{isMicrophoneEnabled ? 'Mute Mic' : 'Unmute Mic'}</span>
                  </button>
                  <button
                    onClick={handleDisconnect}
                    className="flex-1 bg-rose-600 hover:bg-rose-500 text-white font-semibold text-xs rounded-xl py-2.5 transition"
                  >
                    Leave Session
                  </button>
                </div>
              </div>
            )}

            {(error || errorMessage) && (
              <div className="mt-4 bg-rose-500/10 border border-rose-500/20 text-rose-450 text-xs rounded-xl p-3">
                <p className="font-mono font-medium">{error || errorMessage}</p>
              </div>
            )}
          </div>
        </div>

        {/* Center/Right primary panels */}
        <div className="lg:col-span-2 space-y-6">

          {/* Main User Card (User A English to User B Spanish) */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <TranscriptCard 
              englishTranscript={englishTranscript + (englishInterim ? ' ' + englishInterim : '')} 
              englishScrollRef={englishScrollRef} 
            />
            <TranslationCard 
              spanishTranslation={spanishTranslation} 
              spanishScrollRef={spanishScrollRef} 
              playbackState={playbackState} 
            />
          </div>

        </div>

      </div>

      {/* Footer bar */}
      <footer className="border-t border-slate-900/80 bg-slate-950/80 py-4 px-6 text-center text-xs text-slate-500">
        OneMeta Speech-to-Speech Proof of Concept v0.2.0 (Powered by Local Gemma 4 & LiveKit)
      </footer>
    </main>
  );
}
