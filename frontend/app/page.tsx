"use client";

import { useEffect, useState, useRef, useCallback } from 'react';
import { useLiveKit } from '../hooks/useLiveKit';
import { ttsService } from '../services/tts.service';

// Subcomponent imports
import TranscriptCard from '../components/TranscriptCard';
import TranslationCard from '../components/TranslationCard';
import PipelineVisualizer from '../components/PipelineVisualizer';
import TelemetryDashboard from '../components/TelemetryDashboard';
import ChunkHistoryList, { ChunkHistoryItem } from '../components/ChunkHistoryList';
import TimelineVisualizer, { TimelineEvent } from '../components/TimelineVisualizer';
import LogsConsole from '../components/LogsConsole';

export default function Home() {
  const [backendStatus, setBackendStatus] = useState<string>('Checking...');
  const [backendConnected, setBackendConnected] = useState<boolean>(false);

  // Connection config
  const [roomNameInput, setRoomNameInput] = useState<string>('onemeta-demo');
  const [identityInput, setIdentityInput] = useState<string>('User-A');

  // Developer Mode switch
  const [developerMode, setDeveloperMode] = useState<boolean>(true);

  // Audio / translation text state slices
  const [englishTranscript, setEnglishTranscript] = useState<string>('');
  const [spanishTranslation, setSpanishTranslation] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Pipeline execution state slices
  const [pipelineState, setPipelineState] = useState<'Idle' | 'Listening' | 'Speech Detected' | 'Chunk Processing' | 'Translating' | 'Playing Audio' | 'Completed' | 'Error'>('Idle');
  const [playbackState, setPlaybackState] = useState<'idle' | 'playing'>('idle');
  const [chunkHistory, setChunkHistory] = useState<ChunkHistoryItem[]>([]);
  const [lastTimelineEvents, setLastTimelineEvents] = useState<TimelineEvent[]>([]);

  // Logs buffer for developer mode
  const [devLogs, setDevLogs] = useState<string[]>([]);

  const englishScrollRef = useRef<HTMLDivElement>(null);
  const spanishScrollRef = useRef<HTMLDivElement>(null);
  const logsScrollRef = useRef<HTMLDivElement>(null);
  const recognitionRef = useRef<any>(null);
  const [isSpeechRecognitionActive, setIsSpeechRecognitionActive] = useState<boolean>(false);

  const {
    status,
    error,
    isMicrophoneEnabled,
    aiEvents,
    telemetryData,
    connect,
    disconnect,
    toggleMicrophone,
  } = useLiveKit();

  const isConnected = status === 'Connected';
  const isConnecting = status === 'Connecting';

  // Add structured log to buffer helper
  const addLog = useCallback((message: string) => {
    const timeStr = new Date().toISOString().split('T')[1].slice(0, -1);
    setDevLogs((prev) => [...prev.slice(-99), `[${timeStr}] ${message}`]);
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
          let finalTranscript = '';

          for (let i = event.resultIndex; i < event.results.length; ++i) {
            if (event.results[i].isFinal) {
              finalTranscript += event.results[i][0].transcript;
            } else {
              interimTranscript += event.results[i][0].transcript;
            }
          }

          if (finalTranscript || interimTranscript) {
            setEnglishTranscript((prev) => {
              const base = prev.split(' ').slice(-30).join(' '); // Limit memory buffer size
              return (base + ' ' + finalTranscript + interimTranscript).trim();
            });
            setPipelineState('Speech Detected');
            addLog(`ASR Preview Input: "${finalTranscript || interimTranscript}"`);
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

  // Sync speech recognition lifecycle with LiveKit room state
  useEffect(() => {
    if (status === 'Connected') {
      setPipelineState('Listening');
      setIsSpeechRecognitionActive(true);
      setEnglishTranscript('');
      setSpanishTranslation('');
      setChunkHistory([]);
      setLastTimelineEvents([]);
      setDevLogs([]);
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
      if (status === 'Disconnected') {
        setPipelineState('Idle');
      } else if (status === 'Failed') {
        setPipelineState('Error');
      }
    }
  }, [status, addLog]);

  // Process incoming AI pipeline events delivered from LiveKit Data channel
  useEffect(() => {
    if (aiEvents.length === 0) return;

    const event = aiEvents[aiEvents.length - 1];
    const timestamp = event.timestamp || Date.now() / 1000;

    if (event.type === 'AIStartedEvent') {
      setPipelineState('Chunk Processing');
      addLog(`Chunk Received: ID: ${event.payload.chunk_id} | Sequence: ${event.payload.sequence_number}`);
      
      setChunkHistory((prev) => {
        const exists = prev.some((c) => c.chunk_id === event.payload.chunk_id);
        if (exists) return prev;
        return [
          ...prev,
          {
            chunk_id: event.payload.chunk_id,
            sequence: event.payload.sequence_number,
            duration: 0,
            ttft: 0,
            latency: 0,
            status: 'Processing',
            t_started: timestamp
          }
        ];
      });

      const metrics = event.metrics;
      const start_t = metrics.start_timestamp || 0;
      const end_t = metrics.end_timestamp || 0;
      const chunkDur = Math.round((end_t - start_t) * 1000) || 130;

      setLastTimelineEvents([
        { label: 'Speech Started', time_ms: 0 },
        { label: 'Chunk Created', time_ms: chunkDur }
      ]);
    } 
    
    else if (event.type === 'AIPartialEvent') {
      setPipelineState('Translating');
      const textDelta = event.payload.text_delta || '';
      
      if (textDelta) {
        setSpanishTranslation((prev) => {
          const words = prev.split(' ').slice(-30).join(' ');
          return (words + ' ' + textDelta).trim();
        });
        
        setChunkHistory((prev) =>
          prev.map((c) => {
            if (c.chunk_id === event.payload.chunk_id && c.status === 'Processing') {
              return { ...c, status: 'Translating' };
            }
            return c;
          })
        );
      }
    } 
    
    else if (event.type === 'AICompletedEvent') {
      setPipelineState('Completed');
      const fullText = event.payload.full_text || '';
      const metrics = event.metrics;
      const chunkDur = Math.round(metrics.chunk_duration_ms || 0);
      const ttft = Math.round(metrics.ttft_ms || 0);
      const aiLatency = Math.round(metrics.total_ai_latency_ms || event.payload.duration_ms || 0);

      addLog(`Translation Completed: ID: ${event.payload.chunk_id} | Result: "${fullText}" (Latency: ${aiLatency}ms)`);

      setChunkHistory((prev) =>
        prev.map((c) => {
          if (c.chunk_id === event.payload.chunk_id) {
            return {
              ...c,
              duration: chunkDur || 2000,
              ttft: ttft,
              latency: aiLatency,
              status: 'Complete'
            };
          }
          return c;
        })
      );

      setLastTimelineEvents([
        { label: 'Speech Started', time_ms: 0 },
        { label: 'Chunk Created', time_ms: chunkDur },
        { label: 'First Token (TTFT)', time_ms: chunkDur + ttft },
        { label: 'Completed (Total Latency)', time_ms: chunkDur + aiLatency }
      ]);

      // Play Spanish Translation via TTS Service
      if (fullText.trim()) {
        ttsService.speak(
          fullText,
          () => {
            setPlaybackState('playing');
            setPipelineState('Playing Audio');
            addLog(`Audio Playback Started: "${fullText}"`);
            setLastTimelineEvents((prev) => {
              const clean = prev.filter((p) => p.label !== 'Playback Started');
              return [...clean, { label: 'Playback Started', time_ms: chunkDur + aiLatency + 15 }];
            });
          },
          () => {
            setPlaybackState('idle');
            setPipelineState('Completed');
            addLog('Audio Playback Finished.');
            setLastTimelineEvents((prev) => {
              const clean = prev.filter((p) => p.label !== 'Playback Finished');
              return [...clean, { label: 'Playback Finished', time_ms: chunkDur + aiLatency + 1200 }];
            });
          }
        );
      }
    } 
    
    else if (event.type === 'TranslationFailedEvent' || event.type === 'AIErrorEvent') {
      setPipelineState('Error');
      const errMsg = event.payload.error_message || 'Translation pipeline failure';
      setErrorMessage(errMsg);
      addLog(`Error Event: ID: ${event.payload.chunk_id} | ${errMsg}`);
      
      setChunkHistory((prev) =>
        prev.map((c) => {
          if (c.chunk_id === event.payload.chunk_id) {
            return { ...c, status: 'Failed' };
          }
          return c;
        })
      );
    }
  }, [aiEvents, chunkHistory, addLog]);

  // Auto-scroll scrollable areas
  useEffect(() => {
    if (englishScrollRef.current) {
      englishScrollRef.current.scrollTop = englishScrollRef.current.scrollHeight;
    }
  }, [englishTranscript]);

  useEffect(() => {
    if (spanishScrollRef.current) {
      spanishScrollRef.current.scrollTop = spanishScrollRef.current.scrollHeight;
    }
  }, [spanishTranslation]);

  useEffect(() => {
    if (logsScrollRef.current) {
      logsScrollRef.current.scrollTop = logsScrollRef.current.scrollHeight;
    }
  }, [devLogs]);

  // Connect handler
  const handleConnect = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!roomNameInput.trim() || !identityInput.trim()) return;
    setErrorMessage(null);
    try {
      await connect(roomNameInput, identityInput);
    } catch (err: any) {
      setErrorMessage(err.message || 'Room connection failed.');
    }
  };

  // Disconnect handler
  const handleDisconnect = async () => {
    setPipelineState('Idle');
    setPlaybackState('idle');
    ttsService.stop();
    await disconnect();
  };

  const aiTelemetry = telemetryData?.ai;

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
          {/* Developer Mode Toggle */}
          <div className="flex items-center space-x-2 bg-slate-900/60 border border-slate-800 rounded-lg p-1">
            <span className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider px-2">Dev Mode</span>
            <button
              id="dev-mode-toggle"
              onClick={() => setDeveloperMode(!developerMode)}
              className={`text-xs font-bold px-3 py-1 rounded transition ${
                developerMode ? 'bg-indigo-600 text-white shadow-sm' : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              {developerMode ? 'ON' : 'OFF'}
            </button>
          </div>

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

          {/* Operational State Card */}
          <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl">
            <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
              <span className="w-1.5 h-3 bg-indigo-500 rounded" />
              <span>Session Status</span>
            </h2>
            <div className="flex items-center space-x-4">
              <div className="relative">
                <div className={`w-12 h-12 rounded-full flex items-center justify-center font-bold text-xs uppercase transition border ${
                  pipelineState === 'Error' ? 'bg-rose-500/10 border-rose-500 text-rose-400' :
                  pipelineState === 'Translating' || pipelineState === 'Chunk Processing' ? 'bg-indigo-500/10 border-indigo-500 text-indigo-400' :
                  pipelineState === 'Playing Audio' ? 'bg-violet-500/10 border-violet-500 text-violet-400' :
                  isConnected ? 'bg-emerald-500/10 border-emerald-500 text-emerald-400' : 'bg-slate-950 border-slate-850 text-slate-500'
                }`}>
                  {pipelineState === 'Error' ? 'ERR' : 
                   pipelineState === 'Translating' ? 'AI' :
                   pipelineState === 'Playing Audio' ? 'TTS' :
                   isConnected ? 'LIVE' : 'OFF'}
                </div>
                {isConnected && pipelineState !== 'Idle' && (
                  <span className="absolute -top-1 -right-1 flex h-3.5 w-3.5">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                    <span className="relative inline-flex rounded-full h-3.5 w-3.5 bg-emerald-500"></span>
                  </span>
                )}
              </div>
              <div>
                <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Current Pipeline State</p>
                <p className="text-sm font-bold text-white mt-0.5">{pipelineState}</p>
              </div>
            </div>
          </div>

          {/* Quick Latency statistics summary */}
          <div className="bg-slate-900/60 backdrop-blur-md border border-slate-800/80 rounded-2xl p-6 shadow-xl">
            <h2 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center space-x-2">
              <span className="w-1.5 h-3 bg-indigo-500 rounded" />
              <span>Performance Summary</span>
            </h2>
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
                <p className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">Avg TTFT</p>
                <p className="text-xl font-bold text-indigo-400 mt-1">
                  {isConnected && aiTelemetry ? `${Math.round(aiTelemetry.avg_first_token_latency_ms || 0)} ms` : '--'}
                </p>
              </div>
              <div className="bg-slate-950/60 border border-slate-850 rounded-xl p-3">
                <p className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">Avg Latency</p>
                <p className="text-xl font-bold text-indigo-400 mt-1">
                  {isConnected && aiTelemetry ? `${Math.round(aiTelemetry.avg_total_ai_latency_ms || 0)} ms` : '--'}
                </p>
              </div>
            </div>
          </div>

        </div>

        {/* Center/Right primary panels */}
        <div className="lg:col-span-2 space-y-6">

          {/* Main User Card (User A English to User B Spanish) */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <TranscriptCard 
              englishTranscript={englishTranscript} 
              englishScrollRef={englishScrollRef} 
            />
            <TranslationCard 
              spanishTranslation={spanishTranslation} 
              spanishScrollRef={spanishScrollRef} 
              playbackState={playbackState} 
            />
          </div>

          {/* Pipeline stage visualizer */}
          <PipelineVisualizer 
            pipelineState={pipelineState} 
            playbackState={playbackState} 
            isConnected={isConnected} 
          />

          {/* Developer dashboard panels: only visible in dev mode */}
          {developerMode && (
            <div className="space-y-6 animate-fadeIn">
              <TelemetryDashboard telemetryData={telemetryData} />
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChunkHistoryList chunkHistory={chunkHistory} />
                <TimelineVisualizer lastTimelineEvents={lastTimelineEvents} />
              </div>

              <LogsConsole devLogs={devLogs} logsScrollRef={logsScrollRef} />
            </div>
          )}

        </div>

      </div>

      {/* Footer bar */}
      <footer className="border-t border-slate-900/80 bg-slate-950/80 py-4 px-6 text-center text-xs text-slate-500">
        OneMeta Speech-to-Speech Proof of Concept v0.2.0 (Powered by Local Gemma 4 & LiveKit)
      </footer>
    </main>
  );
}
