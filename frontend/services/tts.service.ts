export interface ITTSService {
  speak(text: string, onStart?: () => void, onEnd?: () => void): void;
  stop(): void;
  setVoice(lang: string): void;
}

export class BrowserTTSProvider implements ITTSService {
  private synth: SpeechSynthesis | null = null;
  private activeUtterance: SpeechSynthesisUtterance | null = null;
  private queue: Array<{ text: string; onStart?: () => void; onEnd?: () => void }> = [];
  private speaking = false;
  private lang = 'es-ES'; // Default Spanish voice

  constructor() {
    if (typeof window !== 'undefined' && window.speechSynthesis) {
      this.synth = window.speechSynthesis;
    }
  }

  public setVoice(lang: string) {
    this.lang = lang;
  }

  public speak(text: string, onStart?: () => void, onEnd?: () => void): void {
    if (!this.synth) {
      console.warn('Speech synthesis not supported in this browser environment.');
      onStart?.();
      onEnd?.();
      return;
    }

    // Capacity limit of 20 items. If overflow, drop oldest pending item in the queue.
    // Do NOT interrupt currently playing audio.
    if (this.queue.length >= 20) {
      console.warn('TTS Queue capacity exceeded (limit: 20). Evicting oldest pending item.');
      this.queue.shift(); // Evicts the first pending item in the queue (oldest)
    }

    // Queue text segment
    this.queue.push({ text, onStart, onEnd });
    this.processQueue();
  }

  public stop(): void {
    this.queue = [];
    if (this.synth) {
      this.synth.cancel();
    }
    this.speaking = false;
    this.activeUtterance = null;
  }

  private processQueue() {
    if (this.speaking || this.queue.length === 0 || !this.synth) {
      return;
    }

    const item = this.queue.shift();
    if (!item) return;

    this.speaking = true;
    
    const utterance = new SpeechSynthesisUtterance(item.text);
    utterance.lang = this.lang;
    this.activeUtterance = utterance;

    utterance.onstart = () => {
      item.onStart?.();
    };

    const doneHandler = () => {
      this.speaking = false;
      this.activeUtterance = null;
      item.onEnd?.();
      // Schedule next item execution asynchronously
      setTimeout(() => this.processQueue(), 20);
    };

    utterance.onend = doneHandler;
    utterance.onerror = (e) => {
      console.error('SpeechSynthesis playback error:', e);
      doneHandler();
    };

    this.synth.speak(utterance);
  }
}

export const ttsService: ITTSService = new BrowserTTSProvider();
