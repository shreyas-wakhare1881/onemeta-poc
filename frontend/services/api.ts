export interface LiveKitTokenResponse {
  token: string;
  url: string;
}

export async function getHealth() {
  const base = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000'
  const res = await fetch(`${base}/health`)
  if (!res.ok) throw new Error('Network error')
  return res.json()
}

export async function getLiveKitToken(roomName: string, identity: string): Promise<LiveKitTokenResponse> {
  const base = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000'
  const res = await fetch(`${base}/api/livekit/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ room_name: roomName, identity }),
  })
  if (!res.ok) {
    const errorData = await res.json().catch(() => ({}));
    throw new Error(errorData.detail || `HTTP error ${res.status}: Failed to retrieve token`);
  }
  return res.json()
}
