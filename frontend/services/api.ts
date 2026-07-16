export async function getHealth() {
  const base = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000'
  const res = await fetch(`${base}/health`)
  if (!res.ok) throw new Error('Network error')
  return res.json()
}
