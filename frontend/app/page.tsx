"use client";
import { useEffect, useState } from 'react'
import { getHealth } from '../services/api'

export default function Home() {
  const [backendStatus, setBackendStatus] = useState<string>('Unknown')
  const [connected, setConnected] = useState<boolean>(false)

  useEffect(() => {
    async function check() {
      try {
        const res = await getHealth()
        if (res && res.status === 'ok') {
          setBackendStatus('OK')
          setConnected(true)
        } else {
          setBackendStatus('Unhealthy')
        }
      } catch (e) {
        setBackendStatus('Unavailable')
      }
    }
    check()
  }, [])

  return (
    <main className="p-8 font-sans">
      <h1 className="text-3xl font-bold mb-4">OneMeta Speech-to-Speech POC</h1>
      <p className="mb-2">Project initialization status: Minimal scaffold created</p>
      <p>
        Backend connection status:{' '}
        <strong>{connected ? 'Backend Connected' : backendStatus}</strong>
      </p>
    </main>
  )
}
