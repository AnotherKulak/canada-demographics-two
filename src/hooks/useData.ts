import { useState, useEffect } from 'react'

type Status = 'idle' | 'loading' | 'success' | 'error'

export function useData<T>(filename: string) {
  const [data, setData] = useState<T | null>(null)
  const [status, setStatus] = useState<Status>('idle')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setStatus('loading')
    fetch(`/data/${filename}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => { setData(d); setStatus('success') })
      .catch((e) => { setError(String(e)); setStatus('error') })
  }, [filename])

  return { data, status, loading: status === 'loading', error }
}
