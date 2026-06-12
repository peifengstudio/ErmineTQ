// ─── API error ────────────────────────────────────────────────────────────────

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

// ─── Base helpers ──────────────────────────────────────────────────────────────

const BASE = '/api'

async function parseError(res: Response): Promise<ApiError> {
  try {
    const body = (await res.json()) as { error?: string }
    return new ApiError(res.status, body.error ?? res.statusText)
  } catch {
    return new ApiError(res.status, res.statusText)
  }
}

export async function get<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  let url = BASE + path
  if (params) {
    const qs = new URLSearchParams()
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== '') qs.set(k, String(v))
    }
    const s = qs.toString()
    if (s) url += '?' + s
  }
  const res = await fetch(url)
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<T>
}

export async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<T>
}

export async function patch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<T>
}
