export const BACKEND_URL =
  process.env.BACKEND_API_URL ||
  process.env.NEXT_PUBLIC_API_URL ||
  'http://localhost:8000';

export async function proxyToBackend(req, res, targetPath, options = {}) {
  try {
    const url = new URL(targetPath, BACKEND_URL);
    const exclude = new Set(options.excludeQuery || []);

    // Forward query params
    if (req.query) {
      for (const [key, val] of Object.entries(req.query)) {
        if (!exclude.has(key) && val !== undefined && val !== null) {
          if (Array.isArray(val)) {
            val.forEach(v => url.searchParams.append(key, v));
          } else {
            url.searchParams.set(key, val);
          }
        }
      }
    }

    const headers = {
      Accept: 'application/json',
      ...options.headers,
    };
    if (process.env.POLYPULSE_API_KEY) {
      headers['X-API-Key'] = process.env.POLYPULSE_API_KEY;
    }

    const backendRes = await fetch(url.toString(), {
      method: req.method || 'GET',
      headers,
    });

    // Forward instance id header if available
    const instanceId = backendRes.headers.get('x-instance-id');
    if (instanceId) {
      res.setHeader('x-instance-id', instanceId);
    }

    const contentType = backendRes.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      const data = await backendRes.json();
      return res.status(backendRes.status).json(data);
    } else {
      const text = await backendRes.text();
      return res.status(backendRes.status).send(text);
    }
  } catch (err) {
    return res.status(502).json({
      error: 'Backend proxy error',
      details: err.message,
    });
  }
}
