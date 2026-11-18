import { proxyToBackend } from '../../lib/backend';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  return proxyToBackend(req, res, '/health');
}

