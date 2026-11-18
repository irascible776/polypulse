import { proxyToBackend } from '../../../../lib/backend';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  const { addr } = req.query;
  return proxyToBackend(req, res, `/wallets/${addr}/analytics`, {
    excludeQuery: ['addr'],
  });
}

