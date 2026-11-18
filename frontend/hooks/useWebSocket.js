import { useState, useEffect, useRef, useCallback } from 'react';

export function useWebSocket({ wallet, onMessage, onStatusChange }) {
  const [status, setStatus] = useState('disconnected'); // 'connecting' | 'connected' | 'disconnected' | 'fallback'
  const wsRef = useRef(null);
  const pingIntervalRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const delayRef = useRef(1000);
  const isMountedRef = useRef(true);

  const updateStatus = useCallback(
    (newStatus) => {
      setStatus(newStatus);
      if (onStatusChange) onStatusChange(newStatus);
    },
    [onStatusChange]
  );

  useEffect(() => {
    isMountedRef.current = true;
    if (!wallet) {
      updateStatus('disconnected');
      return;
    }

    function getWsUrl() {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      try {
        const parsed = new URL(apiUrl, typeof window !== 'undefined' ? window.location.href : 'http://localhost:3000');
        const proto = parsed.protocol === 'https:' ? 'wss:' : 'ws:';
        return `${proto}//${parsed.host}/ws/wallets/${wallet}`;
      } catch (e) {
        return `ws://localhost:8000/ws/wallets/${wallet}`;
      }
    }

    function connect() {
      if (!isMountedRef.current) return;
      updateStatus('connecting');

      const url = getWsUrl();
      let ws;
      try {
        ws = new WebSocket(url);
      } catch (err) {
        updateStatus('fallback');
        scheduleReconnect();
        return;
      }
      wsRef.current = ws;

      ws.onopen = () => {
        if (!isMountedRef.current) return;
        updateStatus('connected');
        delayRef.current = 1000;

        // Keepalive ping every 10s
        if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
        pingIntervalRef.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            try {
              ws.send('ping');
            } catch (e) {}
          }
        }, 10000);
      };

      ws.onmessage = (event) => {
        if (!isMountedRef.current) return;
        if (event.data === 'pong') return;
        try {
          const data = JSON.parse(event.data);
          if (onMessage) onMessage(data);
        } catch (e) {
          // Non-JSON message
        }
      };

      ws.onerror = () => {
        if (!isMountedRef.current) return;
        updateStatus('fallback');
      };

      ws.onclose = () => {
        if (!isMountedRef.current) return;
        if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
        updateStatus('fallback');
        scheduleReconnect();
      };
    }

    function scheduleReconnect() {
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = setTimeout(() => {
        if (isMountedRef.current) {
          delayRef.current = Math.min(delayRef.current * 1.5, 15000);
          connect();
        }
      }, delayRef.current);
    }

    connect();

    return () => {
      isMountedRef.current = false;
      if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch (e) {}
      }
    };
  }, [wallet, updateStatus, onMessage]);

  return { status };
}

