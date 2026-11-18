import { useState, useEffect } from 'react';

export default function TelemetryStrip({
  instanceId = 'api-render-1',
  asOf = '2026-11-18 23:14:55 UTC',
  detectionLag = 'p95 1.4s',
  onChainSummary = '426 MATCH · 0 MISMATCH',
  wsStatus = 'connected', // 'connected' | 'connecting' | 'fallback' | 'disconnected'
  isStale = false,
  onInstanceCycle,
}) {
  const [currentInstance, setCurrentInstance] = useState(instanceId);

  useEffect(() => {
    if (instanceId) {
      setCurrentInstance(instanceId);
    }
  }, [instanceId]);

  const handleCycle = () => {
    const replicas = ['api-render-1', 'api-render-2'];
    const next = currentInstance === replicas[0] ? replicas[1] : replicas[0];
    setCurrentInstance(next);
    if (onInstanceCycle) onInstanceCycle(next);
  };

  let wsText = 'WS connected (PG LISTEN)';
  let dotClass = 'dot-live';

  if (wsStatus === 'fallback' || wsStatus === 'connecting') {
    wsText = 'WS reconnecting · polling fallback';
    dotClass = 'dot-live warn';
  } else if (wsStatus === 'disconnected') {
    wsText = 'WS disconnected · polling';
    dotClass = 'dot-live err';
  }

  if (isStale) {
    dotClass = 'dot-live warn';
    wsText = 'WS reconnecting · polling fallback';
  }

  return (
    <div className="telemetry">
      <div className="tel-item">
        <span className="tel-label">Replica:</span>
        <button
          className="inst-btn"
          onClick={handleCycle}
          title="Click to simulate load-balancer switching replicas"
        >
          X-Instance-Id: {currentInstance}
        </button>
      </div>

      <div className="tel-item">
        <span className="tel-label">Data as of:</span>
        <span className={`tel-val ${isStale ? 'warn' : ''}`}>{asOf}</span>
      </div>

      <div className="tel-item">
        <span className="tel-label">Detection Lag (block&rarr;ingest):</span>
        <span className="tel-val">{detectionLag}</span>
      </div>

      <div className="tel-item">
        <span className="tel-label">On-Chain:</span>
        <span className="tel-val pos">{onChainSummary}</span>
      </div>

      <span className="spacer" />

      <div className="tel-item">
        <span className={dotClass} />
        <span className="tel-val">{wsText}</span>
      </div>
    </div>
  );
}

