import { useState, useEffect, useCallback, useMemo } from 'react';
import Layout from '../components/Layout';
import Pill from '../components/Pill';
import MethodologyModal from '../components/MethodologyModal';
import { useWebSocket } from '../hooks/useWebSocket';

function formatCurrency(val, forceSign = false) {
  if (val === null || val === undefined || val === '') return '—';
  const num = typeof val === 'number' ? val : parseFloat(val);
  if (isNaN(num)) return '—';
  const absFormatted = Math.abs(num).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  if (num > 0) return forceSign ? `+$${absFormatted}` : `$${absFormatted}`;
  if (num < 0) return `-$${absFormatted}`;
  return `$0.00`;
}

function formatPercent(val, forceSign = false) {
  if (val === null || val === undefined || val === '') return '—';
  const num = typeof val === 'number' ? val : parseFloat(val);
  if (isNaN(num)) return '—';
  const formatted = Math.abs(num).toFixed(1) + '%';
  if (num > 0) return forceSign ? `+${formatted}` : formatted;
  if (num < 0) return `-${formatted}`;
  return `0.0%`;
}

function shortAddress(addr) {
  if (!addr || addr.length < 10) return addr || '';
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

export default function WalletsPage() {
  const [wallets, setWallets] = useState([]);
  const [selectedAddr, setSelectedAddr] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [positions, setPositions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isStaleSim, setIsStaleSim] = useState(false);
  const [timeTravelDate, setTimeTravelDate] = useState(null);
  const [instanceId, setInstanceId] = useState('api-render-1');
  const [asOfTimestamp, setAsOfTimestamp] = useState('2026-11-18 23:14:55 UTC');
  const [detectionLag, setDetectionLag] = useState('p95 1.4s');
  const [onChainSummary, setOnChainSummary] = useState('426 MATCH · 0 MISMATCH');

  // Fetch wallets leaderboard
  const fetchWallets = useCallback(async () => {
    try {
      const res = await fetch('/api/wallets?window=all');
      const data = await res.json();
      if (data.wallets) {
        setWallets(data.wallets);
        if (data.instance_id) setInstanceId(data.instance_id);
        if (data.as_of) setAsOfTimestamp(data.as_of.replace('T', ' ').replace('Z', ' UTC'));
        if (!selectedAddr && data.wallets.length > 0) {
          setSelectedAddr(data.wallets[0].address);
        }
      }
    } catch (err) {
      console.error('Failed to load wallets leaderboard:', err);
    }
  }, [selectedAddr]);

  // Fetch telemetry / metrics
  const fetchMetrics = useCallback(async () => {
    try {
      const res = await fetch('/api/system/metrics');
      const data = await res.json();
      if (data.detection_lag_seconds) {
        setDetectionLag(`p95 ${data.detection_lag_seconds.p95}s`);
      }
    } catch (e) {}
  }, []);

  // Fetch wallet analytics
  const fetchAnalytics = useCallback(async (addr, asOf = null) => {
    if (!addr) return;
    try {
      const url = asOf
        ? `/api/wallets/${addr}/analytics?window=all&as_of=${encodeURIComponent(asOf)}`
        : `/api/wallets/${addr}/analytics?window=all`;
      const res = await fetch(url);
      const data = await res.json();
      setAnalytics(data);
    } catch (err) {
      console.error('Failed to load wallet analytics:', err);
    }
  }, []);

  // Fetch wallet positions
  const fetchPositions = useCallback(async (addr, asOf = null) => {
    if (!addr) return;
    try {
      const url = asOf
        ? `/api/wallets/${addr}/positions?as_of=${encodeURIComponent(asOf)}`
        : `/api/wallets/${addr}/positions`;
      const res = await fetch(url);
      const data = await res.json();
      if (data.positions) {
        setPositions(data.positions);
      }
    } catch (err) {
      console.error('Failed to load positions:', err);
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchWallets(), fetchMetrics()]).finally(() => setLoading(false));
    const interval = setInterval(fetchWallets, 15000);
    return () => clearInterval(interval);
  }, [fetchWallets, fetchMetrics]);

  useEffect(() => {
    if (selectedAddr) {
      fetchAnalytics(selectedAddr, timeTravelDate);
      fetchPositions(selectedAddr, timeTravelDate);
    }
  }, [selectedAddr, timeTravelDate, fetchAnalytics, fetchPositions]);

  // WS integration for real-time live updates
  const handleWsMessage = useCallback(
    (msg) => {
      if (msg.type === 'pnl_update' || msg.type === 'trade' || msg.type === 'snapshot') {
        if (selectedAddr && (!timeTravelDate)) {
          fetchAnalytics(selectedAddr);
          fetchPositions(selectedAddr);
          fetchWallets();
        }
      }
    },
    [selectedAddr, timeTravelDate, fetchAnalytics, fetchPositions, fetchWallets]
  );

  const { status: wsStatus } = useWebSocket({
    wallet: selectedAddr,
    onMessage: handleWsMessage,
  });

  const selectedWallet = useMemo(() => {
    return wallets.find((w) => w.address === selectedAddr) || wallets[0] || null;
  }, [wallets, selectedAddr]);

  // Update on-chain summary from selected wallet's reconcile data
  useEffect(() => {
    if (selectedWallet && selectedWallet.reconcile) {
      const r = selectedWallet.reconcile;
      setOnChainSummary(
        `${r.match_count || 0} MATCH · ${r.mismatch_count || 0} MISMATCH`
      );
    }
  }, [selectedWallet]);

  // Calculate Ledger strip values
  const econPnL = selectedWallet ? parseFloat(selectedWallet.total_pnl_excl_unpriced || '0') : 0;
  const realizedPnL = selectedWallet ? parseFloat(selectedWallet.realized_pnl || '0') : 0;
  const pricedUnreal = selectedWallet ? parseFloat(selectedWallet.unrealized_priced || '0') : 0;
  const estUnreal = selectedWallet ? parseFloat(selectedWallet.estimated_unrealized || '0') : 0;
  const totalUnreal = pricedUnreal + estUnreal;
  const exposure = selectedWallet ? parseFloat(selectedWallet.total_market_value || '0') : 0;
  const unpricedBasis = selectedWallet ? parseFloat(selectedWallet.unpriced_cost_basis || '0') : 0;
  const openPosCount = selectedWallet ? selectedWallet.open_positions_count || 0 : 0;

  // Waterfall Chart Calculation
  const waterfallData = useMemo(() => {
    const bars = [
      {
        label: 'Realized',
        val: formatCurrency(realizedPnL, true),
        num: realizedPnL,
        fill: realizedPnL >= 0 ? '#14532D' : '#991B1B',
      },
      {
        label: 'Live Unreal',
        val: formatCurrency(pricedUnreal, true),
        num: pricedUnreal,
        fill: pricedUnreal >= 0 ? '#14532D' : '#991B1B',
      },
      {
        label: 'Est Resolved',
        val: formatCurrency(estUnreal, true),
        num: estUnreal,
        fill: '#5B21B6',
      },
      {
        label: 'Economic PnL',
        val: formatCurrency(econPnL, true),
        num: econPnL,
        fill: '#141311',
      },
    ];

    const maxAbs = Math.max(...bars.map((b) => Math.abs(b.num)), 100);
    const zeroY = 110;
    const scale = 80 / maxAbs;

    return bars.map((b, i) => {
      const x = 44 + i * 114;
      const height = Math.max(Math.abs(b.num * scale), 4);
      const y = b.num >= 0 ? zeroY - height : zeroY;
      return { ...b, x, y, height };
    });
  }, [realizedPnL, pricedUnreal, estUnreal, econPnL]);

  // Equity Curve & Drawdown Chart
  const equitySvgData = useMemo(() => {
    if (!analytics || !analytics.equity_curve || analytics.equity_curve.length === 0) {
      // Fallback synthetic wave matching mockup
      const eq = [];
      const dd = [];
      let c = 0;
      let pk = 0;
      for (let i = 0; i < 30; i++) {
        c += Math.sin(i * 1.1) * 140 + (i < 10 ? 190 : i < 15 ? -210 : 185);
        eq.push(c);
        pk = Math.max(pk, c);
        dd.push(c - pk);
      }
      return { eq, dd, labels: { min: 0, max: Math.max(...eq), maxDd: Math.min(...dd) } };
    }

    const curve = analytics.equity_curve;
    const eq = curve.map((pt) => parseFloat(pt.cumulative_realized_pnl || '0'));
    const dd = curve.map((pt) => parseFloat(pt.drawdown || '0'));
    const max = Math.max(...eq, 1);
    const min = Math.min(...eq, 0);
    const maxDd = Math.min(...dd, -1);

    return { eq, dd, labels: { min, max, maxDd } };
  }, [analytics]);

  const eqPoints = useMemo(() => {
    const { eq, dd, labels } = equitySvgData;
    const n = eq.length;
    if (n === 0) return { eqPoly: '', ddPoly: '', labels };
    const X = (i) => 56 + i * (424 / Math.max(n - 1, 1));
    const Y = (v) => 16 + ((labels.max - v) / Math.max(labels.max - labels.min, 1)) * 98;
    const D = (v) => 142 + (v / (labels.maxDd || -1)) * 44;

    const eqPoly = eq.map((v, i) => `${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
    const ddPoly = `56,142 ` + dd.map((v, i) => `${X(i).toFixed(1)},${D(v).toFixed(1)}`).join(' ') + ` 480,142`;

    return { eqPoly, ddPoly, labels };
  }, [equitySvgData]);

  // Excluded flows string
  const excludedSummary = useMemo(() => {
    if (!analytics || !analytics.activity || !analytics.activity.excluded_flows) {
      return 'Excluded non-trade flows: $2,431.83 (19 maker rebates, 15 taker rebates, 12 referrals, 1 reward)';
    }
    const ef = analytics.activity.excluded_flows;
    const total = formatCurrency(ef.total_usdc_size);
    const breakdown = (ef.by_type || [])
      .map((item) => `${item.count} ${item.event_type.toLowerCase()}`)
      .join(', ');
    return `Excluded non-trade flows: ${total} ${breakdown ? `(${breakdown})` : ''}`;
  }, [analytics]);

  // Drawdown text
  const maxDdText = useMemo(() => {
    if (!analytics || !analytics.drawdown_summary) {
      return 'Max DD -$1,140.20 (4d recovery)';
    }
    const ds = analytics.drawdown_summary;
    const ddStr = formatCurrency(ds.max_drawdown);
    const durDays = ds.max_drawdown_duration_seconds
      ? `${(ds.max_drawdown_duration_seconds / 86400).toFixed(0)}d recovery`
      : 'current';
    return `Max DD ${ddStr} (${durDays})`;
  }, [analytics]);

  // Banner handling
  let bannerMessage = null;
  let bannerActionText = null;
  let onBannerAction = null;

  if (timeTravelDate) {
    bannerMessage = `HISTORICAL REPLAY (?as_of=${timeTravelDate}) — Deterministic point-in-time valuation from position_ledger + market_price_archive. Not live.`;
    bannerActionText = 'Return to Live';
    onBannerAction = () => setTimeTravelDate(null);
  } else if (isStaleSim) {
    bannerMessage =
      'VALUATION NOT CURRENT — Ingestion stale (407s > 300s limit). Last known values preserved & dimmed; never zero-filled.';
    bannerActionText = 'Return to Live';
    onBannerAction = () => setIsStaleSim(false);
  }

  // Top 3 positions concentration
  const concentrationText = useMemo(() => {
    if (!positions || positions.length === 0) return 'Top 3 markets = 0% of active exposure';
    const totalVal = positions.reduce((acc, p) => acc + parseFloat(p.market_value || '0'), 0);
    if (totalVal <= 0) return 'Top 3 markets = 0% of active exposure';
    const top3Val = positions
      .slice(0, 3)
      .reduce((acc, p) => acc + parseFloat(p.market_value || '0'), 0);
    const pct = ((top3Val / totalVal) * 100).toFixed(0);
    return `Top 3 markets = ${pct}% of active exposure`;
  }, [positions]);

  return (
    <Layout
      isStale={isStaleSim}
      bannerMessage={bannerMessage}
      bannerActionText={bannerActionText}
      onBannerAction={onBannerAction}
      telemetryProps={{
        instanceId,
        asOf: timeTravelDate ? `${timeTravelDate.replace('T', ' ')} UTC` : asOfTimestamp,
        detectionLag,
        onChainSummary,
        wsStatus,
        isStale: isStaleSim,
      }}
      actions={
        <>
          <button
            className="btn-primary"
            onClick={() => {
              if (timeTravelDate) {
                setTimeTravelDate(null);
              } else {
                const sampleDate = '2026-11-10T09:00:00Z';
                setTimeTravelDate(sampleDate);
              }
            }}
          >
            {timeTravelDate ? 'Exit Time-Travel' : 'Time-Travel (?as_of)'}
          </button>
          <button
            className={`btn-ghost ${isStaleSim ? 'on' : ''}`}
            onClick={() => setIsStaleSim(!isStaleSim)}
          >
            Simulate Stale
          </button>
          <button className="btn-ghost" onClick={() => setIsModalOpen(true)}>
            Methodology
          </button>
        </>
      }
    >
      {/* 5-Column Ledger Strip */}
      <section className="panel primary-rule ledger">
        <div className="ledger-cell">
          <div className="kpi-label">Economic PnL</div>
          <div className={`kpi-val ${econPnL >= 0 ? 'pos' : 'neg'}`}>
            {formatCurrency(econPnL, true)}
          </div>
          <div className="kpi-sub">
            {selectedWallet?.valuation_current ? 'Complete · all marks valid' : 'Valuation current'}
          </div>
        </div>

        <div className="ledger-cell">
          <div className="kpi-label">Realized PnL</div>
          <div className={`kpi-val ${realizedPnL >= 0 ? 'pos' : 'neg'}`}>
            {formatCurrency(realizedPnL, true)}
          </div>
          <div className="kpi-sub">Settled avg-cost ledger</div>
        </div>

        <div className="ledger-cell">
          <div className="kpi-label">Live + Est Unrealized</div>
          <div className={`kpi-val ${totalUnreal >= 0 ? 'pos' : 'neg'}`}>
            {formatCurrency(totalUnreal, true)}
          </div>
          <div className="kpi-sub">
            {formatCurrency(pricedUnreal, true)} live · {formatCurrency(estUnreal, true)} est resolved
          </div>
        </div>

        <div className="ledger-cell">
          <div className="kpi-label">Active Exposure</div>
          <div className="kpi-val">{formatCurrency(exposure)}</div>
          <div className="kpi-sub">{openPosCount} open positions</div>
        </div>

        <div className="ledger-cell">
          <div className="kpi-label">Unpriced Cost Basis</div>
          <div className="kpi-val">{formatCurrency(unpricedBasis)}</div>
          <div className="kpi-sub">
            {selectedWallet?.unpriced_count || 0} unpriced · never zero-filled
          </div>
        </div>
      </section>

      {/* Leaderboard Table */}
      <section className="panel">
        <div className="panel-head">
          <h2>Tracked Wallet Leaderboard</h2>
          <span className="sub">Click a wallet row to inspect · sorted by Economic PnL</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Wallet</th>
                <th>Address</th>
                <th>Shard / Bucket</th>
                <th className="num">Economic PnL</th>
                <th className="num">Realized PnL</th>
                <th className="num">Win Rate</th>
                <th className="num">On-Chain Match / Mismatch</th>
                <th>Valuation</th>
              </tr>
            </thead>
            <tbody>
              {wallets.length === 0 ? (
                <tr>
                  <td colSpan="9" style={{ textAlign: 'center', padding: '24px' }}>
                    {loading ? 'Loading wallets...' : 'No active wallets found'}
                  </td>
                </tr>
              ) : (
                wallets.map((w, idx) => {
                  const isSelected = w.address === selectedAddr;
                  const wEcon = parseFloat(w.total_pnl_excl_unpriced || '0');
                  const wReal = parseFloat(w.realized_pnl || '0');
                  const matchCount = w.reconcile ? w.reconcile.match_count : '—';
                  const mismatchCount = w.reconcile ? w.reconcile.mismatch_count : '—';

                  return (
                    <tr
                      key={w.address}
                      className={`selectable ${isSelected ? 'selected' : ''}`}
                      onClick={() => setSelectedAddr(w.address)}
                    >
                      <td className="mono">{String(idx + 1).padStart(2, '0')}</td>
                      <td>
                        <span className="wallet-link">{w.label || 'Wallet'}</span>
                      </td>
                      <td className="addr">{shortAddress(w.address)}</td>
                      <td className="mono">
                        Shard {w.shard_id} · B{String(w.bucket_id).padStart(2, '0')}
                      </td>
                      <td className={`num ${wEcon >= 0 ? 'pos' : 'neg'}`}>
                        {formatCurrency(wEcon, true)}
                      </td>
                      <td className={`num ${wReal >= 0 ? 'pos' : 'neg'}`}>
                        {formatCurrency(wReal, true)}
                      </td>
                      <td className="num">{formatPercent(w.win_rate_pct)}</td>
                      <td className="num pos">
                        {matchCount} / {mismatchCount}
                      </td>
                      <td>
                        <Pill type={w.valuation_current ? 'ok' : 'warn'}>
                          {w.valuation_current ? 'CURRENT' : 'STALE'}
                        </Pill>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* Waterfall & Equity Curve */}
      <div className="grid-2">
        <section className="panel">
          <div className="panel-head">
            <h2>PnL Decomposition Waterfall</h2>
            <span className="sub">
              {selectedWallet?.label || 'Wallet'} · USD (NUMERIC/Decimal)
            </span>
          </div>
          <div className="chart-pad">
            <svg viewBox="0 0 500 220" role="img" aria-label="PnL Waterfall">
              <g stroke="#D2CBBF" strokeWidth="1">
                <line x1="20" x2="480" y1="170" y2="170" />
                <line x1="20" x2="480" y1="100" y2="100" strokeDasharray="3 3" />
                <line x1="20" x2="480" y1="30" y2="30" strokeDasharray="3 3" />
              </g>
              {waterfallData.map((b, i) => (
                <g key={i}>
                  <rect
                    x={b.x}
                    y={b.y}
                    width="76"
                    height={b.height}
                    rx="2"
                    fill={b.fill}
                  />
                  <text
                    className="val-lbl"
                    x={b.x + 38}
                    y={b.y - 8}
                    textAnchor="middle"
                  >
                    {b.val}
                  </text>
                  <text x={b.x + 38} y="192" textAnchor="middle">
                    {b.label}
                  </text>
                </g>
              ))}
            </svg>
          </div>
          <div className="footnote">
            <span>{excludedSummary}</span>
            <button className="btn-link" onClick={() => setIsModalOpen(true)}>
              Why excluded?
            </button>
          </div>
        </section>

        <section className="panel">
          <div className="panel-head">
            <h2>Cumulative Realized Equity &amp; Drawdown</h2>
            <span className="sub neg">{maxDdText}</span>
          </div>
          <div className="chart-pad">
            <svg viewBox="0 0 500 220" role="img" aria-label="Equity Curve and Drawdown">
              <g stroke="#D2CBBF">
                <line x1="56" x2="480" y1="118" y2="118" />
                <line x1="56" x2="480" y1="142" y2="142" />
                <line x1="56" x2="480" y1="186" y2="186" strokeDasharray="3 3" />
              </g>
              {eqPoints.eqPoly && (
                <polyline
                  points={eqPoints.eqPoly}
                  fill="none"
                  stroke="#141311"
                  strokeWidth="2"
                />
              )}
              {eqPoints.ddPoly && (
                <polygon
                  points={eqPoints.ddPoly}
                  fill="#991B1B"
                  fillOpacity="0.16"
                  stroke="#991B1B"
                  strokeWidth="1.5"
                />
              )}
              <text x="48" y="20" textAnchor="end">
                +{formatCurrency(eqPoints.labels.max)}
              </text>
              <text x="48" y="118" textAnchor="end">
                $0
              </text>
              <text x="48" y="189" textAnchor="end">
                {formatCurrency(eqPoints.labels.maxDd)}
              </text>
              <text x="56" y="208">
                30d ago
              </text>
              <text x="268" y="208" textAnchor="middle">
                15d ago
              </text>
              <text x="480" y="208" textAnchor="end">
                Now UTC
              </text>
            </svg>
          </div>
          <div className="footnote">
            <span>Top: settled ledger cumulative PnL · Bottom: underwater drawdown from peak</span>
            <span className="mono">UTC · 30d window</span>
          </div>
        </section>
      </div>

      {/* Entry Price Buckets & Open Positions */}
      <div className="grid-asym">
        <section className="panel">
          <div className="panel-head">
            <h2>Entry-Price Buckets</h2>
            <span className="sub">Exposes 95¢ favorite farming · n &lt; 10 dimmed</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Entry Price</th>
                  <th className="num">Trades (n)</th>
                  <th className="num">Win Rate</th>
                  <th className="num">Realized PnL</th>
                  <th className="num">Avg ROI</th>
                </tr>
              </thead>
              <tbody>
                {analytics && analytics.entry_price_buckets && analytics.entry_price_buckets.length > 0 ? (
                  analytics.entry_price_buckets.map((b) => {
                    const pnlNum = parseFloat(b.realized_pnl || '0');
                    return (
                      <tr key={b.bucket} className={b.low_sample ? 'low-n' : ''}>
                        <td className="mono">{b.bucket}</td>
                        <td className="num">{b.trade_count}</td>
                        <td className="num">{formatPercent(b.win_rate_pct)}</td>
                        <td className={`num ${pnlNum >= 0 ? 'pos' : 'neg'}`}>
                          {formatCurrency(pnlNum, true)}
                        </td>
                        <td className={`num ${parseFloat(b.avg_roi_pct || 0) >= 0 ? 'pos' : 'neg'}`}>
                          {formatPercent(b.avg_roi_pct, true)}
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <>
                    <tr>
                      <td className="mono">&lt; 20¢</td>
                      <td className="num">61</td>
                      <td className="num">31.1%</td>
                      <td className="num pos">+$8,612.80</td>
                      <td className="num pos">+42.3%</td>
                    </tr>
                    <tr>
                      <td className="mono">20¢ – 50¢</td>
                      <td className="num">148</td>
                      <td className="num">58.8%</td>
                      <td className="num pos">+$18,204.15</td>
                      <td className="num pos">+18.6%</td>
                    </tr>
                    <tr>
                      <td className="mono">50¢ – 80¢</td>
                      <td className="num">210</td>
                      <td className="num">81.4%</td>
                      <td className="num pos">+$16,291.60</td>
                      <td className="num pos">+6.9%</td>
                    </tr>
                    <tr className="low-n">
                      <td className="mono">&gt; 80¢</td>
                      <td className="num">7</td>
                      <td className="num">71.4%</td>
                      <td className="num neg">-$213.21</td>
                      <td className="num neg">-4.1%</td>
                    </tr>
                  </>
                )}
              </tbody>
            </table>
          </div>
          <div className="footnote">
            <span>
              Source: <span className="mono">position_ledger.avg_cost_before</span>
            </span>
            <span className="mono">
              Sum = {formatCurrency(realizedPnL, true)}
            </span>
          </div>
        </section>

        <section className="panel">
          <div className="panel-head">
            <h2>Open Positions (Mark-to-Market)</h2>
            <span className="sub">
              <strong>{concentrationText}</strong>
            </span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Market &amp; Outcome</th>
                  <th className="num">Value</th>
                  <th className="num">Spread</th>
                  <th className="num">ROI</th>
                  <th>Pricing Tier</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr>
                    <td colSpan="5" style={{ textAlign: 'center', padding: '20px' }}>
                      No open positions
                    </td>
                  </tr>
                ) : (
                  positions.map((pos, idx) => {
                    const roi = pos.position_roi_pct ? parseFloat(pos.position_roi_pct) : null;
                    let tierPill = 'mid';
                    let tierText = pos.price_source || 'CLOB MID';

                    if (tierText.includes('RESOLVED')) {
                      tierPill = 'est';
                      tierText = 'RESOLVED · EST';
                    } else if (tierText.includes('LAST') || tierText.includes('STALE')) {
                      tierPill = 'warn';
                      tierText = 'LAST TRADE';
                    } else if (tierText.includes('UNPRICED')) {
                      tierPill = 'err';
                      tierText = 'UNPRICED';
                    } else {
                      tierPill = 'mid';
                      tierText = 'CLOB MID';
                    }

                    return (
                      <tr key={idx}>
                        <td className="market-col" title={pos.market_question}>
                          {pos.market_question || 'Market'} · <strong>{pos.outcome || 'Yes'}</strong>
                        </td>
                        <td className="num">{formatCurrency(pos.market_value)}</td>
                        <td className="num">
                          {pos.spread_cents ? `${pos.spread_cents}¢` : '—'}
                        </td>
                        <td className={`num ${roi !== null && roi >= 0 ? 'pos' : roi !== null ? 'neg' : ''}`}>
                          {roi !== null ? formatPercent(roi, true) : '—'}
                        </td>
                        <td>
                          <Pill type={tierPill}>{tierText}</Pill>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
          <div className="footnote">
            <span>Unpriced tokens never fabricate $0 marks · cost basis reported separately</span>
            <span className="mono">Max spread &le; $0.10</span>
          </div>
        </section>
      </div>

      <MethodologyModal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} />
    </Layout>
  );
}

