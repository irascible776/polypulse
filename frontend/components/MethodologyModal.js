export default function MethodologyModal({ isOpen, onClose }) {
  if (!isOpen) return null;

  return (
    <div
      className="modal-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-box">
        <h3>Accounting &amp; Pricing Methodology</h3>
        <dl>
          <div>
            <dt>Exact Average-Cost Ledger (Zero Floats)</dt>
            <dd>
              All shares, cost basis, and cash deltas are stored as PostgreSQL{' '}
              <code>NUMERIC</code> and computed with Python <code>Decimal</code> (1e-12 USDC
              tolerance). Rebuildable deterministically from append-only <code>source_events</code>.
            </dd>
          </div>
          <div>
            <dt>Mark Policy Priority</dt>
            <dd>
              1) Chain + Gamma agreed resolution payout (labeled <code>RESOLVED · EST</code>, not
              cash until redeemed) &rarr; 2) Fresh two-sided CLOB midpoint within 10¢ spread &rarr; 3)
              Stale quote / last-trade fallback (explicitly badged) &rarr; 4) <code>UNPRICED</code>{' '}
              (never fabricated as $0).
            </dd>
          </div>
          <div>
            <dt>Excluded Non-Trade Cash Flows</dt>
            <dd>
              Maker/taker rebates, referral rewards, and yield are stored in{' '}
              <code>source_events</code> but excluded from trade PnL because upstream payloads omit
              cash-direction proof.
            </dd>
          </div>
        </dl>
        <button className="btn-primary" onClick={onClose}>
          Close
        </button>
      </div>
    </div>
  );
}

