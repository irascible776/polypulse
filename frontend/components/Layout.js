import Link from 'next/link';
import { useRouter } from 'next/router';
import TelemetryStrip from './TelemetryStrip';

export default function Layout({
  children,
  actions,
  bannerMessage,
  bannerActionText,
  onBannerAction,
  telemetryProps = {},
  isStale = false,
}) {
  const router = useRouter();
  const currentPath = router.pathname;

  return (
    <div className={isStale ? 'stale' : ''}>
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" />
          <span>PolyPulse</span>
          <span className="env-tag">LIVE · MEASURED</span>
        </div>

        <nav className="tabs">
          <Link href="/" className={currentPath === '/' ? 'active' : ''}>
            Wallets
          </Link>
          <Link href="/systems" className={currentPath === '/systems' ? 'active' : ''}>
            Systems
          </Link>
        </nav>

        <span className="spacer" />

        <div className="actions">{actions}</div>
      </header>

      <TelemetryStrip {...telemetryProps} isStale={isStale} />

      {bannerMessage && (
        <div className="alert-banner">
          <span>{bannerMessage}</span>
          {bannerActionText && (
            <button onClick={onBannerAction}>{bannerActionText}</button>
          )}
        </div>
      )}

      <main className="main-content">{children}</main>
    </div>
  );
}

