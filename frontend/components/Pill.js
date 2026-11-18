export default function Pill({ type = 'mid', children, className = '' }) {
  const pillClass = `pill pill-${type} ${className}`.trim();
  return <span className={pillClass}>{children}</span>;
}

