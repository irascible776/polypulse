"""Exercise the dashboard's incomplete-valuation display."""

import subprocess
from pathlib import Path


def test_dashboard_never_displays_incomplete_total():
    html = Path("app/serving/dashboard.html").read_text()
    script = html.split("<script>", 1)[1].split("// Auto-init", 1)[0]
    harness = (
        r"""
const nodes = new Map();
const element = id => {
  if (!nodes.has(id)) nodes.set(id, {innerText:'', className:'', style:{},
    innerHTML:'', children:[], appendChild() {}, prepend() {}});
  return nodes.get(id);
};
global.document = {getElementById: element, createElement: () => ({innerHTML:'', className:''})};
const pnl = {total_pnl_excl_unpriced:'42', complete:false, realized_pnl:'20',
  unrealized_priced:'22', estimated_unrealized:'0', ingestion_stale:false,
  resolution_stale:false, valuation_current:false,
  data_as_of:null, ingestion_lag_seconds:null, stale_positions_count:0,
  unpriced_count:1, unpriced_cost_basis:'9', total_market_value:'1', open_positions_count:0};
global.fetch = async url => ({ok:true, json: async () => url === '/health' ? {wallets:{}} :
  url.includes('/pnl') ? pnl : url.includes('/positions') ? {positions:[]} : {trades:[]}});
"""
        + script
        + r"""
socketOk = true;
loadWalletData('0xabc').then(async () => {
  const value = element('kpiTotalPnL').innerText;
  if (value !== 'Incomplete — no total' || value.includes('$')) throw Error(value);
  if (element('staleBanner').innerText !== 'VALUATION NOT CURRENT: unpriced') throw Error('banner');
  pnl.ingestion_stale = true; pnl.resolution_stale = true;
  await loadWalletData('0xabc');
  if (element('staleBanner').innerText !== 'VALUATION NOT CURRENT: ingestion, resolution, unpriced')
    throw Error('all reasons');
});
"""
    )
    result = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_dashboard_unreachable_never_shows_zero():
    html = Path("app/serving/dashboard.html").read_text()
    script = html.split("<script>", 1)[1].split("// Auto-init", 1)[0]
    harness = (
        r"""
const nodes = new Map();
const element = id => {
  if (!nodes.has(id)) nodes.set(id, {innerText:'', style:{}, innerHTML:''});
  return nodes.get(id);
};
global.document = {getElementById: element};
global.fetch = async () => { throw Error('offline'); };
"""
        + script
        + r"""
socketOk = true;
loadWalletData('0xabc').then(() => {
  for (const id of ['kpiTotalPnL', 'kpiRealizedPnL', 'kpiUnrealizedPnL',
                    'kpiMarketValue', 'kpiPositionCount']) {
    if (element(id).innerText !== '—') throw Error(id);
  }
  if (element('connText').innerText !== 'API UNREACHABLE' ||
      element('feedPill').innerText !== 'API UNREACHABLE') throw Error('indicators');
  restOk = true; socketOk = false; updateConnection();
  if (element('connText').innerText !== 'API UNREACHABLE' ||
      element('feedPill').innerText !== 'API UNREACHABLE' ||
      element('kpiPositionCount').innerText !== '—') throw Error('socket indicators');
});
"""
    )
    result = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
