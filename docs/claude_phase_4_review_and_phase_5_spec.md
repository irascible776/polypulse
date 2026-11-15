Claude responded: Final Sign-Off: PolyPulse
Weighing cache staleness and whether to document it

Weighing cache staleness and whether to document it
Final Sign-Off: PolyPulse

Verdict: Signed off — interview-ready, with three things to fix before you let anyone see the repo, and one framing note for how you present the benchmarks.

Fix before showing this to anyone

The hardcoded API key has to go. X-API-Key: [REDACTED historical key] sitting in source or docs is the single thing in this entire project most likely to actively hurt you rather than just be a missed optimization — it signals “doesn’t know not to commit secrets,” which is a worse look at a trading shop than having no auth at all would be. The company name baked into the literal secret value makes it worse, not better. Move it to an environment variable (POLYPULSE_API_KEY), generate something random for local dev, and make sure the old literal string isn’t sitting in git history either (git log -p for it — if it’s there, that’s worth scrubbing or at minimum being ready to explain). This is a 10-minute fix; do it before anything else.

The cache: good fix, but be honest about what the benchmark is measuring

The event-invalidated TTL cache is the right call architecturally — invalidating on every price/trade write rather than relying purely on TTL expiry is exactly how you’d want this to behave. Two things to tighten up:

Your 6,211 req/sec headline is a hot-cache number, not a steady-state number, and you should present it that way rather than letting it stand alone. In a live market, price ticks arrive continuously, and every tick invalidates the cache for every wallet holding that token — which means in a genuinely active market, you may be much closer to the 15ms cold-path number than the 0.15ms hot-path number, depending on tick frequency vs. request frequency. That’s not a flaw, it’s just a nuance an interviewer will probe (“what’s your throughput once the market is actually moving?”), and you want to have already answered it in your own docs rather than be caught flat-footed. Report both numbers with the condition that produces each: “0.15ms p50 hot-cache (no concurrent invalidation), 15.2ms cold-path (cache miss or just-invalidated).”
The cache introduces a staleness source your pricing-tier badges don’t know about. A live_quote-tier price can be correct-but-cached for up to 1 second after a newer tick has already landed, if invalidation granularity or timing has any gap — worth a one-line note in the study guide (“the tier badge reflects price source staleness, not serving-cache staleness; the latter is bounded by cache_ttl_seconds and tightened by write-path invalidation”). You don’t need to build anything new here, just don’t let this be a thing you discover live when asked “could the dashboard ever show a price older than the badge implies?”
Everything else

The None-for-unavailable fix, the auth gate (once the literal key is replaced), the parallel WS broadcast via gather, and the four scenario tests are all exactly what was asked for and close the gaps cleanly. The reconnect-gets-a-fresh-snapshot test in particular is the right instinct — it’s a real bug class (resuming into stale state after a drop) that’s easy to miss and good to have evidence against.

What “100% done” looks like from here

Nothing architectural is left. The remaining work is presentation:

Pull the secret, re-run the test suite once to confirm nothing breaks on the env-var change.
Add the cold/hot distinction to the README’s headline numbers so the strongest-looking stat doesn’t become the thing that gets you caught overstating something.
Rehearse the 90-second demo path from the last review — wallet switch, live WS update, staleness badge, tier hierarchy explained out loud. That’s the actual artifact that gets evaluated; the test suite is what makes you trust the demo won’t break mid-sentence.

That’s a strong, honestly-benchmarked, interview-ready project. Good work getting it here.


Create a doc





just now