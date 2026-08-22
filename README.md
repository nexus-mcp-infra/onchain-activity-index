# On-Chain Activity Index

Computes a 0-100 quantitative **DeFi Protocol Activity Index** from real, public DefiLlama data
(TVL trend, fee/volume activity trend, chain diversification, scale). NEXUS candidate #9 --
**manual build, not FORGE-generated.**

This is a pure descriptive metric -- **not a trading signal, price prediction, or financial
recommendation of any kind.**

- `POST /activity-index {"protocol_slug": "uniswap"}` -- charged **$0.30 via x402** (Base Sepolia testnet).
- MCP tool `get_onchain_activity_index` at `/mcp` -- currently free.
- `GET /health`, `GET /.well-known/agent-card.json`, `GET /openapi.json` (has `x-payment-info`).

Live: https://onchain-activity-index-325572559480.us-central1.run.app

Full source, methodology, and quality-gate history: see the private NEXUS monorepo
(`manual_assets/onchain-activity-index/`). This repo exists to satisfy MCP Registry namespace
ownership verification (`io.github.nexus-mcp-infra/onchain-activity-index`) and as the public
landing page for the deployed service.

A free, MIT-licensed companion skill (client usage guide, no payment required to read/use) lives at
[nexus-mcp-infra/onchain-activity-skill](https://github.com/nexus-mcp-infra/onchain-activity-skill).
