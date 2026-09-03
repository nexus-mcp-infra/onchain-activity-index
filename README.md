# On-Chain Activity Index

Computes a 0-100 quantitative **DeFi Protocol Activity Index** from real, public DefiLlama data
(TVL trend, fee/volume activity trend, chain diversification, scale). NEXUS candidate #9 --
**manual build, not FORGE-generated.**

This is a pure descriptive metric -- **not a trading signal, price prediction, or financial
recommendation of any kind.**

- `POST /activity-index {"protocol_slug": "uniswap"}` -- charged **$0.30 via x402** (Base mainnet, real USDC).
- MCP tool `get_onchain_activity_index` at `/mcp` -- currently free.
- `GET /health`, `GET /.well-known/agent-card.json`, `GET /openapi.json` (has `x-payment-info`).

Live: https://onchain-activity-index-325572559480.us-central1.run.app

## Mainnet cutover (2026-09-03)

Originally built and measured on Base Sepolia testnet. Cut over to Base mainnet: x402 settlement moved to
the CDP facilitator (`create_facilitator_config()`, same swap already applied to
`ws`/`live-entity-verification`/`erc8004-agent-liveness`), and the payto wallet moved to
`NEXUS_X402_PAYTO_ADDRESS` (fail-fast env var, no placeholder default, renamed from
`X402_WALLET_ADDRESS`). `CDP_API_KEY_ID`/`CDP_API_KEY_SECRET` and `NEXUS_X402_PAYTO_ADDRESS` must be set
in Cloud Run before this deploys.

Full source, methodology, and quality-gate history: see the private NEXUS monorepo
(`manual_assets/onchain-activity-index/`). This repo exists to satisfy MCP Registry namespace
ownership verification (`io.github.nexus-mcp-infra/onchain-activity-index`) and as the public
landing page for the deployed service.

A free, MIT-licensed companion skill (client usage guide, no payment required to read/use) lives at
[nexus-mcp-infra/onchain-activity-skill](https://github.com/nexus-mcp-infra/onchain-activity-skill).
