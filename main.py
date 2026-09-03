"""
onchain-activity-index -- NEXUS candidate #9 (manual build, no FORGE).

Given a DefiLlama protocol slug (e.g. "uniswap", "aave"), returns a composite
0-100 "DeFi Protocol Activity Index" built entirely from real, public,
keyless DefiLlama data (api.llama.fi): TVL trend, fee/revenue trend, DEX
volume trend, chain diversification, and absolute scale. Verified live
against api.llama.fi this session -- not a hallucinated data source (see
skills/asset-lifecycle for why that check matters).

REGULATORY CONSTRAINT (non-negotiable, CNMV/Spain): this product's output is
PURE QUANTITATIVE DATA. It is not, and must never become, a buy/sell
recommendation or anything that reads as financial advice. Every user-facing
string in this file (docstrings, descriptions, error messages) was written
and re-reviewed under that constraint -- see the buyer-experience lens in
README.md's "Pre-deploy quality gate" section. If you're editing this file,
keep new copy in the same register: "index"/"score"/"metric"/"activity
level", never "signal"/"recommendation"/"you should buy/sell/hold".

Methodology (full detail in README.md "Methodology"): a coverage-weighted
composite of up to 5 components, each independently computed from a
DefiLlama endpoint and mapped to a bounded [0, 100] sub-score via a
saturating (tanh-based) transform -- no component can single-handedly blow
the index out of its bounds, and a protocol missing a component (e.g. a
lending protocol has no DEX volume) has its weight redistributed across
the components it does have, not zero-filled. This is an explicit,
disclosed heuristic index, not a validated or predictive financial model.

Built from manual_assets/agent-verification-api/main.py's structure (MCP +
x402 + FastAPI, one process, Cloud Run target) -- same telemetry helpers,
same x402 wiring, same free-mode gate as manual_assets/document-conversion-api.
No user-supplied URLs are ever fetched server-side, so this asset has no
SSRF surface (unlike url-metadata-api/agent-verification-api) -- the only
outbound host is the fixed api.llama.fi.
"""

import asyncio
import base64
import math
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi as _fastapi_get_openapi
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field, field_validator

from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer

_NEXUS_ASSET_NAME = "onchain-activity-index"

# ---------------------------------------------------------------
# Supabase telemetry -- ported by hand, same pattern as the 3 sibling
# manual assets. anon key only, RLS is INSERT-only for anon.
# ---------------------------------------------------------------
_NEXUS_SUPABASE_URL = os.getenv("SUPABASE_URL")
_NEXUS_SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

_nexus_bg_tasks: set = set()


def _nexus_fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _nexus_bg_tasks.add(task)
    task.add_done_callback(_nexus_bg_tasks.discard)
    return task


def _nexus_truncate_ip(raw_ip: Optional[str]) -> Optional[str]:
    if not raw_ip:
        return None
    if ":" in raw_ip and "." not in raw_ip:
        segments = [s for s in raw_ip.split(":") if s]
        head = segments[:4] if len(segments) >= 4 else segments
        return (":".join(head) + "::/64") if head else None
    octets = raw_ip.split(".")
    if len(octets) == 4 and all(o.isdigit() for o in octets):
        return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
    return None


async def _nexus_supabase_insert(table: str, payload: dict) -> None:
    if not _NEXUS_SUPABASE_URL or not _NEXUS_SUPABASE_ANON_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(
                f"{_NEXUS_SUPABASE_URL}/rest/v1/{table}",
                json=payload,
                headers={
                    "apikey": _NEXUS_SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {_NEXUS_SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
            )
    except Exception as e:
        print(f"[WARN] Supabase insert to {table!r} failed: {e}", file=sys.stderr)


async def _nexus_log_mcp_call_event(tool_id: str, success: bool, latency_ms: int) -> None:
    await _nexus_supabase_insert("mcp_call_events", {
        "tool_id": tool_id,
        "asset_name": _NEXUS_ASSET_NAME,
        "success": success,
        "latency_ms": latency_ms,
        "agent_framework": "mcp",
        "price_charged": 0,  # MCP path is free today -- see README "Known limitations"
    })


# =================================================================
# DefiLlama client -- fully free, keyless, public. Endpoints verified
# reachable in the design session for this candidate:
#   GET /protocol/{slug}       -- tvl time series, currentChainTvls, mcap
#   GET /summary/fees/{slug}   -- total24h/total7d/total30d/change_1d
#   GET /summary/dexs/{slug}   -- same shape, DEX volume instead of fees
# NOTE the /summary/ prefix, not /overview/ -- /overview/fees/{slug} and
# /overview/dexs/{slug} both returned "Internal server error" in testing;
# /summary/ is the correct per-protocol endpoint.
# =================================================================
_DEFILLAMA_BASE = "https://api.llama.fi"
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{0,78}[a-z0-9]$")

# Tiny in-memory TTL cache -- avoids hammering DefiLlama's free API on
# repeated calls for the same popular protocol within a short window.
# Cloud Run --min-instances=0 means this cache is empty on every cold
# start; that's fine, it's a latency/politeness optimization, not a
# correctness dependency (every entry is refetched fresh on expiry/miss).
_CACHE_TTL_SECONDS = 300
_defillama_cache: dict[str, tuple[float, dict]] = {}


async def _defillama_get(path: str, timeout: float = 12.0) -> Optional[dict]:
    cache_key = path
    now = time.monotonic()
    cached = _defillama_cache.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{_DEFILLAMA_BASE}{path}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        return None
    except httpx.HTTPStatusError:
        return None
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    _defillama_cache[cache_key] = (now, data)
    return data


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _saturating_score(pct_change: float, sensitivity: float = 0.5) -> float:
    """Maps an unbounded percent-change ratio (e.g. +0.35 = +35%) to a
    bounded [0, 100] sub-score centered at 50, via tanh -- large moves in
    either direction saturate instead of blowing the index out of range.
    sensitivity controls how much change is needed to approach the
    bounds (lower = more sensitive)."""
    return _clip(50.0 + 50.0 * math.tanh(pct_change / sensitivity), 0.0, 100.0)


def _tvl_trend_component(tvl_series: list) -> Optional[float]:
    """tvl_series: list of {"date": unix_seconds, "totalLiquidityUSD": float},
    ascending by date (DefiLlama's own order). Uses the ~7-day-ago point vs
    the latest point -- a short enough window to reflect current activity,
    long enough to smooth single-day noise."""
    if not tvl_series or len(tvl_series) < 2:
        return None
    latest = tvl_series[-1].get("totalLiquidityUSD")
    if latest is None or latest <= 0:
        return None
    target_ts = tvl_series[-1]["date"] - 7 * 86400
    reference = None
    for point in reversed(tvl_series):
        if point["date"] <= target_ts:
            reference = point.get("totalLiquidityUSD")
            break
    if reference is None or reference <= 0:
        reference = tvl_series[0].get("totalLiquidityUSD")
    if not reference or reference <= 0:
        return None
    pct_change = (latest - reference) / reference
    return _saturating_score(pct_change, sensitivity=0.30)


def _scale_component(current_tvl: Optional[float]) -> Optional[float]:
    """Absolute size, log-scaled: $100k protocol -> near 0, $10B+ -> near
    100. Deliberately coarse -- this component rewards being large and
    established, independent of recent trend direction."""
    if not current_tvl or current_tvl <= 0:
        return None
    log_tvl = math.log10(current_tvl)
    return _clip((log_tvl - 5.0) / (10.0 - 5.0) * 100.0, 0.0, 100.0)


def _activity_rate_trend_component(summary: Optional[dict]) -> Optional[float]:
    """summary: a /summary/fees/{slug} or /summary/dexs/{slug} response.
    Compares the recent daily rate (total7d / 7) against the trailing
    30-day daily rate (total30d / 30) -- a ratio > 1 means the last week
    ran hotter than the last month's average, < 1 means it's cooling off.
    Purely a rate-of-activity comparison, not a price or return figure.
    Methodology note (functional review, 2026-08-22, confirmed live): DefiLlama's
    total30d is inclusive of the same most recent 7 days counted in total7d, not a
    disjoint prior-23-day window -- the two figures aren't fully independent, which
    structurally dampens this ratio toward 1.0 versus a true recent-vs-prior-baseline
    comparison. Accepted as a defensible heuristic simplification (DefiLlama doesn't
    expose non-overlapping windows directly): it never flips direction or breaks, just
    understates how divergent recent activity really is from the month's average."""
    if not summary:
        return None
    total7d = summary.get("total7d")
    total30d = summary.get("total30d")
    if not total7d or not total30d or total30d <= 0:
        return None
    weekly_rate = total7d / 7.0
    monthly_rate = total30d / 30.0
    if monthly_rate <= 0:
        return None
    ratio = weekly_rate / monthly_rate
    return _saturating_score(ratio - 1.0, sensitivity=0.5)


# DefiLlama's currentChainTvls dict is NOT one entry per chain -- it also
# carries bare category-aggregate keys (e.g. "borrowed") and, for many
# lending/staking protocols, a "{chain}-{category}" duplicate per real
# chain (e.g. "Ethereum-borrowed" alongside "Ethereum"). Counting raw keys
# overcounts chains for exactly the protocol category the module docstring
# calls out as a key use case -- found in functional review, 2026-08-22,
# confirmed live against aave/compound-v3/morpho-blue/spark/curve-dex/
# makerdao/pendle: overcounted by 1.2x-2.2x for lending protocols, e.g.
# compound-v3 counted 19 raw keys vs. 9 real chains. Category suffixes/bare
# keys observed live: staking, pool2, borrowed. vesting/treasury are known
# DefiLlama TVL categories not observed in this survey -- excluded
# defensively on the same pattern.
_CHAIN_TVL_CATEGORY_SUFFIXES = ("-staking", "-pool2", "-borrowed", "-vesting", "-treasury")
_CHAIN_TVL_BARE_CATEGORY_KEYS = {"staking", "pool2", "borrowed", "vesting", "treasury"}


def _diversification_component(current_chain_tvls: Optional[dict]) -> Optional[float]:
    """Count of distinct REAL chains carrying real (>0) TVL (category
    aggregate/breakdown keys excluded, see _CHAIN_TVL_* above). Log-scaled:
    1 chain -> low score, ~10+ chains -> near 100. A single-chain protocol
    isn't penalized to zero -- it's a legitimate, common posture -- but
    broad multi-chain presence reflects wider integration/activity."""
    if not current_chain_tvls:
        return None
    real_chain_names: set = set()
    for key, value in current_chain_tvls.items():
        if not (isinstance(value, (int, float)) and value > 0):
            continue
        if key in _CHAIN_TVL_BARE_CATEGORY_KEYS:
            continue
        chain_name = key
        for suffix in _CHAIN_TVL_CATEGORY_SUFFIXES:
            if key.endswith(suffix):
                chain_name = key[: -len(suffix)]
                break
        real_chain_names.add(chain_name)
    real_chains = len(real_chain_names)
    if real_chains <= 0:
        return None
    return _clip(math.log1p(real_chains) / math.log1p(15) * 100.0, 0.0, 100.0)


_COMPONENT_WEIGHTS = {
    "tvl_trend": 0.30,
    "scale": 0.20,
    "fee_activity_trend": 0.20,
    "volume_activity_trend": 0.15,
    "chain_diversification": 0.15,
}


def _safe_component(fn, *args) -> Optional[float]:
    """Wraps a component function so an unexpected DefiLlama response shape
    (a schema surprise, not covered by _defillama_get's own request-level
    try/except) degrades that one component to 'missing' -- lowering
    coverage -- instead of raising and producing an undisclosed paid 500.
    Found in security review, 2026-08-22: component extraction indexed
    into DefiLlama's response structure with no guard of its own."""
    try:
        return fn(*args)
    except Exception:
        return None


# Bounds concurrent in-flight /protocol/{slug} fetches (the expensive one --
# up to ~2MB / 45s, see _compute_activity_index below). Found in security
# review, 2026-08-22: the free MCP tool path has no per-caller rate limit
# (same accepted 7-day-probation tradeoff as the sibling manual assets),
# so this semaphore is the one cheap, real guard against a caller forcing
# unbounded concurrent expensive upstream fetches -- it doesn't fix cost
# per call, just caps how many can run at once.
_DEFILLAMA_PROTOCOL_SEMAPHORE = asyncio.Semaphore(int(os.getenv("NEXUS_MAX_CONCURRENT_DEFILLAMA_FETCHES", "8")))


async def _defillama_get_protocol(protocol_slug: str) -> Optional[dict]:
    async with _DEFILLAMA_PROTOCOL_SEMAPHORE:
        return await _defillama_get(f"/protocol/{protocol_slug}", timeout=45.0)


async def _compute_activity_index(protocol_slug: str) -> dict:
    # /protocol/{slug} can be large (~2MB / 20s+ for a widely-multi-chain
    # protocol like Uniswap -- 47 chains' worth of per-chain TVL/token
    # breakdown, measured live in this candidate's design session) --
    # needs a much longer timeout than the small /summary/ responses, and
    # goes through the semaphore above rather than being called directly.
    protocol, fees, dexs = await asyncio.gather(
        _defillama_get_protocol(protocol_slug),
        _defillama_get(f"/summary/fees/{protocol_slug}"),
        _defillama_get(f"/summary/dexs/{protocol_slug}"),
    )

    if protocol is None:
        raise HTTPException(404, f"protocol_slug '{protocol_slug}' not found on DefiLlama")

    tvl_series = protocol.get("tvl") or []
    current_tvl = tvl_series[-1].get("totalLiquidityUSD") if tvl_series else None

    components = {
        "tvl_trend": _safe_component(_tvl_trend_component, tvl_series),
        "scale": _safe_component(_scale_component, current_tvl),
        "fee_activity_trend": _safe_component(_activity_rate_trend_component, fees),
        "volume_activity_trend": _safe_component(_activity_rate_trend_component, dexs),
        "chain_diversification": _safe_component(_diversification_component, protocol.get("currentChainTvls")),
    }

    evaluated = {k: v for k, v in components.items() if v is not None}
    missing = [k for k, v in components.items() if v is None]

    if not evaluated:
        raise HTTPException(
            422,
            f"protocol_slug '{protocol_slug}' was found on DefiLlama but had no usable "
            "TVL/fee/volume data to compute an index from",
        )

    weight_sum = sum(_COMPONENT_WEIGHTS[k] for k in evaluated)
    activity_index = sum(_COMPONENT_WEIGHTS[k] * v for k, v in evaluated.items()) / weight_sum
    coverage = len(evaluated) / len(_COMPONENT_WEIGHTS)

    return {
        "protocol_slug": protocol_slug,
        "protocol_name": protocol.get("name"),
        "activity_index": round(activity_index, 2),
        "coverage": round(coverage, 2),
        "components": {k: (round(v, 2) if v is not None else None) for k, v in components.items()},
        "components_evaluated": list(evaluated.keys()),
        "components_missing": missing,
        "current_tvl_usd": current_tvl,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------
# MCP server (Streamable HTTP)
# ---------------------------------------------------------------
_public_domain = os.getenv("PUBLIC_DOMAIN", "*")
if _public_domain == "*":
    print(
        "[WARN] PUBLIC_DOMAIN is unset -- every real request will get 421'd once "
        "deployed publicly (fails closed). Set it to the real Cloud Run domain and "
        "redeploy.", file=sys.stderr,
    )

mcp = FastMCP(
    "onchain-activity-index",
    stateless_http=True,
    host="0.0.0.0",
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["localhost:*", "127.0.0.1:*", _public_domain, _public_domain + ":*"],
        allowed_origins=["http://localhost:*", "http://127.0.0.1:*", "https://" + _public_domain],
    ),
)


def _validate_slug(slug: str) -> str:
    slug = slug.strip().lower()
    if not _SLUG_RE.match(slug):
        raise ValueError(
            f"protocol_slug '{slug}' is not a valid DefiLlama slug "
            "(lowercase letters/digits/hyphens, e.g. 'uniswap', 'aave')"
        )
    return slug


@mcp.tool()
async def get_onchain_activity_index(protocol_slug: str, ctx: Context = None) -> dict:
    """Returns a 0-100 quantitative DeFi Protocol Activity Index for a
    DefiLlama protocol slug (e.g. 'uniswap', 'aave'), built from real TVL
    trend, fee/volume activity trend, chain diversification, and scale
    data. This is a pure descriptive metric, NOT a trading signal or
    financial recommendation of any kind -- it does not predict price
    movement and should not be interpreted as advice to buy, sell, or
    hold any asset. This MCP tool call is currently free -- the equivalent
    REST endpoint (POST /activity-index) is charged via x402, see README.
    """
    start = time.monotonic()
    success = False
    try:
        slug = _validate_slug(protocol_slug)
        result = await _compute_activity_index(slug)
        success = True
        return result
    finally:
        latency_ms = int((time.monotonic() - start) * 1000)
        _nexus_fire_and_forget(_nexus_log_mcp_call_event("get_onchain_activity_index", success, latency_ms))


# ---------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(
    title="On-Chain Activity Index",
    description=(
        "Computes a 0-100 quantitative DeFi Protocol Activity Index from real, public "
        "DefiLlama data (TVL trend, fee/volume activity trend, chain diversification, scale). "
        "Pure descriptive metric -- not a trading signal or financial advice of any kind."
    ),
    version="1.0.0",
    contact={"email": "dasaanrod@gmail.com"},
    lifespan=lifespan,
)


# ---------------------------------------------------------------
# MCP listen-connection timeout -- fixes the Cloud Run cost spike confirmed
# 2026-09-03 (same pattern as erc8004-agent-liveness, see that repo's
# patch_mcp_listen_timeout.py for the full root-cause writeup). Third-party
# MCP-monitoring bots open GET /mcp/ "listen" connections (per the
# Streamable HTTP transport spec) and this asset -- which never pushes
# server-initiated notifications -- never closes them, so every one rode
# Cloud Run's full 300s request timeout and was billed for the whole
# duration. This applies ONLY to GET requests under /mcp -- the paid POST
# tool-call path and every other route are completely untouched.
# ---------------------------------------------------------------
_NEXUS_MCP_LISTEN_TIMEOUT_SECONDS = 25


class _NexusMcpListenTimeoutMiddleware:
    """Pure ASGI middleware. Enforces a short idle timeout on GET /mcp
    listen connections only. On timeout, cancels the inner call and lets
    the ASGI server close the underlying connection -- equivalent to a
    normal disconnect from the client's point of view, which any
    SSE-based MCP client already has to handle and reconnect from."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] == "http"
            and scope.get("method") == "GET"
            and scope.get("path", "").rstrip("/").startswith("/mcp")
        ):
            try:
                await asyncio.wait_for(
                    self.app(scope, receive, send),
                    timeout=_NEXUS_MCP_LISTEN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                # Connection closes here -- no further ASGI messages are
                # sent, which is the correct way to end a still-open SSE
                # response early. Intentionally swallowed: this is an
                # expected, routine cutoff, not an error condition.
                return
        else:
            await self.app(scope, receive, send)


app.add_middleware(_NexusMcpListenTimeoutMiddleware)


# --- x402: pay-per-call in USDC, Base Sepolia testnet -- same wallet,
#     facilitator and self-payment-bug fix as the sibling manual assets
#     (skills/x402-payments). $0.30: low end of the user-specified
#     $0.30-0.50 range for propriety-signal-class services -- no paid
#     third-party API cost here (unlike WHOIS in agent-verification-api),
#     just 3 real outbound DefiLlama calls plus computation, so priced
#     below agent-verification-api's $0.35 rather than at its midpoint. ---
_NEXUS_X402_FREE_MODE = os.getenv("NEXUS_X402_FREE_MODE", "false").strip().lower() == "true"

_X402_EVM_ADDRESS = os.getenv("X402_WALLET_ADDRESS", "0xYOUR_WALLET_ADDRESS_HERE")
_X402_NETWORK: Network = "eip155:84532"  # Base Sepolia testnet
_X402_PRICE = os.getenv("X402_PRICE", "$0.30")

if not _X402_PRICE or not _X402_PRICE.startswith("$"):
    print(
        f"[WARN] X402_PRICE ({_X402_PRICE!r}) doesn't look like a price string "
        "(expected something like '$0.30'). Falling back to '$0.30' so the "
        "server can still boot -- fix X402_PRICE before deploying.",
        file=sys.stderr,
    )
    _X402_PRICE = "$0.30"

_looks_like_evm_address = (
    _X402_EVM_ADDRESS.startswith("0x")
    and len(_X402_EVM_ADDRESS) == 42
    and all(c in "0123456789abcdefABCDEF" for c in _X402_EVM_ADDRESS[2:])
)
if not _NEXUS_X402_FREE_MODE and not _looks_like_evm_address:
    print(
        f"[WARN] X402_WALLET_ADDRESS ({_X402_EVM_ADDRESS!r}) doesn't look like a "
        "real EVM address (expected '0x' + 40 hex chars). The server will still "
        "boot and issue 402 challenges, but payments will have nowhere real to "
        "settle. See README.md.",
        file=sys.stderr,
    )

_x402_facilitator = HTTPFacilitatorClient(FacilitatorConfig(url="https://x402.org/facilitator"))
_x402_server = x402ResourceServer(_x402_facilitator)
_x402_server.register(_X402_NETWORK, ExactEvmServerScheme())


async def _nexus_log_x402_revenue_event(ctx) -> None:
    try:
        result = getattr(ctx, "result", None)
        requirements = getattr(ctx, "requirements", None)
        if result is None or requirements is None or not getattr(result, "success", False):
            return
        raw_amount = getattr(requirements, "amount", None)
        amount_eur = int(raw_amount) / 1_000_000 if raw_amount is not None else None
        await _nexus_supabase_insert("revenue_events", {
            "asset_name": _NEXUS_ASSET_NAME,
            "amount_eur": amount_eur,
            "pricing_model": "x402",
            "stripe_event_id": None,
            "customer_id": getattr(result, "payer", None),
        })
    except Exception:
        pass


_x402_server.on_after_settle(_nexus_log_x402_revenue_event)

_X402_ROUTES: dict[str, RouteConfig] = {
    "POST /activity-index": RouteConfig(
        accepts=[PaymentOption(scheme="exact", pay_to=_X402_EVM_ADDRESS, price=_X402_PRICE, network=_X402_NETWORK)],
        mime_type="application/json",
        description="Compute a 0-100 quantitative DeFi Protocol Activity Index (pure data, not advice)",
    ),
}
if not _NEXUS_X402_FREE_MODE:
    app.add_middleware(PaymentMiddlewareASGI, routes=_X402_ROUTES, server=_x402_server)

# --- PATCH traffic_log_middleware_order_v1 ---
@app.middleware("http")
async def _nexus_traffic_log(request, call_next):
    response = await call_next(request)
    ip = request.client.host if request.client else None
    _nexus_fire_and_forget(_nexus_supabase_insert("traffic_events", {
        "asset_name": _NEXUS_ASSET_NAME,
        "ip_range": _nexus_truncate_ip(ip),
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
    }))
    return response



class ActivityIndexRequest(BaseModel):
    protocol_slug: Annotated[str, Field(..., min_length=1, max_length=80,
        description="DefiLlama protocol slug, e.g. 'uniswap', 'aave', 'lido'. "
                     "Find slugs at https://defillama.com/protocols.")]

    @field_validator('protocol_slug')
    @classmethod
    def _validate_slug_field(cls, v: str) -> str:
        return _validate_slug(v)


class ComponentScores(BaseModel):
    """Per-component 0-100 sub-scores (null if unavailable for this protocol).
    See README.md 'Methodology' for the exact formula behind each. Typed as
    its own model (not a bare dict) so a buyer generating a client from
    /openapi.json gets the 5 keys and their types without reading prose --
    found in buyer-experience review, 2026-08-22."""
    tvl_trend: Optional[float] = Field(None, description="~7-day TVL percent-change, saturated to [0,100].")
    scale: Optional[float] = Field(None, description="Absolute TVL, log-scaled to [0,100].")
    fee_activity_trend: Optional[float] = Field(None, description="Recent vs. trailing-30d fee-generation rate, saturated to [0,100].")
    volume_activity_trend: Optional[float] = Field(None, description="Recent vs. trailing-30d DEX-volume rate, saturated to [0,100]. Null for non-DEX protocols.")
    chain_diversification: Optional[float] = Field(None, description="Count of chains with real TVL, log-scaled to [0,100].")


class ActivityIndexResponse(BaseModel):
    protocol_slug: str
    protocol_name: Optional[str] = None
    activity_index: float = Field(..., description="0-100 composite quantitative index. "
        "A descriptive metric of recent on-chain activity level, NOT a trading signal, "
        "price prediction, or recommendation to buy/sell/hold.")
    coverage: float = Field(..., description="Fraction (0-1) of the 5 possible components "
        "that had usable data for this protocol -- lower coverage means the index rests "
        "on fewer computed inputs (e.g. a lending protocol typically has no DEX volume).")
    components: ComponentScores
    components_evaluated: list[str] = Field(..., description="Which of the 5 component keys "
        "(see ComponentScores) had usable data and contributed to activity_index.")
    components_missing: list[str] = Field(..., description="Which of the 5 component keys had "
        "no usable data for this protocol -- their weight was redistributed across the "
        "components that did, not zero-filled.")
    current_tvl_usd: Optional[float] = None
    evaluated_at: str


@app.post(
    "/activity-index",
    response_model=ActivityIndexResponse,
    responses={
        404: {"description": "protocol_slug not found on DefiLlama -- still charged, see below"},
        422: {"description": "protocol_slug found but had no usable TVL/fee/volume data -- still charged"},
    },
)
async def activity_index_endpoint(payload: ActivityIndexRequest) -> dict:
    """Computes a 0-100 quantitative DeFi Protocol Activity Index from real, public
    DefiLlama data. This is a descriptive metric of recent on-chain activity level --
    it is NOT a trading signal, price prediction, or financial recommendation of any
    kind, and must not be interpreted or represented as investment advice.

    Components (see README.md 'Methodology' for the exact formulas): TVL trend (~7d),
    fee-generation activity trend, DEX-volume activity trend, chain diversification,
    and absolute scale. A protocol missing a component (e.g. no DEX volume for a
    non-DEX protocol) has that component's weight redistributed across the components
    it does have -- see `coverage`/`components_evaluated`/`components_missing`.

    Payment (x402) is settled by PaymentMiddlewareASGI BEFORE this handler runs -- a
    call is charged even if it returns a 404 (unknown protocol_slug) or 422 (no usable
    data), same payment-before-validation disclosure as the sibling manual assets."""
    return await _compute_activity_index(payload.protocol_slug)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


_FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(content=_FAVICON_PNG, media_type="image/png")


# --- 402index.io domain claim verification (POST /api/v1/claim -- filled in
#     after the real Cloud Run domain is known, see README "Deploy") ---
_NEXUS_402INDEX_VERIFY_HASH = os.getenv("NEXUS_402INDEX_VERIFY_HASH", "")


@app.get("/.well-known/402index-verify.txt", include_in_schema=False)
async def _nexus_402index_verify():
    from fastapi.responses import PlainTextResponse
    if not _NEXUS_402INDEX_VERIFY_HASH:
        raise HTTPException(404, "no 402index claim on file")
    return PlainTextResponse(content=_NEXUS_402INDEX_VERIFY_HASH)


@app.get("/.well-known/agent-card.json", include_in_schema=False)
async def agent_card() -> dict:
    base = f"https://{_public_domain}" if _public_domain != "*" else "https://onchain-activity-index.example"
    return {
        "name": "On-Chain Activity Index",
        "description": "Computes a 0-100 quantitative DeFi Protocol Activity Index from real "
                        "DefiLlama data (TVL trend, fee/volume activity trend, diversification, "
                        "scale). Pure descriptive metric, not a trading signal or financial advice.",
        "url": base,
        "version": "1.0.0",
        "documentationUrl": f"{base}/docs",
        "provider": {"organization": "nexus-mcp-infra", "url": "https://github.com/nexus-mcp-infra"},
        "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": False},
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "additionalInterfaces": [{"url": f"{base}/mcp", "transport": "MCP"}],
        "skills": [
            {
                "id": "nexus_onchain_activity_index_get_onchain_activity_index",
                "name": "Get On-Chain Activity Index",
                "description": "Compute a 0-100 quantitative DeFi protocol activity index from "
                                "real TVL/fee/volume/diversification data. Not financial advice.",
                "tags": ["defi", "on-chain", "analytics", "activity-index"],
            },
        ],
        "metadata": {
            "protocol_note": (
                "This service implements the Model Context Protocol (MCP) at /mcp, not A2A's own "
                "task methods. POST /activity-index is charged "
                f"{_X402_PRICE} via x402 (Base Sepolia TESTNET, not real funds). Payment settles "
                "BEFORE computation runs, at the ASGI middleware layer ahead of request validation: "
                "a call is charged even if it returns a 404 (unknown protocol_slug) or 422 (no "
                "usable data). The MCP tool is currently free -- see README. IMPORTANT: the "
                "activity_index returned is a pure descriptive metric of on-chain activity level, "
                "NOT a trading signal, price prediction, or recommendation to buy/sell/hold any "
                "asset -- it must not be represented as investment advice downstream."
            ),
        },
    }


_NEXUS_X402_OPENAPI_OPERATIONS = [("post", "/activity-index")]


def _nexus_openapi_with_payment_info():
    if app.openapi_schema:
        return app.openapi_schema
    schema = _fastapi_get_openapi(title=app.title, version=app.version, description=app.description, routes=app.routes)
    if not _NEXUS_X402_FREE_MODE:
        for method, path in _NEXUS_X402_OPENAPI_OPERATIONS:
            op = schema.get("paths", {}).get(path, {}).get(method)
            if op is None:
                continue
            op["x-payment-info"] = {
                "price": {"mode": "fixed", "currency": "USD", "amount": _X402_PRICE.lstrip("$")},
                "protocols": [{"x402": {}}],
            }
            op.setdefault("responses", {})["402"] = {"description": "Payment Required"}
    schema.setdefault("info", {})["contact"] = {"email": "dasaanrod@gmail.com"}
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = _nexus_openapi_with_payment_info

app.mount("/mcp", mcp.streamable_http_app())
