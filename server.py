"""
Search-Demand MCP Connector (SRC-SEARCHTREND: demand ignition)
==============================================================

A Model Context Protocol server for the demand-ignition signal SRC-SEARCHTREND
feeds CSR Gate 2 ("durable, company-independent demand driver — e.g. GLP-1") and
ThirdWave ("adjacent-TAM tailwind" / "demand-duration class"). It has TWO engines:

  RELIABLE CORE — Wikipedia pageviews (Wikimedia REST API)
    * demand_trend   -> public attention to a topic over time, with a velocity /
                        ignition read. ABSOLUTE daily/monthly view counts (not a
                        rebased index), full history since 2015 -> backtestable,
                        which fixes SRC-SEARCHTREND's PIT_NONE rebasing weakness.
    * compare_demand -> several topics side by side (valid because counts are
                        absolute, unlike Google Trends' per-query rebasing).

  BEST-EFFORT — Google Trends (pytrends)
    * google_trends  -> relative search interest. HONEST CAVEAT: pytrends is
                        unmaintained (archived 2025) and Google 429-blocks cloud
                        IPs hard, so this usually FAILS on a hosted server. It
                        works best run locally (Claude Desktop, home IP). On
                        failure it degrades gracefully and points you to
                        demand_trend (Wikipedia) and web search — the blueprint's
                        stated free fallback (acc: WEB_SEARCH).

Why Wikipedia is the workhorse: the frameworks want the SIGNAL (is demand for a
theme igniting and durable?), not Google's specific rebased index. Wikipedia
pageviews are a clean, free, cloud-reachable, ABSOLUTE proxy with real history —
better for the anti-hindsight / backtest rule than rebased Trends data.

PIT note: still snapshot raw pulls (PIT_NONE per blueprint); the edge is the
12-24mo lead and it's worthless unless captured from today (archiver candidate).

Data access: Wikimedia REST API is FREE, no key. Wikimedia refuses clients whose
User-Agent carries no contact URL or email ("Please respect our robot policy",
HTTP 403), so the default User-Agent names this repository. Polite client (~3 req/s).

Article matching (build-2): a term is first looked up as an exact title (redirects
followed to their target, disambiguation pages skipped); only if that fails does it
fall back to the top search hit. Answers say which happened in "match"
("exact" or "search"), so callers that need precision can reject search guesses.

Transports: stdio (default) or streamable-http (MCP_TRANSPORT=http -> /mcp).
"""

from __future__ import annotations

import asyncio
import os
import time
import urllib.parse
from datetime import date, timedelta
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

WIKI_REST = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
MAX_RPS = float(os.environ.get("SEARCHTREND_MAX_RPS", "3"))
DEFAULT_USER_AGENT = (
    "search-demand-mcp/1.1 (https://github.com/michaelrogan/searchtrend-connector) "
    f"python-httpx/{httpx.__version__}")
USER_AGENT = os.environ.get("SEARCHTREND_USER_AGENT", "").strip() or DEFAULT_USER_AGENT
GOOGLE_TRENDS_PROXY = os.environ.get("GOOGLE_TRENDS_PROXY", "").strip()

_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
mcp = FastMCP("search-demand", transport_security=_security)


# ----------------------------------------------------------------------------
# Rate limiter
# ----------------------------------------------------------------------------

class _RateLimiter:
    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / rps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            delta = time.monotonic() - self._last
            if delta < self._min_interval:
                await asyncio.sleep(self._min_interval - delta)
            self._last = time.monotonic()


_limiter = _RateLimiter(MAX_RPS)


# ----------------------------------------------------------------------------
# Wikipedia engine
# ----------------------------------------------------------------------------

async def _wiki_resolve(term: str, project: str) -> tuple[str | None, str | None]:
    """Resolve a term to an article title.

    First the exact title (redirects followed to their target; disambiguation pages
    skipped), then the top search hit. Returns (title, "exact" | "search"), or
    (None, None) when nothing matches.
    """
    lang_host = f"https://{project}"
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True,
                                 headers={"User-Agent": USER_AGENT}) as c:
        await _limiter.wait()
        r = await c.get(f"{lang_host}/w/api.php",
                        params={"action": "query", "titles": term, "redirects": "1",
                                "prop": "pageprops", "ppprop": "disambiguation",
                                "format": "json", "formatversion": "2"})
        r.raise_for_status()
        pages = ((r.json() or {}).get("query") or {}).get("pages") or []
        page = pages[0] if pages and isinstance(pages[0], dict) else {}
        if (page.get("title") and not page.get("missing") and not page.get("invalid")
                and page.get("ns", 0) == 0
                and "disambiguation" not in (page.get("pageprops") or {})):
            return page["title"], "exact"
        await _limiter.wait()
        r = await c.get(f"{lang_host}/w/api.php",
                        params={"action": "opensearch", "search": term, "limit": "5",
                                "namespace": "0", "redirects": "resolve",
                                "format": "json"})
        r.raise_for_status()
        data = r.json()
    titles = data[1] if isinstance(data, list) and len(data) > 1 else []
    return (titles[0], "search") if titles else (None, None)


async def _wiki_pageviews(article: str, granularity: str, start: str,
                          end: str, project: str) -> list[dict]:
    await _limiter.wait()
    art = urllib.parse.quote(article.replace(" ", "_"), safe="")
    url = (f"{WIKI_REST}/{project}/all-access/user/{art}/"
           f"{granularity}/{start}/{end}")
    async with httpx.AsyncClient(timeout=40.0, follow_redirects=True,
                                 headers={"User-Agent": USER_AGENT}) as c:
        r = await c.get(url)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        body = r.json()
    out = []
    for it in (body.get("items") or []):
        ts = str(it.get("timestamp", ""))[:8]
        iso = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}" if len(ts) == 8 else ts
        out.append({"date": iso, "views": it.get("views")})
    return out


def _velocity(series: list[dict], value_key: str) -> dict:
    """Demand-ignition read: recent vs trailing baseline + breakout."""
    vals = [s[value_key] for s in series if isinstance(s.get(value_key), (int, float))]
    if len(vals) < 4:
        return {"read": "insufficient data", "points": len(vals)}
    k = max(1, min(3, len(vals) // 4))
    recent = sum(vals[-k:]) / k
    baseline_vals = vals[:-k]
    baseline = sum(baseline_vals) / len(baseline_vals)
    pct = ((recent / baseline) - 1) * 100 if baseline else None
    peak = max(vals)
    breakout = vals[-1] >= 0.98 * peak and vals[-1] >= baseline
    if pct is None:
        read = "n/a"
    elif pct >= 50:
        read = "IGNITING (accelerating demand)"
    elif pct >= 15:
        read = "rising"
    elif pct > -15:
        read = "steady"
    else:
        read = "fading"
    return {
        "read": read,
        "recent_avg": round(recent, 1),
        "baseline_avg": round(baseline, 1),
        "pct_vs_baseline": round(pct, 1) if pct is not None else None,
        "latest": vals[-1], "series_peak": peak,
        "at_or_near_peak": breakout,
        "points": len(vals),
    }


def _window(months: int) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=int(months * 30.5) + 5)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


# ----------------------------------------------------------------------------
# Tools — Wikipedia (reliable core)
# ----------------------------------------------------------------------------

@mcp.tool()
async def demand_trend(term: str, months: int = 12, granularity: str = "monthly",
                       project: str = "en.wikipedia.org") -> dict:
    """Public attention to a topic over time, with a velocity / ignition read.

    Uses Wikipedia pageviews (absolute counts, real history) as a free, reliable
    demand proxy — the SRC-SEARCHTREND workhorse for CSR Gate 2 (is a
    company-independent demand driver like GLP-1 real and durable?) and ThirdWave
    adoption velocity.

    Args:
        term: A topic/term (resolved to the best-matching Wikipedia article).
        months: Lookback window (default 12).
        granularity: "monthly" (default) or "daily".
        project: Wikipedia project (default en.wikipedia.org).
    Returns:
        The matched article, the pageview series, and a velocity read
        (igniting / rising / steady / fading + breakout flag).
    """
    article, match = await _wiki_resolve(term, project)
    if not article:
        return {"term": term, "found": False,
                "note": "No matching Wikipedia article. Try a more specific term."}
    start, end = _window(months)
    series = await _wiki_pageviews(article, granularity, start, end, project)
    if not series:
        return {"term": term, "article": article, "match": match, "found": False,
                "note": "No pageview data for that article/window."}
    return {
        "term": term, "article": article, "match": match, "found": True,
        "granularity": granularity, "source": "Wikipedia pageviews (absolute)",
        "velocity": _velocity(series, "views"),
        "series": series,
        "pit_note": "Absolute counts (backtestable). Still snapshot raw pulls; the "
                    "edge is the 12-24mo lead (PIT_NONE per blueprint).",
    }


@mcp.tool()
async def compare_demand(terms: list[str], months: int = 12,
                         project: str = "en.wikipedia.org") -> dict:
    """Compare public attention across several topics (absolute, so valid to rank).

    Unlike Google Trends (which rebases each query to its own 0-100), Wikipedia
    pageviews are absolute — so cross-term comparison is meaningful.

    Args:
        terms: Topics/terms to compare.
        months: Lookback window (default 12).
        project: Wikipedia project (default en.wikipedia.org).
    Returns:
        Each term's matched article, latest level, and velocity read — ranked.
    """
    start, end = _window(months)
    rows = []
    for term in terms[:10]:
        article, match = await _wiki_resolve(term, project)
        if not article:
            rows.append({"term": term, "found": False})
            continue
        series = await _wiki_pageviews(article, "monthly", start, end, project)
        if not series:
            rows.append({"term": term, "article": article, "match": match, "found": False})
            continue
        v = _velocity(series, "views")
        rows.append({"term": term, "article": article, "match": match, "found": True,
                     "latest_views": v.get("latest"),
                     "velocity_read": v.get("read"),
                     "pct_vs_baseline": v.get("pct_vs_baseline")})
    ranked = sorted([r for r in rows if r.get("found")],
                    key=lambda r: r.get("latest_views") or 0, reverse=True)
    return {"terms": terms, "source": "Wikipedia pageviews (absolute)",
            "ranked_by_latest_level": ranked,
            "unmatched": [r["term"] for r in rows if not r.get("found")]}


# ----------------------------------------------------------------------------
# Tool — Google Trends (best-effort via pytrends)
# ----------------------------------------------------------------------------

def _get_pytrends():
    try:
        from pytrends.request import TrendReq  # type: ignore
    except Exception as e:  # pragma: no cover
        return None, f"pytrends not available ({e})"
    try:
        kwargs: dict[str, Any] = {"hl": "en-US", "tz": 360, "timeout": (10, 25)}
        if GOOGLE_TRENDS_PROXY:
            kwargs["proxies"] = [GOOGLE_TRENDS_PROXY]
            kwargs["retries"] = 2
        return TrendReq(**kwargs), None
    except Exception as e:
        return None, f"pytrends init failed ({e})"


_FALLBACK = ("Google Trends is unavailable from this host — Google rate-limits "
             "cloud IPs (pytrends is also unmaintained). Use the demand_trend / "
             "compare_demand tools (Wikipedia, reliable) for the same demand-"
             "ignition signal, or web search (the blueprint's stated fallback). "
             "Google Trends works best when this connector is run locally via "
             "Claude Desktop on a home network.")


@mcp.tool()
async def google_trends(terms: list[str], timeframe: str = "today 12-m",
                        geo: str = "") -> dict:
    """Relative Google search interest (BEST-EFFORT — often blocked on servers).

    Returns Google Trends' rebased 0-100 interest. This usually FAILS from a hosted
    server (Google blocks cloud IPs); on failure it degrades to a clear pointer to
    the reliable Wikipedia tools and web search. Runs best locally (Claude Desktop).

    Args:
        terms: Up to 5 search terms.
        timeframe: e.g. "today 12-m", "today 5-y", "2024-01-01 2024-12-31".
        geo: "" for worldwide, or a code like "US".
    Returns:
        Per-term interest series + velocity read, OR a graceful fallback message.
    """
    pt, err = _get_pytrends()
    if pt is None:
        return {"available": False, "engine": "google_trends", "reason": err,
                "fallback": _FALLBACK}
    try:
        kw = [t for t in terms][:5]
        await _limiter.wait()
        pt.build_payload(kw, cat=0, timeframe=timeframe, geo=geo, gprop="")
        df = pt.interest_over_time()
        if df is None or df.empty:
            return {"available": False, "engine": "google_trends",
                    "reason": "empty result (likely throttled)", "fallback": _FALLBACK}
        out = {}
        for term in kw:
            if term not in df.columns:
                continue
            series = [{"date": str(idx.date()), "interest": int(val)}
                      for idx, val in df[term].items()]
            out[term] = {"velocity": _velocity(series, "interest"), "series": series}
        return {"available": True, "engine": "google_trends",
                "timeframe": timeframe, "geo": geo or "worldwide",
                "results": out,
                "note": "Rebased 0-100 per query (NOT absolute; cross-query levels "
                        "not comparable). Snapshot raw pulls (PIT_NONE)."}
    except Exception as e:
        return {"available": False, "engine": "google_trends",
                "reason": f"request failed ({type(e).__name__})", "fallback": _FALLBACK}


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    _pt = "available" if _get_pytrends()[0] is not None else "unavailable"
    print(f"[search-demand] build-2 | dns_rebinding_protection="
          f"{_security.enable_dns_rebinding_protection} | "
          f"engines=wikipedia+google_trends({_pt}) | "
          f"proxy={'set' if GOOGLE_TRENDS_PROXY else 'none'} | "
          f"user_agent={'env' if USER_AGENT != DEFAULT_USER_AGENT else 'default'} | "
          f"transport={os.environ.get('MCP_TRANSPORT', 'stdio')}",
          file=sys.stderr, flush=True)
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable-http", "streamable_http"):
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")
