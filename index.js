import { XMLParser } from "fast-xml-parser";
import { gunzipSync } from "node:zlib";
import { fetch } from "undici";

/**
 * @param {string} xmlUrl - URL to an XML (sitemap/sitemapindex, news/video sitemaps, RSS/Atom/RDF)
 * @param {string|number|Date} [fromDate] - OPTIONAL lower bound (ISO/RFC822, epoch s/ms, Date). If omitted, no date filter.
 * @returns {Promise<string[]>} deduped absolute URLs
 */

export async function getSitemapUrls(xmlUrl, fromDate) {
  // ---- helpers kept INSIDE to preserve "one function" rule
  const toTs = (v) => {
    if (v == null || v === "") return NaN;
    if (v instanceof Date) return v.getTime();
    if (typeof v === "number") return v < 1e12 ? v * 1000 : v;
    const s = String(v).trim();

    // Common numeric epochs
    if (/^-?\d{9,13}$/.test(s)) {
      const n = Number(s);
      return n < 1e12 ? n * 1000 : n;
    }

    // Basic ISO date (YYYYMMDD) — not parsed by Date.parse in some runtimes
    if (/^\d{8}$/.test(s)) {
      const y = s.slice(0, 4),
        m = s.slice(4, 6),
        d = s.slice(6, 8);
      const t = Date.parse(`${y}-${m}-${d}T00:00:00Z`);
      if (Number.isFinite(t)) return t;
    }

    // If looks like "YYYY-MM-DD HH:mm:ss" without zone, treat as UTC
    if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$/.test(s)) {
      const t = Date.parse(s.replace(" ", "T") + "Z");
      if (Number.isFinite(t)) return t;
    }

    // Else let Date.parse try RFC822/RFC2822/ISO/W3C-DTF/etc.
    const t = Date.parse(s);
    return Number.isFinite(t) ? t : NaN;
  };

  const firstTs = (...vals) => {
    for (const v of vals) {
      const t = toTs(v);
      if (Number.isFinite(t)) return t;
    }
    return NaN;
  };

  const includeByFrom = (ts, fromTs) => (fromTs == null ? true : Number.isFinite(ts) && ts >= fromTs);

  const decodeBuffer = (buf, contentType) => {
    // Try to respect declared charset, default to utf-8, fallback latin1.
    const m = /charset=([\w-]+)/i.exec(contentType || "");
    const charset = (m && m[1].toLowerCase()) || "utf-8";
    try {
      let xml = new TextDecoder(charset).decode(buf);
      // strip BOM just in case
      if (xml.charCodeAt(0) === 0xfeff) xml = xml.slice(1);
      return xml;
    } catch {
      try {
        return new TextDecoder("utf-8").decode(buf);
      } catch {
        return Buffer.from(buf).toString("latin1");
      }
    }
  };

  const fromTs = fromDate == null || fromDate === "" ? null : toTs(fromDate);
  if (fromDate != null && fromDate !== "" && !Number.isFinite(fromTs)) {
    throw new Error('Invalid "fromDate"');
  }

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "", // attributes come as plain keys
    removeNSPrefix: true, // news:publication_date -> publication_date
    trimValues: true,
  });

  const queue = [xmlUrl];
  const visited = new Set();
  const results = [];
  const seen = new Set();

  while (queue.length) {
    const current = queue.pop();
    if (!current || visited.has(current)) continue;
    visited.add(current);

    let res;
    try {
      res = await fetch(current, { redirect: "follow" });
    } catch {
      continue;
    }
    if (!res.ok) continue;

    let buf = new Uint8Array(await res.arrayBuffer());
    const enc = (res.headers.get("content-encoding") || "").toLowerCase();
    const ctype = (res.headers.get("content-type") || "").toLowerCase();

    // Gunzip server-encoded or .gz file magic
    if (enc.includes("gzip") || ctype.includes("gzip") || (buf[0] === 0x1f && buf[1] === 0x8b)) {
      try {
        buf = gunzipSync(buf);
      } catch {}
    }

    const xml = decodeBuffer(buf, ctype);

    let data;
    try {
      data = parser.parse(xml);
    } catch {
      continue;
    }

    const base = res.url || current;

    // ========== 1) SITEMAP INDEX ==========
    // <sitemapindex><sitemap><loc>... + optional <lastmod> (we still crawl all)
    if (data?.sitemapindex?.sitemap) {
      const arr = Array.isArray(data.sitemapindex.sitemap) ? data.sitemapindex.sitemap : [data.sitemapindex.sitemap];

      for (const it of arr) {
        const loc = it?.loc || it?.link || (it?.url && it.url.loc);
        if (!loc) continue;
        try {
          const abs = new URL(String(loc).trim(), base).toString();
          if (!visited.has(abs)) queue.push(abs);
        } catch {}
      }
      continue;
    }

    // ========== 2) URLSET (Standard + Google News + Video + Hreflang) ==========
    // <urlset><url>…</url></urlset>
    if (data?.urlset?.url) {
      const arr = Array.isArray(data.urlset.url) ? data.urlset.url : [data.urlset.url];

      for (const it of arr) {
        const loc = it?.loc;

        // Standard lastmod variants
        const lastmod = it?.lastmod ?? it?.lastModified ?? it?.modified ?? it?.changeDate;

        // Google News: <news:news><news:publication_date>...</news:publication_date>
        // With removeNSPrefix: it.news.publication_date
        const newsNode = it?.news || it?.news_news;
        const newsTs = firstTs(
          newsNode?.publication_date,
          newsNode?.published,
          newsNode?.updated,
          newsNode?.pubDate,
          it?.["news:publication_date"], // rare flattened
        );

        // Google Video (common date fields)
        const vid = it?.video || it?.video_video;
        const videoTs = firstTs(
          vid?.publication_date,
          vid?.upload_date,
          vid?.expiration_date,
          vid?.live_stream_start_time,
          vid?.live_stream_end_time,
          vid?.release_date,
        );

        // Choose the best per-URL timestamp
        const urlTs = firstTs(lastmod, newsTs, videoTs);

        // Decide inclusion
        let include = false;
        if (fromTs == null) {
          include = !!loc;
        } else {
          include = !!loc && includeByFrom(urlTs, fromTs);
        }
        if (!include) {
          // If filtering and there's no direct date, try to borrow date from nested news/video nodes
          // (handled above already). If still no match, skip.
          continue;
        }

        // Primary URL
        try {
          const abs = new URL(String(loc).trim(), base).toString();
          if (!seen.has(abs)) {
            seen.add(abs);
            results.push(abs);
          }
        } catch {}

        // Also include any <xhtml:link rel="alternate" href="..."> alternates with SAME timestamp
        // (They usually refer to the same document in other languages/regions)
        const xh = it?.xhtml_link || it?.xhtml || it?.link;
        const links = Array.isArray(xh) ? xh : xh ? [xh] : [];
        for (const l of links) {
          // handle shapes: {href, rel}, or nested, or attribute-like after parsing
          const href = l?.href ?? l?.["@_href"] ?? l?.url ?? null;
          const rel = (l?.rel ?? l?.["@_rel"] ?? "alternate").toLowerCase();
          if (!href || rel !== "alternate") continue;
          try {
            const absAlt = new URL(String(href).trim(), base).toString();
            if (!seen.has(absAlt)) {
              // respect filter: use same ts as parent
              if (includeByFrom(urlTs, fromTs)) {
                seen.add(absAlt);
                results.push(absAlt);
              }
            }
          } catch {}
        }
      }
      continue;
    }

    // ========== 3) RSS 2.0 ==========
    if (data?.rss?.channel) {
      const ch = data.rss.channel;
      const items = ch?.item ? (Array.isArray(ch.item) ? ch.item : [ch.item]) : [];

      for (const it of items) {
        const link = it?.link || it?.enclosure?.url || it?.guid; // guid is sometimes a permalink URL
        const ts = firstTs(it?.pubDate, it?.updated, it?.date, it?.["dc:date"], it?.["dcterms:modified"], it?.issued);
        if (!link) continue;
        if (!includeByFrom(ts, fromTs)) continue;

        try {
          const abs = new URL(String(link).trim(), base).toString();
          if (!seen.has(abs)) {
            seen.add(abs);
            results.push(abs);
          }
        } catch {}
      }
      continue;
    }

    // ========== 4) ATOM ==========
    if (data?.feed) {
      const entries = data.feed.entry ? (Array.isArray(data.feed.entry) ? data.feed.entry : [data.feed.entry]) : [];

      for (const it of entries) {
        let linkVal = null;
        const L = it?.link;
        if (Array.isArray(L)) {
          let chosen = null;
          for (const l of L) {
            const rel = (l?.rel || l?.["@_rel"] || "alternate").toLowerCase();
            const href = l?.href || l?.["@_href"];
            if (!chosen) chosen = href;
            if (rel === "alternate" && href) {
              chosen = href;
              break;
            }
          }
          linkVal = chosen;
        } else if (L && typeof L === "object") {
          linkVal = L?.href || L?.["@_href"];
        } else if (typeof L === "string") {
          linkVal = L;
        }

        const ts = firstTs(
          it?.updated,
          it?.published,
          it?.issued,
          it?.["dc:date"],
          it?.["dcterms:modified"],
          // sometimes dates are attributes on entry
          it?.updated_at,
          it?.created_at,
        );

        if (!linkVal) continue;
        if (!includeByFrom(ts, fromTs)) continue;

        try {
          const abs = new URL(String(linkVal).trim(), base).toString();
          if (!seen.has(abs)) {
            seen.add(abs);
            results.push(abs);
          }
        } catch {}
      }
      continue;
    }

    // ========== 5) RDF/RSS 1.0 ==========
    if (data?.RDF || data?.rdf || data?.["rdf:RDF"]) {
      const rdf = data.RDF || data.rdf || data["rdf:RDF"];
      const items = rdf?.item ? (Array.isArray(rdf.item) ? rdf.item : [rdf.item]) : [];
      for (const it of items) {
        const link = it?.link || it?.["rss:link"] || it?.["rdf:about"] || it?.guid;
        const ts = firstTs(it?.["dc:date"], it?.["dcterms:modified"], it?.pubDate, it?.date, it?.issued);
        if (!link) continue;
        if (!includeByFrom(ts, fromTs)) continue;

        try {
          const abs = new URL(String(link).trim(), base).toString();
          if (!seen.has(abs)) {
            seen.add(abs);
            results.push(abs);
          }
        } catch {}
      }
      continue;
    }

    // ========== 6) GENERIC FALLBACK ==========
    // Pair URL-like and date-like scalars within the same object when filtering.
    // If no filter, include URL-like scalars from reasonable keys.
    const stack = [data];
    const urlLike = /^(https?:)?\/\/|^[a-z]+:\/\/|^\/[A-Za-z0-9._~!$&'()*+,;=:@/%-]+/i;
    const dateKeyLike =
      /(^|:)(lastmod|updated|pubdate|date|modified|issued|created|time|timestamp|publication_date|upload_date|expiration_date|release_date)$/i;
    const urlKeyLike = /(^|:)(loc|url|link|href|canonical|@href|@_href)$/i;

    while (stack.length) {
      const node = stack.pop();
      if (!node) continue;

      if (Array.isArray(node)) {
        for (const v of node) stack.push(v);
        continue;
      }
      if (typeof node !== "object") continue;

      const scalars = [];
      for (const [k, v] of Object.entries(node)) {
        if (v && typeof v === "object") stack.push(v);
        else scalars.push([k, v]);
      }

      let foundUrl = null,
        foundDate = null;
      for (const [k, v] of scalars) {
        const sv = String(v ?? "").trim();
        if (!foundUrl && (urlKeyLike.test(k) || urlLike.test(sv))) foundUrl = sv;
        if (
          !foundDate &&
          (dateKeyLike.test(k) ||
            /^-?\d{9,13}$/.test(sv) ||
            /^\d{8}$/.test(sv) || // basic YYYYMMDD
            /^\d{4}-\d{2}-\d{2}/.test(sv) ||
            /^[A-Z][a-z]{2},/.test(sv) ||
            /^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}/.test(sv))
        ) {
          foundDate = sv;
        }
      }

      if (foundUrl) {
        if (fromTs == null) {
          try {
            const abs = new URL(foundUrl, base).toString();
            if (!seen.has(abs)) {
              seen.add(abs);
              results.push(abs);
            }
          } catch {}
        } else if (foundDate) {
          const ts = toTs(foundDate);
          if (includeByFrom(ts, fromTs)) {
            try {
              const abs = new URL(foundUrl, base).toString();
              if (!seen.has(abs)) {
                seen.add(abs);
                results.push(abs);
              }
            } catch {}
          }
        }
      }
    }
  }

  return results;
}
