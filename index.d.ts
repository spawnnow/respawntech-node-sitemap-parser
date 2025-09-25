/**
 * @param {string} xmlUrl - URL to an XML (sitemap/sitemapindex, news/video sitemaps, RSS/Atom/RDF)
 * @param {string|number|Date} [fromDate] - OPTIONAL lower bound (ISO/RFC822, epoch s/ms, Date). If omitted, no date filter.
 * @returns {Promise<string[]>} deduped absolute URLs
 */

export function getSitemapUrls(xmlUrl: string, fromDate?: string | number | Date): Promise<string[]>;
