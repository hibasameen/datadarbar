# Keeping bots out of the data

Two layers. The first is in the repo and live; the second needs a Cloudflare
account and about half an hour, and is the only one that stops a bot that
ignores the rules.

## 1. robots.txt (live, built by `scripts/build_seo.py`)

- Every crawler is kept out of `/data/` — the warehouse Parquet files and the
  JavaScript payloads the charts and maps load.
- AI-training and bulk crawlers (GPTBot, ClaudeBot, CCBot, Google-Extended,
  PerplexityBot, Bytespider, meta-externalagent and ~35 others, listed in
  `AI_AND_BULK_CRAWLERS`) are kept off the whole site.
- Search engines can still index the pages and the dataset pages, so Google,
  Bing and Google Dataset Search keep working.

This binds only crawlers that honour robots.txt. The reputable ones do; a
scraper written to ignore it is not stopped by it.

## 2. Cloudflare in front of GitHub Pages (enforcement)

GitHub Pages cannot rate-limit, challenge or block anyone. Cloudflare's free
plan can, with GitHub Pages left as the origin.

### Move the DNS

1. Create a Cloudflare account and **Add a site**: `adaad.org`, Free plan.
2. Cloudflare imports the existing DNS records from Squarespace. **Check every
   record before switching** — the main adaad.org site, `www`, and above all
   any **MX / TXT records for email** must be there, or mail stops.
3. Make sure this record exists and is **Proxied** (orange cloud):
   `CNAME  darbar  hibasameen.github.io`
4. In Squarespace Domains, replace the nameservers with the two Cloudflare
   gives you. Propagation takes minutes to a few hours.
5. Cloudflare → SSL/TLS → mode **Full**. (GitHub Pages keeps serving its own
   certificate; leave "Enforce HTTPS" on in the repo's Pages settings.)

### Turn on the bot controls

Security → Bots:

- **Block AI bots**: on, for all pages.
- **Bot Fight Mode**: on.
- **AI Labyrinth** (optional): on — feeds non-compliant crawlers decoy pages.

### Rules for the data (Security → WAF)

**Custom rule — challenge anything that fetches the data from outside the site.**
The site's own pages fetch `/data/` with `Sec-Fetch-Site: same-origin`; a
browser following a download link from a dataset page does too. A script, or
a URL pasted straight in, does not, and gets a challenge a person can pass
and a bot generally cannot.

```
Expression:
(starts_with(http.request.uri.path, "/data/")
 and not any(http.request.headers["sec-fetch-site"][*] in {"same-origin"}))
Action: Managed Challenge
```

**Rate limiting rule — cap bulk downloading.** A person browsing loads a few
dozen data files a minute; a scraper loads hundreds.

```
Expression:  starts_with(http.request.uri.path, "/data/")
Characteristic: IP
Rate: 120 requests per 10 seconds   (the free plan's period is 10 s)
Action: Block, for 10 seconds
```

### Test after switching

- Open Places (district and tehsil), Economy, State and the **Query** page and
  run a query — the query engine fetches Parquet files from a worker, and
  must still load.
- Download a CSV and a Parquet file from a dataset page.
- `curl -I https://darbar.adaad.org/data/places_index.js` should now return a
  challenge (403) rather than the file.

If the Query page breaks, change the custom rule's action to **Log** for a day
and look at the Sec-Fetch-Site values it records before tightening it again.

## What none of this changes

The data is published under open licences — CC BY 4.0 for Data Darbar's own
work, and PBS's open licence for most of what it builds on. Blocking bots
controls traffic, not the right to reuse: anyone who obtains the data may
still reuse it on those terms. Restricting reuse itself would mean changing
the licence, which the PBS terms underneath do not allow for their figures.
