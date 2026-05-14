/**
 * scraper.js — New DB2 API Scraper
 *
 * Reads:  career_page_configs WHERE source_type='api' AND is_verified=true
 * Writes: raw_jobs (dedup + queue + history)
 * Logs:   scrape_runs (audit + first-scrape detection)
 *
 * Usage:
 *   node scraper.js               → scrape all due API configs
 *   node scraper.js --dry-run     → print only, no writes
 *   node scraper.js --limit=10    → only first N configs
 *   node scraper.js --force       → ignore last_scraped_at cooldown
 */

import { createClient } from '@supabase/supabase-js';

// ─── CONFIG ──────────────────────────────────────────────────────────────────

const DB2_URL = process.env.DB2_URL || 'https://buowaosqezcvdpdjcewq.supabase.co';
const DB2_KEY = process.env.DB2_KEY;

const SCRAPE_COOLDOWN_HOURS = 6;     // skip configs scraped within this window
const MAX_FAILURES           = 3;    // disable config after this many consecutive failures
const REQUEST_DELAY_MS       = 300;  // polite delay between requests

// ─── ARGS ─────────────────────────────────────────────────────────────────────

const DRY_RUN      = process.argv.includes('--dry-run');
const FORCE        = process.argv.includes('--force');
const RANDOM_JOBS  = process.argv.includes('--random');        // shuffle before --max-jobs slice
const limitArg     = process.argv.find(a => a.startsWith('--limit='));
const LIMIT        = limitArg ? parseInt(limitArg.split('=')[1]) : null;
const provArg      = process.argv.find(a => a.startsWith('--provider='));
const PROVIDER     = provArg ? provArg.split('=')[1] : null;
const maxJobsArg   = process.argv.find(a => a.startsWith('--max-jobs='));
const MAX_JOBS     = maxJobsArg ? parseInt(maxJobsArg.split('=')[1]) : null;
const companiesArg = process.argv.find(a => a.startsWith('--companies='));
// Comma-separated company slugs, e.g. --companies=dropbox,gitlab,figma
const TARGET_SLUGS = companiesArg ? companiesArg.split('=')[1].split(',').map(s => s.trim()).filter(Boolean) : null;
const minNewArg    = process.argv.find(a => a.startsWith('--min-new-jobs='));
const MIN_NEW_JOBS = minNewArg ? parseInt(minNewArg.split('=')[1]) : null;
const minCoArg     = process.argv.find(a => a.startsWith('--min-companies='));
// Stop after this many different companies have contributed at least 1 new pending job
const MIN_COMPANIES = minCoArg ? parseInt(minCoArg.split('=')[1]) : null;
const maxPerCoArg  = process.argv.find(a => a.startsWith('--max-pending-per-company='));
// Cap new pending jobs saved per company (prevents one big company flooding the queue)
const MAX_PENDING_PER_COMPANY = maxPerCoArg ? parseInt(maxPerCoArg.split('=')[1]) : null;
// Only scrape providers whose API returns a trustworthy posting date
const RELIABLE_DATES_ONLY = process.argv.includes('--reliable-dates-only');
const RELIABLE_DATE_PROVIDERS = ['Greenhouse', 'Lever', 'Ashby', 'Workable', 'Eightfold', 'Recruitee', 'BambooHR', 'Personio'];

const db2 = createClient(DB2_URL, DB2_KEY);

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('══════════════════════════════════════════');
  console.log('DB2 API SCRAPER');
  if (DRY_RUN)       console.log('DRY RUN — no writes');
  if (MAX_JOBS)      console.log(`MAX JOBS — ${MAX_JOBS} job(s) per config${RANDOM_JOBS ? ' (random)' : ''}`);
  if (FORCE)         console.log('FORCE — ignoring cooldown');
  if (TARGET_SLUGS)  console.log(`TARGET — ${TARGET_SLUGS.join(', ')}`);
  if (MIN_NEW_JOBS)   console.log(`MIN NEW JOBS — stop after ${MIN_NEW_JOBS} new pending jobs`);
  if (MIN_COMPANIES)  console.log(`MIN COMPANIES — stop after ${MIN_COMPANIES} companies contribute new jobs`);
  if (MAX_PENDING_PER_COMPANY) console.log(`MAX PER COMPANY — cap at ${MAX_PENDING_PER_COMPANY} new pending jobs per company`);
  if (RELIABLE_DATES_ONLY) console.log(`RELIABLE DATES ONLY — ${RELIABLE_DATE_PROVIDERS.join(', ')}`);
  console.log('══════════════════════════════════════════\n');

  // Resolve target company slugs → IDs (if --companies= was passed)
  let targetCompanyIds = null;
  if (TARGET_SLUGS) {
    const { data: targetCos } = await db2.from('companies').select('id, slug').in('slug', TARGET_SLUGS);
    targetCompanyIds = (targetCos || []).map(c => c.id);
    if (targetCompanyIds.length === 0) {
      console.error(`No companies found for slugs: ${TARGET_SLUGS.join(', ')}`);
      process.exit(1);
    }
    console.log(`Resolved ${targetCompanyIds.length}/${TARGET_SLUGS.length} company slugs to IDs\n`);
  }

  // Load configs
  const threshold = new Date(Date.now() - SCRAPE_COOLDOWN_HOURS * 3600000).toISOString();
  let q = db2.from('career_page_configs')
    .select('id, company_id, ats_provider, source_type, api_endpoint, api_endpoint_detail, career_page_url, discovered_from')
    .eq('source_type', 'api')
    .eq('is_verified', true);

  if (!FORCE) q = q.or(`last_scraped_at.is.null,last_scraped_at.lt.${threshold}`);
  if (PROVIDER) q = q.eq('ats_provider', PROVIDER);
  if (RELIABLE_DATES_ONLY) q = q.in('ats_provider', RELIABLE_DATE_PROVIDERS);
  // --min-new-jobs: ignore --companies filter, scrape ALL verified configs stalest-first
  if (!MIN_NEW_JOBS && targetCompanyIds) q = q.in('company_id', targetCompanyIds);

  const { data: configs, error } = await q.order('last_scraped_at', { ascending: true, nullsFirst: true });
  if (error) { console.error('Failed to load configs:', error.message); process.exit(1); }

  const toScrape = LIMIT ? configs.slice(0, LIMIT) : configs;
  console.log(`Configs to scrape: ${toScrape.length} (of ${configs.length} due)\n`);

  // Track which companies have been scraped before (for first-scrape detection).
  // Check raw_jobs (not just scrape_runs) so companies scraped by the old scraper
  // are correctly treated as non-first-scrape and their new jobs go to pending.
  const { data: priorRuns }  = await db2.from('scrape_runs').select('company_id').eq('status', 'success');
  const { data: rawJobCos }  = await db2.from('raw_jobs').select('company_id').limit(300000);
  const scrapedCompanies = new Set([
    ...(priorRuns  || []).map(r => r.company_id),
    ...(rawJobCos  || []).map(r => r.company_id),
  ]);
  console.log(`Companies with existing raw_jobs: ${scrapedCompanies.size}\n`);

  const totals = { configs: 0, jobs_found: 0, jobs_new: 0, jobs_updated: 0, errors: 0 };

  for (let i = 0; i < toScrape.length; i++) {
    const config = toScrape[i];
    const isFirstScrape = !scrapedCompanies.has(config.company_id);
    console.log(`[${i + 1}/${toScrape.length}] ${config.ats_provider || 'API'} | ${config.api_endpoint.substring(0, 60)}`);

    const runStart = new Date().toISOString();
    let runStatus = 'success';
    let runError = null;
    let jobsFound = 0, jobsNew = 0, jobsUpdated = 0;

    try {
      let jobs = await fetchJobs(config);
      if (MAX_JOBS) {
        // Shuffle first so we get a random selection, not just the first N
        if (RANDOM_JOBS) {
          for (let j = jobs.length - 1; j > 0; j--) {
            const k = Math.floor(Math.random() * (j + 1));
            [jobs[j], jobs[k]] = [jobs[k], jobs[j]];
          }
        }
        jobs = jobs.slice(0, MAX_JOBS);
      }
      // Cap first-scrape companies at 100 jobs — no point storing thousands of
      // skipped_first_scrape entries; we'll catch the rest on the second scrape
      if (isFirstScrape && jobs.length > 100) {
        jobs = jobs.slice(0, 100);
      }
      jobsFound = jobs.length;
      totals.jobs_found += jobsFound;

      console.log(`  → ${jobsFound} jobs${isFirstScrape ? ' (first scrape — will skip_first_scrape)' : ''}`);

      if (!DRY_RUN) {
        let jobsPending = 0;
        for (const job of jobs) {
          // Stop inserting new pending jobs for this company once cap is reached
          if (MAX_PENDING_PER_COMPANY && jobsPending >= MAX_PENDING_PER_COMPANY && !isFirstScrape) {
            const result = await upsertRawJob(job, config, isFirstScrape, true);
            if (result === 'updated') jobsUpdated++;
            continue;
          }
          const result = await upsertRawJob(job, config, isFirstScrape);
          if (result === 'new')          { jobsNew++; if (!isFirstScrape) jobsPending++; }
          else if (result === 'updated') { jobsUpdated++; }
        }
        totals.jobs_new     += jobsNew;
        totals.jobs_updated += jobsUpdated;
        totals.jobs_pending  = (totals.jobs_pending || 0) + jobsPending;
        if (jobsPending > 0) totals.companies_with_new = (totals.companies_with_new || 0) + 1;
        console.log(`  → new: ${jobsNew}  updated: ${jobsUpdated}${jobsPending ? `  pending: ${jobsPending}` : ''}`);

        // Mark this company as scraped
        scrapedCompanies.add(config.company_id);

        // Update config
        await db2.from('career_page_configs').update({
          last_scraped_at: new Date().toISOString(),
          consecutive_failures: 0,
          last_error: null,
          last_error_at: null,
        }).eq('id', config.id);
      }

    } catch (err) {
      runStatus = 'failed';
      runError = err.message;
      totals.errors++;
      console.log(`  ❌ ${err.message}`);

      if (!DRY_RUN) {
        // Increment consecutive failures, disable after MAX_FAILURES
        const { data: cur } = await db2.from('career_page_configs')
          .select('consecutive_failures').eq('id', config.id).single();
        const newFailures = (cur?.consecutive_failures || 0) + 1;
        await db2.from('career_page_configs').update({
          consecutive_failures: newFailures,
          is_verified: newFailures >= MAX_FAILURES ? false : true,
          last_error: err.message,
          last_error_at: new Date().toISOString(),
        }).eq('id', config.id);
        if (newFailures >= MAX_FAILURES) {
          console.log(`  ⚠ Disabled after ${MAX_FAILURES} failures`);
        }
      }
    }

    // Log to scrape_runs
    if (!DRY_RUN) {
      await db2.from('scrape_runs').insert({
        company_id:   config.company_id,
        config_id:    config.id,
        is_first_scrape: isFirstScrape,
        jobs_found:   jobsFound,
        jobs_new:     jobsNew,
        jobs_updated: jobsUpdated,
        started_at:   runStart,
        completed_at: new Date().toISOString(),
        status:       runStatus,
        error_message: runError
      });
    }

    totals.configs++;

    // Stop early once enough companies have contributed new pending jobs
    if (MIN_COMPANIES && (totals.companies_with_new || 0) >= MIN_COMPANIES) {
      console.log(`\n✅ ${totals.companies_with_new} companies contributed new jobs — stopping early.`);
      break;
    }
    if (MIN_NEW_JOBS && (totals.jobs_pending || 0) >= MIN_NEW_JOBS) {
      console.log(`\n✅ Reached ${totals.jobs_pending} pending jobs — stopping early.`);
      break;
    }

    await sleep(REQUEST_DELAY_MS);
  }

  // Summary
  console.log('\n══════════════════════════════════════════');
  console.log('DONE');
  console.log(`Configs scraped:  ${totals.configs}`);
  console.log(`Jobs found:       ${totals.jobs_found}`);
  console.log(`Jobs new:         ${totals.jobs_new}`);
  console.log(`Jobs updated:     ${totals.jobs_updated}`);
  console.log(`Errors:           ${totals.errors}`);

  if (!DRY_RUN) {
    const { count: pending } = await db2.from('raw_jobs').select('*', { count: 'exact', head: true }).eq('status', 'pending');
    const { count: total }   = await db2.from('raw_jobs').select('*', { count: 'exact', head: true });
    console.log(`\nraw_jobs total:   ${total}`);
    console.log(`status=pending:   ${pending}`);
  }
}

// ─── FETCH JOBS ───────────────────────────────────────────────────────────────

async function fetchJobs(config) {
  const { ats_provider, api_endpoint } = config;

  if (ats_provider === 'Workable')        return fetchWorkable(config);
  if (ats_provider === 'Jobvite')         return fetchJobvite(config);
  if (ats_provider === 'Eightfold')       return fetchEightfold(config);
  if (ats_provider === 'SmartRecruiters') return fetchSmartRecruiters(config);
  if (ats_provider === 'BambooHR')        return fetchBambooHR(config);
  if (ats_provider === 'Personio')        return fetchPersonio(config);

  const res = await fetch(api_endpoint, {
    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; JobScraper/1.0)' },
    signal: AbortSignal.timeout(15000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const data = await res.json();
  return normalizeResponse(data, config);
}

async function fetchSmartRecruiters(config) {
  const jobs = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const url = `${config.api_endpoint}?limit=${limit}&offset=${offset}`;
    const res = await fetch(url, {
      headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; JobScraper/1.0)' },
      signal: AbortSignal.timeout(15000)
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const page = data.content || [];
    jobs.push(...page.map(raw => normalizeJob(raw, config)));
    if (jobs.length >= (data.totalFound || 0) || page.length < limit) break;
    offset += limit;
    await sleep(200);
  }

  return jobs.filter(Boolean);
}

async function fetchWorkable(config) {
  const res = await fetch(config.api_endpoint, {
    method: 'POST',
    headers: { 'Accept': 'application/json', 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0' },
    body: JSON.stringify({ query: '', location: [], department: [], worktype: [], remote: [] }),
    signal: AbortSignal.timeout(15000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data.results || []).map(raw => normalizeJob(raw, config)).filter(Boolean);
}

async function fetchEightfold(config) {
  const jobs = [];
  const PAGE_SIZE = 10; // Eightfold caps at 10 per request
  let start = 0;
  let total = null;

  do {
    const url = `${config.api_endpoint}?num=${PAGE_SIZE}&start=${start}`;
    const res = await fetch(url, {
      headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; JobScraper/1.0)' },
      signal: AbortSignal.timeout(15000)
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (total === null) total = data.count || 0;
    const positions = data.positions || [];
    if (positions.length === 0) break;
    for (const pos of positions) {
      const job = normalizeJob(pos, config);
      if (job) jobs.push(job);
    }
    start += PAGE_SIZE;
    if (start >= total) break;
    await sleep(200);
  } while (true);

  return jobs;
}

async function fetchJobvite(config) {
  const res = await fetch(config.api_endpoint, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; JobScraper/1.0)' },
    signal: AbortSignal.timeout(15000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const html = await res.text();
  return parseJobviteHTML(html, config);
}

async function fetchBambooHR(config) {
  const res = await fetch(config.api_endpoint, {
    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' },
    signal: AbortSignal.timeout(15000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data.result || []).map(raw => normalizeJob(raw, config)).filter(Boolean);
}

async function fetchPersonio(config) {
  const res = await fetch(config.api_endpoint, {
    headers: { 'Accept': 'text/xml,application/xml', 'User-Agent': 'Mozilla/5.0' },
    signal: AbortSignal.timeout(15000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const xml = await res.text();
  return parsePersonioXML(xml, config);
}

function parsePersonioXML(xml, config) {
  const jobs = [];
  const posRe = /<position[^>]*id="(\d+)"[^>]*>([\s\S]*?)<\/position>/g;
  let m;
  while ((m = posRe.exec(xml)) !== null) {
    const id = m[1];
    const block = m[2];
    const get = tag => {
      const r = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`));
      return r ? r[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim() : null;
    };
    const title = get('name');
    const url   = get('url');
    if (!title || !url) continue;
    // Extract all <value> blocks from jobDescriptions
    let description = null;
    const descBlock = block.match(/<jobDescriptions>([\s\S]*?)<\/jobDescriptions>/);
    if (descBlock) {
      const vals = [];
      const vRe = /<value>([\s\S]*?)<\/value>/g;
      let vm;
      while ((vm = vRe.exec(descBlock[1])) !== null) {
        const decoded = vm[1].replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&quot;/g,'"');
        if (decoded.trim()) vals.push(decoded.trim());
      }
      description = vals.join('\n') || null;
    }
    const raw_data = {
      id, name: title,
      office:         get('office'),
      department:     get('department'),
      employmentType: get('employmentType'),
      schedule:       get('schedule'),
      createdAt:      get('createdAt'),
    };
    jobs.push({
      title,
      application_url: url,
      location_raw:    raw_data.office || null,
      external_id:     id,
      posted_date:     raw_data.createdAt ? new Date(raw_data.createdAt).toISOString() : null,
      description,
      raw_data,
    });
  }
  return jobs;
}

// ─── NORMALIZE ────────────────────────────────────────────────────────────────

function normalizeResponse(data, config) {
  let raw = [];
  if (Array.isArray(data))      raw = data;
  else if (data.jobs)           raw = data.jobs;
  else if (data.results)        raw = data.results;
  else if (data.content)        raw = data.content;
  else if (data.offers)         raw = data.offers;
  else if (data.jobPostings)    raw = data.jobPostings;
  return raw.map(r => normalizeJob(r, config)).filter(Boolean);
}

function normalizeJob(raw, config) {
  const ats = config.ats_provider;
  let title, application_url, location_raw, external_id, posted_date, description, raw_data;

  switch (ats) {
    case 'Greenhouse':
      title           = raw.title;
      application_url = raw.absolute_url;
      location_raw    = raw.location?.name || null;
      external_id     = String(raw.id);
      posted_date     = raw.first_published || raw.updated_at || null;
      raw_data        = raw;
      break;

    case 'Lever':
      title           = raw.text;
      application_url = raw.hostedUrl;
      location_raw    = raw.categories?.location || null;
      external_id     = raw.id;
      posted_date     = raw.createdAt ? new Date(raw.createdAt).toISOString() : null;
      description     = raw.descriptionPlain || null;
      raw_data        = raw;
      break;

    case 'Ashby':
      title           = raw.title;
      application_url = raw.jobUrl || raw.applyUrl;
      location_raw    = raw.locationName || raw.location || null;
      external_id     = raw.id;
      posted_date     = raw.publishedAt || null;
      description     = raw.descriptionHtml || null;
      raw_data        = raw;
      break;

    case 'SmartRecruiters':
      title           = raw.name;
      application_url = raw.ref || `https://jobs.smartrecruiters.com/${raw.company?.identifier}/${raw.id}`;
      location_raw    = raw.location ? [raw.location.city, raw.location.country].filter(Boolean).join(', ') : null;
      external_id     = raw.id;
      posted_date     = raw.releasedDate || null;
      raw_data        = raw;
      break;

    case 'Breezy HR':
      title           = raw.name;
      application_url = raw.url;
      location_raw    = raw.location ? [raw.location.city, raw.location.country].filter(Boolean).join(', ') : null;
      external_id     = raw._id || raw.id;
      posted_date     = raw.published_date || null;
      raw_data        = raw;
      break;

    case 'Workable':
      title           = raw.title;
      application_url = raw.url;
      location_raw    = raw.location ? [raw.location.city, raw.location.country].filter(Boolean).join(', ') : null;
      external_id     = raw.id;
      posted_date     = raw.created_at || null;
      raw_data        = raw;
      break;

    case 'Eightfold':
      title           = raw.name || raw.posting_name;
      application_url = raw.canonicalPositionUrl || null;
      location_raw    = raw.location || (raw.locations?.length ? raw.locations[0] : null);
      external_id     = String(raw.id || '');
      posted_date     = raw.t_create ? new Date(raw.t_create * 1000).toISOString() : null;
      raw_data        = raw;
      break;

    case 'Recruitee':
      title           = raw.title;
      application_url = raw.careers_apply_url || raw.careers_url;
      location_raw    = raw.location || [raw.city, raw.country].filter(Boolean).join(', ') || null;
      external_id     = String(raw.id || raw.guid || '');
      posted_date     = raw.published_at ? new Date(raw.published_at).toISOString() : null;
      description     = raw.description || null;
      raw_data        = raw;
      break;

    case 'BambooHR':
      title           = raw.title;
      application_url = raw.url;
      location_raw    = [raw.location?.city, raw.location?.state].filter(Boolean).join(', ')
                        || raw.location?.country || null;
      external_id     = String(raw.id);
      posted_date     = raw.datePosted ? new Date(raw.datePosted).toISOString() : null;
      raw_data        = raw;
      break;

    default:
      title           = raw.title || raw.name;
      application_url = raw.absolute_url || raw.url || raw.hostedUrl || raw.jobUrl;
      external_id     = String(raw.id || '');
      raw_data        = raw;
  }

  if (!title || !application_url) return null;

  return {
    title,
    application_url: cleanUrl(application_url),
    location_raw:    location_raw || null,
    external_id:     external_id || null,
    posted_date:     posted_date || null,
    description:     description || null,
    raw_data:        raw_data || null,
  };
}

function parseJobviteHTML(html, config) {
  const jobs = [];
  const seen = new Set();
  const baseUrl = new URL(config.api_endpoint).origin;
  const cleanHtml = html.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '');

  for (const m of cleanHtml.matchAll(/<(?:tr|li)[^>]*>([\s\S]*?)<\/(?:tr|li)>/gi)) {
    const block = m[1];
    const linkMatch = block.match(/<a[^>]*href=["']([^"']*\/job\/([^"'/?]+))["'][^>]*>([\s\S]*?)<\/a>/i);
    if (!linkMatch) continue;
    const [, jobPath, jobId, linkContent] = linkMatch;
    if (seen.has(jobId)) continue;
    seen.add(jobId);
    const title = linkContent.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim();
    const locMatch = block.match(/jv-job-list-location[^>]*>([\s\S]*?)<\/(?:td|div)>/i);
    const location = locMatch ? locMatch[1].replace(/<[^>]*>/g, '').trim() : null;
    const url = jobPath.startsWith('http') ? jobPath : `${baseUrl}${jobPath}`;
    if (title) jobs.push({ title, application_url: url, location_raw: location, external_id: jobId, posted_date: null, raw_data: null });
  }
  return jobs;
}

// ─── UPSERT RAW JOB ──────────────────────────────────────────────────────────

async function upsertRawJob(job, config, isFirstScrape, skipIfNew = false) {
  try {
    // Check if already exists
    const { data: existing } = await db2.from('raw_jobs')
      .select('id, seen_count')
      .eq('application_url', job.application_url)
      .maybeSingle();

    if (existing) {
      // Already seen — update last_seen_at + increment seen_count
      await db2.from('raw_jobs').update({
        last_seen_at: new Date().toISOString(),
        seen_count: existing.seen_count + 1
      }).eq('id', existing.id);
      return 'updated';
    }

    // Per-company cap reached — just update last_seen_at without inserting
    if (skipIfNew) return 'updated';

    // New job — fetch detail API for providers that don't include description in list API
    const detailData = await fetchJobDetail(job, config);
    if (detailData) {
      job.description = detailData.description || job.description || null;
      job.raw_data    = { ...job.raw_data, ...detailData.extra };
    }

    // New job — if first scrape but ATS gives a posted_date within 24h, treat as pending
    const CUTOFF_24H = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const isRecentlyPosted = job.posted_date && job.posted_date >= CUTOFF_24H;
    const status = (isFirstScrape && !isRecentlyPosted) ? 'skipped_first_scrape' : 'pending';
    const { error } = await db2.from('raw_jobs').insert({
      company_id:      config.company_id,
      config_id:       config.id,
      title:           job.title,
      description:     job.description || null,
      location_raw:    job.location_raw || null,
      application_url: job.application_url,
      external_id:     job.external_id || null,
      posted_date:     job.posted_date || null,
      raw_data:        job.raw_data || null,
      is_first_scrape: isFirstScrape,
      status,
    });

    if (error) {
      if (error.code === '23505') return 'updated'; // race condition dupe
      throw error;
    }
    return 'new';
  } catch (err) {
    console.log(`    ⚠ job error (${job.application_url?.substring(0, 50)}): ${err.message}`);
    return 'error';
  }
}

// ─── FETCH JOB DETAIL ─────────────────────────────────────────────────────────
// Only called for NEW jobs from providers that don't include description in list API

async function fetchJobDetail(job, config) {
  const ats = config.ats_provider;

  try {
    // Greenhouse: boards-api.greenhouse.io/v1/boards/{board}/jobs/{id}
    if (ats === 'Greenhouse') {
      const match = job.application_url.match(/greenhouse\.io\/([^\/]+)\/jobs\/(\d+)/);
      if (!match) return null;
      const url = `https://boards-api.greenhouse.io/v1/boards/${match[1]}/jobs/${match[2]}`;
      const res = await fetch(url, {
        headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' },
        signal: AbortSignal.timeout(10000)
      });
      if (!res.ok) return null;
      const data = await res.json();
      return {
        description: data.content || null,
        extra: {
          departments: data.departments || [],
          offices:     data.offices || [],
          education:   data.education || null,
        }
      };
    }

    // SmartRecruiters: api.smartrecruiters.com/v1/companies/{company}/postings/{id}
    if (ats === 'SmartRecruiters') {
      const companyId = job.raw_data?.company?.identifier;
      if (!companyId || !job.external_id) return null;
      const url = `https://api.smartrecruiters.com/v1/companies/${companyId}/postings/${job.external_id}`;
      const res = await fetch(url, {
        headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' },
        signal: AbortSignal.timeout(10000)
      });
      if (!res.ok) return null;
      const data = await res.json();
      const sections = data.jobAd?.sections || {};
      const desc = [
        sections.jobDescription?.text,
        sections.qualifications?.text,
        sections.additionalInformation?.text
      ].filter(Boolean).join('\n\n');
      return {
        description: desc || null,
        extra: {
          applyUrl:    data.applyUrl || null,
          postingUrl:  data.postingUrl || null,
        }
      };
    }

    // BambooHR: {slug}.bamboohr.com/careers/{id} — scrape description from HTML
    if (ats === 'BambooHR') {
      const slugMatch = config.api_endpoint.match(/https?:\/\/([^.]+)\.bamboohr\.com/);
      if (!slugMatch || !job.external_id) return null;
      const url = `https://${slugMatch[1]}.bamboohr.com/careers/${job.external_id}`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' },
        signal: AbortSignal.timeout(10000)
      });
      if (!res.ok) return null;
      const html = await res.text();
      // BambooHR embeds job data as JSON in a <script> tag
      const jsonMatch = html.match(/<script[^>]*type="application\/json"[^>]*>([\s\S]*?)<\/script>/);
      if (jsonMatch) {
        try {
          const data = JSON.parse(jsonMatch[1]);
          const desc = data?.job?.description || data?.description;
          if (desc) return { description: desc, extra: {} };
        } catch {}
      }
      // Fallback: extract from BambooRich div
      const richMatch = html.match(/<div[^>]*class="[^"]*BambooRich[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
      if (richMatch) return { description: richMatch[1].trim(), extra: {} };
      return null;
    }

    // Breezy HR: {company}.breezy.hr/json/{id}
    if (ats === 'Breezy HR') {
      const match = config.api_endpoint.match(/https:\/\/([^.]+)\.breezy\.hr/);
      if (!match || !job.external_id) return null;
      const url = `https://${match[1]}.breezy.hr/json/${job.external_id}`;
      const res = await fetch(url, {
        headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' },
        signal: AbortSignal.timeout(10000)
      });
      if (!res.ok) return null;
      const data = await res.json();
      return {
        description: data.description || null,
        extra: {
          tags:         data.tags || [],
          education:    data.education || null,
          experience:   data.experience || null,
        }
      };
    }

  } catch {
    // Detail fetch failed — not critical, continue without description
    return null;
  }

  return null;
}

// ─── UTILS ───────────────────────────────────────────────────────────────────

const STRIP_PARAMS = new Set(['utm_source','utm_medium','utm_campaign','utm_content','utm_term','ref','source','gh_src','lever-source','lever-origin']);

function cleanUrl(url) {
  if (!url) return url;
  try {
    const u = new URL(url);
    for (const key of [...u.searchParams.keys()]) {
      if (STRIP_PARAMS.has(key.toLowerCase())) u.searchParams.delete(key);
    }
    return u.toString();
  } catch { return url; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

main().catch(console.error);
