import { createClient } from '@supabase/supabase-js';
import AI from 'groq-sdk';
import { US_CITY_STATE } from './us-city-state.js';

const DB2_URL      = process.env.DB2_URL     ;
const DB2_KEY      = process.env.DB2_KEY     ;
const MAIN_URL     = process.env.MAIN_DB_URL ;
const MAIN_KEY     = process.env.MAIN_DB_KEY ;
const DB1_URL      = process.env.DB1_URL     ;
const DB1_KEY      = process.env.DB1_KEY     ;
const AI_KEY = process.env.AI_KEY;

const AI_MODEL = process.env.AI_MODEL || 'meta-llama/llama-4-scout-17b-16e-instruct';
const BATCH_SIZE = 10;
const DESC_TRUNCATE   = 1500;  
const FETCH_BATCH     = 200;
const CUTOFF_48H      = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();

const DRY_RUN         = process.argv.includes('--dry-run');
const limitArg        = process.argv.find(a => a.startsWith('--limit='));
const LIMIT           = limitArg ? parseInt(limitArg.split('=')[1]) : null;
const maxPerCoArg     = process.argv.find(a => a.startsWith('--max-per-company='));
const MAX_PER_COMPANY = maxPerCoArg ? parseInt(maxPerCoArg.split('=')[1]) : 1;
const FROM_PROMOTED   = process.argv.includes('--from-promoted');   
const SKIP_FRESHNESS  = process.argv.includes('--skip-freshness');  

const db  = createClient(DB2_URL,  DB2_KEY);
const main = createClient(MAIN_URL, MAIN_KEY);
const db1  = createClient(DB1_URL,  DB1_KEY);
const ai = new AI({ apiKey: AI_KEY });

const SYSTEM_PROMPT = `You are a job posting data extractor. Return ONLY a valid JSON object. No explanation, no markdown, no extra text.`;

function cleanDesc(html) {
  if (!html) return '';
  return html
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;|&apos;/g, "'").replace(/&nbsp;/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function smartTruncate(text, maxChars) {
  if (text.length <= maxChars) return text;
  const markers = [

    /\b(requirements?|qualifications?|what (you|we) (need|require|are looking for)|must.have|you (should|will) have|about you|what you.ll bring|minimum qualifications?)\b/i,
    /\b(responsibilities|what you.ll do|your role|key duties|the role|what you will do|in this role)\b/i,

    /\b(das bringst du mit|anforderungen|qualifikationen|dein profil|was du mitbringst|voraussetzungen|deine aufgaben)\b/i,

    /\b(profil recherché|compétences requises|vos missions|ce que nous recherchons)\b/i,
  ];
  for (const marker of markers) {
    const idx = text.search(marker);
    if (idx > 80 && idx < text.length - 200) {
      return text.substring(idx, idx + maxChars);
    }
  }
  return text.substring(0, maxChars);
}

const NON_TECH_CATEGORIES = new Set([
  'Sales', 'Marketing', 'HR', 'Admin', 'Executive', 'Operations',
  'Customer Support', 'Hospitality', 'Retail', 'Project Management',
  'Finance', 'Legal', 'Creative', 'Healthcare', 'Other',
]);

const TECH_ONLY_LOWER = new Set([
  'python','java','javascript','typescript','golang','go','ruby','rust','scala','kotlin',
  'swift','php','elixir','perl','cobol','dart','lua','bash','powershell','groovy',
  'react','vue','angular','node.js','next.js','spring','django','flask','fastapi',
  'express','svelte','helm','selenium','playwright','cypress','jest','pytest','junit',
  'kubernetes','docker','terraform','ansible','linux','git','github',
  'machine learning','deep learning','nlp','computer vision','mlops','devops',
  'soc','siem','soar','xdr','edr','sast','dast',
  'mongodb','postgresql','mysql','redis','kafka','spark','hadoop','elasticsearch',
  'aws','gcp','azure','s3','ec2','lambda','cloudformation','bigquery','snowflake',
]);

const SHORT_TECH_NAMES_LOWER = new Set(['r','go','ml','ai','bi','c#','c++','ts','js']);

const ALWAYS_STRIP_LOWER = new Set([
  'cloud','software','technology','systems','data','integration','platforms','tools',
  'web','api','saas','erp','it','mobile','digital','internet','hardware',
  'customer service','customer support','communication','leadership','teamwork',
  'agile methodology','agile methods',
  'instagram','facebook','twitter','tiktok','snapchat','youtube',
]);

function filterSkillsByCategory(skills, category) {
  if (!skills?.length) return [];
  const isTech = !NON_TECH_CATEGORIES.has(category);

  return skills.filter(s => {
    const sl = s.toLowerCase().trim();
    if (ALWAYS_STRIP_LOWER.has(sl))                    return false;
    if (!isTech && TECH_ONLY_LOWER.has(sl))            return false;
    if (!isTech && SHORT_TECH_NAMES_LOWER.has(sl))     return false;
    if (s.trim().length < 2)                            return false;
    return true;
  });
}

function buildPrompt(jobs) {
  const jobsText = jobs.map((job, i) => {
    const full = cleanDesc(job.description || '');
    const desc = smartTruncate(full, DESC_TRUNCATE);
    const loc  = job.location_raw ? `Location hint: ${job.location_raw}` : '';
    return `[JOB ${i}]\nTitle: ${job.title || 'Unknown'}\n${loc}\n${desc}`.trim();
  }).join('\n\n');

  return `Extract structured data from each job posting below.

RULES:
- job_type: Use "Location hint" as the primary signal. "Remote" anywhere in hint = "remote". A city name = "onsite". "Hybrid" = "hybrid". If hint is ambiguous AND description is unclear — return null. NEVER guess.
- skills: Only extract skills that appear in the requirements/qualifications/responsibilities section — NOT from the company description or product overview. Rules:
  * Specific named tools, platforms, certifications only (e.g. Salesforce, HubSpot, Jira, AWS, Python)
  * NO soft skills: communication, leadership, teamwork, problem-solving, organisation, attention to detail
  * NO generic terms: cloud, software, technology, systems, databases, tools, web, data, integration, API
  * NO single/two-letter names (R, Go, ML, AI, BI) unless the title is explicitly a Data Science, Statistics, or Go/Golang engineering role
  * For non-technical roles (Sales, Marketing, HR, Operations, Admin, Executive, Healthcare, Finance, Legal): only extract specific named software the person must use daily (e.g. Salesforce, HubSpot, SAP, Workday, Jira) — NO programming languages, NO cloud infra (AWS/GCP/Azure/S3), NO infra tools (Docker/Kubernetes/Linux)
  * Social media platforms (Instagram, Facebook, Twitter, TikTok, LinkedIn, YouTube) are NOT skills — only include them for Marketing/Creative roles where managing those channels is the core job
  * If fewer than 2 genuinely specific skills exist, return []
- requirements_summary: One sentence — years of experience required + key tools + domain. Return null if only company intro is visible.
- responsibilities: One sentence — what the person does day-to-day. Return null if not visible.
- min_years_exp: Minimum years of experience as a plain integer (e.g. 3 for "3+ years", 5 for "at least 5 years"). Return null if not stated.
- location_country: 2-letter ISO country code for the job's physical location (e.g. "US", "DE", "IN", "GB", "CA", "AU"). Use the "Location hint" as your primary signal — a city name alone is enough (Chicago→"US", Munich→"DE", Mumbai→"IN", São Paulo→"BR", Québec City→"CA", Cape Town→"ZA"). For fully remote jobs with no country hint return null. NEVER guess if truly ambiguous.

VALID VALUES:
- job_type: "remote" | "hybrid" | "onsite" | null
- commitment_type: "full_time" | "part_time" | "contract" | "internship" | null
- experience_level: "entry" | "mid" | "senior" | "lead" | "executive" | null
- category: Engineering | Design | Product | Marketing | Sales | Data | Finance | HR | Legal | Customer Support | Security | Research | Healthcare | Hospitality | Retail | Admin | Creative | Operations | Project Management | Executive | Other
- location_country: 2-letter ISO code | null

RETURN FORMAT (JSON only, no other text):
{"jobs":[{"job_type":"...","commitment_type":"...","experience_level":"...","category":"...","skills":["..."],"requirements_summary":"...","responsibilities":"...","location_country":"..."}]}

--- JOBS ---

${jobsText}`;
}

async function callAI(jobs) {
  let attempt = 0;
  while (attempt < 3) {
    try {
      const { data: completion, response } = await ai.chat.completions.create({
        model:           AI_MODEL,
        messages:        [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user',   content: buildPrompt(jobs) },
        ],
        response_format: { type: 'json_object' },
        temperature:     0,
        max_tokens:      2048,
      }).withResponse();

      const remainingTokens = parseInt(response.headers.get('x-ratelimit-remaining-tokens') || '30000');
      const resetTokensStr  = response.headers.get('x-ratelimit-reset-tokens') || '0s';
      const resetMs         = parseFloat(resetTokensStr) * 1000;
      if (remainingTokens < 4000 && resetMs > 0) {
        console.log(`  ⏳ Groq tokens low (${remainingTokens} left), waiting ${Math.ceil(resetMs/1000)}s...`);
        await sleep(resetMs + 500);
      }

      const parsed  = JSON.parse(completion.choices[0].message.content);
      const results = parsed.jobs || parsed.results || parsed.data || [];
      if (!Array.isArray(results)) return [];

      return results.map(r => ({
        job_type:             r.job_type             || null,
        commitment_type:      r.commitment_type      || null,
        experience_level:     r.experience_level     || null,
        category:             r.category             || null,
        skills:               Array.isArray(r.skills) ? r.skills : [],
        requirements_summary: r.requirements_summary || r.requirement_summary || r.requirements || null,
        responsibilities:     r.responsibilities     || r.responsibility       || r.duties       || null,
        min_years_exp:        (typeof r.min_years_exp === 'number' && r.min_years_exp > 0)
                                ? Math.round(r.min_years_exp) : null,
        location_country:     (typeof r.location_country === 'string' && r.location_country.length === 2)
                                ? r.location_country.toUpperCase() : null,
      }));

    } catch (err) {
      if (err?.status === 429) {
        const retryAfter = parseInt(err.headers?.['retry-after'] || '15');
        console.log(`  ⏳ Rate limited, waiting ${retryAfter}s...`);
        await sleep(retryAfter * 1000);
        attempt++;
        continue;
      }
      if (err?.status >= 500) {
        console.log(`  ⚠️  Groq server error (${err.status}), retrying in 5s...`);
        await sleep(5000);
        attempt++;
        continue;
      }

      console.log(`  ⚠️  Groq parse error: ${err.message?.substring(0, 80)}`);
      return [];
    }
  }
  console.log('  ⚠️  Groq failed after 3 attempts — falling back to rule-based for this batch');
  return [];
}

async function extractAll(jobs) {
  const results = new Map(); 

  for (let i = 0; i < jobs.length; i += BATCH_SIZE) {
    const batch     = jobs.slice(i, i + BATCH_SIZE);
    const batchEnd  = Math.min(i + BATCH_SIZE, jobs.length);
    process.stdout.write(`  Groq: ${batchEnd}/${jobs.length} jobs...\r`);

    const aiResults = await callAI(batch);

    for (let j = 0; j < batch.length; j++) {
      results.set(i + j, aiResults[j] || null);
    }
  }

  process.stdout.write('\n');
  return results;
}

function fuzzyKey(name) {
  return name.toLowerCase().replace(/[\s.\-_/]/g, '');
}

async function normalizeSkills(names, skillsMap) {
  if (!names || names.length === 0) return [];

  if (!normalizeSkills._fuzzyCache) {
    normalizeSkills._fuzzyCache = new Map();
    for (const [, skill] of skillsMap) {
      normalizeSkills._fuzzyCache.set(fuzzyKey(skill.name), skill);
    }
  }
  const fuzzyCache = normalizeSkills._fuzzyCache;

  const ids     = [];
  const seen    = new Set();

  for (const rawName of names) {
    const name = rawName.trim();
    if (!name || name.length < 2) continue;

    const slug = makeSlug(name, new Set());
    if (skillsMap.has(slug)) {
      const id = skillsMap.get(slug).id;
      if (!seen.has(id)) { seen.add(id); ids.push(id); }
      continue;
    }

    const fk = fuzzyKey(name);
    if (fuzzyCache.has(fk)) {
      const id = fuzzyCache.get(fk).id;
      if (!seen.has(id)) { seen.add(id); ids.push(id); }
      continue;
    }

    if (!DRY_RUN) {
      const newSlug = makeUniqueSkillSlug(slug, skillsMap);
      const { data, error } = await main.from('skills').upsert(
        { name, slug: newSlug, category: null },
        { onConflict: 'name', ignoreDuplicates: true }
      ).select('id').single();

      if (!error && data) {
        skillsMap.set(newSlug, { id: data.id, name, slug: newSlug });
        fuzzyCache.set(fk, { id: data.id, name });
        if (!seen.has(data.id)) { seen.add(data.id); ids.push(data.id); }
      }
    } else {

      if (!seen.has(name)) { seen.add(name); ids.push(`[new:${name}]`); }
    }
  }

  return ids;
}

function makeUniqueSkillSlug(base, skillsMap) {
  let slug = base;
  let i = 2;
  while (skillsMap.has(slug)) { slug = `${base}-${i++}`; }
  return slug;
}

async function run() {
  console.log('══════════════════════════════════════════');
  console.log('REFINER (Groq) — raw_jobs → Main DB');
  console.log(`Model: ${GROQ_MODEL}`);
  if (DRY_RUN) console.log('DRY RUN — no writes');
  console.log('══════════════════════════════════════════\n');

  console.log('Loading Main DB state...');
  const { data: existingCompanies } = await main.from('companies').select('id, domain, slug');
  const { data: existingLocations } = await main.from('locations').select('id, display_name');

  let skillsMap = new Map();
  let skillsOffset = 0;
  while (true) {
    const { data: batch } = await main.from('skills').select('id, name, slug, category')
      .not('category', 'is', null)
      .neq('category', 'remove')
      .range(skillsOffset, skillsOffset + 999);
    if (!batch || batch.length === 0) break;
    for (const s of batch) skillsMap.set(s.slug, s);
    if (batch.length < 1000) break;
    skillsOffset += 1000;
  }

  const companyByDomain = new Map((existingCompanies || []).map(c => [c.domain, c]));
  const locationByName  = new Map((existingLocations || []).map(l => [l.display_name, l]));
  const slugsUsed       = new Set((existingCompanies || []).map(c => c.slug));
  console.log(`  Companies: ${companyByDomain.size}  Locations: ${locationByName.size}  Skills: ${skillsMap.size}\n`);

  if (!DRY_RUN) {
    const { count: c1 } = await db.from('raw_jobs')
      .update({ status: 'skipped_no_desc' }, { count: 'exact' })
      .eq('status', 'pending').is('description', null);
    const { count: c2 } = await db.from('raw_jobs')
      .update({ status: 'skipped_stale' }, { count: 'exact' })
      .eq('status', 'pending').is('posted_date', null);
    const { count: c3 } = await db.from('raw_jobs')
      .update({ status: 'skipped_stale' }, { count: 'exact' })
      .eq('status', 'pending').lt('posted_date', CUTOFF_48H);
    console.log(`Pre-marked: ${c1||0} no_desc  ${(c2||0)+(c3||0)} stale\n`);
  }

  const fetchStatus = FROM_PROMOTED ? 'promoted' : 'pending';
  if (FROM_PROMOTED) console.log('TEST MODE: reading from promoted jobs (no real pending jobs available)\n');

  let rawJobs = [];
  let offset  = 0;
  while (true) {
    const { data: batch, error } = await db.from('raw_jobs')
      .select('*, companies(id, name, slug, domain, logo_url, sources), career_page_configs(ats_provider)')
      .eq('status', fetchStatus)
      .not('description', 'is', null)
      .order('first_seen_at', { ascending: false })
      .range(offset, offset + FETCH_BATCH - 1);
    if (error) { console.error('Failed to fetch raw_jobs:', error.message); process.exit(1); }
    if (!batch || batch.length === 0) break;
    rawJobs = rawJobs.concat(batch);
    if (LIMIT && rawJobs.length >= LIMIT) { rawJobs = rawJobs.slice(0, LIMIT); break; }
    if (batch.length < FETCH_BATCH) break;
    offset += FETCH_BATCH;
  }
  console.log(`Jobs to process: ${rawJobs.length}\n`);

  if (rawJobs.length === 0) {
    console.log('Nothing to do. Run the scraper first to get new pending jobs.');
    return;
  }

  console.log('Running Groq extraction...');
  const aiResultsMap = await extractAll(rawJobs);
  const aiSuccesses  = [...aiResultsMap.values()].filter(Boolean).length;
  const aiFallbacks  = rawJobs.length - aiSuccesses;
  console.log(`  Groq: ${aiSuccesses} extracted  ${aiFallbacks} fallback to rules\n`);

  const totals = { processed: 0, promoted: 0, skipped: 0, errors: 0, newSkills: 0 };
  const promotedPerCompany = {};
  const toMarkJunk  = [];
  const toMarkStale = [];

  for (let i = 0; i < rawJobs.length; i++) {
    const job      = rawJobs[i];
    const provider = job.career_page_configs?.ats_provider || 'Unknown';
    const aiData = aiResultsMap.get(i);

    try {

      const extracted = mergeExtraction(aiData, job, provider);

      if (!extracted.title) { toMarkJunk.push(job.id); totals.skipped++; continue; }
      if (isJunkJob(extracted.title)) { toMarkJunk.push(job.id); totals.skipped++; continue; }

      const isFresh = SKIP_FRESHNESS || (job.posted_date && job.posted_date >= CUTOFF_48H);
      if (!isFresh) { toMarkStale.push(job.id); totals.skipped++; continue; }

      if (!extracted.location) { toMarkJunk.push(job.id); totals.skipped++; continue; }

      const rawCompanyId = job.company_id || job.companies?.id;
      if (MAX_PER_COMPANY && rawCompanyId) {
        if ((promotedPerCompany[rawCompanyId] || 0) >= MAX_PER_COMPANY) {
          totals.skipped++;
          continue;
        }
      }

      const rawSkillNames = aiData !== null
        ? (aiData.skills || [])
        : extractSkillNamesFallback(job.description || '', skillsMap);
      const filteredSkillNames = filterSkillsByCategory(rawSkillNames, extracted.category);
      const skillIds = await normalizeSkills(filteredSkillNames, skillsMap);

      const companyId  = await resolveCompany(job.companies, companyByDomain, slugsUsed);
      if (!companyId) { totals.skipped++; continue; }
      const locationId = await resolveLocation(extracted.location, locationByName, extracted.location_country_hint);

      const slug = makeJobSlug(extracted.title, job.companies?.name || '', slugsUsed);
      slugsUsed.add(slug);

      if (!DRY_RUN) {
        const { error: insertErr, data: insertedJob } = await main.from('jobs').insert({
          company_id:           companyId,
          location_id:          locationId || null,
          title:                extracted.title,
          slug,
          description:          job.description || null,
          requirements_summary: extracted.requirements_summary || null,
          responsibilities:     extracted.responsibilities || null,
          min_years_exp:        extracted.min_years_exp    ?? null,
          application_url:      job.application_url,
          job_type:             extracted.job_type,
          commitment_type:      extracted.commitment_type,
          experience_level:     extracted.experience_level,
          category:             extracted.category,
          salary_min:           extracted.salary_min,
          salary_max:           extracted.salary_max,
          salary_currency:      extracted.salary_currency,
          salary_period:        extracted.salary_period,
          posted_date:          job.posted_date || null,
          first_seen_at:        job.first_seen_at,
          last_seen_at:         job.last_seen_at,
          raw_job_id:           job.id,
          ats_provider:         provider,
          external_id:          job.external_id || null,
          is_published:         false,
        }).select('id').single();

        if (insertErr) {
          if (insertErr.code === '23505') { totals.skipped++; continue; }
          throw insertErr;
        }

        if (insertedJob && skillIds.length > 0) {
          const realIds = skillIds.filter(id => typeof id === 'string' && id.length === 36);
          if (realIds.length > 0) {
            await main.from('job_skills').upsert(
              realIds.map(skill_id => ({ job_id: insertedJob.id, skill_id })),
              { ignoreDuplicates: true }
            );
          }
        }

        await db.from('raw_jobs').update({
          status: 'promoted', is_promoted: true, promoted_at: new Date().toISOString()
        }).eq('id', job.id);

      } else {

        const newSkillsList = skillIds.filter(id => typeof id === 'string' && id.startsWith('[new:'));
        const realSkillIds  = skillIds.filter(id => !String(id).startsWith('[new:'));
        const skillNames_   = [
          ...realSkillIds.map(id => { for (const [,s] of skillsMap) if (s.id === id) return s.name; return id; }),
          ...newSkillsList.map(s => s.replace('[new:', '').replace(']', '') + '*'),
        ];

        const stripped = rawSkillNames.filter(s => !filteredSkillNames.includes(s));
        console.log(`\n[${i+1}/${rawJobs.length}] ${provider.padEnd(14)} | ${extracted.title}`);
        console.log(`  job_type:     ${extracted.job_type        || 'null'}  ${aiData ? '(groq)' : '(rules)'}`);
        console.log(`  commitment:   ${extracted.commitment_type || 'null'}`);
        console.log(`  experience:   ${extracted.experience_level || 'null'}`);
        console.log(`  category:     ${extracted.category        || 'null'}`);
        console.log(`  location:     ${extracted.location}`);
        console.log(`  salary:       ${extracted.salary_min ? `$${extracted.salary_min}-$${extracted.salary_max}` : 'null'}`);
        if (stripped.length) {
          console.log(`  groq skills:  (${rawSkillNames.length}) ${rawSkillNames.join(', ')}`);
          console.log(`  filtered out: ${stripped.join(', ')}`);
        }
        console.log(`  skills (${skillNames_.length}):  ${skillNames_.join(', ') || 'none'}  (* = new)`);
        console.log(`  requirements: ${extracted.requirements_summary || 'null'}`);
        console.log(`  duties:       ${extracted.responsibilities || 'null'}`);
      }

      totals.promoted++;
      totals.processed++;
      if (rawCompanyId) promotedPerCompany[rawCompanyId] = (promotedPerCompany[rawCompanyId] || 0) + 1;

    } catch (err) {
      console.log(`  ❌ Error on job ${job.id}: ${err.message}`);
      totals.errors++;
    }

    if (!DRY_RUN && i % 100 === 0 && i > 0) {
      console.log(`  [${i}/${rawJobs.length}] promoted: ${totals.promoted}  errors: ${totals.errors}`);
    }
  }

  if (!DRY_RUN) {
    const CHUNK = 500;
    const bulkMark = async (ids, status) => {
      for (let i = 0; i < ids.length; i += CHUNK) {
        await db.from('raw_jobs').update({ status }).in('id', ids.slice(i, i + CHUNK));
      }
    };
    if (toMarkJunk.length)  await bulkMark(toMarkJunk,  'skipped_junk');
    if (toMarkStale.length) await bulkMark(toMarkStale, 'skipped_stale');
  }

  console.log('\n══════════════════════════════════════════');
  console.log('DONE');
  console.log(`Promoted:       ${totals.promoted}`);
  console.log(`Skipped total:  ${totals.skipped}`);
  console.log(`  → stale:      ${toMarkStale.length}`);
  console.log(`  → junk:       ${toMarkJunk.length}`);
  console.log(`  → cap:        ${totals.skipped - toMarkStale.length - toMarkJunk.length}`);
  console.log(`Errors:         ${totals.errors}`);
  console.log(`Groq fallbacks: ${aiFallbacks}`);

  if (!DRY_RUN) {
    const { count } = await main.from('jobs').select('*', { count: 'exact', head: true });
    console.log(`\nMain DB jobs total: ${count}`);
  }
}

function mergeExtraction(aiData, job, provider) {

  const rules = extractFields(job, provider);

  if (!aiData) return rules;

  const VALID_JOB_TYPES    = new Set(['remote','hybrid','onsite']);
  const VALID_COMMITMENT   = new Set(['full_time','part_time','contract','internship']);
  const VALID_EXPERIENCE   = new Set(['entry','mid','senior','lead','executive']);
  const VALID_CATEGORIES   = new Set(['Engineering','Design','Product','Marketing','Sales','Data','Finance','HR','Legal','Customer Support','Security','Research','Healthcare','Hospitality','Retail','Admin','Creative','Operations','Project Management','Executive','Other']);

  return {
    title:                rules.title,
    job_type:             (VALID_JOB_TYPES.has(aiData.job_type))   ? aiData.job_type   : rules.job_type,
    commitment_type:      (VALID_COMMITMENT.has(aiData.commitment_type)) ? aiData.commitment_type : rules.commitment_type,
    experience_level:     (VALID_EXPERIENCE.has(aiData.experience_level)) ? aiData.experience_level : rules.experience_level,
    category:             (VALID_CATEGORIES.has(aiData.category))  ? aiData.category   : rules.category,
    location:             rules.location,  
    location_country_hint: aiData.location_country || null,  
    salary_min:           rules.salary_min,
    salary_max:           rules.salary_max,
    salary_currency:      rules.salary_currency,
    salary_period:        rules.salary_period,
    requirements_summary: aiData.requirements_summary || null,
    responsibilities:     aiData.responsibilities     || null,
    min_years_exp:        aiData.min_years_exp        ?? null,
  };
}

function extractSkillNamesFallback(text, skillsMap) {
  if (!text || skillsMap.size === 0) return [];
  const stripped = cleanDesc(text);
  const lower    = stripped.toLowerCase();
  const found    = [];
  for (const [, skill] of skillsMap) {
    const escaped       = skill.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const caseSensitive = skill.name.length <= 3;
    const src           = caseSensitive ? stripped : lower;
    const pattern       = caseSensitive ? escaped  : escaped.toLowerCase();
    const re = new RegExp('(?<![a-z0-9])' + pattern + '(?![a-z0-9])', caseSensitive ? '' : 'i');
    if (re.test(src)) found.push(skill.name);
    if (found.length >= 12) break;
  }
  return found;
}

function isJunkJob(title) {
  if (!title) return true;
  if (/\binternal\b.*referral|referral program/i.test(title))                                     return true;
  if (/^(custom sources?|employee referral|talent community|join our talent pool|general application)$/i.test(title.trim())) return true;
  return false;
}

function extractFields(job, provider) {
  const rd    = job.raw_data || {};
  const title = job.title    || '';
  const desc  = job.description || '';

  let job_type        = null;
  let commitment_type = null;
  let experience_level = null;
  let category        = null;
  let location        = job.location_raw || null;
  let salary_min      = null;
  let salary_max      = null;
  let salary_currency = 'USD';
  let salary_period   = 'yearly';

  if (provider === 'Lever') {
    job_type        = normJobType(rd.workplaceType);
    commitment_type = normCommitment(rd.categories?.commitment);
    category        = normCategory(rd.categories?.team || title);
    location        = rd.categories?.location || location;
  }
  if (provider === 'Ashby') {
    job_type        = rd.isRemote ? 'remote' : normJobType(rd.workplaceType);
    commitment_type = normCommitment(rd.employmentType);
    category        = normCategory(rd.team || title);
    if (!location) location = rd.locationName || rd.location || (rd.isRemote ? 'Remote' : null);
  }
  if (provider === 'SmartRecruiters') {
    const loc       = rd.location || {};
    job_type        = loc.remote ? 'remote' : loc.hybrid ? 'hybrid' : loc.city ? 'onsite' : null;
    commitment_type = normCommitment(rd.typeOfEmployment?.label);
    const catFromLabel = rd.function?.label ? normCategory(rd.function.label) : null;
    category = (catFromLabel && catFromLabel !== 'Other') ? catFromLabel : normCategory(title);
  }
  if (provider === 'Breezy HR') {
    commitment_type = normCommitment(rd.type?.name || rd.type?.id);
    category        = normCategory(rd.department || title);
    const salParsed = parseSalary(rd.salary);
    if (salParsed) { salary_min = salParsed.min; salary_max = salParsed.max; salary_currency = salParsed.currency; salary_period = salParsed.period; }
  }
  if (provider === 'BambooHR') {
    commitment_type = normCommitment(rd.employmentType);
    category        = normCategory(rd.department?.label || title);
    if (!location) location = [rd.location?.city, rd.location?.state].filter(Boolean).join(', ') || rd.location?.country || null;
  }
  if (provider === 'Personio') {
    commitment_type = normCommitment(rd.schedule || rd.employmentType);
    category        = normCategory(rd.department || rd.occupationCategory || title);
    if (!location) location = rd.office || null;
  }
  if (provider === 'Greenhouse') {
    category = normCategory(rd.departments?.[0]?.name || title);
    if (!location) location = rd.offices?.[0]?.location?.name || rd.offices?.[0]?.name || null;
    for (const m of (rd.metadata || [])) {
      if (/contract|employment|type/i.test(m.name) && m.value) commitment_type = normCommitment(m.value);
    }
  }

  if (!job_type)         job_type         = extractJobTypeFromText(title + ' ' + desc);
  if (!commitment_type)  commitment_type  = extractCommitmentFromText(title + ' ' + desc);
  if (!experience_level) experience_level = extractExperienceFromText(title, desc);
  if (!category)         category         = normCategory(title);
  if (!salary_min) {
    const salParsed = parseSalary(desc + ' ' + title);
    if (salParsed) { salary_min = salParsed.min; salary_max = salParsed.max; salary_currency = salParsed.currency; salary_period = salParsed.period; }
  }

  return { title, job_type, commitment_type, experience_level, category, location, salary_min, salary_max, salary_currency, salary_period };
}

function normJobType(val) {
  if (!val || typeof val !== 'string') return null;
  const v = val.toLowerCase();
  if (v.includes('remote')) return 'remote';
  if (v.includes('hybrid')) return 'hybrid';
  if (v.includes('onsite') || v.includes('on-site') || v.includes('office') || v.includes('in-person')) return 'onsite';
  return null;
}

function normCommitment(val) {
  if (!val || typeof val !== 'string') return null;
  const v = val.toLowerCase().replace(/[-_]/g, ' ');
  if (v.includes('full'))                                      return 'full_time';
  if (v.includes('part'))                                      return 'part_time';
  if (v.includes('contract') || v.includes('freelance'))      return 'contract';
  if (v.includes('intern'))                                    return 'internship';
  if (v.includes('temporary') || v.includes('temp'))          return 'contract';
  if (v.includes('permanent'))                                 return 'full_time';
  return null;
}

function normCategory(val) {
  if (!val || typeof val !== 'string') return null;
  const v = val.toLowerCase();
  if (/engineer|develop|programm|software|frontend|backend|fullstack|devops|sre|platform|mobile|ios|android|cloud|\bml\b|machine learning|infrastructure|architect|information technology|\bqa\b|quality assurance|test automation|integration specialist|systems admin|it support|it manager|helpdesk/.test(v)) return 'Engineering';
  if (/\bdesign\b|\bux\b|\bui\b|graphic|motion|illustrat|figma|creative direct/.test(v)) return 'Design';
  if (/\bnurs\b|\bdoctor\b|\bphysician\b|\bsurgeon\b|dentist|dental|therapist|pharmacist|healthcare|health care|patient care|caregiver|paramedic|radiolog|veterinar|immunis|immuniz|medical officer|anaesth|obstetric|midwif|optometr|dietitian|audiolog|occupational health|\bbehavior analyst\b/.test(v)) return 'Healthcare';
  if (/product manager|product owner|product director|product lead|head of product|vp of product|chief product|product management/.test(v)) return 'Product';
  if (/project manager|programme manager|program manager|delivery manager|scrum master|agile coach|\bpmo\b|project lead/.test(v)) return 'Project Management';
  if (/market|growth hacker|\bseo\b|\bsem\b|social media|demand gen|campaign|brand manager|brand director|brand strateg|content strateg|communications|public relation/.test(v)) return 'Marketing';
  if (/\bsales\b|account exec|account manag|business dev|business development|\bbdr\b|\bsdr\b|revenue|pre-sales|presales|client partner|client solution/.test(v)) return 'Sales';
  if (/data science|data analyst|data engineer|\banalytics\b|business intelligence|\bbi\b|reporting analyst|data warehouse|\betl\b|database admin/.test(v)) return 'Data';
  if (/financ|accountant|accounting|payroll|\btax\b|\baudit\b|controller|\bcfo\b|bookkeep|treasurer|fp&a|financial plan|underwriter|actuar|credit analyst|\baml\b|anti.money/.test(v)) return 'Finance';
  if (/\bhr\b|human resource|recruiter|recruiting|talent acqui|talent manag|people ops|people partner|compensation|benefit|workforce/.test(v)) return 'HR';
  if (/\blegal\b|compliance|counsel|attorney|solicitor|paralegal|privacy|gdpr|regulatory affairs/.test(v)) return 'Legal';
  if (/customer success|customer support|customer experience|customer service|customer care|support agent|helpdesk|client service/.test(v)) return 'Customer Support';
  if (/security|infosec|cybersec|\bsoc\b|penetration|threat intel|vulnerability|\bsiem\b|identity access/.test(v)) return 'Security';
  if (/research|scientist|\bphd\b|laboratory|scientific|genomic|biolog|chemi|physicist|\br&d\b/.test(v)) return 'Research';
  if (/\bchef\b|\bcook\b|culinary|hospitality|restaurant|\bhotel\b|kitchen|catering|barista|bartender|sommelier|food.beverage/.test(v)) return 'Hospitality';
  if (/\bretail\b|cashier|store associate|shop assistant|visual merchandis|store manager|shop manager/.test(v)) return 'Retail';
  if (/secretary|receptionist|administrative assist|personal assist|executive assist|office manager|office admin|\bclerk\b|data entry/.test(v)) return 'Admin';
  if (/\bwriter\b|\beditor\b|copywriter|journalist|content creat|scriptwriter|technical writer|game design|gameplay|unreal engine|unity engine|\bproducer\b/.test(v)) return 'Creative';
  if (/operations|supply chain|logistics|procurement|warehouse|fleet|dispatch|fulfillment|distribution/.test(v)) return 'Operations';
  if (/\bceo\b|\bcoo\b|\bcto\b|\bcfo\b|\bcpo\b|chief.*officer|vice president|general manager|managing director/.test(v)) return 'Executive';
  return 'Other';
}

function extractJobTypeFromText(text) {
  const t = text.toLowerCase();
  if (/\bremote\b|work from home|fully remote|wfh\b/.test(t)) return 'remote';
  if (/\bhybrid\b/.test(t))                                    return 'hybrid';
  if (/\bonsite\b|\bon-site\b|\bin.office\b|\bin person\b/.test(t)) return 'onsite';
  return null;
}

function extractCommitmentFromText(text) {
  const t = text.toLowerCase();
  if (/full.time|fulltime/.test(t))               return 'full_time';
  if (/part.time|parttime/.test(t))               return 'part_time';
  if (/\bcontract\b|\bfreelance\b/.test(t))       return 'contract';
  if (/\bintern(ship)?\b/.test(t))                return 'internship';
  return null;
}

function extractExperienceFromText(title, desc) {
  const t = (title || '').toLowerCase();
  const d = (desc  || '').toLowerCase();
  if (/\b(chief|ceo|coo|cto|cfo|cpo|president|vice president|\bvp\b|svp|evp)\b/.test(t)) return 'executive';
  if (/\b(director|head of|managing director|general manager)\b/.test(t))                 return 'executive';
  if (/\b(staff engineer|principal |senior lead|lead [a-z])/i.test(title))               return 'lead';
  if (/\bsenior\b|\bsr\.?\s|\bsr\b/.test(t))                                              return 'senior';
  if (/\b(manager|management)\b/.test(t))                                                 return 'senior';
  if (/\b(junior|jr\.?\s|\bjr\b|entry.level|entry level|graduate)\b/.test(t))            return 'entry';
  if (/\b(intern(ship)?|trainee|apprentice|student worker|werkstudent|stagiai)\b/.test(t)) return 'entry';
  if (/\b(associate|coordinator|specialist|consultant|advisor|analyst)\b/.test(t))       return 'mid';
  const yearsMatches = [...d.matchAll(/(\d+)\+?\s*(?:to\s*(\d+)\s*)?years?\s*(?:of\s*)?(?:relevant\s*)?(?:experience|exp\.?)/g)];
  if (yearsMatches.length > 0) {
    const yrs = parseInt(yearsMatches[0][1]);
    if (yrs <= 1)  return 'entry';
    if (yrs <= 3)  return 'mid';
    if (yrs <= 6)  return 'senior';
    if (yrs > 6)   return 'lead';
  }
  if (/\b(entry.level|entry level|no experience required)\b/.test(d)) return 'entry';
  if (/\b(mid.level|intermediate level|experienced professional)\b/.test(d))  return 'mid';
  if (/\b(senior.level|senior position|senior role)\b/.test(d))               return 'senior';
  return null;
}

function parseSalary(text) {
  if (!text || typeof text !== 'string') return null;
  const sanitize = (r) => { if (r && r.min > r.max) [r.min, r.max] = [r.max, r.min]; return r; };
  const patterns = [
    { re: /([£$€])(\d+(?:\.\d+)?)k?\s*[-–to]+\s*[£$€]?(\d+(?:\.\d+)?)k/i, fn: (m) => sanitize({ currency: currencyFromSymbol(m[1]), min: Math.round(parseFloat(m[2]) * (parseFloat(m[2]) < 1000 ? 1000 : 1)), max: Math.round(parseFloat(m[3]) * 1000), period: 'yearly' }) },
    { re: /([£$€])(\d{2,3}),(\d{3})\s*[-–to]+\s*[£$€]?(\d{2,3}),(\d{3})/i, fn: (m) => sanitize({ currency: currencyFromSymbol(m[1]), min: parseInt(m[2]+m[3]), max: parseInt(m[4]+m[5]), period: 'yearly' }) },
    { re: /([£$€])(\d+(?:\.\d+)?)\s*(?:\/hr|per hour|\/hour)/i, fn: (m) => ({ currency: currencyFromSymbol(m[1]), min: Math.round(parseFloat(m[2])), max: Math.round(parseFloat(m[2])), period: 'hourly' }) },
    { re: /([£$€])(\d+)k/i, fn: (m) => ({ currency: currencyFromSymbol(m[1]), min: parseInt(m[2]) * 1000, max: parseInt(m[2]) * 1000, period: 'yearly' }) },
  ];
  for (const { re, fn } of patterns) {
    const m = text.match(re);
    if (m) { try { return fn(m); } catch { continue; } }
  }
  return null;
}

function currencyFromSymbol(sym) {
  if (sym === '£') return 'GBP';
  if (sym === '€') return 'EUR';
  return 'USD';
}

async function resolveCompany(db2Company, companyByDomain, slugsUsed) {
  if (!db2Company) return null;
  let domain = db2Company.domain;
  if (!domain || domain.length < 4 || !domain.includes('.')) {
    domain = db2Company.slug || makeSlug(db2Company.name, new Set());
  }
  if (companyByDomain.has(domain)) return companyByDomain.get(domain).id;

  let enrichment = {};
  try {
    const { data: db1Companies } = await db1.from('companies')
      .select('linkedin_url, year_founded, number_employees, industries, funding_stage, is_public, headquarters, headquarters_country, description')
      .ilike('website', '%' + domain + '%').limit(1);
    if (db1Companies?.[0]) enrichment = db1Companies[0];
  } catch {  }

  const slug = makeSlug(db2Company.name, slugsUsed);
  slugsUsed.add(slug);

  if (!DRY_RUN) {
    const { data: inserted, error } = await main.from('companies').insert({
      name: db2Company.name, slug, domain,
      logo_url:             db2Company.logo_url || null,
      linkedin_url:         enrichment.linkedin_url || null,
      year_founded:         enrichment.year_founded || null,
      employee_count:       enrichment.number_employees || null,
      industries:           enrichment.industries || null,
      funding_stage:        enrichment.funding_stage || null,
      is_public:            enrichment.is_public || false,
      headquarters:         enrichment.headquarters || null,
      headquarters_country: enrichment.headquarters_country || null,
      description:          enrichment.description || null,
      sources:              db2Company.sources || [],
    }).select('id').single();

    if (error) {
      if (error.code === '23505') {
        const { data: existing } = await main.from('companies').select('id').eq('domain', domain).single();
        if (existing) { companyByDomain.set(domain, existing); return existing.id; }
      }
      return null;
    }
    companyByDomain.set(domain, inserted);
    return inserted.id;
  }
  return 'dry-run-company-id';
}

async function resolveLocation(locationRaw, locationByName, countryHint = null) {
  if (!locationRaw) return null;
  const parsed  = parseLocation(locationRaw, countryHint);
  const display = parsed.display_name;
  if (locationByName.has(display)) return locationByName.get(display).id;
  if (!DRY_RUN) {
    const { data: inserted, error } = await main.from('locations').insert(parsed).select('id').single();
    if (error) {
      if (error.code === '23505') {
        const { data: existing } = await main.from('locations').select('id').eq('display_name', display).single();
        if (existing) { locationByName.set(display, existing); return existing.id; }
      }
      return null;
    }
    locationByName.set(display, inserted);
    return inserted.id;
  }
  return 'dry-run-location-id';
}

function preProcessLocation(raw) {
  let s = raw.trim();
  if (/home.?based/i.test(s)) return 'Remote';
  s = s.replace(/,?\s*\b\d{5}(?:-\d{4})?\b\s*$/, '').trim();
  if (/^\d+\s+\w/.test(s) && s.includes(',')) {
    const parts = s.split(',');
    const afterSuffix = parts[0].match(/\b(?:St\.?|Street|Ave\.?|Avenue|Blvd\.?|Boulevard|Dr\.?|Drive|Rd\.?|Road|Way|Ln\.?|Lane|Ct\.?|Court|Pl\.?|Place|Pkwy\.?|Parkway|Hwy\.?|Highway|Loop|Trl\.?|Trail|NW|NE|SW|SE)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*$/);
    if (afterSuffix && parts.length > 1) { parts[0] = afterSuffix[1]; s = parts.join(','); }
    else { s = parts.slice(1).join(',').trim(); }
    s = s.replace(/,?\s*\b\d{5}(?:-\d{4})?\b\s*$/, '').trim();
  }
  s = s.replace(/\s*\([^)]*(?:hybrid|remote|on.?site|in\s*office|flex)[^)]*\)/gi, '').trim();
  s = s.replace(/\s+in\s+office.*$/i, '').trim();
  s = s.replace(/^(?:hybrid|remote|on.?site)\s*[-–]\s*/i, '').trim();
  const codeOfficeCity = s.match(/^([A-Z]{2})\s+[Oo]ffice\s+(.+)$/);
  if (codeOfficeCity) return codeOfficeCity[2].trim() + ', ' + codeOfficeCity[1];
  s = s.replace(/\s+[Oo]ffice\s*$/, '').trim();
  s = s.replace(/\s*[-–]\s*(?:hq|headquarters|main\s+office|office)\s*$/i, '').trim();
  const dashMatch = s.match(/^(.+?)\s+[-–]\s+(.+)$/);
  if (dashMatch) {
    const before = dashMatch[1].trim(), after = dashMatch[2].trim();
    const looksLikeCountry = ['france','italy','germany','spain','ireland','mexico','india','uk','usa','us','united states','united kingdom','australia','canada','brazil','netherlands','poland','portugal','switzerland','belgium','austria','sweden','norway','denmark','finland','new zealand','south africa'].includes(before.toLowerCase());
    if (looksLikeCountry) {
      let city = after.replace(/\s+[Oo]ffice\s*$/i, '').trim();
      const stateTrail = city.match(/^(.+?)\s+([A-Z]{2})\s*$/);
      if (stateTrail) return stateTrail[1].trim() + ', ' + stateTrail[2] + ', ' + before;
      return city + ', ' + before;
    } else { return before; }
  }
  return s;
}

function extractRemoteCountry(t, hint) {

  if (/\b(world.?wide|worldwide|global|international|anywhere|latin america|americas|europe|apac|emea|latam|mena|nordics?)\b/.test(t)) {
    return hint || null;
  }

  const REMOTE_CC = new Set(['us','uk','gb','de','fr','ca','au','in','nl','sg','jp','br','mx','pl','es','it','se','no','dk','fi','ch','be','at','pt','ie','nz','za','ae','tr','il','kr','hk','tw','ua','ph','pk','ng','ke','gh','ro','cz','gr','ar','cl','co','pe','id','my','th','vn','eg','ma','rw','uz','bd','lk','mn','np','ir','sa','jo','lb','qa','kw','bh','om','tz','ug','zm','zw']);
  const REMOTE_NAMES = {'united states':'US','united states of america':'US','usa':'US','u.s.':'US','united kingdom':'GB','great britain':'GB','england':'GB','scotland':'GB','wales':'GB','germany':'DE','france':'FR','canada':'CA','australia':'AU','india':'IN','brazil':'BR','mexico':'MX','spain':'ES','italy':'IT','netherlands':'NL','holland':'NL','singapore':'SG','poland':'PL','sweden':'SE','portugal':'PT','switzerland':'CH','belgium':'BE','austria':'AT','ireland':'IE','denmark':'DK','norway':'NO','finland':'FI','israel':'IL','turkey':'TR','south korea':'KR','korea':'KR','hong kong':'HK','taiwan':'TW','new zealand':'NZ','south africa':'ZA','ukraine':'UA','pakistan':'PK','philippines':'PH','nigeria':'NG','kenya':'KE','ghana':'GH','romania':'RO','czech republic':'CZ','czechia':'CZ','greece':'GR','argentina':'AR','chile':'CL','colombia':'CO','peru':'PE','indonesia':'ID','malaysia':'MY','thailand':'TH','vietnam':'VN','egypt':'EG','morocco':'MA','saudi arabia':'SA','united arab emirates':'AE','uae':'AE'};

  let s = t
    .replace(/\bremote\b/g, '')
    .replace(/\b(opportunity|position|job|work|only|within|based|employees can work remotely|exclusively|first)\b/g, '')
    .replace(/[,\-–|()[\]&]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const namesSorted = Object.keys(REMOTE_NAMES).sort((a, b) => b.length - a.length);
  for (const name of namesSorted) {
    if (s.includes(name)) return REMOTE_NAMES[name];
  }

  for (const word of s.split(/\s+/)) {
    const w = word.replace(/[^a-z]/g, '');
    if (w.length === 2 && REMOTE_CC.has(w)) return w === 'uk' ? 'GB' : w.toUpperCase();
  }

  return (hint && hint.length === 2) ? hint.toUpperCase() : null;
}

function parseLocation(raw, countryHint = null) {
  if (!raw) return { display_name: 'Unknown', country: null, is_remote: false };
  const t = raw.toLowerCase().trim();

  if (/\bremote\b/.test(t)) {

    const country = extractRemoteCountry(t, countryHint);
    const CODE_TO_FULL_R = {'US':'United States','GB':'United Kingdom','DE':'Germany','FR':'France','CA':'Canada','AU':'Australia','IN':'India','BR':'Brazil','MX':'Mexico','ES':'Spain','IT':'Italy','NL':'Netherlands','SG':'Singapore','PL':'Poland','SE':'Sweden','PT':'Portugal','CH':'Switzerland','BE':'Belgium','AT':'Austria','IE':'Ireland','DK':'Denmark','NO':'Norway','FI':'Finland','NZ':'New Zealand','ZA':'South Africa','AE':'UAE','TR':'Turkey','IL':'Israel','KR':'South Korea','HK':'Hong Kong','TW':'Taiwan','UA':'Ukraine','MT':'Malta','PH':'Philippines','PK':'Pakistan','NG':'Nigeria','KE':'Kenya','GH':'Ghana','RO':'Romania','CZ':'Czech Republic','GR':'Greece','AR':'Argentina','CL':'Chile','CO':'Colombia','PE':'Peru','ID':'Indonesia','MY':'Malaysia','TH':'Thailand','VN':'Vietnam','EG':'Egypt','MA':'Morocco'};
    const fullCountry = country ? (CODE_TO_FULL_R[country] || country) : null;

    const display = fullCountry || 'Worldwide';
    return { city: null, state: null, country: country || null, display_name: display, is_remote: true };
  }

  const preprocessed = preProcessLocation(raw);
  if (preprocessed === 'Remote') return { display_name: 'Remote', country: null, is_remote: true };
  const cleaned = preprocessed.replace(/,?\s*\[object Object\]/g, '').trim();
  const parts = cleaned.split(',').map(p => p.trim()).filter(Boolean);

  const US_STATES      = new Set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC']);
  const US_STATE_NAMES = { 'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA','colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA','hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA','kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD','massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS','missouri':'MO','montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH','new jersey':'NJ','new mexico':'NM','new york':'NY','north carolina':'NC','north dakota':'ND','ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA','rhode island':'RI','south carolina':'SC','south dakota':'SD','tennessee':'TN','texas':'TX','utah':'UT','vermont':'VT','virginia':'VA','washington':'WA','west virginia':'WV','wisconsin':'WI','wyoming':'WY','district of columbia':'DC' };
  const COUNTRY_CODES  = new Set(['us','uk','gb','de','fr','ca','au','in','nl','sg','jp','br','mx','pl','es','it','se','no','dk','fi','ch','be','at','pt','ie','nz','za','ae','tr','il','kr','hk','tw','uz','ua','mt','ph','ng','ke','gh','rw','et','eg','ma','pk','bd','lk','mm','th','vn','id','my','ro','cz','gr','ar','cl','co','pe','ve','ec','gt','cr','pa','do','pr','cu','jm','tt','bo','py','uy','hn','sv','ni','bz','gy','sr','ge','am','az','kz','kg','tj','tm','mn','np','af','ir','iq','sa','jo','lb','sy','ye','om','kw','bh','qa','ly','tn','dz','sd','so','tz','ug','mz','zm','zw','bw','na','mw','mg','ci','cm','sn','ml','bf','ne','td','cg','ao','rw','bi','mz','ls','sz','er','dj']);
  const FULL_COUNTRY_NAMES = { 'united states':'US','united states of america':'US','usa':'US','united kingdom':'GB','great britain':'GB','england':'GB','scotland':'GB','wales':'GB','ukraine':'UA','india':'IN','germany':'DE','france':'FR','canada':'CA','australia':'AU','brazil':'BR','mexico':'MX','spain':'ES','italy':'IT','netherlands':'NL','holland':'NL','singapore':'SG','poland':'PL','sweden':'SE','portugal':'PT','switzerland':'CH','belgium':'BE','austria':'AT','ireland':'IE','denmark':'DK','norway':'NO','finland':'FI','israel':'IL','turkey':'TR','south korea':'KR','korea':'KR','hong kong':'HK','taiwan':'TW','new zealand':'NZ','south africa':'ZA','greece':'GR','romania':'RO','czech republic':'CZ','czechia':'CZ','hungary':'HU','slovakia':'SK','croatia':'HR','serbia':'RS','bulgaria':'BG','lithuania':'LT','latvia':'LV','estonia':'EE','slovenia':'SI','luxembourg':'LU','malta':'MT','cyprus':'CY','iceland':'IS','united arab emirates':'AE','uae':'AE','dubai':'AE','abu dhabi':'AE','saudi arabia':'SA','ksa':'SA','qatar':'QA','kuwait':'KW','bahrain':'BH','oman':'OM','jordan':'JO','lebanon':'LB','egypt':'EG','morocco':'MA','tunisia':'TN','algeria':'DZ','nigeria':'NG','kenya':'KE','ghana':'GH','ethiopia':'ET','tanzania':'TZ','uganda':'UG','zimbabwe':'ZW','zambia':'ZM','mozambique':'MZ','angola':'AO','cameroon':'CM','senegal':'SN','ivory coast':'CI',"cote d'ivoire":'CI','rwanda':'RW','mali':'ML','burkina faso':'BF','pakistan':'PK','bangladesh':'BD','sri lanka':'LK','nepal':'NP','afghanistan':'AF','myanmar':'MM','burma':'MM','thailand':'TH','vietnam':'VN','viet nam':'VN','indonesia':'ID','malaysia':'MY','philippines':'PH','cambodia':'KH','laos':'LA','china':'CN','japan':'JP','mongolia':'MN','argentina':'AR','chile':'CL','colombia':'CO','peru':'PE','venezuela':'VE','ecuador':'EC','bolivia':'BO','paraguay':'PY','uruguay':'UY','guatemala':'GT','costa rica':'CR','panama':'PA','dominican republic':'DO','puerto rico':'PR','cuba':'CU','jamaica':'JM','uzbekistan':'UZ','kazakhstan':'KZ','georgia':'GE','armenia':'AM','azerbaijan':'AZ','russia':'RU','belarus':'BY','moldova':'MD' };

  let city = null, state = null, country = null, countryCode = null;

  const AMBIGUOUS = {
    CA: { country:'CA', defaultToUS:true,  cities: new Set(['toronto','vancouver','montreal','calgary','ottawa','edmonton','winnipeg','hamilton','kitchener','waterloo','london','halifax','victoria','saskatoon','regina','kelowna','barrie','guelph','abbotsford','surrey','burnaby','richmond','mississauga','brampton','oshawa','lethbridge','red deer','medicine hat','grande prairie','sherwood park','kamloops','prince george','moncton','fredericton','saint john','charlottetown','whitehorse','yellowknife','iqaluit','markham','vaughan','pickering','ajax','newmarket','aurora','richmond hill','oakville','burlington','st catharines','niagara falls','windsor','kingston','sudbury','thunder bay','nanaimo','prince albert']) },
    CO: { country:'CO', defaultToUS:true,  cities: new Set(['bogota','bogotá','medellin','medellín','cali','barranquilla','cartagena','cucuta','cúcuta','bucaramanga','pereira','santa marta','manizales','ibague','ibagué','envigado','bello','itagüi','palmira','armenia','villavicencio','soacha','pasto','neiva','montería','sincelejo','valledupar','tunja','riohacha']) },
    IN: { country:'IN', defaultToUS:true,  cities: new Set(['mumbai','delhi','new delhi','bangalore','bengaluru','hyderabad','ahmedabad','chennai','kolkata','surat','pune','jaipur','lucknow','kanpur','nagpur','visakhapatnam','bhopal','patna','ludhiana','agra','nashik','vadodara','faridabad','meerut','rajkot','noida','gurgaon','gurugram','thane','navi mumbai','kochi','indore','coimbatore','bhubaneswar','chandigarh','mysore','mysuru','trichy','tiruchirappalli','jabalpur','gwalior','vijayawada','jodhpur','raipur','kota','guwahati','thiruvananthapuram','trivandrum','amritsar','ranchi','howrah']) },
    DE: { country:'DE', defaultToUS:false, cities: new Set(['wilmington','dover','newark','middletown','smyrna','milford','lewes','georgetown','seaford','bridgeville','claymont','bear','elsmere','edgemoor']) },
    GA: { country:'GE', defaultToUS:true,  cities: new Set(['tbilisi','kutaisi','batumi','rustavi','zugdidi','gori','poti','telavi','akhaltsikhe','ozurgeti','senaki','zestafoni','marneuli']) },
    MT: { country:'MT', defaultToUS:true,  cities: new Set(['valletta','birkirkara','qormi','mosta','zabbar','fgura','zejtun','sliema','st julians','paola','hamrun','swieqi','naxxar','mellieha','rabat','mdina','victoria','san gwann','msida','gzira','marsaskala','marsaxlokk','birgu','senglea']) },
    IL: { country:'IL', defaultToUS:true,  cities: new Set(['tel aviv','jerusalem','haifa','rishon lezion','petah tikva','ashdod','netanya','beer sheva','beersheba','holon','bnei brak','bat yam','rehovot','ashkelon','herzliya','kfar saba','modiin','ramat gan','lod','raanana','ramat hasharon','givatayim','kiryat gat','nazareth','eilat']) },
    ID: { country:'ID', defaultToUS:true,  cities: new Set(['jakarta','surabaya','bandung','bekasi','medan','tangerang','depok','semarang','palembang','makassar','batam','bogor','pekanbaru','bandar lampung','malang','padang','denpasar','samarinda','tasikmalaya','pontianak','balikpapan','cimahi','yogyakarta','mataram','banjarmasin','manado','jayapura','ambon','kupang','kendari','gorontalo','ternate','sorong']) },
  };

  if (parts.length >= 3) {
    city = parts[0];
    const secondLo = parts[1].toLowerCase(), secondUp = parts[1].toUpperCase();
    const stateAbbr = US_STATE_NAMES[secondLo] || (US_STATES.has(secondUp) ? secondUp : null);
    state = stateAbbr || null;
    const thirdLo = parts[2].toLowerCase();
    if (FULL_COUNTRY_NAMES[thirdLo]) country = FULL_COUNTRY_NAMES[thirdLo];
    else if (COUNTRY_CODES.has(thirdLo)) { countryCode = thirdLo; country = countryCode === 'uk' ? 'GB' : countryCode.toUpperCase(); }
    else country = parts[2];
  } else if (parts.length === 2) {
    city = parts[0];
    const second = parts[1].trim(), secondUp = second.toUpperCase(), secondLo = second.toLowerCase();
    if (US_STATE_NAMES[secondLo])          { state = US_STATE_NAMES[secondLo]; country = 'US'; }
    else if (secondUp in AMBIGUOUS) {
      const amb = AMBIGUOUS[secondUp], cityLo = city.toLowerCase().trim();
      if (amb.defaultToUS) {
        if (amb.cities.has(cityLo))                         { country = amb.country; }
        else if (US_CITY_STATE[cityLo] === secondUp)        { state = secondUp; country = 'US'; }
        else                                                 { state = secondUp; country = 'US'; }
      } else {
        if (amb.cities.has(cityLo)) { state = secondUp; country = 'US'; }
        else                        { country = amb.country; }
      }
    } else if (secondUp === 'SA') {
      const SAUDI_CITIES = new Set(['riyadh','jeddah','mecca','medina','dammam','khobar','al khobar','tabuk','abha','taif','buraidah','khamis mushait','jubail','yanbu','najran','hail','hofuf','al ahsa']);
      country = SAUDI_CITIES.has(city.toLowerCase().trim()) ? 'SA' : 'ZA';
    } else if (COUNTRY_CODES.has(secondLo) && !US_STATES.has(secondUp)) { countryCode = secondLo; country = countryCode === 'uk' ? 'GB' : countryCode.toUpperCase(); }
    else if (US_STATES.has(secondUp))      { state = secondUp; country = 'US'; }
    else if (FULL_COUNTRY_NAMES[secondLo]) { country = FULL_COUNTRY_NAMES[secondLo]; }
    else                                   { country = second; }
  } else {
    const singleLo = (parts[0] || cleaned).toLowerCase();
    if (FULL_COUNTRY_NAMES[singleLo])      { country = FULL_COUNTRY_NAMES[singleLo]; city = null; }
    else if (COUNTRY_CODES.has(singleLo))  { country = singleLo === 'uk' ? 'GB' : singleLo.toUpperCase(); city = null; }
    else                                   { city = parts[0] || cleaned; }
  }

  if (state && !country) country = 'US';

  if (countryHint && countryHint.length === 2) {
    if (!country) {
      country = countryHint;
      if (country !== 'US') state = null;
    } else if (state && country === 'US' && countryHint !== 'US') {
      country = countryHint;
      state   = null;
    } else if (country.length > 2 && !/^[A-Z]{2}$/.test(country)) {

      country = countryHint;
      state   = null;
    }
  }

  const CODE_TO_FULL = { 'GB':'United Kingdom','DE':'Germany','FR':'France','IN':'India','NL':'Netherlands','SG':'Singapore','JP':'Japan','BR':'Brazil','MX':'Mexico','PL':'Poland','ES':'Spain','IT':'Italy','SE':'Sweden','PT':'Portugal','CH':'Switzerland','BE':'Belgium','AT':'Austria','IE':'Ireland','DK':'Denmark','NO':'Norway','FI':'Finland','IL':'Israel','TR':'Turkey','KR':'South Korea','HK':'Hong Kong','TW':'Taiwan','NZ':'New Zealand','ZA':'South Africa','AE':'UAE','UA':'Ukraine','MT':'Malta','PH':'Philippines','NG':'Nigeria','KE':'Kenya','GH':'Ghana','AU':'Australia','RO':'Romania','CZ':'Czech Republic','GE':'Georgia','PK':'Pakistan','BD':'Bangladesh','TH':'Thailand','VN':'Vietnam','ID':'Indonesia','MY':'Malaysia','EG':'Egypt','MA':'Morocco','ET':'Ethiopia','CA':'Canada','UZ':'Uzbekistan','RW':'Rwanda','LK':'Sri Lanka','MM':'Myanmar','GR':'Greece','HU':'Hungary','SK':'Slovakia','HR':'Croatia','RS':'Serbia','BG':'Bulgaria','LT':'Lithuania','LV':'Latvia','EE':'Estonia','SI':'Slovenia','LU':'Luxembourg','CY':'Cyprus','IS':'Iceland','SA':'Saudi Arabia','QA':'Qatar','KW':'Kuwait','BH':'Bahrain','OM':'Oman','JO':'Jordan','LB':'Lebanon','DZ':'Algeria','TN':'Tunisia','LY':'Libya','TZ':'Tanzania','UG':'Uganda','ZW':'Zimbabwe','ZM':'Zambia','MZ':'Mozambique','AO':'Angola','CM':'Cameroon','SN':'Senegal','CI':'Ivory Coast','ML':'Mali','BF':'Burkina Faso','NP':'Nepal','AF':'Afghanistan','KH':'Cambodia','LA':'Laos','CN':'China','MN':'Mongolia','AR':'Argentina','CL':'Chile','CO':'Colombia','PE':'Peru','VE':'Venezuela','EC':'Ecuador','BO':'Bolivia','PY':'Paraguay','UY':'Uruguay','GT':'Guatemala','CR':'Costa Rica','PA':'Panama','DO':'Dominican Republic','PR':'Puerto Rico','CU':'Cuba','JM':'Jamaica','KZ':'Kazakhstan','AM':'Armenia','AZ':'Azerbaijan','RU':'Russia','BY':'Belarus','MD':'Moldova','TT':'Trinidad and Tobago','HN':'Honduras','SV':'El Salvador','NI':'Nicaragua','BZ':'Belize','GY':'Guyana','SR':'Suriname','KG':'Kyrgyzstan','TJ':'Tajikistan','TM':'Turkmenistan','IR':'Iran','IQ':'Iraq','SY':'Syria','YE':'Yemen','SD':'Sudan','SO':'Somalia','BW':'Botswana','NA':'Namibia','MW':'Malawi','MG':'Madagascar','NE':'Niger','TD':'Chad','CG':'Congo','BI':'Burundi','LS':'Lesotho','SZ':'Eswatini','ER':'Eritrea','DJ':'Djibouti' };
  const US_STATE_FULL = { 'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut','DE':'Delaware','FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming','DC':'District of Columbia' };

  const fullCountry = country ? (CODE_TO_FULL[country] || (country === 'US' ? 'United States' : country)) : null;
  const fullState   = state   ? (US_STATE_FULL[state]  || state) : null;
  const display     = [city, fullState, fullCountry].filter(Boolean).join(', ');

  return { city, state: state||null, country: country||null, country_code: countryCode?.toUpperCase()||null, display_name: display||cleaned, is_remote: false };
}

function makeSlug(text, usedSlugs) {
  let base = text.toLowerCase().replace(/[^a-z0-9\s-]/g, '').replace(/\s+/g, '-').replace(/-+/g, '-').substring(0, 60).replace(/^-|-$/g, '');
  let slug = base, i = 2;
  while (usedSlugs.has(slug)) { slug = base + '-' + i++; }
  return slug;
}

function makeJobSlug(title, company, usedSlugs) {
  const base = makeSlug(title + '-at-' + company, new Set());
  let slug = base, i = 2;
  while (usedSlugs.has(slug)) { slug = base + '-' + i++; }
  return slug;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

run().catch(console.error);

