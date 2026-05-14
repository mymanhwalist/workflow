import { createClient } from '@supabase/supabase-js';

const MAIN_URL = process.env.MAIN_DB_URL;
const MAIN_KEY = process.env.MAIN_DB_KEY;

const DRY_RUN   = process.argv.includes('--dry-run');
const limitArg  = process.argv.find(a => a.startsWith('--limit='));
const LIMIT     = limitArg ? parseInt(limitArg.split('=')[1]) : 6;

const db = createClient(MAIN_URL, MAIN_KEY);

async function main() {
  const { data: unpublished, error } = await db
    .from('jobs')
    .select('id, title, category, posted_date, companies(name)')
    .eq('is_published', false)
    .not('category', 'is', null)
    .order('posted_date', { ascending: true, nullsFirst: false })
    .limit(500);

  if (error) { console.error('Fetch failed:', error.message); process.exit(1); }
  if (!unpublished?.length) { console.log('No unpublished jobs.'); return; }

  // Pick 1 per category first (category coverage)
  const seen = new Set();
  const picked = [];

  for (const job of unpublished) {
    if (!seen.has(job.category) && picked.length < LIMIT) {
      seen.add(job.category);
      picked.push(job);
    }
  }

  // Fill remaining slots from any category
  for (const job of unpublished) {
    if (picked.length >= LIMIT) break;
    if (!picked.find(p => p.id === job.id)) picked.push(job);
  }

  console.log(`Publishing ${picked.length} jobs:`);
  picked.forEach(j => console.log(`  [${j.category}] ${j.title} — ${j.companies?.name}`));

  if (!DRY_RUN) {
    const { error: updateErr } = await db
      .from('jobs')
      .update({ is_published: true })
      .in('id', picked.map(j => j.id));

    if (updateErr) { console.error('Update failed:', updateErr.message); process.exit(1); }
    console.log('Done.');
  }
}

main();
