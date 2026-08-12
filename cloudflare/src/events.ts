import type { Env } from "./index";
import { insertNotificationsIfMissing, type NotificationCandidate } from "./notifications";

interface DiscoveredEvent {
  external_id: string;
  title: string;
  description: string | null;
  venue_name: string | null;
  city: string;
  event_date: string;
  url: string | null;
  image_url: string | null;
}

interface TicketmasterResponse {
  _embedded?: {
    events?: {
      id: string;
      name: string;
      info?: string;
      pleaseNote?: string;
      url?: string;
      images?: { url: string }[];
      dates?: { start?: { localDate?: string } };
      _embedded?: { venues?: { name?: string }[] };
    }[];
  };
}

// Interface, not a hardcoded call (section 4.5): Ticketmaster is the only
// source implemented today (free dev key, real search API — unlike
// Eventbrite, whose public discovery API was discontinued). SerpApi (paid)
// could be added as a second provider later without touching the job
// below. Manual entries (source: 'manual') never go through a provider —
// they're written directly by the /events POST endpoint as the
// always-available fallback for coverage a search API won't have.
type EventProvider = (city: string, env: Env) => Promise<DiscoveredEvent[]>;

const LITERARY_KEYWORDS = "book fair OR literary festival OR author signing OR book launch";

async function fetchTicketmasterEvents(city: string, env: Env): Promise<DiscoveredEvent[]> {
  if (!env.TICKETMASTER_API_KEY) return [];

  const params = new URLSearchParams({
    apikey: env.TICKETMASTER_API_KEY,
    city,
    keyword: LITERARY_KEYWORDS,
    sort: "date,asc",
  });
  const res = await fetch(`https://app.ticketmaster.com/discovery/v2/events.json?${params}`);
  if (!res.ok) return [];

  const data = await res.json<TicketmasterResponse>();
  const events = data._embedded?.events ?? [];

  return events
    .filter((e) => e.dates?.start?.localDate)
    .map((e) => ({
      external_id: e.id,
      title: e.name,
      description: e.info ?? e.pleaseNote ?? null,
      venue_name: e._embedded?.venues?.[0]?.name ?? null,
      city,
      event_date: e.dates!.start!.localDate!,
      url: e.url ?? null,
      image_url: e.images?.[0]?.url ?? null,
    }));
}

const EVENT_PROVIDERS: EventProvider[] = [fetchTicketmasterEvents];

async function getWatchedCities(env: Env): Promise<Map<string, number[]>> {
  const result = await env.DB.prepare(
    `SELECT owner_id, city FROM user_event_preferences WHERE city IS NOT NULL AND trim(city) != ''`
  ).all<{ owner_id: number; city: string }>();

  // Group owners by city so a city watched by multiple owners is only
  // queried against the provider once per run.
  const byCity = new Map<string, number[]>();
  for (const row of result.results) {
    const key = row.city.trim();
    if (!byCity.has(key)) byCity.set(key, []);
    byCity.get(key)!.push(row.owner_id);
  }
  return byCity;
}

// Upserts on (source, external_id) so re-running the weekly job doesn't
// duplicate rows or re-notify for an event already seen — only a genuinely
// new external_id produces a notification.
async function upsertEvent(env: Env, ev: DiscoveredEvent): Promise<{ eventId: number; isNew: boolean }> {
  const existing = await env.DB.prepare(
    `SELECT event_id FROM literary_events WHERE source = 'ticketmaster' AND external_id = ?`
  )
    .bind(ev.external_id)
    .first<{ event_id: number }>();

  if (existing) {
    await env.DB.prepare(
      `UPDATE literary_events SET title=?, description=?, venue_name=?, city=?, event_date=?, url=?, image_url=?, cached_at=datetime('now') WHERE event_id=?`
    )
      .bind(ev.title, ev.description, ev.venue_name, ev.city, ev.event_date, ev.url, ev.image_url, existing.event_id)
      .run();
    return { eventId: existing.event_id, isNew: false };
  }

  const inserted = await env.DB.prepare(
    `INSERT INTO literary_events (source, external_id, title, description, venue_name, city, event_date, url, image_url)
     VALUES ('ticketmaster', ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(ev.external_id, ev.title, ev.description, ev.venue_name, ev.city, ev.event_date, ev.url, ev.image_url)
    .run();
  return { eventId: Number(inserted.meta.last_row_id), isNew: true };
}

export async function checkLiteraryEventsAndNotify(
  env: Env
): Promise<{ citiesChecked: number; eventsUpserted: number; inAppCreated: number }> {
  const ownersByCity = await getWatchedCities(env);
  const candidates: NotificationCandidate[] = [];
  let eventsUpserted = 0;

  for (const [city, owners] of ownersByCity) {
    let events: DiscoveredEvent[] = [];
    for (const provider of EVENT_PROVIDERS) {
      events = await provider(city, env);
      if (events.length > 0) break;
    }
    if (events.length === 0) continue; // no provider had results — skip, not an error

    for (const ev of events) {
      const { isNew } = await upsertEvent(env, ev);
      eventsUpserted++;
      if (!isNew) continue;

      for (const ownerId of owners) {
        candidates.push({
          owner_id: ownerId,
          type: "EVENT_NEARBY",
          title: `📚 ${ev.title}`,
          body: `${ev.venue_name ? `${ev.venue_name} — ` : ""}${ev.event_date}`,
          link: "/events",
        });
      }
    }
  }

  const inAppCreated = await insertNotificationsIfMissing(env, candidates);
  return { citiesChecked: ownersByCity.size, eventsUpserted, inAppCreated };
}
