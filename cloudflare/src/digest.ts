import type { Env } from "./index";
import { sendEmail, type EmailMessage } from "./email";

interface DigestRow {
  owner_id: number;
  owner_email: string;
  owner_first_name: string;
  type: "OVERDUE_LOAN" | "PRICE_DROP" | "BORROW_DUE_SOON" | "EVENT_NEARBY";
  title: string;
  body: string | null;
}

const TYPE_LABEL: Record<DigestRow["type"], string> = {
  OVERDUE_LOAN: "📚 Overdue loans",
  BORROW_DUE_SOON: "📦 Books to return",
  PRICE_DROP: "💰 Price drops on your wishlist",
  EVENT_NEARBY: "🎟️ Events nearby",
};

// Reads straight from the notifications table (the same rows the bell
// shows) instead of re-deriving overdue/borrow-due/price-drop state itself
// — one source of truth for "what does Liane need to know today" across
// both surfaces (section 5.1: "don't build 3 parallel notification
// systems"). Recomputed fresh each run rather than tracking "already
// emailed", matching how the pre-existing overdue reminder already behaved
// (resent daily until read) — read a notification in the bell to drop it
// from tomorrow's digest.
async function getUnreadNotificationsByOwner(env: Env): Promise<DigestRow[]> {
  const result = await env.DB.prepare(
    `SELECT n.owner_id, u.email as owner_email, u.first_name as owner_first_name, n.type, n.title, n.body
     FROM notifications n
     JOIN users u ON u.user_id = n.owner_id
     WHERE n.read_at IS NULL
     ORDER BY n.owner_id, n.type, n.created_at`
  ).all<DigestRow>();
  return result.results;
}

function buildDigestEmail(ownerFirstName: string, items: DigestRow[]): EmailMessage {
  const byType = new Map<DigestRow["type"], DigestRow[]>();
  for (const item of items) {
    if (!byType.has(item.type)) byType.set(item.type, []);
    byType.get(item.type)!.push(item);
  }

  const textSections: string[] = [];
  const htmlSections: string[] = [];
  for (const [type, rows] of byType) {
    textSections.push(`${TYPE_LABEL[type]} (${rows.length}):\n${rows.map((r) => `- ${r.title}${r.body ? ` — ${r.body}` : ""}`).join("\n")}`);
    htmlSections.push(
      `<h3>${TYPE_LABEL[type]} (${rows.length})</h3><ul>${rows.map((r) => `<li>${r.title}${r.body ? ` — ${r.body}` : ""}</li>`).join("")}</ul>`
    );
  }

  return {
    to: "",
    subject: `📖 Your Liane's Library digest — ${items.length} update(s)`,
    text: `Hi ${ownerFirstName},\n\nHere's what's happening in your library today:\n\n${textSections.join("\n\n")}`,
    html: `<p>Hi ${ownerFirstName},</p><p>Here's what's happening in your library today:</p>${htmlSections.join("")}`,
  };
}

export async function sendDailyDigest(env: Env): Promise<{ ownersNotified: number }> {
  const rows = await getUnreadNotificationsByOwner(env);

  const byOwner = new Map<number, DigestRow[]>();
  for (const row of rows) {
    if (!byOwner.has(row.owner_id)) byOwner.set(row.owner_id, []);
    byOwner.get(row.owner_id)!.push(row);
  }

  let ownersNotified = 0;
  for (const [, items] of byOwner) {
    const email = buildDigestEmail(items[0].owner_first_name, items);
    email.to = items[0].owner_email;
    await sendEmail(env, email);
    ownersNotified++;
  }

  return { ownersNotified };
}
