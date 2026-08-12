import type { Env } from "./index";

interface OverdueRow {
  owner_id: number;
  owner_email: string;
  owner_first_name: string;
  transaction_id: number;
  book_title: string;
  person_id: number;
  borrower_first_name: string;
  borrower_last_name: string;
  borrower_email: string | null;
  due_date: string;
  days_overdue: number;
}

interface BorrowDueRow {
  owner_id: number;
  borrow_id: number;
  title: string;
  lender_name: string;
  due_date: string;
  days_until_due: number; // negative once overdue
}

interface EmailMessage {
  to: string;
  subject: string;
  html: string;
  text: string;
}

interface NotificationCandidate {
  owner_id: number;
  type: "OVERDUE_LOAN" | "PRICE_DROP" | "BORROW_DUE_SOON" | "EVENT_NEARBY";
  title: string;
  body: string;
  link: string;
}

// In-app notification center (section 5.1). Dedupes on (owner, type, link):
// if an unread notification with the same link already exists we skip it,
// so a still-overdue item doesn't spam a fresh row every single day — a new
// one only appears once the user has read (dismissed) the previous one.
async function insertNotificationsIfMissing(env: Env, candidates: NotificationCandidate[]): Promise<number> {
  let inserted = 0;
  for (const c of candidates) {
    const existing = await env.DB.prepare(
      `SELECT notification_id FROM notifications WHERE owner_id = ? AND type = ? AND link = ? AND read_at IS NULL LIMIT 1`
    )
      .bind(c.owner_id, c.type, c.link)
      .first();
    if (existing) continue;

    await env.DB.prepare(`INSERT INTO notifications (owner_id, type, title, body, link) VALUES (?, ?, ?, ?, ?)`)
      .bind(c.owner_id, c.type, c.title, c.body, c.link)
      .run();
    inserted++;
  }
  return inserted;
}

async function getOverdueLoans(env: Env): Promise<OverdueRow[]> {
  const result = await env.DB.prepare(
    `
    SELECT
      u.user_id as owner_id, u.email as owner_email, u.first_name as owner_first_name,
      t.transaction_id, b.title as book_title,
      t.person_id, br.first_name as borrower_first_name, br.last_name as borrower_last_name, br.email as borrower_email,
      t.due_date,
      CAST(julianday('now') - julianday(t.due_date) AS INTEGER) as days_overdue
    FROM transactions t
    JOIN users u ON u.user_id = t.owner_id
    JOIN books b ON b.book_id = t.book_id
    JOIN borrowers br ON br.person_id = t.person_id
    WHERE t.actual_return_date IS NULL AND date(t.due_date) < date('now')
    ORDER BY u.user_id, t.person_id, t.due_date ASC
    `
  ).all<OverdueRow>();
  return result.results;
}

// No email provider is wired up yet (this Cloudflare account has no domain
// onboarded to Email Sending, and no third-party provider key is configured).
// This logs what would be sent so the detection/grouping/template logic can
// be exercised end-to-end today; swap the body for a real provider call
// (Cloudflare Email Service `env.EMAIL.send()`, or an HTTP call to
// Resend/SendGrid) once one is set up.
async function sendEmail(_env: Env, message: EmailMessage): Promise<void> {
  console.log(`[email:mock] to=${message.to} subject="${message.subject}"\n${message.text}`);
}

function borrowerReminderEmail(borrowerName: string, ownerName: string, loans: OverdueRow[]): EmailMessage {
  const bookList = loans.map((l) => `- "${l.book_title}" (${l.days_overdue} day(s) overdue, was due ${l.due_date})`);
  const single = loans.length === 1;

  const text = `Hi ${borrowerName},

Just a friendly reminder from ${ownerName}'s library — you still have ${single ? "a book" : "some books"} checked out past the due date:

${bookList.join("\n")}

No rush, just wanted to flag it in case it slipped your mind. Whenever you get a chance to return ${single ? "it" : "them"} would be great!

Thanks,
${ownerName}`;

  const html = `<p>Hi ${borrowerName},</p>
<p>Just a friendly reminder from ${ownerName}'s library — you still have ${single ? "a book" : "some books"} checked out past the due date:</p>
<ul>${loans.map((l) => `<li>"${l.book_title}" (${l.days_overdue} day(s) overdue, was due ${l.due_date})</li>`).join("")}</ul>
<p>No rush, just wanted to flag it in case it slipped your mind. Whenever you get a chance to return ${single ? "it" : "them"} would be great!</p>
<p>Thanks,<br/>${ownerName}</p>`;

  return {
    to: "",
    subject: single ? `Friendly reminder: "${loans[0].book_title}" is overdue` : `Friendly reminder: ${loans.length} books are overdue`,
    text,
    html,
  };
}

function ownerDigestEmail(ownerFirstName: string, loans: OverdueRow[]): EmailMessage {
  const rows = loans.map(
    (l) =>
      `- "${l.book_title}" — ${l.borrower_first_name} ${l.borrower_last_name} (${l.borrower_email ?? "no email on file"}), ${l.days_overdue} day(s) overdue`
  );

  const text = `Hi ${ownerFirstName},

Here's today's overdue-loans digest for your library (${loans.length} total):

${rows.join("\n")}

Reminder emails were also sent to any borrower with an email on file.`;

  const html = `<p>Hi ${ownerFirstName},</p>
<p>Here's today's overdue-loans digest for your library (${loans.length} total):</p>
<ul>${loans.map((l) => `<li>"${l.book_title}" — ${l.borrower_first_name} ${l.borrower_last_name} (${l.borrower_email ?? "no email on file"}), ${l.days_overdue} day(s) overdue</li>`).join("")}</ul>`;

  return {
    to: "",
    subject: `📚 ${loans.length} overdue book(s) in your library`,
    text,
    html,
  };
}

export async function checkOverdueAndNotify(env: Env): Promise<{ borrowersNotified: number; ownersNotified: number; inAppCreated: number }> {
  const overdue = await getOverdueLoans(env);
  let borrowersNotified = 0;
  let ownersNotified = 0;

  const byOwner = new Map<number, OverdueRow[]>();
  for (const row of overdue) {
    if (!byOwner.has(row.owner_id)) byOwner.set(row.owner_id, []);
    byOwner.get(row.owner_id)!.push(row);
  }

  const candidates: NotificationCandidate[] = [];

  for (const [, ownerLoans] of byOwner) {
    const owner = ownerLoans[0];

    // One reminder per borrower, listing all of that borrower's overdue books.
    const byBorrower = new Map<number, OverdueRow[]>();
    for (const row of ownerLoans) {
      if (!byBorrower.has(row.person_id)) byBorrower.set(row.person_id, []);
      byBorrower.get(row.person_id)!.push(row);
    }

    for (const [, borrowerLoans] of byBorrower) {
      const b = borrowerLoans[0];
      if (!b.borrower_email) continue;
      const email = borrowerReminderEmail(`${b.borrower_first_name}`, owner.owner_first_name, borrowerLoans);
      email.to = b.borrower_email;
      await sendEmail(env, email);
      borrowersNotified++;
    }

    // One digest per owner, listing everything overdue across their library.
    const digest = ownerDigestEmail(owner.owner_first_name, ownerLoans);
    digest.to = owner.owner_email;
    await sendEmail(env, digest);
    ownersNotified++;

    // One in-app notification per overdue loan, so the owner sees it in the bell too.
    for (const loan of ownerLoans) {
      candidates.push({
        owner_id: loan.owner_id,
        type: "OVERDUE_LOAN",
        title: `"${loan.book_title}" is overdue`,
        body: `${loan.borrower_first_name} ${loan.borrower_last_name} has had it ${loan.days_overdue} day(s) past the due date.`,
        link: "/loans",
      });
    }
  }

  const inAppCreated = await insertNotificationsIfMissing(env, candidates);

  return { borrowersNotified, ownersNotified, inAppCreated };
}

async function getBorrowsNeedingReminder(env: Env): Promise<BorrowDueRow[]> {
  const result = await env.DB.prepare(
    `
    SELECT owner_id, borrow_id, title, lender_name, due_date,
           CAST(julianday(due_date) - julianday('now') AS INTEGER) as days_until_due
    FROM borrow_records
    WHERE returned_date IS NULL
      AND due_date IS NOT NULL
      AND julianday(due_date) - julianday('now') <= reminder_lead_days
    ORDER BY due_date ASC
    `
  ).all<BorrowDueRow>();
  return result.results;
}

// Books Liane borrowed FROM someone else (borrow_records) — the opposite
// direction from checkOverdueAndNotify's transactions/Loans check. In-app
// only for now (section 5.1 item 1); the daily email digest for this joins
// in once Resend is wired up (Fase 3).
export async function checkBorrowDueAndNotify(env: Env): Promise<{ inAppCreated: number }> {
  const dueSoon = await getBorrowsNeedingReminder(env);

  const candidates: NotificationCandidate[] = dueSoon.map((b) => {
    const overdue = b.days_until_due < 0;
    return {
      owner_id: b.owner_id,
      type: "BORROW_DUE_SOON",
      title: overdue
        ? `"${b.title}" is overdue to return to ${b.lender_name}`
        : `"${b.title}" is due back to ${b.lender_name} soon`,
      body: overdue
        ? `${Math.abs(b.days_until_due)} day(s) past the date you agreed to return it.`
        : `Due back in ${b.days_until_due} day(s) (${b.due_date}).`,
      link: "/borrowed-by-me",
    };
  });

  const inAppCreated = await insertNotificationsIfMissing(env, candidates);
  return { inAppCreated };
}
