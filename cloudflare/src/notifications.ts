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

interface EmailMessage {
  to: string;
  subject: string;
  html: string;
  text: string;
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

export async function checkOverdueAndNotify(env: Env): Promise<{ borrowersNotified: number; ownersNotified: number }> {
  const overdue = await getOverdueLoans(env);
  let borrowersNotified = 0;
  let ownersNotified = 0;

  const byOwner = new Map<number, OverdueRow[]>();
  for (const row of overdue) {
    if (!byOwner.has(row.owner_id)) byOwner.set(row.owner_id, []);
    byOwner.get(row.owner_id)!.push(row);
  }

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
  }

  return { borrowersNotified, ownersNotified };
}
