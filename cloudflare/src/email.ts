import type { Env } from "./index";

export interface EmailMessage {
  to: string;
  subject: string;
  html: string;
  text: string;
}

// Workers (V8) has no SMTP, so Resend's HTTP API is the send path (section
// 5.1.2 of the redesign spec). Falls back to the previous console.log mock
// when RESEND_API_KEY isn't set, so local dev / accounts that haven't
// configured it yet don't crash — same graceful-degradation rule as every
// other external-API integration in this app.
export async function sendEmail(env: Env, message: EmailMessage): Promise<void> {
  if (!env.RESEND_API_KEY) {
    console.log(`[email:mock] to=${message.to} subject="${message.subject}"\n${message.text}`);
    return;
  }

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: env.RESEND_FROM_EMAIL || "Liane's Library <onboarding@resend.dev>",
      to: message.to,
      subject: message.subject,
      html: message.html,
      text: message.text,
    }),
  });

  if (!res.ok) {
    console.error(`[email] Resend send failed (${res.status}): ${await res.text()}`);
  }
}
