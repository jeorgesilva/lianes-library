import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type LiteraryEvent } from "../lib/api";
import { Card } from "../components/Card";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { EmptyState } from "../components/EmptyState";

function escapeIcs(text: string): string {
  return text.replace(/[\\,;]/g, (m) => `\\${m}`).replace(/\n/g, "\\n");
}

function downloadIcs(event: LiteraryEvent) {
  const dtstart = event.event_date.replaceAll("-", "");
  const dtstamp = new Date().toISOString().replace(/[-:]/g, "").split(".")[0] + "Z";
  const lines = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//Liane's Library//Events//EN",
    "BEGIN:VEVENT",
    `UID:event-${event.event_id}@lianeslibrary`,
    `DTSTAMP:${dtstamp}`,
    `DTSTART;VALUE=DATE:${dtstart}`,
    `SUMMARY:${escapeIcs(event.title)}`,
    event.venue_name ? `LOCATION:${escapeIcs(event.venue_name)}` : "",
    event.description ? `DESCRIPTION:${escapeIcs(event.description)}` : "",
    event.url ? `URL:${event.url}` : "",
    "END:VEVENT",
    "END:VCALENDAR",
  ].filter(Boolean);

  const blob = new Blob([lines.join("\r\n")], { type: "text/calendar;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${event.title.replace(/[^a-z0-9]/gi, "-").toLowerCase()}.ics`;
  a.click();
  URL.revokeObjectURL(url);
}

function CityPreferenceForm() {
  const queryClient = useQueryClient();
  const { data: prefs } = useQuery({ queryKey: ["events", "preferences"], queryFn: api.events.preferences });
  const [city, setCity] = useState("");

  const mutation = useMutation({
    mutationFn: () => api.events.setPreferences({ city }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["events"] });
      setCity("");
    },
  });

  const currentCity = prefs?.city;

  return (
    <Card className="mb-6">
      <h2 className="mb-1 font-display font-semibold text-text">Watching for events near</h2>
      <p className="mb-3 text-sm text-text-muted">
        {currentCity ? (
          <>Currently watching <span className="text-text">{currentCity}</span>. The weekly discovery job checks this city — coverage of small/independent events isn't guaranteed, add your own below if it's missing.</>
        ) : (
          "Set a city to start the weekly literary-events discovery job (requires an API key configured server-side — see README)."
        )}
      </p>
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="City" className="max-w-xs flex-1">
          <Input value={city} onChange={(e) => setCity(e.target.value)} placeholder={currentCity ?? "e.g. São Paulo"} />
        </Field>
        <Button type="submit" disabled={mutation.isPending || !city}>
          Save
        </Button>
      </form>
    </Card>
  );
}

function AddManualEventForm() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [title, setTitle] = useState("");
  const [eventDate, setEventDate] = useState("");
  const [venueName, setVenueName] = useState("");
  const [city, setCity] = useState("");
  const [url, setUrl] = useState("");

  const mutation = useMutation({
    mutationFn: () =>
      api.events.create({
        title,
        event_date: eventDate,
        venue_name: venueName || undefined,
        city: city || undefined,
        url: url || undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["events"] });
      setTitle("");
      setEventDate("");
      setVenueName("");
      setCity("");
      setUrl("");
      setOpen(false);
    },
  });

  if (!open) {
    return (
      <button type="button" onClick={() => setOpen(true)} className="mb-6 text-sm text-primary hover:underline">
        + Add an event manually
      </button>
    );
  }

  return (
    <Card className="mb-6">
      <h2 className="mb-3 font-display font-semibold text-text">Add an event manually</h2>
      <form
        className="grid grid-cols-1 gap-3 sm:grid-cols-2"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="Title">
          <Input required value={title} onChange={(e) => setTitle(e.target.value)} />
        </Field>
        <Field label="Date">
          <Input required type="date" value={eventDate} onChange={(e) => setEventDate(e.target.value)} />
        </Field>
        <Field label="Venue">
          <Input value={venueName} onChange={(e) => setVenueName(e.target.value)} />
        </Field>
        <Field label="City">
          <Input value={city} onChange={(e) => setCity(e.target.value)} />
        </Field>
        <Field label="Link (optional)" className="sm:col-span-2">
          <Input type="url" value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://..." />
        </Field>
        <div className="flex gap-3 sm:col-span-2">
          <Button type="submit" disabled={mutation.isPending || !title || !eventDate}>
            Add Event
          </Button>
          <Button type="button" variant="secondary" onClick={() => setOpen(false)}>
            Cancel
          </Button>
        </div>
      </form>
    </Card>
  );
}

function EventCard({ event }: { event: LiteraryEvent }) {
  const queryClient = useQueryClient();
  const removeMutation = useMutation({
    mutationFn: () => api.events.remove(event.event_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["events"] }),
  });

  const date = new Date(`${event.event_date}T00:00:00`).toLocaleDateString(undefined, {
    weekday: "short",
    day: "numeric",
    month: "short",
    year: "numeric",
  });

  return (
    <Card className="flex gap-3">
      {event.image_url ? (
        <img src={event.image_url} alt={event.title} className="h-20 w-20 shrink-0 rounded object-cover" />
      ) : (
        <div className="flex h-20 w-20 shrink-0 items-center justify-center rounded bg-surface-hover text-2xl">
          🎟️
        </div>
      )}
      <div className="min-w-0 flex-1">
        <div className="flex items-start justify-between gap-2">
          <p className="truncate text-sm font-medium text-text" title={event.title}>
            {event.title}
          </p>
          {event.source === "manual" && (
            <button
              type="button"
              aria-label="Remove"
              onClick={() => removeMutation.mutate()}
              className="shrink-0 text-xs text-text-muted hover:text-danger"
            >
              ✕
            </button>
          )}
        </div>
        <p className="text-xs text-text-muted">{date}</p>
        {(event.venue_name || event.city) && (
          <p className="truncate text-xs text-text-muted">
            {[event.venue_name, event.city].filter(Boolean).join(", ")}
          </p>
        )}
        <div className="mt-2 flex flex-wrap items-center gap-3">
          {event.url && (
            <a href={event.url} target="_blank" rel="noreferrer" className="text-xs text-primary hover:underline">
              Details →
            </a>
          )}
          <button type="button" onClick={() => downloadIcs(event)} className="text-xs text-primary hover:underline">
            Export to calendar
          </button>
        </div>
      </div>
    </Card>
  );
}

export function Events() {
  const { data: events, isLoading } = useQuery({ queryKey: ["events"], queryFn: () => api.events.list() });

  return (
    <div>
      <h1 className="mb-6 font-display text-2xl font-bold text-text">🎟️ Literary Events</h1>
      <CityPreferenceForm />
      <AddManualEventForm />

      {isLoading ? (
        <p className="text-text-muted">Loading...</p>
      ) : !events || events.length === 0 ? (
        <EmptyState
          icon="🎟️"
          title="No upcoming events"
          description="Set a city above to enable weekly discovery, or add one manually — coverage of small/independent events isn't guaranteed from the API alone."
        />
      ) : (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {events.map((event) => (
            <EventCard key={event.event_id} event={event} />
          ))}
        </div>
      )}
    </div>
  );
}
