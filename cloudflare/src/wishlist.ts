import type { Env } from "./index";
import { insertNotificationsIfMissing, type NotificationCandidate } from "./notifications";

interface WishlistItemRow {
  wishlist_item_id: number;
  owner_id: number;
  title: string;
  author: string | null;
  isbn: string | null;
  target_price: number | null;
}

interface PriceResult {
  price: number;
  currency: string;
  url: string;
  source: string;
}

// Interface, not a hardcoded call (section 4.1/4.2): more sources
// (Bookshop.org affiliate, Amazon affiliate, BookScouter for used prices)
// can be added as additional providers later without touching the job
// below. Only Google Books is implemented today — it's free without a key
// (100 req/day) and covers the common case, which keeps the job usable out
// of the box per the "works without paid APIs" principle (section 1.5).
type PriceProvider = (item: WishlistItemRow, env: Env) => Promise<PriceResult | null>;

async function fetchGoogleBooksPrice(item: WishlistItemRow, env: Env): Promise<PriceResult | null> {
  const q = item.isbn
    ? `isbn:${item.isbn}`
    : `intitle:${encodeURIComponent(item.title)}${item.author ? `+inauthor:${encodeURIComponent(item.author)}` : ""}`;
  const key = env.GOOGLE_BOOKS_API_KEY ? `&key=${env.GOOGLE_BOOKS_API_KEY}` : "";

  const res = await fetch(`https://www.googleapis.com/books/v1/volumes?q=${q}${key}&maxResults=1`);
  if (!res.ok) return null;

  const data = await res.json<{
    items?: { volumeInfo?: { infoLink?: string }; saleInfo?: { saleability?: string; retailPrice?: { amount?: number; currencyCode?: string }; buyLink?: string } }[];
  }>();
  const volume = data.items?.[0];
  const sale = volume?.saleInfo;
  if (!sale || sale.saleability !== "FOR_SALE" || sale.retailPrice?.amount == null) return null;

  return {
    price: sale.retailPrice.amount,
    currency: sale.retailPrice.currencyCode ?? "BRL",
    url: sale.buyLink ?? volume?.volumeInfo?.infoLink ?? "",
    source: "google_books",
  };
}

const PRICE_PROVIDERS: PriceProvider[] = [fetchGoogleBooksPrice];

async function getActiveWishlistItems(env: Env): Promise<WishlistItemRow[]> {
  const result = await env.DB.prepare(
    `SELECT wishlist_item_id, owner_id, title, author, isbn, target_price
     FROM wishlist_items WHERE status = 'ACTIVE'`
  ).all<WishlistItemRow>();
  return result.results;
}

async function getLowestPriceExcluding(env: Env, wishlistItemId: number, excludeSnapshotId: number): Promise<number | null> {
  const row = await env.DB.prepare(
    `SELECT MIN(price) as min_price FROM wishlist_price_snapshots WHERE wishlist_item_id = ? AND snapshot_id != ?`
  )
    .bind(wishlistItemId, excludeSnapshotId)
    .first<{ min_price: number | null }>();
  return row?.min_price ?? null;
}

// Drop meaningful enough to flag: >10% below the lowest price seen so far,
// or at/under the price Liane said she'd be happy to pay (section 4.2).
const MEANINGFUL_DROP_RATIO = 0.9;

export async function checkWishlistPricesAndNotify(
  env: Env
): Promise<{ itemsChecked: number; snapshotsRecorded: number; inAppCreated: number }> {
  const items = await getActiveWishlistItems(env);
  let snapshotsRecorded = 0;
  const candidates: NotificationCandidate[] = [];

  for (const item of items) {
    let result: PriceResult | null = null;
    for (const provider of PRICE_PROVIDERS) {
      result = await provider(item, env);
      if (result) break;
    }
    if (!result) continue; // no source had a price today — skip, not an error

    const inserted = await env.DB.prepare(
      `INSERT INTO wishlist_price_snapshots (wishlist_item_id, source, price, currency, url) VALUES (?, ?, ?, ?, ?)`
    )
      .bind(item.wishlist_item_id, result.source, result.price, result.currency, result.url)
      .run();
    snapshotsRecorded++;

    const previousLowest = await getLowestPriceExcluding(env, item.wishlist_item_id, Number(inserted.meta.last_row_id));
    const droppedMeaningfully = previousLowest != null && result.price <= previousLowest * MEANINGFUL_DROP_RATIO;
    const hitTarget = item.target_price != null && result.price <= item.target_price;

    if (droppedMeaningfully || hitTarget) {
      candidates.push({
        owner_id: item.owner_id,
        type: "PRICE_DROP",
        title: `Price drop: "${item.title}"`,
        body: hitTarget
          ? `Now ${result.currency} ${result.price.toFixed(2)} — at or below your target of ${result.currency} ${item.target_price!.toFixed(2)}.`
          : `Now ${result.currency} ${result.price.toFixed(2)}, down from ${result.currency} ${previousLowest!.toFixed(2)}.`,
        link: "/wishlist",
      });
    }
  }

  const inAppCreated = await insertNotificationsIfMissing(env, candidates);
  return { itemsChecked: items.length, snapshotsRecorded, inAppCreated };
}
