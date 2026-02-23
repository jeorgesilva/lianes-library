"""Merged CRUD blueprint module.

Exports the CRUD functions defined in `notebooks/CRUD_Blueprint.ipynb`.
Notebook implementations take precedence. The module prefers the
centralized `src.db_connection.get_engine()` but will fall back to
legacy helpers if necessary.
"""

from typing import Any, Dict, List, Optional
from datetime import date, timedelta, datetime
import requests
import pandas as pd
from sqlalchemy import text


def get_engine():
	"""Return a SQLAlchemy Engine.

	Prefer `src.db_connection.get_engine()`; fall back to legacy helpers.
	"""
	try:
		from .db_connection import get_engine as _get_engine

		return _get_engine()
	except Exception:
		try:
			from .sql_to_python import get_engine as _get_engine

			return _get_engine()
		except Exception:
			try:
				from .sql_to_python import engine as _engine

				if _engine is None:
					raise RuntimeError("Engine exists but is None")
				return _engine
			except Exception:
				raise RuntimeError("Database engine is not configured. Create `src/db_connection.py`.")


# ----------------
# BOOKS CRUD
# ----------------
def fetch_all_books(engine) -> pd.DataFrame:
	"""Fetch all records from the books table using a provided engine."""
	query = text("SELECT * FROM books;")
	with engine.connect() as connection:
		result = connection.execute(query)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def create_book(title, author, isbn=None, cost=None):
	"""Insert a new book into `books` and return a confirmation string."""
	if not title:
		raise ValueError("title is required")
	query = text("""
	INSERT INTO books (title, author, ISBN, cost_book, book_status)
	VALUES (:title, :author, :isbn, :cost, :status)
	""")
	with get_engine().connect() as conn:
		transaction = conn.begin()
		try:
			conn.execute(
				query,
				{
					"title": title,
					"author": author,
					"isbn": isbn,
					"cost": cost,
					"status": "AVAILABLE",
				},
			)
			transaction.commit()
			return f"Added book '{title}' by {author}."
		except Exception:
			transaction.rollback()
			raise


def get_books(title: Optional[str] = None, author: Optional[str] = None, genre: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
	query = "SELECT * FROM books WHERE 1=1"
	params: Dict[str, Any] = {}
	if title:
		query += " AND title LIKE :title"
		params["title"] = f"%{title}%"
	if author:
		query += " AND author LIKE :author"
		params["author"] = f"%{author}%"
	if genre:
		query += " AND genre = :genre"
		params["genre"] = genre
	if status:
		query += " AND status = :status"
		params["status"] = status
	query += " LIMIT :limit"
	params["limit"] = int(limit)
	query_text = text(query)
	with get_engine().connect() as conn:
		result = conn.execute(query_text, params)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def get_book_by_id(book_id):
	query = text("SELECT * FROM books WHERE book_id = :book_id")
	with get_engine().connect() as conn:
		result = conn.execute(query, {"book_id": book_id})
		row = result.fetchone()
		if row:
			try:
				return dict(row._mapping)
			except Exception:
				return dict(row)
		return None


def update_book_details(book_id, title=None, author=None, isbn=None, genre=None, cost=None):
	set_clauses = []
	params: Dict[str, Any] = {"book_id": book_id}
	if title is not None:
		set_clauses.append("title = :title")
		params["title"] = title
	if author is not None:
		set_clauses.append("author = :author")
		params["author"] = author
	if isbn is not None:
		set_clauses.append("isbn = :isbn")
		params["isbn"] = isbn
	if genre is not None:
		set_clauses.append("genre = :genre")
		params["genre"] = genre
	if cost is not None:
		set_clauses.append("cost_book = :cost")
		params["cost"] = cost
	if not set_clauses:
		raise ValueError("No fields to update.")
	set_clause = ", ".join(set_clauses)
	query = text(f"UPDATE books SET {set_clause} WHERE book_id = :book_id")
	with get_engine().connect() as conn:
		transaction = conn.begin()
		try:
			conn.execute(query, params)
			transaction.commit()
			return f"Updated book id {book_id}."
		except Exception:
			transaction.rollback()
			raise


def update_book_status(book_id, new_status):
	allowed_statuses = {"AVAILABLE", "BORROWED", "LOST", "DAMAGED", "REMOVED"}
	if new_status not in allowed_statuses:
		raise ValueError(f"Invalid status '{new_status}'. Allowed statuses: {allowed_statuses}")
	query = text("UPDATE books SET book_status = :new_status WHERE book_id = :book_id")
	with get_engine().connect() as conn:
		transaction = conn.begin()
		try:
			conn.execute(query, {"new_status": new_status, "book_id": book_id})
			transaction.commit()
			return f"Updated status of book id {book_id} to '{new_status}'."
		except Exception:
			transaction.rollback()
			raise


def delete_book(book_id):
	# Notebook recommends logical delete: set status to 'REMOVED'
	query_status = text("SELECT book_status FROM books WHERE book_id = :book_id")
	query_update = text("UPDATE books SET book_status = 'REMOVED' WHERE book_id = :book_id")
	with get_engine().connect() as conn:
		transaction = conn.begin()
		try:
			result = conn.execute(query_status, {"book_id": book_id})
			row = result.mappings().fetchone()
			if row is None:
				raise ValueError(f"Book id {book_id} not found.")
			current_status = row.get("book_status")
			if str(current_status).upper() == "BORROWED":
				raise ValueError(f"Cannot delete book id {book_id} as it is currently BORROWED.")
			conn.execute(query_update, {"book_id": book_id})
			transaction.commit()
			return f"Book id {book_id} marked as REMOVED."
		except Exception:
			transaction.rollback()
			raise


# --------------------------------------
# Google Books lookup, parsing and bulk price update
# --------------------------------------
GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"


def get_exchange_rate(from_currency: str, to_currency: str = "EUR") -> Optional[float]:
	"""Try to fetch a live FX rate from exchangerate.host. Returns None on failure.

	Falls back to None so caller may choose a default or skip conversion.
	"""
	if not from_currency:
		return None
	try:
		url = "https://api.exchangerate.host/latest"
		params = {"base": from_currency.upper(), "symbols": to_currency.upper()}
		r = requests.get(url, params=params, timeout=6)
		r.raise_for_status()
		data = r.json()
		rates = data.get("rates", {})
		rate = rates.get(to_currency.upper())
		if rate:
			return float(rate)
	except Exception as e:
		print(f"[FX] Could not fetch FX rate {from_currency}->{to_currency}: {e}")
	return None


def google_books_lookup(query: str, max_results: int = 5, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
	"""Searchs Google Books through query (isbn:xxx or title/author).

	Returns items list (can be null).
	"""
	params = {
		"q": query,
		"maxResults": int(max_results),
		"printType": "books",
		"country": "BR",
	}
	if api_key is None:
		api_key = None
	if api_key:
		params["key"] = api_key

	try:
		resp = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=10)
		resp.raise_for_status()
		data = resp.json()
		return data.get("items", [])
	except Exception as e:
		print(f"[GoogleBooks] API error for query '{query}': {e}")
		return []


def parse_google_item(item: Dict[str, Any]) -> Dict[str, Any]:
	"""Extracts títle, autthor, isbns and price from items returned by the API."""
	info = item.get("volumeInfo", {}) or {}
	sale = item.get("saleInfo", {}) or {}

	title = info.get("title")
	authors = info.get("authors", []) or []

	isbn10 = None
	isbn13 = None
	for ident in info.get("industryIdentifiers", []):
		t = ident.get("type")
		v = ident.get("identifier")
		if t == "ISBN_10":
			isbn10 = v
		elif t == "ISBN_13":
			isbn13 = v

	price = None
	currency = None
	if sale.get("saleability") == "FOR_SALE":
		price_info = sale.get("listPrice") or {}
		price = price_info.get("amount")
		currency = price_info.get("currencyCode")

	return {
		"title": title,
		"authors": authors,
		"isbn10": isbn10,
		"isbn13": isbn13,
		"price": price,
		"currency": currency,
		"sale_status": sale.get("saleability"),
		"raw": item,
	}


def _normalize_text(s: Optional[str]) -> str:
	if not s:
		return ""
	import re

	s = s.lower().strip()
	s = re.sub(r"[^a-z0-9\s]", " ", s)
	s = re.sub(r"\s+", " ", s)
	return s


def _author_matches(db_author: Optional[str], api_authors: List[str]) -> bool:
	if not db_author:
		return True
	db_norm = _normalize_text(db_author)
	for a in api_authors:
		if a and db_norm in _normalize_text(a):
			return True
		if a and _normalize_text(a) in db_norm:
			return True
	return False


def _title_matches(db_title: Optional[str], api_title: Optional[str]) -> bool:
	if not db_title or not api_title:
		return False
	dbn = _normalize_text(db_title)
	apin = _normalize_text(api_title)
	return dbn in apin or apin in dbn


def update_missing_prices_from_web(limit: int = 50, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
	"""
	Objetivo final: para cada livro sem preço, tentar achar preço via Google Books
	seguindo a estratégia: ISBN -> validar título/autor -> se falhar, buscar por título+autor

	Retorna lista de registros atualizados com detalhes.
	"""
	engine = get_engine()

	select_sql = text("""
		SELECT book_id, ISBN, title, author, cost_book
		FROM books
		WHERE (cost_book IS NULL OR cost_book = 0)
		  AND (ISBN IS NULL OR ISBN <> '')
		LIMIT :limit
	""")

	with engine.connect() as conn:
		rows = conn.execute(select_sql, {"limit": int(limit)}).mappings().all()

	if not rows:
		print("ℹ️ No books with missing prices found.")
		return []

	updated_rows: List[Dict[str, Any]] = []
	print(f"🔍 Found {len(rows)} book(s) with missing prices. Processing...")

	for row in rows:
		book_id = row.get("book_id")
		isbn = row.get("ISBN")
		title = row.get("title")
		author = row.get("author")
		old_cost = row.get("cost_book")
		updated = False
		updated_info: Dict[str, Any] = {
			"book_id": book_id,
			"title": title,
			"old_isbn": isbn,
			"old_cost": old_cost,
			"new_isbn": None,
			"new_cost": None,
			"currency": None,
			"matched_by": None,
			"updated_at": None,
		}

		# 1) Try ISBN search if ISBN present
		candidates = []
		if isbn:
			q = f"isbn:{isbn}"
			items = google_books_lookup(q, max_results=3, api_key=api_key)
			candidates = items or []

		# parse candidates and validate title/author
		chosen = None
		for it in candidates:
			parsed = parse_google_item(it)
			if parsed.get("price") is None:
				continue
			if _title_matches(title, parsed.get("title")) and _author_matches(author, parsed.get("authors", [])):
				chosen = parsed
				updated_info["matched_by"] = "isbn"
				break

		# 2) If not chosen by ISBN, try title+author search
		if chosen is None:
			qparts = []
			if title:
				qparts.append(f"intitle:{title}")
			if author:
				qparts.append(f"inauthor:{author}")
			q = "+".join(qparts) if qparts else title or author or ""
			if q:
				items = google_books_lookup(q, max_results=5, api_key=api_key)
				for it in items:
					parsed = parse_google_item(it)
					# if parsed has an isbn and price, consider it
					if parsed.get("price") is None:
						continue
					# if title matches reasonably
					if _title_matches(title, parsed.get("title")):
						chosen = parsed
						updated_info["matched_by"] = "title_author"
						break

		# 3) If we found a candidate with price, apply DB updates
		if chosen is not None:
			new_price = chosen.get("price")
			currency = chosen.get("currency") or "UNK"
			new_isbn = chosen.get("isbn13") or chosen.get("isbn10")

			try:
				with engine.begin() as conn:
					# Prepare price and currency; convert BRL -> EUR so function returns EUR values
					price_raw = float(new_price)
					currency_label = (currency or "").upper()
					price_to_store = price_raw
					# If Google Books reports BRL, convert to EUR
					if currency_label in {"BRL", "R$"}:
						rate = get_exchange_rate("BRL", "EUR")
						if rate is None:
							# fallback conservative rate if live fetch fails
							rate = 0.18
							print(f"[FX] Using fallback BRL->EUR rate {rate}")
						price_to_store = round(price_raw * float(rate), 2)
						currency_db = "€"
					else:
						currency_db = currency or "UNK"

					with engine.begin() as conn:
						# update cost_book (store converted price if applicable)
						conn.execute(
							text("""
								UPDATE books SET cost_book = :price WHERE book_id = :book_id
							"""),
							{"price": float(price_to_store), "book_id": book_id},
						)

						# update ISBN if we found a valid one and it's different
						if new_isbn and (not isbn or str(new_isbn) != str(isbn)):
							conn.execute(text("UPDATE books SET ISBN = :new_isbn WHERE book_id = :book_id"), {"new_isbn": new_isbn, "book_id": book_id})
							updated_info["new_isbn"] = new_isbn

						# insert price_history (price in EUR when conversion applied)
						conn.execute(
							text("""
								INSERT INTO price_history (book_id, isbn, price, currency, source)
								VALUES (:book_id, :isbn, :price, :currency, :source)
							"""),
							{"book_id": book_id, "isbn": new_isbn or isbn, "price": float(price_to_store), "currency": currency_db, "source": "google_books"},
						)

					updated = True
					updated_info["new_cost"] = float(price_to_store)
					updated_info["currency"] = currency_db
					updated_info["updated_at"] = datetime.utcnow()
					updated_rows.append(updated_info)
					print(f"✅ Updated book_id={book_id} (ISBN {isbn} -> {new_isbn}) price {price_to_store} {currency_db}")
			except Exception as e:
				print(f"⚠️ DB error updating book_id={book_id}: {e}")

		else:
			print(f"❌ No suitable Google Books match for book_id={book_id}, ISBN={isbn}, title='{title}'")

	print(f"🎉 Processing finished. {len(updated_rows)} book(s) updated.")
	return updated_rows


def reprocess_with_fuzzy(book_ids: List[int], api_key: Optional[str] = None, title_threshold: float = 0.7, author_threshold: float = 0.6) -> List[Dict[str, Any]]:
	"""Reprocess a specific list of `book_id`s using a more permissive fuzzy matching.

	Uses difflib.SequenceMatcher to compute similarity between DB title/author and API results.
	If a candidate has a price and meets either the title or author threshold, it will be
	applied using the same update/insert logic as `update_missing_prices_from_web` (including
	BRL->EUR conversion).
	"""
	from difflib import SequenceMatcher

	engine = get_engine()
	updated_rows: List[Dict[str, Any]] = []

	for bid in book_ids:
		# fetch book
		book = get_book_by_id(bid)
		if not book:
			print(f"[Fuzzy] Book id {bid} not found; skipping.")
			continue
		title = book.get("title")
		author = book.get("author")
		isbn = book.get("ISBN")
		old_cost = book.get("cost_book")

		print(f"[Fuzzy] Processing book_id={bid} title='{title}'")

		# Search by title+author first; expand queries if isbn not helpful
		qparts = []
		if title:
			qparts.append(f"intitle:{title}")
		if author:
			qparts.append(f"inauthor:{author}")
		q = "+".join(qparts) if qparts else title or author or isbn or ""
		if not q:
			print(f"[Fuzzy] No viable query for book_id={bid}; skipping.")
			continue

		items = google_books_lookup(q, max_results=10, api_key=api_key)
		chosen = None
		chosen_parsed = None

		for it in items:
			parsed = parse_google_item(it)
			if parsed.get("price") is None:
				continue

			# compute normalized similarity scores
			title_score = 0.0
			author_score = 0.0
			if title and parsed.get("title"):
				a = _normalize_text(title)
				b = _normalize_text(parsed.get("title"))
				title_score = SequenceMatcher(None, a, b).ratio()
			if author and parsed.get("authors"):
				a = _normalize_text(author)
				# compare against each api author
				for pa in parsed.get("authors", []):
					if not pa:
						continue
					s = SequenceMatcher(None, a, _normalize_text(pa)).ratio()
					if s > author_score:
						author_score = s

			# accept if thresholds met
			if title_score >= float(title_threshold) or author_score >= float(author_threshold):
				chosen = parsed
				chosen_parsed = {"title_score": title_score, "author_score": author_score}
				print(f"[Fuzzy] Candidate accepted for book_id={bid} (title_score={title_score:.2f} author_score={author_score:.2f})")
				break

		if chosen is None:
			print(f"[Fuzzy] No fuzzy match for book_id={bid}")
			continue

		# Apply same DB update logic (with conversion)
		new_price = chosen.get("price")
		currency = chosen.get("currency") or "UNK"
		new_isbn = chosen.get("isbn13") or chosen.get("isbn10")

		try:
			price_raw = float(new_price)
			currency_label = (currency or "").upper()
			price_to_store = price_raw
			if currency_label in {"BRL", "R$"}:
				rate = get_exchange_rate("BRL", "EUR")
				if rate is None:
					rate = 0.18
					print(f"[FX] Using fallback BRL->EUR rate {rate}")
				price_to_store = round(price_raw * float(rate), 2)
				currency_db = "€"
			else:
				currency_db = currency or "UNK"

			with engine.begin() as conn:
				conn.execute(text("UPDATE books SET cost_book = :price WHERE book_id = :book_id"), {"price": float(price_to_store), "book_id": bid})
				if new_isbn and (not isbn or str(new_isbn) != str(isbn)):
					conn.execute(text("UPDATE books SET ISBN = :new_isbn WHERE book_id = :book_id"), {"new_isbn": new_isbn, "book_id": bid})
				conn.execute(text("INSERT INTO price_history (book_id, isbn, price, currency, source) VALUES (:book_id, :isbn, :price, :currency, :source)"), {"book_id": bid, "isbn": new_isbn or isbn, "price": float(price_to_store), "currency": currency_db, "source": "google_books"})

			updated_info = {
				"book_id": bid,
				"title": title,
				"old_isbn": isbn,
				"old_cost": old_cost,
				"new_isbn": new_isbn,
				"new_cost": float(price_to_store),
				"currency": currency_db,
				"matched_by": "fuzzy_title_author",
				"title_score": chosen_parsed.get("title_score") if chosen_parsed else None,
				"author_score": chosen_parsed.get("author_score") if chosen_parsed else None,
				"updated_at": datetime.utcnow(),
			}
			updated_rows.append(updated_info)
			print(f"[Fuzzy] ✅ Updated book_id={bid} price {price_to_store} {currency_db}")
		except Exception as e:
			print(f"[Fuzzy] ⚠️ DB error updating book_id={bid}: {e}")

	return updated_rows


# ----------------
# AUTHORS (not fully implemented in notebook — keep stubs)
# ----------------
def create_author(name):
	# TODO: implement
	raise NotImplementedError("create_author not implemented")


def get_author_by_name(name):
	raise NotImplementedError("get_author_by_name not implemented")


def get_author_by_id(author_id):
	raise NotImplementedError("get_author_by_id not implemented")


def update_author(author_id, new_name):
	raise NotImplementedError("update_author not implemented")


def delete_author(author_id):
	raise NotImplementedError("delete_author not implemented")


# ----------------
# BORROWERS
# ----------------
def create_borrower(first_name, last_name, email=None, phone_number=None, relationship_type=None, address=None):
	if not first_name and not last_name:
		raise ValueError("Name is required for a borrower.")
	engine = get_engine()
	insert_sql = text(
		"""
		INSERT INTO borrowers (first_name, last_name, email, phone_number, relationship_type, address)
		VALUES (:first_name, :last_name, :email, :phone_number, :relationship_type, :address)
	"""
	)
	params = {
		"first_name": first_name,
		"last_name": last_name,
		"email": email,
		"phone_number": phone_number,
		"relationship_type": relationship_type,
		"address": address,
	}
	with engine.begin() as conn:
		result = conn.execute(insert_sql, params)
		try:
			person_id = result.inserted_primary_key[0]
		except Exception:
			person_id = getattr(result, "lastrowid", None)
		row = conn.execute(text('SELECT * FROM borrowers WHERE person_id = :person_id'), {"person_id": person_id}).mappings().one()
	return dict(row)


def get_borrower_by_id(person_id):
	engine = get_engine()
	sql = text("SELECT * FROM borrowers WHERE person_id = :person_id")
	with engine.connect() as conn:
		row = conn.execute(sql, {"person_id": person_id}).mappings().one_or_none()
	return dict(row) if row is not None else None


def get_borrowers(first_name=None, last_name=None, limit=100):
	engine = get_engine()
	query = "SELECT * FROM borrowers WHERE 1 = 1"
	params: Dict[str, Any] = {}
	if first_name:
		query += " AND first_name LIKE :first_name"
		params["first_name"] = f"%{first_name}"
	if last_name:
		query += " AND last_name = :last_name"
		params["last_name"] = last_name
	query += " ORDER BY first_name DESC LIMIT :limit"
	params["limit"] = int(limit)
	sql = text(query)
	with engine.connect() as conn:
		rows = conn.execute(sql, params).mappings().all()
	return [dict(r) for r in rows]


def update_borrower_contact(person_id, first_name=None, last_name=None, email=None, phone=None, address=None):
	fields: List[str] = []
	params: Dict[str, Any] = {"person_id": person_id}
	if first_name is not None:
		fields.append('first_name = :first_name')
		params['first_name'] = first_name
	if last_name is not None:
		fields.append('last_name = :last_name')
		params['last_name'] = last_name
	if email is not None:
		fields.append('email = :email')
		params['email'] = email
	if phone is not None:
		fields.append('phone = :phone')
		params['phone'] = phone
	if address is not None:
		fields.append('address = :address')
		params['address'] = address
	if not fields:
		raise ValueError("No contact fields provided to update.")
	set_clause = ", ".join(fields)
	sql = text(f"UPDATE borrowers SET {set_clause} WHERE person_id = :person_id")
	engine = get_engine()
	with engine.begin() as conn:
		conn.execute(sql, params)
		row = conn.execute(text("SELECT * FROM borrowers WHERE person_id = :person_id"), {"person_id": person_id}).mappings().one()
	return dict(row)


def set_borrower_status(person_id, new_status):
	allowed = {"ACTIVE", "INACTIVE"}
	if new_status not in allowed:
		raise ValueError(f"Invalid status '{new_status}'. Allowed: {allowed}")
	sql = text("""
		UPDATE borrowers
			   SET status = :status
			   WHERE person_id = :person_id
	""")
	engine = get_engine()
	with engine.begin() as conn:
		conn.execute(sql, {"status": new_status, "person_id": person_id})
		# attempt to return updated row
		row = conn.execute(text('SELECT * FROM borrowers WHERE person_id = :person_id'), {"person_id": person_id}).mappings().one()
	return dict(row)


def delete_borrower(person_id=None, first_name=None, last_name=None):
	engine = get_engine()
	query = "DELETE FROM borrowers WHERE 1 = 1"
	params: Dict[str, Any] = {}
	if person_id:
		query += " AND person_id LIKE :person_id"
		params["person_id"] = f"%{person_id}"
	if first_name:
		query += " AND first_name LIKE :first_name"
		params["first_name"] = f"%{first_name}"
	if last_name:
		query += " AND last_name = :last_name"
		params["last_name"] = last_name
	if not params:
		raise ValueError("You must pass at least one filter (person_id, first_name or last_name).")
	sql = text(query)
	with engine.begin() as conn:
		result = conn.execute(sql, params)
	return result.rowcount


# ----------------
# LOANS / TRANSACTIONS
# ----------------
def create_loan(book_id: int, person_id: int, loan_date: Optional[date] = None, due_date: Optional[date] = None, loan_period_days: int = 14) -> Dict[str, Any]:
	engine = get_engine()
	if loan_date is None:
		loan_date = date.today()
	if due_date is None:
		due_date = loan_date + timedelta(days=loan_period_days)
	check_book_sql = text("""
		SELECT book_id, title, book_status 
		FROM books 
		WHERE book_id = :book_id
	""")
	check_borrower_sql = text("""
		SELECT person_id, first_name, last_name 
		FROM borrowers 
		WHERE person_id = :person_id
	""")
	insert_transaction_sql = text("""
		INSERT INTO transactions (book_id, person_id, loan_date, due_date)
		VALUES (:book_id, :person_id, :loan_date, :due_date)
	""")
	update_book_status_sql = text("""
		UPDATE books 
		SET book_status = 'borrowed' 
		WHERE book_id = :book_id
	""")
	with engine.begin() as conn:
		book = conn.execute(check_book_sql, {"book_id": book_id}).mappings().one_or_none()
		if book is None:
			raise ValueError(f"Book with ID {book_id} does not exist.")
		if str(book["book_status"]).lower() != "available":
			raise ValueError(f"Book '{book['title']}' is not available (status: {book['book_status']}).")
		borrower = conn.execute(check_borrower_sql, {"person_id": person_id}).mappings().one_or_none()
		if borrower is None:
			raise ValueError(f"Borrower with ID {person_id} does not exist.")
		result = conn.execute(insert_transaction_sql, {"book_id": book_id, "person_id": person_id, "loan_date": loan_date, "due_date": due_date})
		try:
			transaction_id = result.lastrowid
		except Exception:
			transaction_id = getattr(result, "lastrowid", None)
		conn.execute(update_book_status_sql, {"book_id": book_id})
	return {
		"transaction_id": transaction_id,
		"book_id": book_id,
		"book_title": book["title"],
		"person_id": person_id,
		"borrower_name": f"{borrower['first_name']} {borrower['last_name']}",
		"loan_date": loan_date,
		"due_date": due_date,
		"status": "active",
	}


def process_return(transaction_id: int, return_date: Optional[date] = None) -> Dict[str, Any]:
	engine = get_engine()
	if return_date is None:
		return_date = date.today()
	get_transaction_sql = text("""
		SELECT t.transaction_id, t.book_id, t.person_id, 
			   t.loan_date, t.due_date, t.actual_return_date,
			   b.title as book_title,
			   br.first_name, br.last_name
		FROM transactions t
		JOIN books b ON t.book_id = b.book_id
		JOIN borrowers br ON t.person_id = br.person_id
		WHERE t.transaction_id = :transaction_id
	""")
	update_transaction_sql = text("""
		UPDATE transactions 
		SET actual_return_date = :return_date 
		WHERE transaction_id = :transaction_id
	""")
	update_book_status_sql = text("""
		UPDATE books 
		SET book_status = 'available' 
		WHERE book_id = :book_id
	""")
	with engine.begin() as conn:
		trans = conn.execute(get_transaction_sql, {"transaction_id": transaction_id}).mappings().one_or_none()
		if trans is None:
			raise ValueError(f"Transaction {transaction_id} does not exist.")
		if trans["actual_return_date"] is not None:
			raise ValueError(f"Transaction {transaction_id} already closed on {trans['actual_return_date']}.")
		conn.execute(update_transaction_sql, {"transaction_id": transaction_id, "return_date": return_date})
		conn.execute(update_book_status_sql, {"book_id": trans["book_id"]})
	is_late = return_date > trans["due_date"]
	days_late = (return_date - trans["due_date"]).days if is_late else 0
	return {
		"transaction_id": transaction_id,
		"book_id": trans["book_id"],
		"book_title": trans["book_title"],
		"borrower_name": f"{trans['first_name']} {trans['last_name']}",
		"loan_date": trans["loan_date"],
		"due_date": trans["due_date"],
		"return_date": return_date,
		"is_late": is_late,
		"days_late": days_late,
		"status": "returned",
	}


def process_return_by_book(
	book_id: Optional[int] = None,
	book_title: Optional[str] = None,
	return_date: Optional[date] = None,
) -> Dict[str, Any]:
	"""
	UPDATE: Process a book return using book_id or book_title.

	Finds the active loan for the given book and processes the return.

	Args:
		book_id: ID of the book being returned (optional)
		book_title: Title of the book being returned (optional, partial match)
		return_date: Date of return (defaults to today)

	Returns:
		Dict with return details including late status

	Raises:
		ValueError: If no active loan found for this book or no identifier provided
	"""
	engine = get_engine()

	if book_id is None and book_title is None:
		raise ValueError("Must provide either book_id or book_title.")

	if return_date is None:
		return_date = date.today()

	# Build query based on provided identifier
	if book_id is not None:
		get_active_loan_sql = text("""
			SELECT t.transaction_id, t.book_id, t.person_id, 
				   t.loan_date, t.due_date, t.actual_return_date,
				   b.title as book_title,
				   br.first_name, br.last_name
			FROM transactions t
			JOIN books b ON t.book_id = b.book_id
			JOIN borrowers br ON t.person_id = br.person_id
			WHERE t.book_id = :book_id
			  AND t.actual_return_date IS NULL
			ORDER BY t.loan_date DESC
			LIMIT 1
		""")
		params = {"book_id": book_id}
	else:
		# Search by title (partial match)
		get_active_loan_sql = text("""
			SELECT t.transaction_id, t.book_id, t.person_id, 
				   t.loan_date, t.due_date, t.actual_return_date,
				   b.title as book_title,
				   br.first_name, br.last_name
			FROM transactions t
			JOIN books b ON t.book_id = b.book_id
			JOIN borrowers br ON t.person_id = br.person_id
			WHERE b.title LIKE :book_title
			  AND t.actual_return_date IS NULL
			ORDER BY t.loan_date DESC
			LIMIT 1
		""")
		params = {"book_title": f"%{book_title}%"}

	update_transaction_sql = text("""
		UPDATE transactions 
		SET actual_return_date = :return_date 
		WHERE transaction_id = :transaction_id
	""")

	update_book_status_sql = text("""
		UPDATE books 
		SET book_status = 'available' 
		WHERE book_id = :book_id
	""")

	# Execute in transaction
	with engine.begin() as conn:
		# Step 1: Find active loan for this book
		trans = conn.execute(get_active_loan_sql, params).mappings().one_or_none()

		if trans is None:
			identifier = f"book ID {book_id}" if book_id else f"title '{book_title}'"
			raise ValueError(f"No active loan found for {identifier}.")

		# Step 2: Update transaction with return date
		conn.execute(update_transaction_sql, {
			"transaction_id": trans["transaction_id"],
			"return_date": return_date,
		})

		# Step 3: Update book status to available
		conn.execute(update_book_status_sql, {"book_id": trans["book_id"]})

	# Calculate if return was late
	is_late = return_date > trans["due_date"]
	days_late = (return_date - trans["due_date"]).days if is_late else 0

	return {
		"transaction_id": trans["transaction_id"],
		"book_id": trans["book_id"],
		"book_title": trans["book_title"],
		"borrower_name": f"{trans['first_name']} {trans['last_name']}",
		"loan_date": trans["loan_date"],
		"due_date": trans["due_date"],
		"return_date": return_date,
		"is_late": is_late,
		"days_late": days_late,
		"status": "returned",
	}


def get_active_loans() -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			t.transaction_id,
			t.book_id,
			b.title as book_title,
			b.author,
			t.person_id,
			CONCAT(br.first_name, ' ', br.last_name) as borrower_name,
			t.loan_date,
			t.due_date,
			DATEDIFF(CURRENT_DATE, t.due_date) as days_overdue,
			CASE 
				WHEN CURRENT_DATE > t.due_date THEN 'overdue'
				ELSE 'active'
			END as status
		FROM transactions t
		JOIN books b ON t.book_id = b.book_id
		JOIN borrowers br ON t.person_id = br.person_id
		WHERE t.actual_return_date IS NULL
		ORDER BY t.due_date ASC
	""")
	with engine.connect() as conn:
		result = conn.execute(query)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def get_overdue_loans() -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			t.transaction_id,
			t.book_id,
			b.title as book_title,
			t.person_id,
			CONCAT(br.first_name, ' ', br.last_name) as borrower_name,
			br.email,
			br.phone_number,
			t.loan_date,
			t.due_date,
			DATEDIFF(CURRENT_DATE, t.due_date) as days_overdue
		FROM transactions t
		JOIN books b ON t.book_id = b.book_id
		JOIN borrowers br ON t.person_id = br.person_id
		WHERE t.actual_return_date IS NULL
		  AND t.due_date < CURRENT_DATE
		ORDER BY days_overdue DESC
	""")
	with engine.connect() as conn:
		result = conn.execute(query)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def get_loan_history_by_book(book_id: int) -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			t.transaction_id,
			t.person_id,
			CONCAT(br.first_name, ' ', br.last_name) as borrower_name,
			t.loan_date,
			t.due_date,
			t.actual_return_date,
			CASE 
				WHEN t.actual_return_date IS NULL THEN 'active'
				WHEN t.actual_return_date > t.due_date THEN 'returned_late'
				ELSE 'returned_on_time'
			END as status
		FROM transactions t
		JOIN borrowers br ON t.person_id = br.person_id
		WHERE t.book_id = :book_id
		ORDER BY t.loan_date DESC
	""")
	with engine.connect() as conn:
		result = conn.execute(query, {"book_id": book_id})
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def get_loan_history_by_borrower(person_id: int) -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			t.transaction_id,
			t.book_id,
			b.title as book_title,
			b.author,
			t.loan_date,
			t.due_date,
			t.actual_return_date,
			CASE 
				WHEN t.actual_return_date IS NULL THEN 'active'
				WHEN t.actual_return_date > t.due_date THEN 'returned_late'
				ELSE 'returned_on_time'
			END as status
		FROM transactions t
		JOIN books b ON t.book_id = b.book_id
		WHERE t.person_id = :person_id
		ORDER BY t.loan_date DESC
	""")
	with engine.connect() as conn:
		result = conn.execute(query, {"person_id": person_id})
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


# Aliases to keep compatibility with older callers
get_loan_history_for_book = get_loan_history_by_book
get_loan_history_for_borrower = get_loan_history_by_borrower


# ----------------
# REPORTS / DASHBOARD
# ----------------
def get_dashboard_stats() -> Dict[str, Any]:
	engine = get_engine()
	query = text("""
		SELECT 
			COUNT(DISTINCT CASE WHEN t.actual_return_date IS NULL THEN t.transaction_id END) as active_loans,
			COUNT(DISTINCT CASE WHEN t.actual_return_date IS NULL AND t.due_date < CURRENT_DATE THEN t.transaction_id END) as overdue_loans,
			COUNT(DISTINCT CASE WHEN b.book_status = 'available' THEN b.book_id END) as available_books,
			COUNT(DISTINCT CASE WHEN b.book_status = 'borrowed' THEN b.book_id END) as borrowed_books,
			COUNT(DISTINCT b.book_id) as total_books,
			COUNT(DISTINCT br.person_id) as total_borrowers
		FROM books b
		CROSS JOIN borrowers br
		LEFT JOIN transactions t ON b.book_id = t.book_id AND t.actual_return_date IS NULL
	""")
	with engine.connect() as conn:
		result = conn.execute(query).mappings().one()
	return dict(result)


def get_most_borrowed_books(limit: int = 10) -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			b.book_id,
			b.title,
			b.author,
			COUNT(t.transaction_id) as total_loans,
			COUNT(CASE WHEN t.actual_return_date IS NULL THEN 1 END) as currently_borrowed
		FROM books b
		LEFT JOIN transactions t ON b.book_id = t.book_id
		GROUP BY b.book_id, b.title, b.author
		ORDER BY total_loans DESC
		LIMIT :limit
	""")
	with engine.connect() as conn:
		result = conn.execute(query, {"limit": int(limit)})
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def get_most_active_borrowers(limit: int = 10) -> pd.DataFrame:
	engine = get_engine()
	query = text("""
		SELECT 
			br.person_id,
			CONCAT(br.first_name, ' ', br.last_name) as borrower_name,
			br.relationship_type,
			COUNT(t.transaction_id) as total_loans,
			COUNT(CASE WHEN t.actual_return_date IS NULL THEN 1 END) as currently_borrowed,
			COUNT(CASE WHEN t.actual_return_date > t.due_date THEN 1 END) as late_returns
		FROM borrowers br
		LEFT JOIN transactions t ON br.person_id = t.person_id
		GROUP BY br.person_id, borrower_name, br.relationship_type
		ORDER BY total_loans DESC
		LIMIT :limit
	""")
	with engine.connect() as conn:
		result = conn.execute(query, {"limit": int(limit)})
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def main():
	"""Placeholder test runner as suggested in the notebook."""
	# TODO: implement small integration tests that exercise CRUD flows
	pass


if __name__ == '__main__':
	main()


