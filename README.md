# 📚 Liane’s Library  
*A personal book-loan tracking system*

---

## 💼 Case Study: Why This Project Exists

Liane is an avid reader, with a growing collection of books she loves to share.  
Unfortunately, when she lends books to friends, colleagues, and even distant acquaintances, many of them forget to return them.

After losing several favorites, Liane shared her frustration over coffee:  

> *“I love sharing my books, but I can’t keep track of who has them.”*

So, if people treat her like a library…  
**Why not become one?**

This project aims to build a simple, user-friendly system so Liane can track:
- Who borrowed each book
- When they should return it
- Which books are overdue

Our mission: **Make book sharing joyful again.**

---

## 🚀 Project Roadmap

| Phase | Description | Deliverables |
|-------|-------------|--------------|
| 1️⃣ Planning | Define requirements, data schema, tech stack | ER diagram, SQL schema |
| 2️⃣ Database | Create tables + relationships | `books`, `borrowers`, `transactions` |
| 3️⃣ Backend | CRUD logic | DB connector, data validation |
| 4️⃣ Frontend | Streamlit UI | Book list, loan & return forms |
| 5️⃣ Integration | Connect UI to DB | Database-powered app |
| 6️⃣ Testing | UX + functional tests | Fixes & improvements |
| 7️⃣ Deployment | Host final version | GitHub/Streamlit Cloud |
| 8️⃣ Documentation | Guide for Liane | Tutorial + README update |

---

## 🚀 Future Implementation Roadmap

This section outlines planned enhancements and implementation-ready improvements for upcoming versions of Liane’s Library Management System. Items are organized by functional domains and written in technical language to help guide development work.

### 📚 Book Management Improvements
1. Damage & Loss Registry

    - Create a dedicated `book_incidents` table to register damaged, lost, or missing books.
    - Suggested columns: `id`, `book_id`, `person_id`, `incident_type`, `incident_date`, `compensation_status`, `notes`.
    - Add a UI section **Book Incidents** to display and filter incidents.
    - Automatically synchronize `books.book_status` with incident types (e.g., set to `lost`, `damaged`, `removed`).

2. Enhanced Book Status Automation

    - Extend status-change logic to:
      - Auto-flag loans/books as `overdue` when the due date passes.
      - Auto-change status to `lost` after a configurable threshold (e.g., 60 days overdue).
      - Cascade status changes into transactions/history (recording who and when a status change occurred).

3. Book Wishlist + Price Tracking

    - Add a `wishlist` table for desired books not yet owned (columns: `id`, `isbn`, `title`, `requested_by`, `created_at`, `notes`).
    - Integrate Google Books or other APIs to fetch metadata and price history.
    - Track sale events and send email/UI alerts when price drops are detected.
    - Support manual wishlist additions by ISBN or title and display cover, edition, and authors.

### 👤 Borrower Management Enhancements
4. Borrower Waiting List

    - Implement a waiting-list mechanism where multiple borrowers can queue for the same book.
    - Store queue position, timestamp and notify/mark the first borrower when a book becomes available.

5. Borrower Status System

    - Add borrower states: `active`, `inactive`, `suspended`.
    - Automate transitions: e.g., multiple overdue returns → `suspended`; long inactivity → `inactive`.
    - Block `suspended` borrowers from creating new loans.

6. Borrower History Flags & Risk Alerts

    - Compute and store risk indicators (e.g., `has_delayed_returns`, `has_lost_books`, `has_damaged_books`).
    - Surface visual warnings in the UI when selecting borrowers or creating loans.

7. Multi-Book Borrowing Counter

    - Add analytics to identify borrowers with multiple active loans and historically high activity.
    - Expose as dashboard widgets and borrower-specific metadata.

### 🔄 Transactions & Workflow Enhancements
8. Multi-parameter Transaction Search

    - Implement query endpoints and UI filters allowing combinations of `book_id`, `person_id`, `loan_date`, `expected_return_date`, `actual_return_date`, and `status`.

9. Fix Negative Overdue Values

    - Ensure `overdue_days` is calculated as `max(0, calculated_days)` and apply corrections retroactively if needed.

10. Return Book by Book ID

     - ✔ Already implemented: support returns by `book_id` without a transaction id.

11. Return Transaction ID on Loan Creation

     - ✔ Already implemented: `create_loan()` returns the generated `transaction_id`.

### 🧠 New Suggested Enhancements
12. Automatic Author Matching & Creation

     - Build a pipeline to normalize and match author names against an `authors` table.
     - Automatically create author records when missing and support a many-to-many relationship between books and authors.
     - Integrate this with Google Books metadata parsing.

13. Scheduled Background Jobs

     - Add scheduled/background jobs for daily overdue detection, nightly price updates, and weekly borrower reports.
     - Options: a simple cron on the host, a lightweight scheduler (APScheduler), or a Celery worker for scale.

14. Full Audit Logging

     - Add an `audit_log` table to record `action_type`, `user`, `timestamp`, `old_values`, `new_values` for critical operations.
     - Make audit trails queryable and exportable for compliance and debugging.

---


## 🧱 Tech Stack

| Layer | Tool |
|-------|------|
| 🗄️ Database | MySQL |
| 🐍 Backend | Python |
| 🎨 Frontend | Streamlit |
| 🔗 Version Control | Git + GitHub |
| 📐 Diagrams | Draw.io + Mermaid |
| ⚙️ IDE | VS Code |

---

### 🗄️ Database Design (ER Diagram)

```mermaid
erDiagram
    books {
        INT book_id PK
        VARCHAR title
        VARCHAR author
        VARCHAR publisher
        VARCHAR ISBN
        VARCHAR edition
        DATE publishing_date
        DATE acquisition_date
        ENUM reading_status
        INT number_of_pages
    }

    borrowers {
        INT person_id PK
        VARCHAR first_name
        VARCHAR last_name
        VARCHAR relationship_type
        VARCHAR phone_number
        VARCHAR email
        VARCHAR address
    }

    transactions {
        INT transaction_id PK
        INT book_id FK
        INT person_id FK
        DATE loan_date
        DATE expected_return_date
        DATE actual_return_date
    }

    books ||--o{ transactions : contains
    borrowers ||--o{ transactions : borrows

