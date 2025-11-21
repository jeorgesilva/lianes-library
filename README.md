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

