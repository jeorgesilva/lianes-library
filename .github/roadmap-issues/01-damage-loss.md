# Damage & Loss Registry

Create a dedicated `book_incidents` table to register damaged, lost, or missing books.

Suggested schema:
- `id` INT PK
- `book_id` INT FK -> `books.book_id`
- `person_id` INT FK -> `borrowers.person_id` (nullable)
- `incident_type` ENUM('damaged','lost','missing')
- `incident_date` DATETIME
- `compensation_status` VARCHAR(64)
- `notes` TEXT

UI & behaviour:
- New UI section **Book Incidents** to display and filter incidents.
- Automatically synchronize `books.book_status` with incident types (e.g., set to `lost`, `damaged`, `removed`).

Suggested labels: `roadmap`, `backend`, `database`, `enhancement`
Estimated effort: 2-3 days
