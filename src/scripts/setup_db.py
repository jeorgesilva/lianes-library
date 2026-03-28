from dotenv import load_dotenv
load_dotenv() # 👈 Isto força o Python a ler o ficheiro .env IMEDIATAMENTE!

from sqlalchemy import text
from src.db.connection import get_engine

def create_tables():
    engine = get_engine()
    
    create_books_table = """
    CREATE TABLE IF NOT EXISTS books (
        book_id INT AUTO_INCREMENT PRIMARY KEY,
        ISBN VARCHAR(20),
        title VARCHAR(255) NOT NULL,
        author VARCHAR(255),
        author_id INT NULL,
        genre VARCHAR(100),
        cost_book DECIMAL(10,2),
        book_status ENUM('AVAILABLE','BORROWED','LOST','DAMAGED') DEFAULT 'AVAILABLE' NOT NULL,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    create_borrowers_table = """
    CREATE TABLE IF NOT EXISTS borrowers (
        person_id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        relationship_type VARCHAR(100),
        phone_number VARCHAR(50),
        email VARCHAR(255),
        address VARCHAR(255),
        status ENUM('ACTIVE','INACTIVE') DEFAULT 'ACTIVE',
        date_joined DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    create_transactions_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INT AUTO_INCREMENT PRIMARY KEY,
        book_id INT NOT NULL,
        person_id INT NOT NULL,
        loan_date DATE NOT NULL,
        due_date DATE NOT NULL,
        actual_return_date DATE,
        FOREIGN KEY (book_id) REFERENCES books(book_id),
        FOREIGN KEY (person_id) REFERENCES borrowers(person_id)
    );
    """

    print("⏳ Conectando à nuvem e criando tabelas...")
    with engine.begin() as conn:
        conn.execute(text(create_books_table))
        conn.execute(text(create_borrowers_table))
        conn.execute(text(create_transactions_table))
        
    print("✅ Tabelas criadas com sucesso no banco de dados da nuvem!")

if __name__ == "__main__":
    create_tables()