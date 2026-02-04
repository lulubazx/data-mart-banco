import os
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÃO ---
# MUDANÇA CRUCIAL: Porta alterada para 5433 para fugir de conflitos do Windows
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_DB = os.getenv("POSTGRES_DB")
PG_SSLMODE = os.getenv("POSTGRES_SSLMODE")
PG_DRIVER = os.getenv("POSTGRES_DRIVER", "pg8000")

if not all([PG_USER, PG_PASSWORD, PG_DB]):
    raise RuntimeError("Missing Postgres env vars: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB")

DB_CONNECTION = f"postgresql+{PG_DRIVER}://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
if PG_SSLMODE and PG_DRIVER == "psycopg2":
    DB_CONNECTION = f"{DB_CONNECTION}?sslmode={PG_SSLMODE}"

def reset_database():
    print("🧹 Iniciando criação do banco de dados...")
    print(f"   -> Conectando em: {DB_CONNECTION.split('@')[1]}") 
    
    try:
        engine = create_engine(DB_CONNECTION)
        
        with engine.connect() as conn:
            print("   -> Conexão aceita! Recriando tabelas...")
            
            # 1. Criar tabelas
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS loans (
                    id SERIAL PRIMARY KEY,
                    user_id INT,
                    amount DECIMAL(10,2),
                    status VARCHAR(20),
                    interest_rate DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS investments (
                    id SERIAL PRIMARY KEY,
                    user_id INT,
                    type VARCHAR(20),
                    amount DECIMAL(10,2),
                    date_invested DATE
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS card_transactions (
                    id SERIAL PRIMARY KEY,
                    user_id INT,
                    amount DECIMAL(10,2),
                    category VARCHAR(50),
                    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # 2. Limpar dados
            conn.execute(text("TRUNCATE TABLE card_transactions, investments, loans, users;"))

            # 3. Inserir dados
            print("   -> Inserindo dados...")
            
            # Users
            conn.execute(text("INSERT INTO users (name, email) VALUES ('João Silva', 'joao@email.com')"))
            conn.execute(text("INSERT INTO users (name, email) VALUES ('Maria Souza', 'maria@email.com')"))
            conn.execute(text("INSERT INTO users (name, email) VALUES ('Carlos Pereira', 'carlos@email.com')"))

            # Loans
            conn.execute(text("INSERT INTO loans (user_id, amount, status, interest_rate) VALUES (1, 5000.00, 'active', 2.5)"))
            conn.execute(text("INSERT INTO loans (user_id, amount, status, interest_rate) VALUES (2, 10000.00, 'defaulted', 3.0)"))
            conn.execute(text("INSERT INTO loans (user_id, amount, status, interest_rate) VALUES (3, 2000.00, 'paid', 1.5)"))

            # Investments
            conn.execute(text("INSERT INTO investments (user_id, type, amount, date_invested) VALUES (1, 'CDB', 1000.00, '2023-01-10')"))
            conn.execute(text("INSERT INTO investments (user_id, type, amount, date_invested) VALUES (1, 'TESOURO', 500.00, '2023-02-15')"))
            conn.execute(text("INSERT INTO investments (user_id, type, amount, date_invested) VALUES (2, 'FUNDO', 15000.00, '2023-03-01')"))

            # Cards
            conn.execute(text("INSERT INTO card_transactions (user_id, amount, category, transaction_date) VALUES (1, 50.00, 'Alimentação', NOW())"))
            conn.execute(text("INSERT INTO card_transactions (user_id, amount, category, transaction_date) VALUES (1, 120.00, 'Transporte', NOW())"))
            conn.execute(text("INSERT INTO card_transactions (user_id, amount, category, transaction_date) VALUES (2, 5000.00, 'Viagem', NOW())"))
            conn.execute(text("INSERT INTO card_transactions (user_id, amount, category, transaction_date) VALUES (3, 15.00, 'Alimentação', NOW())"))
            
            conn.commit()

        print("✅ Banco de dados recriado e populado com sucesso!")

    except Exception as e:
        print(f"\n❌ ERRO: {e}")

if __name__ == "__main__":
    reset_database()
