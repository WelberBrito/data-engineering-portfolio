import psycopg2
import pandas as pd

# 1. Parâmetros de conexão (os mesmos do docker-compose.yml)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "data_engineering"
DB_USER = "admin"
DB_PASS = "admin"

def connect_to_db():
    """Cria a conexão com o banco de dados PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conexão com o PostgreSQL bem-sucedida!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def create_table(conn):
    """Cria a tabela de veículos se ela não existir."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS veiculos_manutencao (
                    id INTEGER PRIMARY KEY,
                    placa VARCHAR(10) NOT NULL,
                    modelo VARCHAR(50),
                    ano INTEGER,
                    tipo_motor VARCHAR(50)
                );
            """)
            conn.commit() # Confirma a transação
            print("Tabela 'veiculos_manutencao' verificada/criada.")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
        conn.rollback() # Desfaz em caso de erro

def load_data(conn):
    """Carrega os dados do CSV para a tabela."""
    df = pd.read_csv('veiculos.csv')
    print(f"Lendo {len(df)} linhas do veiculos.csv")

    try:
        with conn.cursor() as cur:
            # Limpa a tabela antes de inserir para evitar erros de duplicidade (só para este exemplo)
            cur.execute("TRUNCATE TABLE veiculos_manutencao;")

            # Itera sobre o DataFrame e insere linha a linha
            for index, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO veiculos_manutencao (id, placa, modelo, ano, tipo_motor)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row['id'], row['placa'], row['modelo'], row['ano'], row['tipo_motor'])
                )

            conn.commit()
            print("Dados carregados com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
        conn.rollback()

# --- Execução Principal ---
if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        create_table(conn)
        load_data(conn)
        conn.close()
        print("Conexão fechada.")