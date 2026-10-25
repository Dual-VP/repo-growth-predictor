import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_repo_stats")
DB_CONN_STRING = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/repogrowth")

def init_db(conn):
    """Crée la table PostgreSQL si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS repo_metrics (
                id SERIAL PRIMARY KEY,
                repo_name VARCHAR(255) NOT NULL,
                collected_at TIMESTAMPTZ NOT NULL,
                stars INTEGER NOT NULL,
                forks INTEGER NOT NULL,
                watchers INTEGER NOT NULL,
                open_issues INTEGER NOT NULL,
                closed_issues INTEGER NOT NULL,
                issue_resolution_ratio FLOAT NOT NULL,
                commits_last_4_weeks INTEGER NOT NULL,
                contributors_count INTEGER NOT NULL,
                last_pushed_at TIMESTAMPTZ,
                is_archived BOOLEAN NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_repo_date ON repo_metrics (repo_name, collected_at);
        """)
    conn.commit()

def clean_and_validate(raw_data: dict) -> dict | None:
    """Nettoie les valeurs manquantes et calcule les métriques de base."""
    if not raw_data.get("repo_name") or not raw_data.get("timestamp"):
        return None

    # Gestion des valeurs manquantes (imputation par 0 ou valeurs par défaut)
    stars = max(0, int(raw_data.get("stars") or 0))
    forks = max(0, int(raw_data.get("forks") or 0))
    watchers = max(0, int(raw_data.get("watchers") or 0))
    open_issues = max(0, int(raw_data.get("open_issues") or 0))
    closed_issues = max(0, int(raw_data.get("closed_issues") or 0))
    
    # Si GitHub n'a pas encore calculé le cache des commits/contributeurs (None), on met 0 (ou -1 selon votre convention)
    commits_4w = int(raw_data.get("commits_last_4_weeks") or 0)
    contributors = int(raw_data.get("contributors_count") or 1)

    # Calcul d'une métrique propre : le taux de résolution des issues
    total_issues = open_issues + closed_issues
    resolution_ratio = round(closed_issues / total_issues, 4) if total_issues > 0 else 1.0

    return {
        "repo_name": raw_data["repo_name"].strip().lower(),
        "collected_at": raw_data["timestamp"],
        "stars": stars,
        "forks": forks,
        "watchers": watchers,
        "open_issues": open_issues,
        "closed_issues": closed_issues,
        "issue_resolution_ratio": resolution_ratio,
        "commits_last_4_weeks": commits_4w,
        "contributors_count": contributors,
        "last_pushed_at": raw_data.get("last_pushed_at"),
        "is_archived": bool(raw_data.get("is_archived", False))
    }

def main():
    # Connexion à PostgreSQL
    conn = psycopg2.connect(DB_CONN_STRING)
    init_db(conn)
    print("Connecté à PostgreSQL et table vérifiée.")

    # Configuration du Consumer Kafka
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    consumer_config = {
        'bootstrap_servers': [KAFKA_BROKER],
        'group_id': 'repogrowth_cleaner_group',
        'auto_offset_reset': 'earliest',
        'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
    }
    
    if security_protocol == "SASL_SSL":
        consumer_config.update({
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': '$ConnectionString',
            'sasl_plain_password': os.getenv("AZURE_EVENT_HUB_CONNECTION_STRING")
        })

    consumer = KafkaConsumer(KAFKA_TOPIC, **consumer_config)
    print(f"En écoute sur le topic Kafka '{KAFKA_TOPIC}'...")

    insert_query = """
        INSERT INTO repo_metrics (
            repo_name, collected_at, stars, forks, watchers,
            open_issues, closed_issues, issue_resolution_ratio,
            commits_last_4_weeks, contributors_count, last_pushed_at, is_archived
        ) VALUES (
            %(repo_name)s, %(collected_at)s, %(stars)s, %(forks)s, %(watchers)s,
            %(open_issues)s, %(closed_issues)s, %(issue_resolution_ratio)s,
            %(commits_last_4_weeks)s, %(contributors_count)s, %(last_pushed_at)s, %(is_archived)s
        );
    """

    for message in consumer:
        raw_data = message.value
        cleaned_data = clean_and_validate(raw_data)
        
        if cleaned_data:
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_query, cleaned_data)
                conn.commit()
                print(f"[OK] Inséré en base : {cleaned_data['repo_name']} ({cleaned_data['stars']} stars)")
            except Exception as e:
                conn.rollback()
                print(f"[ERREUR DB] Échec d'insertion pour {cleaned_data['repo_name']} : {e}")
        else:
            print(f"[IGNORÉ] Message invalide reçu : {raw_data}")

if __name__ == "__main__":
    main()