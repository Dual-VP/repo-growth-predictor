import os
import json
import time

import psycopg2
from datetime import datetime, timedelta, timezone
from github import Github, Auth, RateLimitExceededException, GithubException
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_repo_stats")
DB_CONN_STRING = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/repogrowth")

# Nombre de cibles de dépôts à suivre quotidiennement
TARGET_COHORT_SIZE = int(os.getenv("TARGET_COHORT_SIZE", "90"))

def get_kafka_producer():
    """
    Initialise le producer Kafka (compatible local et Azure Event Hubs.
    :return: L'instance du producer Kafka.
    """
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    config = {
        'bootstrap_servers': [KAFKA_BROKER],
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    }
    if security_protocol == "SASL_SSL":
        config.update({
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': '$ConnectionString',
            'sasl_plain_password': os.getenv("AZURE_EVENT_HUBS_CONNECTION_STRING"),
        })
    return KafkaProducer(**config)


def get_tracked_repos_from_db() -> set:
    """
    Récupère la liste des dépôts déjà suivis dans PostgreSQL pour garder un historique des dépôts suivis.
    :return: Liste des dépôts déjà suivis.
    """
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        with conn.cursor() as cur:
            # Vérifie si la table existe déjà
            cur.execute("""
                SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'repo_metrics'
                );
            """)
            if not cur.fetchone()[0]:
                return set()  # La table n'existe pas encore, donc aucun dépôt suivi

            cur.execute("SELECT DISTINCT repo_name FROM repo_metrics;")
            repos = {row[0] for row in cur.fetchall()}
        conn.close()
        return repos
    except Exception as e:
        print(f"[Info] Impossible de lire la base existante (premier lancement ?) : {e}")
        return set()


def discover_repositories(g: Github, needed_count: int, existing_repos: set) -> list:
    """
    Découvre automatiquement de nouveaux dépôts via l'API Search de GitHub
    en stratifiant selon 3 profils (Croissance, Maintenu, Stagnant/Abandonné).
    :param g: L'instance de Github pour interagir avec l'API.
    :param needed_count: Le nombre de dépôts à découvrir pour atteindre la taille cible.
    :param existing_repos: La liste des dépôts déjà suivis.
    :return: Liste des dépôts découverts.
    """
    discovered = set()
    quota_per_categiry = max(1, needed_count // 3) # Répartir le quota entre les trois catégories

    now = datetime.now(timezone.utc)
    six_months_ago = (now - timedelta(days=180)).strftime("%Y-%m-%d")
    one_year_ago = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    recent_push = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    # Définition des 3 requêtes de découverte pour équilibrer le futur dataset ML
    discovery_queries = [
        # 1. Profil "Forte croissance / Viral" : dépôts créés récemment, déjà populaire
        {
            "label": "croissance_rapide",
            "query": f"created:>{six_months_ago} stars:100..5000",
            "sort": "stars",
            "order": "desc",
        },
        # 2. Profil "Maintenu / Régulier" : Ancienneté > 1 an, actif cette semaine, taille moyenne
        {
            "label": "maintenu",
            "query": f"created:<{one_year_ago} pushed:>{recent_push} stars:100..5000",
            "sort": "stars",
            "order": "desc",
        },
        # 3. Profil "Stagnant / Abandonné" : Ancienneté > 1 an, inactif depuis longtemps, taille faible
        {
            "label": "stagnant",
            "query": f"created:<{one_year_ago} pushed:>{recent_push} stars:200..3000 is:public",
            "sort": "updated",
            "order": "desc",
        },
        # 3. Profil "Stagnant / Abandon potentiel" : Pas de push depuis 6 à 12 mois malgré des stars
        {
            "label": "stagnant_ou_abandon",
            "query": f"pushed:{one_year_ago}..{six_months_ago} stars:50..1500 is:public",
            "sort": "updated",
            "order": "desc",
        }
    ]

    for category in discovery_queries:
        print(f"[Info] Découverte automatique - Catégorie: '{category['label']}' (Requête: {category['query']})...")
        try:
            results = g.search_repositories(
                query=category["query"],
                sort=category["sort"],
                order=category["order"],
            )

            count = 0
            for repo in results:
                repo_full_name = repo.full_name.lower()
                if repo_full_name not in existing_repos and repo_full_name not in discovered:
                    discovered.add(repo_full_name)
                    count += 1
                    if count >= quota_per_categiry:
                        break
            time.sleep(3) # Respct du rate limit de la Search API
        except Exception as e:
            print(f"[Erreur] Échec de la découverte automatique pour la catégorie '{category['label']}': {e}")

    return list(discovered)


def get_commits_last_4_weeks(repo) -> int:
    """
    Récupère les statistiques de participation en gérant le code HTTP 202 de GitHub
    (lorsque GitHub calcule le cache en arrière-plan au premier appel).
    :param repo: Le dépôt GitHub pour lequel récupérer les statistiques de participation.
    :return: Le nombre de commits sur les 4 dernières semaines.
    """
    for attempt in range(3):
        stats = repo.get_stats_participation()
        if stats is not None and stats.all:
            return sum(stats.all[-4:])  # Somme des commits sur les 4 dernières semaines
        time.sleep(2) # Attente que GitHub génère le cache statistique
    return 0


def fetch_repo_metrics(g: Github, repo_name: str) -> dict:
    """
    Extrait les métriques clés d'un dépôt GitHub via PyGithub.
    :param g: L'instance de Github pour interagir avec l'API.
    :param repo_name: Le nom du dépôt
    :return: Un dictionnaire contenant les métriques clés du dépôt.
    """
    repo = g.get_repo(repo_name)

    # 1. Issues ouvertes et fermées via Search API
    closed_issues_count = g.search_issues(query=f"repo:{repo_name} is:issue is:closed").totalCount
    open_issues_count = g.search_issues(query=f"repo:{repo_name} is:issue is:open").totalCount

    # 2. Fréquence des commits sur les 4 dernières semaines (avec retry sur le cache GitHub)
    recent_commits = get_commits_last_4_weeks(repo)

    # 3. Le nombre de contributeurs uniques (via l'API des contributeurs)
    try:
        contributors_count = repo.get_contributors(anon="true").totalCount
    except GithubException as e:
        print(f"[Erreur] Impossible de récupérer le nombre de contributeurs pour {repo_name}: {e}")
        contributors_count = 1

    return {
        "repo_name": repo.full_name.lower(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stars": repo.stargazers_count,
        "forks": repo.forks_count,
        "watchers": repo.subscribers_count,
        "open_issues": open_issues_count,
        "closed_issues": closed_issues_count,
        "commits_last_4_weeks": recent_commits,
        "contributors_count": contributors_count,
        "last_pushed_at": repo.pushed_at.isoformat() if repo.pushed_at else None,
        "is_archived": repo.archived,
    }

def main():
    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth, retry=3)
    producer = get_kafka_producer()

    # 1. Récupération des dépôts déjà suivis en base
    tracked_repos = get_tracked_repos_from_db()
    print(f"[Info] Dépôts déjà suivis en base : {len(tracked_repos)}")

    # 2. Découverte automatique si le quota n'est pas atteint
    if len(tracked_repos) < TARGET_COHORT_SIZE:
        missing_count = TARGET_COHORT_SIZE - len(tracked_repos)
        print(f"[Info] Lancement de la découverte automatique pour {missing_count} dépôts supplémentaires...")
        new_repos = discover_repositories(g, missing_count, tracked_repos)
        print(f"[Info] {len(new_repos)} nouveaux dépôts découverts !")
        tracked_repos.update(new_repos)

    # 3. Collecte quotidienne et envoi dans Kafka
    print(f"[Info] Démarrage de l'extraction des métriques pour {len(tracked_repos)} dépôts...")
    for idx, repo_name in enumerate(tracked_repos, 1):
        try:
            print(f"[Info] ({idx}/{len(tracked_repos)}) Extraction des métriques pour {repo_name}...")
            data = fetch_repo_metrics(g, repo_name)

            producer.send(KAFKA_TOPIC, value=data)

            # Attention : L'API Search (utilisé 2 fois par dépôt pour les issues) est limitée à 30 requêtes par minute.
            # 2 requêtes d'issues = maximum 15 dépôts par minute -> pause de 4.5 secondes.
            time.sleep(4.5)

        except RateLimitExceededException:
            reset_time = g.get_rate_limit().search.reset
            sleep_duration = max((reset_time - datetime.now(timezone.utc)).total_seconds() + 5, 60)
            print(f"[Info] Rate limit atteint. Attente de {sleep_duration} secondes...")
            time.sleep(sleep_duration)
        except Exception as e:
            print(f"[Erreur] Échec de l'extraction pour {repo_name}: {e}")

    producer.flush()
    producer.close()
    print("[Info] Pipeline d'ingestion des métriques GitHub terminé avec succès !")

if __name__ == "__main__":
    main()