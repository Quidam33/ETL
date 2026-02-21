import random
import string
from datetime import datetime, timedelta
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "etl_project"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def rand_id(prefix):
    return prefix + "_" + ''.join(random.choices(string.digits, k=5))


def generate_user_sessions(n=100):
    sessions = []
    for _ in range(n):
        start = datetime.utcnow() - timedelta(days=random.randint(0, 10))
        duration = random.randint(5, 60)

        sessions.append({
            "session_id": rand_id("sess"),
            "user_id": rand_id("user"),
            "start_time": start,
            "end_time": start + timedelta(minutes=duration),
            "pages_visited": random.sample(
                ["/home", "/products", "/cart", "/profile", "/search"],
                k=random.randint(1, 4)
            ),
            "device": random.choice(["mobile", "desktop"]),
            "actions": random.sample(
                ["login", "view_product", "add_to_cart", "logout"],
                k=random.randint(1, 4)
            )
        })
    db.UserSessions.insert_many(sessions)


def generate_support_tickets(n=50):
    tickets = []
    for _ in range(n):
        created = datetime.utcnow() - timedelta(days=random.randint(0, 10))
        updated = created + timedelta(hours=random.randint(1, 48))

        tickets.append({
            "ticket_id": rand_id("ticket"),
            "user_id": rand_id("user"),
            "status": random.choice(["open", "closed", "in_progress"]),
            "issue_type": random.choice(["payment", "delivery", "account"]),
            "messages": [],
            "created_at": created,
            "updated_at": updated
        })
    db.SupportTickets.insert_many(tickets)


if __name__ == "__main__":
    generate_user_sessions()
    generate_support_tickets()
    print("Generation finished")