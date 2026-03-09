import argparse
import random
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

fake = Faker()


def random_timestamp():
    base = datetime.utcnow() - timedelta(days=60)
    return base + timedelta(minutes=random.randint(0, 60 * 24 * 60))


def generate_user_sessions(n):
    sessions = []

    urls = ["/dashboard", "/library", "/favorites", "/billing", "/notifications"]
    events = ["signin_attempt", "item_preview", "wishlist_add", "payment_submit", "signout"]
    devices = ["android", "windows_pc", "macbook"]

    for _ in range(n):
        start = random_timestamp()
        end = start + timedelta(minutes=random.randint(15, 300))

        sessions.append(
            {
                "session_id": f"sess_{uuid.uuid4().hex[:10]}",
                "user_id": f"user_{random.randint(5, 120)}",
                "start_time": start,
                "end_time": end,
                "pages_visited": random.sample(
                    urls,
                    k=random.randint(1, len(urls)),
                ),
                "device": random.choice(devices),
                "actions": random.sample(
                    events,
                    k=random.randint(1, len(events)),
                ),
            }
        )

    return sessions


def generate_event_logs(n):
    events_list = []

    types = ["hover", "expand_view", "transaction", "system_error"]

    for _ in range(n):
        events_list.append(
            {
                "event_id": f"evt_{uuid.uuid4().hex[:10]}",
                "timestamp": random_timestamp(),
                "event_type": random.choice(types),
                "details": {
                    "page": random.choice(["/dashboard", "/library", "/favorites"]),
                    "product_id": f"prod_{random.randint(25, 750)}",
                },
            }
        )

    return events_list


def generate_support_tickets(n):
    tickets = []

    states = ["pending_review", "completed", "escalated"]
    categories = ["subscription_issue", "delivery_delay", "account_problem"]

    for _ in range(n):
        created = random_timestamp()
        updated = created + timedelta(hours=random.randint(3, 120))

        tickets.append(
            {
                "ticket_id": f"ticket_{uuid.uuid4().hex[:8]}",
                "user_id": f"user_{random.randint(5, 120)}",
                "status": random.choice(states),
                "issue_type": random.choice(categories),
                "messages": [
                    {
                        "sender": "user",
                        "message": fake.text(max_nb_chars=70),
                        "timestamp": created,
                    },
                    {
                        "sender": "support",
                        "message": fake.text(max_nb_chars=90),
                        "timestamp": updated,
                    },
                ],
                "created_at": created,
                "updated_at": updated,
            }
        )

    return tickets


def generate_user_recommendations(n):
    recs = []

    for user_id in range(10, 10 + n):
        recs.append(
            {
                "user_id": f"user_{user_id}",
                "recommended_products": [
                    f"prod_{random.randint(40, 900)}" for _ in range(random.randint(3, 8))
                ],
                "last_updated": random_timestamp(),
            }
        )

    return recs


def generate_moderation_queue(n):
    reviews = []

    moderation = ["under_review", "approved_auto", "blocked_manual"]
    flags_pool = ["embedded_media", "suspicious_pattern", "policy_violation"]

    for _ in range(n):
        reviews.append(
            {
                "review_id": f"rev_{uuid.uuid4().hex[:8]}",
                "user_id": f"user_{random.randint(5, 120)}",
                "product_id": f"prod_{random.randint(40, 900)}",
                "review_text": fake.text(max_nb_chars=110),
                "rating": random.randint(2, 5),
                "moderation_status": random.choice(moderation),
                "flags": random.sample(
                    flags_pool,
                    k=random.randint(0, len(flags_pool)),
                ),
                "submitted_at": random_timestamp(),
            }
        )

    return reviews


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--uri", default="mongodb://airflow:airflow@mongodb:27017/?authSource=admin")
    parser.add_argument("--db", default="airflow")
    parser.add_argument("--sessions", type=int, default=130)
    parser.add_argument("--events", type=int, default=520)
    parser.add_argument("--tickets", type=int, default=85)
    parser.add_argument("--recommendations", type=int, default=70)
    parser.add_argument("--reviews", type=int, default=110)

    return parser.parse_args()


def main():
    args = parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]
    db["UserSessions"].insert_many(generate_user_sessions(args.sessions))
    db["EventLogs"].insert_many(generate_event_logs(args.events))
    db["SupportTickets"].insert_many(generate_support_tickets(args.tickets))
    db["UserRecommendations"].insert_many(generate_user_recommendations(args.recommendations))
    db["ModerationQueue"].insert_many(generate_moderation_queue(args.reviews))


if __name__ == "__main__":
    main()