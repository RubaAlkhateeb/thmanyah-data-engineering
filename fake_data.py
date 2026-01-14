import csv
import uuid
import random
import json
from datetime import datetime, timedelta, timezone

# -------- CONFIG --------
CONTENT_COUNT = 60
EVENTS_PER_CONTENT = 40   # ~2400 events
# ------------------------

CONTENT_TYPES = ["podcast", "newsletter", "video"]
EVENT_TYPES = ["play", "pause", "finish", "click"]
DEVICES = ["ios", "android", "web-chrome", "web-safari"]


def generate_content_csv():
    content_rows = []

    with open("content.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "slug", "title",
            "content_type", "length_seconds", "publish_ts"
        ])

        for i in range(1, CONTENT_COUNT + 1):
            content_id = uuid.uuid4()
            content_type = random.choice(CONTENT_TYPES)

            length_seconds = (
                random.randint(1800, 3600) if content_type == "podcast"
                else random.randint(600, 1200) if content_type == "video"
                else random.randint(200, 800)
            )

            publish_ts = (
                datetime.now(timezone.utc)
                - timedelta(days=random.randint(1, 90))
            )

            writer.writerow([
                str(content_id),
                f"content-{i:03d}",
                f"Content Title {i:03d}",
                content_type,
                length_seconds,
                publish_ts.isoformat().replace("+00:00", "Z")
            ])

            content_rows.append((content_id, length_seconds))

    return content_rows


def generate_engagement_csv(content_rows):
    with open("engagement_events.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "content_id", "user_id", "event_type",
            "event_ts", "duration_ms", "device", "raw_payload"
        ])

        for content_id, length_seconds in content_rows:
            for _ in range(EVENTS_PER_CONTENT):
                event_type = random.choice(EVENT_TYPES)

                event_ts = (
                    datetime.now(timezone.utc)
                    - timedelta(minutes=random.randint(0, 60))
                )

                duration_ms = None
                if event_type != "click" and random.random() > 0.2:
                    duration_ms = random.randint(
                        1000,
                        length_seconds * 1000
                    )

                # ✅ Approved, realistic payload
                raw_payload = {
                    "country": "SA",
                    "city": "Riyadh",
                    "language": "ar",
                    "user_type": "registered",
                    "is_subscribed": True,
                    "trial_user": False,
                    "autoplay": False,
                    "is_background_play": random.choice([True, False])
                }

                writer.writerow([
                    str(content_id),
                    str(uuid.uuid4()),
                    event_type,
                    event_ts.isoformat().replace("+00:00", "Z"),
                    duration_ms,
                    random.choice(DEVICES),
                    json.dumps(raw_payload)
                ])


def main():
    content_rows = generate_content_csv()
    generate_engagement_csv(content_rows)

    print("✅ CSV files generated:")
    print(" - content.csv")
    print(" - engagement_events.csv")


if __name__ == "__main__":
    main()