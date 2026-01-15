import csv
from datetime import datetime
from elasticsearch import Elasticsearch

ES_URL = "http://localhost:9200"
INDEX_PREFIX = "listings_v0_"
CSV_FILE = "data/listings_kathmandu.csv"

es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=30,
    max_retries=3,
    retry_on_timeout=True
)



def build_phones(phone_numbers, masked_numbers, primary_index):
    numbers = phone_numbers.split("|")
    masked = masked_numbers.split("|")
    primary_index = int(primary_index)

    phones = []
    for i in range(len(numbers)):
        phones.append({
            "number": numbers[i],
            "masked_number": masked[i],
            "primary": i == primary_index
        })

    return phones


def ingest():
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            city = row["city"]
            index_name = f"{INDEX_PREFIX}{city}"

            document = {
                "listing_id": row["listing_id"],
                "name": row["name"],
                "category": row["category"],
                "phones": build_phones(
                    row["phone_numbers"],
                    row["masked_phone_numbers"],
                    row["primary_phone_index"]
                ),
                "location": {
                    "lat": float(row["latitude"]),
                    "lon": float(row["longitude"])
                },
                "city": city,
                "confidence_score": float(row["confidence_score"]),
                "source": row["source"],
                "updated_at": row["updated_at"]
            }

            es.index(
                index=index_name,
                id=row["listing_id"],
                document=document
            )

            print(f"[INDEXED] {row['listing_id']}")


if __name__ == "__main__":
    ingest()
