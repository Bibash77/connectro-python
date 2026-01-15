import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import BadRequestError
from config.cities import CITIES

ES_URL = "http://localhost:9200"
INDEX_PREFIX = "listings_v0_"
SEARCH_ALIAS = "listings_v0_search"
MAPPING_FILE = "config/elastic_mapping.json"

es = Elasticsearch(ES_URL)


def normalize_city(city):
    print(f"[DEBUG] raw city value: {city!r} ({type(city)})")
    return str(city).strip().lower()


def load_mapping():
    print(f"[DEBUG] loading mapping from {MAPPING_FILE}")
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    print("[DEBUG] mapping keys:", mapping.keys())
    return mapping


def ensure_index_and_alias(city, mapping):
    city_slug = normalize_city(city)
    index_name = f"{INDEX_PREFIX}{city_slug}"

    print(f"[DEBUG] computed index_name = {index_name!r}")

    try:
        exists = es.indices.exists(index=index_name)
        print(f"[DEBUG] exists({index_name}) -> {exists}")
    except BadRequestError as e:
        print("[ERROR] BadRequestError while checking index existence")
        print("        index_name =", index_name)
        print("        error      =", e)
        raise

    if not exists:
        try:
            es.indices.create(index=index_name, body=mapping)
            print(f"[CREATED] {index_name}")
        except BadRequestError as e:
            print("[ERROR] Failed to create index")
            print("        index_name =", index_name)
            print("        error      =", e)
            raise

    try:
        if not es.indices.exists_alias(name=SEARCH_ALIAS, index=index_name):
            es.indices.put_alias(index=index_name, name=SEARCH_ALIAS)
            print(f"[ALIAS ADDED] {SEARCH_ALIAS} -> {index_name}")
    except BadRequestError as e:
        print("[ERROR] Failed to add alias")
        print("        index_name =", index_name)
        print("        alias      =", SEARCH_ALIAS)
        print("        error      =", e)
        raise


def bootstrap():
    print("[DEBUG] CITIES =", CITIES)
    mapping = load_mapping()
    for city in CITIES:
        ensure_index_and_alias(city, mapping)


if __name__ == "__main__":
    bootstrap()
