import sys

sys.path.append("/app")

import json, os, pinecone
import src


LOOKBACK_DAYS = 45
SUCCESS_RATE_THRESHOLD = 0.8
PINECONE_ID_FIELD = "point_id"


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    bq_client = src.bigquery.init_bigquery_client(credentials_dict=gcp_credentials)

    pinecone_client = pinecone.Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    query = src.bigquery.query_points_to_delete(LOOKBACK_DAYS)
    iterator = src.bigquery.run_query(bq_client, query, to_list=False)

    print(f"Total rows: {iterator.total_rows:,}")

    success_rate, failed = src.pinecone.delete_points_from_bigquery_iterator(
        index=pinecone_index,
        iterator=iterator,
        id_field=PINECONE_ID_FIELD,
        verbose=True,
    )

    print(f"Pinecone: {success_rate:.2f}")

    if failed:
        src.utils.save_json(failed, "failed.json")
        print(f"Failed: {len(failed)}")

    if success_rate > SUCCESS_RATE_THRESHOLD:
        query = src.bigquery.query_delete_points(LOOKBACK_DAYS)
        success_points = src.bigquery.run_query(bq_client, query, to_list=False)

        query = src.bigquery.query_delete_items(LOOKBACK_DAYS)
        success_items = src.bigquery.run_query(bq_client, query, to_list=False)

        query = src.bigquery.query_delete_sold(LOOKBACK_DAYS)
        success_sold = src.bigquery.run_query(bq_client, query, to_list=False)

        print(f"BigQuery items: {success_items}")
        print(f"BigQuery points: {success_points}")
        print(f"BigQuery sold: {success_sold}")


if __name__ == "__main__":
    main()
