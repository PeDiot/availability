import sys

sys.path.append("../")

import json, os, pinecone
import src


LOOKBACK_MONTHS = 2
SUCCESS_RATE_THRESHOLD = 0.8


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    bq_client = src.bigquery.init_bigquery_client(credentials_dict=gcp_credentials)

    pinecone_client = pinecone.Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    query = src.bigquery.query_points_to_delete(LOOKBACK_MONTHS)
    response = src.bigquery.run_query(bq_client, query, to_list=False)
    point_ids = [row.point_id for row in response]

    success_rate = src.pinecone.delete_points(
        index=pinecone_index, ids=point_ids, loop=True
    )

    print(f"Pinecone: {success_rate:.2f}")

    if success_rate > SUCCESS_RATE_THRESHOLD:
        query = src.bigquery.delete_points(LOOKBACK_MONTHS)
        success = src.bigquery.run_query(bq_client, query, to_list=False)

        print(f"BigQuery: {success}")


if __name__ == "__main__":
    main()
