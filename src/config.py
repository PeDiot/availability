from typing import Dict, Tuple, Optional

import random
from google.cloud import bigquery
from pinecone import Pinecone
from supabase import Client
from selenium.webdriver.chrome.webdriver import WebDriver

from .models import RunnerMode, JobConfig
from .vinted.client import Vinted
from .bigquery import init_bigquery_client
from .driver import init_webdriver
from .supabase import init_supabase_client
from .enums import PINECONE_INDEX_NAME


def init_clients(
    secrets: Dict, mode: RunnerMode = "api", with_supabase: bool = False
) -> Tuple[
    bigquery.Client,
    Pinecone.Index,
    Vinted,
    Optional[WebDriver],
    Optional[Client],
]:
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    bq_client = init_bigquery_client(credentials_dict=gcp_credentials)

    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(PINECONE_INDEX_NAME)

    vinted_client = Vinted()

    if mode == "api":
        driver = None
    else:
        driver = init_webdriver()

    if with_supabase:
        supabase_client = init_supabase_client(
            secrets.get("SUPABASE_URL"), secrets.get("SUPABASE_SERVICE_ROLE_KEY")
        )
    else:
        supabase_client = None

    return bq_client, pinecone_index, vinted_client, driver, supabase_client


def init_config(
    bq_client: bigquery.Client,
    pinecone_index: Pinecone.Index,
    vinted_client: Vinted,
    driver: Optional[WebDriver] = None,
    supabase_client: Optional[Client] = None,
    top_brands_alpha: float = 0.5,
    vintage_dressing_alpha: float = 0.0,
    sort_by_likes_alpha: float = 0.0,
    sort_by_date_alpha: float = 0.0,
    is_women_alpha: float = 0.0,
    from_interactions: bool = False,
    from_saved: bool = False,
) -> JobConfig:
    if from_saved:
        if not supabase_client:
            raise ValueError("Supabase client is required for from_saved mode")

        if from_interactions:
            raise ValueError("from_interactions is not supported for from_saved mode")

    only_top_brands = random.random() < top_brands_alpha
    only_vintage_dressing = random.random() < vintage_dressing_alpha
    sort_by_likes = random.random() < sort_by_likes_alpha
    sort_by_date = random.random() < sort_by_date_alpha
    is_women = random.random() < is_women_alpha

    return JobConfig(
        bq_client=bq_client,
        supabase_client=supabase_client,
        pinecone_index=pinecone_index,
        vinted_client=vinted_client,
        driver=driver,
        only_top_brands=only_top_brands,
        only_vintage_dressing=only_vintage_dressing,
        sort_by_likes=sort_by_likes,
        sort_by_date=sort_by_date,
        from_interactions=from_interactions,
        from_saved=from_saved,
        is_women=is_women,
    )
