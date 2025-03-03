from typing import List, Tuple, Optional, Dict, Union
from datetime import datetime
import random

import tqdm
from pinecone import Pinecone
from google.cloud import bigquery

import src


DOMAIN = "fr"
DRIVER_RESTART_EVERY = 500
UPDATE_EVERY = 200


class Runner:
    def __init__(
        self,
        secrets: Dict,
        use_api: bool,
        from_interactions: bool,
        top_brands_alpha: Optional[float] = 0.0,
        sort_by_likes_alpha: Optional[float] = 0.0,
        sort_by_date_alpha: Optional[float] = 0.0,
        domain: str = DOMAIN,
        driver_restart_every: int = DRIVER_RESTART_EVERY,
        update_every: int = UPDATE_EVERY,
    ):
        self.use_api = use_api
        self.domain = domain

        self.from_interactions = from_interactions
        self.top_brands_alpha = top_brands_alpha
        self.sort_by_likes_alpha = sort_by_likes_alpha
        self.sort_by_date_alpha = sort_by_date_alpha

        self.driver_restart_every = driver_restart_every
        self.update_every = update_every

        self._init_clients(secrets)
        self._init_config()

    def run(
        self,
        data_loader: Union[bigquery.table.RowIterator, src.models.PineconeDataLoader],
    ) -> None:
        item_ids, vinted_ids, point_ids = [], [], []
        n, n_success, n_available, n_unavailable, n_updated = 0, 0, 0, 0, 0
        loop = tqdm.tqdm(iterable=data_loader, total=data_loader.total_rows)

        for row in loop:
            n += 1

            (
                vinted_ids,
                item_ids,
                point_ids,
                n_available,
                n_unavailable,
                n_success,
                restart_driver,
            ) = self._process_item(
                row,
                vinted_ids,
                item_ids,
                point_ids,
                n_available,
                n_unavailable,
                n_success,
            )

            if (self.driver and n % DRIVER_RESTART_EVERY == 0) or restart_driver:
                self.restart_driver()

            if self._check_update(n, data_loader, item_ids, vinted_ids):
                success = self._update(item_ids, vinted_ids, point_ids)

                if success:
                    n_updated += len(item_ids)

                item_ids, vinted_ids, point_ids = [], [], []

            loop.set_description(
                f"Processed: {n} | "
                f"Success: {n_success} | "
                f"Success rate: {n_success / n:.2f} | "
                f"Available: {n_available} | "
                f"Unavailable: {n_unavailable} | "
                f"Updated: {n_updated}"
            )

    def restart_driver(self):
        self.driver.quit()
        self.driver = src.driver.init_webdriver()
        src.driver.gaussian_sleep(self.driver)

    def _check_update(
        self,
        n: int,
        data_loader: Union[bigquery.table.RowIterator, src.models.PineconeDataLoader],
        item_ids: List[str],
        vinted_ids: List[str],
    ) -> bool:
        first_condition = n % self.update_every == 0 or n == data_loader.total_rows
        second_condition = item_ids and len(item_ids) == len(vinted_ids)

        return first_condition and second_condition

    def _update(
        self, item_ids: List[str], vinted_ids: List[str], point_ids: List[str]
    ) -> bool:
        current_time = datetime.now().isoformat()

        if len(point_ids) == 0:
            pinecone_points_query = src.bigquery.query_pinecone_points(item_ids)
            loader = src.bigquery.run_query(
                self.bq_client, pinecone_points_query, to_list=False
            )

            if loader.total_rows == 0:
                return False

            point_ids = [row.point_id for row in loader]

        if src.pinecone.delete_points(self.pinecone_index, point_ids):
            try:
                rows = [
                    {"vinted_id": vinted_id, "updated_at": current_time}
                    for vinted_id in vinted_ids
                ]

                errors = self.bq_client.insert_rows_json(
                    table=f"{src.enums.VINTED_DATASET_ID}.{src.enums.SOLD_TABLE_ID}",
                    json_rows=rows,
                )
                return not errors

            except:
                return False

        return False

    def _process_item(
        self,
        row: Union[bigquery.Row, src.models.PineconeEntry],
        vinted_ids: List[str],
        item_ids: List[str],
        point_ids: List[str],
        n_available: int,
        n_unavailable: int,
        n_success: int,
    ) -> Tuple[
        List[str],
        List[str],
        List[str],
        int,
        int,
        int,
        bool,
    ]:
        restart_driver = False
        status = src.status.get_item_status_from_web(row.url, self.driver)

        if status in [src.models.ItemStatus.NOT_FOUND, src.models.ItemStatus.UNKNOWN]:
            status = src.status.get_item_status_from_api(
                self.vinted_client, int(row.vinted_id)
            )

        restart_driver = status == src.models.ItemStatus.UNKNOWN
        is_available = src.status.is_available(status)

        if is_available is None:
            pass
        elif is_available:
            n_available += 1
            n_success += 1
        else:
            n_unavailable += 1
            n_success += 1
            item_ids.append(row.id)
            vinted_ids.append(row.vinted_id)

            if isinstance(row, src.models.PineconeEntry):
                point_ids.append(row.point_id)

        return (
            vinted_ids,
            item_ids,
            point_ids,
            n_available,
            n_unavailable,
            n_success,
            restart_driver,
        )

    def _init_clients(self, secrets: Dict) -> None:
        gcp_credentials = secrets.get("GCP_CREDENTIALS")
        self.bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

        pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
        self.pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

        if self.use_api:
            self.vinted_client = src.vinted.client.Vinted(domain=self.domain)
            self.driver = None
        else:
            self.driver = src.driver.init_webdriver()
            self.vinted_client = None

    def _init_config(self):
        only_top_brands = random.random() < self.top_brands_alpha
        sort_by_likes = random.random() < self.sort_by_likes_alpha
        sort_by_date = random.random() < self.sort_by_date_alpha

        if self.from_interactions:
            job_id = "interactions"
        elif only_top_brands:
            job_id = "top_brands"
        elif sort_by_likes:
            job_id = "likes"
        elif sort_by_date:
            job_id = "date"
        else:
            job_id = "all"

        index = src.bigquery.get_job_index(self.bq_client, job_id)

        self.config = src.models.JobConfig(
            id=job_id,
            index=index,
            only_top_brands=only_top_brands,
            sort_by_likes=sort_by_likes,
            sort_by_date=sort_by_date,
            from_interactions=self.from_interactions,
        )
