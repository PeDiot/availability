from typing import List, Tuple, Union, Optional
from datetime import datetime

import tqdm
from google.cloud import bigquery

import src
from src.models import RunnerMode, JobConfig


DOMAIN = "fr"
DRIVER_RESTART_EVERY = 500
UPDATE_EVERY = 100
SUCCESS_RATE_THRESHOLD = 0.8


class Runner:
    def __init__(
        self,
        mode: RunnerMode,
        config: JobConfig,
    ):
        self.mode = mode
        self.config = config

        self.driver_restart_every = DRIVER_RESTART_EVERY
        self.update_every = UPDATE_EVERY

    def run(
        self,
        data_loader: Union[bigquery.table.RowIterator, src.models.PineconeDataLoader],
        loop: Optional[tqdm.tqdm] = None,
    ) -> None:
        item_ids, vinted_ids, point_ids = [], [], []
        n, n_success, n_available, n_unavailable, n_updated = 0, 0, 0, 0, 0

        if loop is None:
            iterator = tqdm.tqdm(iterable=data_loader, total=data_loader.total_rows)
        else:
            iterator = data_loader

        for entry in iterator:
            n += 1

            if not isinstance(entry, src.models.PineconeEntry):
                entry = src.models.PineconeEntry.from_dict(dict(entry))

            (
                vinted_ids,
                item_ids,
                point_ids,
                n_available,
                n_unavailable,
                n_success,
            ) = self._process_entry(
                entry,
                vinted_ids,
                item_ids,
                point_ids,
                n_available,
                n_unavailable,
                n_success,
            )

            if self._check_update(n, data_loader, item_ids, vinted_ids):
                success = self._update(item_ids, vinted_ids, point_ids)

                if success:
                    n_updated += len(item_ids)

                item_ids, vinted_ids, point_ids = [], [], []

            info = (
                f"Processed: {n} | "
                f"Success: {n_success} | "
                f"Success rate: {n_success / n:.2f} | "
                f"Available: {n_available} | "
                f"Unavailable: {n_unavailable} | "
                f"Updated: {n_updated}"
            )

            if loop is not None:
                loop.set_description(info)
            else:
                iterator.set_description(info)

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
                self.config.bq_client, pinecone_points_query, to_list=False
            )

            if loader.total_rows == 0:
                return False

            point_ids = [row.point_id for row in loader]

        if self.config.supabase_client:
            success = src.supabase.set_items_unavailable(
                self.config.supabase_client, item_ids
            )

        success_rate, failed = src.pinecone.delete_points_from_ids(
            index=self.config.pinecone_index, ids=point_ids, verbose=False
        )

        if success_rate > SUCCESS_RATE_THRESHOLD:
            try:
                rows = [
                    {"vinted_id": vinted_id, "updated_at": current_time}
                    for vinted_id in vinted_ids
                ]

                errors = self.config.bq_client.insert_rows_json(
                    table=f"{src.enums.VINTED_DATASET_ID}.{src.enums.SOLD_TABLE_ID}",
                    json_rows=rows,
                )
                return not errors

            except:
                return False

        return False

    def _process_entry(
        self,
        entry: src.models.PineconeEntry,
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
    ]:
        status = self._get_status(entry)
        is_available = src.status.is_available(status)
        success = status != src.models.ItemStatus.UNKNOWN

        n_available += int(is_available)
        n_success += int(success)

        if not is_available:
            n_unavailable += 1

            item_ids.append(entry.id)
            vinted_ids.append(entry.vinted_id)
            point_ids.append(entry.point_id)

        return (
            vinted_ids,
            item_ids,
            point_ids,
            n_available,
            n_unavailable,
            n_success,
        )

    def _get_status(self, entry: src.models.PineconeEntry) -> src.models.ItemStatus:
        if self.mode == "api":
            status = src.status.get_status_api(
                self.config.vinted_client, int(entry.vinted_id)
            )

            if status in [
                src.models.ItemStatus.UNKNOWN,
                src.models.ItemStatus.NOT_FOUND,
            ]:
                status = src.status.get_status_web(entry.url)

            if status == src.models.ItemStatus.UNKNOWN:
                self._switch_mode("driver")

                return self._get_status(entry)

        else:
            status = src.status.get_status_web(entry.url, self.config.driver)
            switch_mode = status == src.models.ItemStatus.UNKNOWN

            if status in [
                src.models.ItemStatus.UNKNOWN,
                src.models.ItemStatus.NOT_FOUND,
            ]:
                status = src.status.get_status_api(
                    client=self.config.vinted_client,
                    item_id=entry.vinted_id,
                )

            if switch_mode:
                self._switch_mode("api")

                return self._get_status(entry)

        return status

    def _switch_mode(self, new_mode: RunnerMode) -> None:
        self.mode = new_mode

        if new_mode == "api":
            self._quit_driver(restart=False)
        else:
            self._quit_driver(restart=True)

    def _quit_driver(self, restart: bool = False):
        if self.config.driver:
            self.config.driver.quit()
            self.config.driver = None

        if restart:
            self.config.driver = src.driver.init_webdriver()
