import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from dateutil.relativedelta import relativedelta

from aws.s3_client import S3Client
from port.client import PortClient
from port.entities import build_cost_entity

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AWSCostReportHandler:
    def __init__(self, config):
        self.config = config
        self.s3_client = S3Client()
        self.port_client = PortClient(client_id=config['port_client_id'], client_secret=config['port_client_secret'],
                                      user_agent='', api_url=config['port_base_url'])

    def handle(self):
        self._upsert_cost_entities()
        logger.info(f'Done upsert new entities')
        self._delete_old_cost_entities()

    def _upsert_cost_entities(self):
        cost_report_files = self.s3_client.get_files(bucket_name=self.config['aws_bucket_name'],
                                                     file_key_prefix=self.config['aws_cost_report_s3_path_prefix'])
        for cost_report_file in cost_report_files:
            logger.info(f'Handle cost report file: {cost_report_file.key}')
            cost_report_records = self.s3_client.get_csv_gzipped_file_lines(cost_report_file)
            logger.info(f'Start aggregating file by resource id and bill start date')
            aggregated_report = AWSCostReportHandler._aggregate_cost_report(cost_report_records)
            logger.info(f'Done aggregating file, start constructing entities to upsert')
            port_entities = build_cost_entity(report_data=aggregated_report, blueprint=self.config['port_blueprint'])
            with ThreadPoolExecutor(max_workers=self.config['port_max_workers']) as executor:
                executor.map(self.port_client.upsert_entity, port_entities)

    def _delete_old_cost_entities(self):
        max_date_to_keep = (datetime.utcnow() +
                            relativedelta(days=-1, months=-self.config['port_months_to_keep'])).isoformat("T") + "Z"
        logger.info(f'Searching for entities to delete with bill start date before: {max_date_to_keep}')
        query = {
            "combinator": "and",
            "rules": [{"property": "$blueprint", "operator": "=", "value": self.config['port_blueprint']},
                      {"operator": "between", "property": "bill_start_date",
                       "value": {"from": "1970-01-01T00:00:00Z", "to": max_date_to_keep}}],
        }
        entities_to_delete = self.port_client.search_entities(query)
        logger.info(f'Found {len(entities_to_delete)} entities to delete')
        with ThreadPoolExecutor(max_workers=self.config['port_max_workers']) as executor:
            executor.map(self.port_client.delete_entity, entities_to_delete)

    @staticmethod
    def _aggregate_cost_report(records):
        report_data = defaultdict(list)
        headers = next(records)
        headers_count = len(headers)
        for line in records:
            obj = {}
            for count in range(0, headers_count):
                obj[headers[count]] = line[count]
            key = AWSCostReportHandler._build_aggregated_key(obj)
            if not key:
                continue
            report_data[key].append(obj)

        return report_data


    @staticmethod
    def _build_aggregated_key(obj):
        key_parts = []
        resource_id = obj.get("lineItem/ResourceId", "")
        usage_account_id = obj.get("lineItem/UsageAccountId", "")
        bill_start_date = obj.get("bill/BillingPeriodStartDate", "")
        if resource_id:
            if usage_account_id not in resource_id:
                key_parts.append(usage_account_id)
            key_parts.append(resource_id)
            key_parts.append(bill_start_date)
        else:
            key_parts = [usage_account_id, obj.get("lineItem/LineItemType", ""),
                         obj.get("lineItem/ProductCode", ""), obj.get("lineItem/UsageType", ""),
                         obj.get("lineItem/Operation", ""), bill_start_date]

        return '@'.join([part for part in key_parts if part])
