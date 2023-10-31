import logging
import os

from aws.cost_report_handler import AWSCostReportHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        config = {
            'port_client_id': os.environ["PORT_CLIENT_ID"],
            'port_client_secret': os.environ["PORT_CLIENT_SECRET"],
            'port_base_url': os.getenv("PORT_BASE_URL", "https://api.getport.io/v1"),
            'port_blueprint': os.getenv("PORT_BLUEPRINT", "awsCost"),
            'port_max_workers': int(os.getenv("PORT_MAX_WORKERS", "5")),
            'port_months_to_keep': int(os.getenv("PORT_MONTHS_TO_KEEP", "3")),
            'aws_bucket_name': os.environ["AWS_BUCKET_NAME"],
            'aws_cost_report_s3_path_prefix': os.getenv(
                "AWS_COST_REPORT_S3_PATH_PREFIX", "cost-reports/aws-monthly-cost-report-for-port"
            ),
        }
    except KeyError as e:
        raise Exception(f"Missing env variable: {e}")

    logger.info("Sync AWS cost reports")
    AWSCostReportHandler(config).handle()
    logger.info("Sync is done! exiting...")


if __name__ == "__main__":
    main()
