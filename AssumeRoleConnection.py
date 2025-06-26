"""
Author: Eduardo Timal Montes
Date: 2025-05-1
Version: 1.0.0

Overview:
This script provides a class for connecting to AWS S3, assuming roles for enhanced security,
and retrieving files from a specified S3 bucket and folder. It includes logging for debugging and operational visibility.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# Display logger in lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
SOURCE_BUCKET = "inversolarmx"
SOURCE_FOLDER = "pricelists"


class AwsConnection:
    """
    AWS connection handler for S3 operations with optional role assumption
    """

    def __init__(
        self,
        assume_role_arn: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
    ):
        """
        Initialize the processor with optional role assumption

        Args:
            assume_role_arn: ARN of the role to assume for enhanced security
            aws_access_key_id: AWS Access Key ID for initial authentication
            aws_secret_access_key: AWS Secret Access Key for initial authentication
            region_name: AWS region name
        """
        self.region_name = region_name
        self.assume_role_arn = assume_role_arn
        self.s3_client = None

        # Initialize AWS clients
        self._initialize_aws_clients(aws_access_key_id, aws_secret_access_key)

    def _initialize_aws_clients(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """
        Initialize AWS clients with optional role assumption
        """
        try:
            # Create initial session with provided credentials or default
            if aws_access_key_id and aws_secret_access_key:
                session = boto3.Session(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=self.region_name,
                )
                logger.info("Using provided AWS credentials")
            else:
                session = boto3.Session(region_name=self.region_name)
                logger.info("Using default AWS credentials")

            # Assume role if specified (for enhanced security)
            if self.assume_role_arn:
                logger.info(f"Assuming role: {self.assume_role_arn}")
                credentials = self._assume_role(session)

                # Create new session with assumed role credentials
                session = boto3.Session(
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                    region_name=self.region_name,
                )
                logger.info("Successfully assumed role")

            # Initialize S3 client
            self.s3_client = session.client("s3")

            # Test connection
            self._test_connection()

        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {str(e)}")
            raise

    def _assume_role(self, session: boto3.Session) -> Dict[str, str]:
        """
        Assume the specified IAM role

        Args:
            session: Boto3 session to use for assuming role

        Returns:
            Dictionary containing temporary credentials
        """
        try:
            sts_client = session.client("sts")

            response = sts_client.assume_role(
                RoleArn=self.assume_role_arn,
                RoleSessionName=f"S3ExcelProcessor-{int(datetime.now().timestamp())}",
            )

            return response["Credentials"]

        except Exception as e:
            logger.error(f"Failed to assume role {self.assume_role_arn}: {str(e)}")
            raise

    def _test_connection(self):
        """
        Test AWS S3 connection
        """
        try:
            # Try to list buckets to test connection
            response = self.s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
            logger.info(
                f"Successfully connected to AWS. Founded Buckets: {response['Name']}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to AWS S3: {str(e)}")
            raise

    def get_files_from_s3_folder(self) -> List[str]:
        """
        Get all file names from the source S3 folder
        """
        try:
            prefix = f"{SOURCE_FOLDER}/" if SOURCE_FOLDER else ""

            # response = self.s3_client.list_objects_v2(
            #     Bucket=SOURCE_BUCKET, Prefix=prefix
            # )
            response = self.s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)

            # Convert response to dataframe
            if "Contents" not in response:
                logger.info("No files found in the source folder")
                return []
            logger.info(f"Found {len(response['Contents'])} objects in source folder")

            files_df = pd.DataFrame(response["Contents"])
            files_df["Key"] = files_df["Key"].astype(str)
            files_df["LastModified"] = pd.to_datetime(
                files_df["LastModified"], utc=True
            )
            files_df["LastModified"] = files_df["LastModified"].dt.tz_convert(
                "America/Mexico_City"
            )
            logger.info(
                f"Files DataFrame created with {len(files_df)} rows and {len(files_df.columns)} columns"
            )
            return files_df

        except Exception as e:
            logger.error(f"Error getting files from S3: {str(e)}")
            return []


def run_manual_test(
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    assume_role_arn: str = None,
    region_name: str = None,
) -> Dict[str, Any]:
    """
    Manual testing function with credential and role assumption support

    Args:
        aws_access_key_id: AWS Access Key ID
        aws_secret_access_key: AWS Secret Access Key
        assume_role_arn: Optional ARN of role to assume
        region_name: AWS region name

    Returns:
        Dictionary with processing results
    """
    try:
        logger.info("Starting manual test execution")
        logger.info("=" * 50)

        # Get credentials from environment if not provided
        if not aws_access_key_id:
            aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        if not aws_secret_access_key:
            aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not assume_role_arn:
            assume_role_arn = os.environ.get("ASSUME_ROLE_ARN")
        if not region_name:
            region_name = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        # Initialize processor with credentials and role assumption
        aws_client = AwsConnection(
            assume_role_arn=assume_role_arn,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        aws_client.get_files_from_s3_folder()
        return True

    except Exception as e:
        raise e


# Example usage for local testing
if __name__ == "__main__":
    """
    Local testing examples
    """
    print("S3 Excel Processor - Local Testing")
    print("=" * 40)

    # Load environment variables from .env file
    from dotenv import load_dotenv

    load_dotenv()

    # Example 1: Test with environment variables
    print("\nTesting with environment variables...")
    result1 = run_manual_test()
