import logging
import polling
import requests
from os import getenv

logger = logging.getLogger(__name__)


def hostname_from_environment(environment="staging"):
  hostnames = {
    "staging": "api-staging.connect.bloomreach.com",
    "production": "api.connect.bloomreach.com"
  }

  if environment not in hostnames:
    raise Exception("Invalid environment: %s" % environment)
  return hostnames[environment]


def patch_catalog(
    patch_fp,
    account_id="",
    environment_name="",
    catalog_name="",
    token=""):

  dc_endpoint = "dataconnect/api/v1"

  hostname = hostname_from_environment(environment_name)

  account_endpoint = f"accounts/{account_id}"
  catalog_endpoint = f"catalogs/{catalog_name}"

  url = f"https://{hostname}/{dc_endpoint}/{account_endpoint}/{catalog_endpoint}/products"

  headers = {
    "Content-Type": "application/json-patch+jsonlines",
    "Content-Encoding": "gzip",
    "Authorization": "Bearer " + token
  }

  feed_job_id = ""
  with open(patch_fp, 'rb') as payload:
    response = requests.put(url, data=payload, headers=headers)
    response.raise_for_status()

    logger.info("Feed API: HTTP PUT: %s", response.url)
    logger.info("Feed Job response: %s", response.json())
    job_id = response.json()["jobId"]

  polling.poll(lambda: br_check_status(job_id=job_id, environment_name=environment_name, token=token), step=10, timeout=7200)
  

def br_check_status(job_id="", environment_name="", token=""):
  dc_endpoint = "dataconnect/api/v1"
  hostname = hostname_from_environment(environment_name)
  url = f"https://{hostname}/{dc_endpoint}/jobs/{job_id}"
  headers = {
    "Authorization": "Bearer " + token
  }
  logger.info("Checking status for job: %s", url)
  response = requests.get(url, headers=headers)
  response.raise_for_status()
  state = response.json()["status"]
  logger.info("Current job status: %s", state)

  if state == "success":
    return True

  if state in ["failed", "killed"]:
    logger.error("Job did not complete successfully: %s, %s", job_id, state)
    raise ValueError("Full feed job did not complete successfully")
  
  # TODO: check for the pending and queued states and return false on those
  return False

if __name__ == '__main__':
  import argparse
  from os import getenv

  from sys import stdout
  
  # Define logger
  loglevel = getenv('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(
    stream=stdout, 
    level=loglevel,
    format="%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
  )
  
  parser = argparse.ArgumentParser(
    description="Makes a full feed API call using the patch jsonl as a request body using gzip compression."
  )

  parser.add_argument(
    "--input-file",
    help="File path of Generic Products jsonl",
    type=str,
    default=getenv("BR_INPUT_FILE"),
    required=not getenv("BR_INPUT_FILE")
  )
  
  parser.add_argument(
    "--br-environment",
    help="Which Bloomreach Account environment to send catalog data to",
    type=str,
    default=getenv("BR_ENVIRONMENT_NAME"),
    required=not getenv("BR_ENVIRONMENT_NAME")
  )

  parser.add_argument(
    "--br-account-id",
    help="Which Bloomreach Account ID to send catalog data to",
    type=str,
    default=getenv("BR_ACCOUNT_ID"),
    required=not getenv("BR_ACCOUNT_ID")
  )

  parser.add_argument(
    "--br-catalog-name",
    help="Which Bloomreach Catalog Name to send catalog data to.\nThis is the same as the value of domain_key parameter in Search API requests.",
    type=str,
    default=getenv("BR_CATALOG_NAME"),
    required=not getenv("BR_CATALOG_NAME")
  )

  parser.add_argument(
    "--br-api-token",
    help=
    "The BR Feed API bearer token",
    type=str,
    default=getenv("BR_API_TOKEN"),
    required=not getenv("BR_API_TOKEN")
  )

  args = parser.parse_args()
  fp_in = args.input_file
  environment_name = args.br_environment
  account_id = args.br_account_id
  catalog_name = args.br_catalog_name
  api_token = args.br_api_token

  patch_catalog(fp_in, 
       environment_name=environment_name,
       account_id=account_id,
       catalog_name=catalog_name,
       token=api_token)
