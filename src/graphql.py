import gzip
import json
import logging
import polling
import requests
import shopify
import shutil
from os import getenv
from pathlib import Path

logger = logging.getLogger(__name__)


def export_jsonl(context):
  """
  Attempts to run a Bulk Operation query to initiate a job
  that will extract a JSONL file with all of a Shop's product information.

  If the job is successfully submitted, returns True and adds job id to context.

  If the job can't be submitted because there is another Bulk Operation running,
  returns False.

  If the job can't be submitted for an unknown reason, a RuntimeError is raised.

  The job is async and this response will return a job id, 
  so status needs to be polled via another query.

  When the job is completed, a JSONL output file is 
  available for download at the URL specified in an additional query.

  https://shopify.dev/api/usage/bulk-operations/queries

  Extracts all of the following for every product in a shop:
      * general fields(handle, title, etc)
      * metafields
      * collections
      * variants
          * general fields (sku, price, etc)
          * metafields
          * selected options
  """
  query = Path('./src/graphql_queries/export_data_job.graphql').read_text()
  logger.info("ExportDataJob attempt")
  result = shopify.GraphQL().execute(query=query,
                                     operation_name="ExportDataJob")
  result_json = json.loads(result)

  if 'errors' in result_json:
    raise RuntimeError("Errors encountered while running ExportDataJob query")

  bulkOperation = result_json["data"]["bulkOperationRunQuery"]["bulkOperation"]

  # If bulkOperation object is None, then the job wasn't submitted successfully
  if bulkOperation is not None:
    job_id = bulkOperation['id']
    logger.info("GraphQL Bulk Operation submitted successfully. Job id: %s", job_id)
    context["job_id"] = job_id
    return True
  elif "already in progress" in result:
    logger.info("GraphQL Bulk Operation not submitted, trying again after delay. Another operation already in progress: %s", result_json)
    return False
  else:
    raise RuntimeError("Unable to start ExportDataJob")


def get_jsonl_url(job_id, context):
  """
  Given a Bulk Operation job id, polls for status and objectCount.

  Executes GraphQL Bulk Operation queries for a given job.

  If job is still in progress, returns False.

  If job is completed successfully, returns True with jsonl url added to context.
  
  If job does not complete successfully, raises a RuntimeError.

  https://shopify.dev/api/usage/bulk-operations/queries#option-b-poll-a-running-bulk-operation
  """
  query = Path('./src/graphql_queries/get_job.graphql').read_text()
  logger.info("GetJob query for job_id: %s" % job_id)
  result = shopify.GraphQL().execute(query=query,
                                     operation_name="GetJob",
                                     variables={"job_id": job_id})
  result_json = json.loads(result)

  if 'errors' in result_json:
    raise RuntimeError("Errors encountered while running ExportDataJob query")

  node = result_json["data"]["node"]
  state = node["status"]
  logger.info("GraphQL Bulk Operation current state: %s", state)

  # https://shopify.dev/api/admin-graphql/2023-01/enums/bulkoperationstatus
  if state == "COMPLETED":
    logger.info("GraphQL Bulk Operation completed successfully, jsonl at url: %s", node["url"])
    logger.info("GraphQL Bulk Operation objectCount: %s", node["objectCount"])
    context["url"] = node["url"]
    return True

  if state in ["CANCELED", "CANCELING", "EXPIRED", "FAILED"]:
    logger.error("GraphQL Bulk Operation did not complete successfully: %s, %s", job_id, state)
    raise RuntimeError("Full feed job did not complete successfully")

  logger.info("GraphQL Bulk Operation current objectCount: %s", node["objectCount"])

  return False


def download_file(url, local_filename):
  with requests.get(url, stream=True) as r:
    with gzip.open(local_filename, 'wb') as f:
      shutil.copyfileobj(r.raw, f)
  return local_filename


def get_shopify_jsonl_fp(shop_url, api_version, token, output_dir, run_num=""):
  session = shopify.Session(shop_url, api_version, token)
  shopify.ShopifyResource.activate_session(session)

  # Submit a job to export jsonl data.
  context = {}
  polling.poll(lambda: export_jsonl(context), step=20, timeout=7200)

  job_id = context["job_id"]

  # Get jsonl url path
  context = {}
  polling.poll(lambda: get_jsonl_url(job_id, context), step=20, timeout=7200)

  jsonl_url = context["url"]
  job_id_short = job_id.split('/')[-1]
  
  jsonl_fp = output_dir + "/0_shopify_bulk_op.jsonl.gz"
  logger.info("Saving jsonl file to: %s", jsonl_fp)
  download_file(jsonl_url, jsonl_fp)

  shopify.ShopifyResource.clear_session()

  return jsonl_fp, job_id_short


if __name__ == '__main__':
  import argparse

  from sys import stdout
  
  # Define logger
  loglevel = getenv('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(
    stream=stdout, 
    level=loglevel,
    format="%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
  )

  parser = argparse.ArgumentParser(
    description="Extracts a full set of products and their categories from the shopify store and runs a full feed into a Bloomreach Discovery catalog.\n \nUses Shopify's GraphQL Bulk Operation API.\n \nDuring processing, it will save the Shopify bulk operation output jsonl file locally named with the BulkOperation ID value.\n \n From there, the file will run through different transforms to create a Bloomreach patch that is then run in full feed mode via the BR Feed API.\n \nEach transform step will save its intermediate output locally as well for debugging purposes prefixed with a step number.\n \nFor example:\n23453245234_0.jsonl\n23453245234_1_shopify_products.jsonl\n23453245234_2_generic_products.jsonl\n23453245234_3_br_products.jsonl\n23453245234_4_br_patch.jsonl"
  )
  
  parser.add_argument(
    "--shopify-url",
    help="Hostname of the shopify Shop, e.g. xyz.myshopify.com.",
    type=str,
    default=getenv("BR_SHOPIFY_URL"),
    required=not getenv("BR_SHOPIFY_URL")
  )

  parser.add_argument(
    "--shopify-pat",
    help="Shopify PAT token, e.g shpat_casdcaewras82342dczasdf3",
    type=str,
    default=getenv("BR_SHOPIFY_PAT"),
    required=not getenv("BR_SHOPIFY_PAT")
  )

  parser.add_argument(
    "--output-dir",
    help="Directory path to store the output files to",
    type=str,
    default=getenv("BR_OUTPUT_DIR"),
    required=not getenv("BR_OUTPUT_DIR")
  )

  args = parser.parse_args()
  shopify_url = args.shopify_url
  shopify_pat = args.shopify_pat
  output_dir = args.output_dir

  get_shopify_jsonl_fp(shopify_url, '2022-04', shopify_pat, output_dir)
