import logging
from datetime import datetime
from os import getenv
from bloomreach_generics import main as brGenerics
from bloomreach_products import main as brProducts
from feed import patch_catalog
from shopify_products import main as shopifyProducts
from patch import main as brPatch
from graphql import get_shopify_jsonl_fp


def main(shopify_url="",
         shopify_pat="",
         br_account_id="",
         br_catalog_name="",
         br_environment="",
         br_api_token="",
         output_dir=""):

  run_num = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
  api_version = '2022-04'
  shopify_jsonl_fp, job_id = get_shopify_jsonl_fp(shopify_url, api_version,
                                          shopify_pat, output_dir, run_num=run_num)

  shopify_products_fp = f"{output_dir}/{run_num}_{job_id}_1_shopify_products.jsonl"
  generic_products_fp = f"{output_dir}/{run_num}_{job_id}_2_generic_products.jsonl"
  br_products_fp = f"{output_dir}/{run_num}_{job_id}_3_br_products.jsonl"
  br_patch_fp = f"{output_dir}/{run_num}_{job_id}_4_br_patch.jsonl"

  shopifyProducts(shopify_jsonl_fp, shopify_products_fp)
  brGenerics(shopify_products_fp,
             generic_products_fp,
             pid_props="handle",
             vid_props="sku,id")
  brProducts(generic_products_fp, br_products_fp, shopify_url)
  brPatch(br_products_fp, br_patch_fp)
  patch_catalog(br_patch_fp,
                account_id=br_account_id,
                environment_name=br_environment,
                catalog_name=br_catalog_name,
                token=br_api_token)


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
    help="The BR Feed API bearer token",
    type=str,
    default=getenv("BR_API_TOKEN"),
    required=not getenv("BR_API_TOKEN")
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
  environment = args.br_environment
  account_id = args.br_account_id
  catalog_name = args.br_catalog_name
  api_token = args.br_api_token
  output_dir = args.output_dir

  main(shopify_url=shopify_url,
       shopify_pat=shopify_pat,
       br_environment=environment,
       br_account_id=account_id,
       br_catalog_name=catalog_name,
       br_api_token=api_token,
       output_dir=output_dir)

# TODO: add a dry-run mode
# TODO: add counters to each module and provide examples of invalid
