import gzip
import json
import jsonlines
import logging
from os import getenv

logger = logging.getLogger(__name__)


def create_patch_from_products_fp(fp_in):
  patch = []

  with gzip.open(fp_in, "rb") as file:
    for line in file:
      patch.append(create_add_product_op(json.loads(line)))
  
  return patch


# construct an add product operation from shopify product
def create_add_product_op(product):
  path = "/products/" + product["id"].replace("/", "~1") # JSONPointer compliant replacement
 
  return {
    "op": "add", 
    "path": path, 
    "value": {
      "attributes": product["attributes"],
      "variants": product["variants"]
    }}


def main(fp_in, fp_out):
  patch = create_patch_from_products_fp(fp_in)

  from sys import stdout
  
  # Define logger
  loglevel = getenv('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(
    stream=stdout, 
    level=loglevel,
    format="%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
  )
  
  # write JSONLines to stdout
  with gzip.open(fp_out, "wb") as file:
    writer = jsonlines.Writer(file)
    for object in patch:
      writer.write(object)
    writer.close()

if __name__ == '__main__':
  import argparse
  from os import getenv
  
  parser = argparse.ArgumentParser(
    description="Transforms Bloomreach products into a Bloomreach Discovery catalog patch, where each patch operation is an `Add Product` operation. This patch can be used as a Full or Delta feed data source either directly in API request or SFTP."
  )
  
  parser.add_argument(
    "--input-file",
    help="File path of Bloomreach Products jsonl",
    type=str,
    default=getenv("BR_INPUT_FILE"),
    required=not getenv("BR_INPUT_FILE")
  )

  parser.add_argument(
    "--output-file",
    help="Filename of output jsonl file",
    type=str,
    default=getenv("BR_OUTPUT_FILE"),
    required=not getenv("BR_OUTPUT_FILE")
  )

  args = parser.parse_args()
  fp_in = args.input_file
  fp_out = args.output_file

  main(fp_in, fp_out)
  
