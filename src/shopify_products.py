import gzip
import json
import jsonlines
import logging
from collections import defaultdict
from os import getenv

logger = logging.getLogger(__name__)

# iterate over shopify file and return a list of patch ops
def parse_shopify_objects(fp):
  objects = {}
  parent_to_children = defaultdict(list)
  products = []

  # stream over file and index each object in bulk output
  with gzip.open(fp, 'rb') as file:
    for line in file:
      index_object(json.loads(line), objects, parent_to_children)

    # iterate over all objects and constructs an aggregated product
    for k in objects.keys():
      if "/Product/" in k and "/Collection/" not in k:
        product = create_product_from_objects(k, objects, parent_to_children)
        products.append(product)

  return products


# process each shopify file object and load data into memory for later lookups
def index_object(shopify_object, objects, parent_to_children):
  shopify_id = shopify_object["id"]

  # Collection IDs are not globally unique so need to combine with __parentId
  if "/shopify/Collection/" in shopify_id:
    shopify_id = shopify_object["id"] + shopify_object["__parentId"]

  objects[shopify_id] = shopify_object

  if "__parentId" in shopify_object:
    parent_to_children[shopify_object["__parentId"]].append(shopify_id)


# constructs an aggregated product from it's children objects
def create_product_from_objects(k, objects, parent_to_children):
  collections, variants, metafields = [], [], []

  children_ids = parent_to_children[k]
  for child_id in children_ids:
    if "/Collection/" in child_id:
      collections.append(objects[child_id])
    elif "/ProductVariant/" in child_id:
      variants.append(create_variant(child_id, objects, parent_to_children))
    elif "/Metafield/" in child_id:
      if "/Product/" in objects[child_id]["__parentId"]:
        metafields.append(objects[child_id])

  product = objects[k]
  product["collections"] = collections
  product["variants"] = variants
  product["metafields"] = metafields

  return product


# constructs the variants for a product
def create_variant(variant_id, objects, parent_to_children):
  metafields = []

  children_ids = parent_to_children[variant_id]
  for child_id in children_ids:
    if "/Metafield/" in child_id:
      if "/ProductVariant/" in objects[child_id]["__parentId"]:
        metafields.append(objects[child_id])

  variant = objects[variant_id]
  variant["metafields"] = metafields
  return variant


def main(fp_in, fp_out):
  products = parse_shopify_objects(fp_in)

  # write JSONLines to stdout
  with gzip.open(fp_out, "wb") as out:
    writer = jsonlines.Writer(out)
    for object in products:
      writer.write(object)
    writer.close()


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
    prog="Transform Shopify bulk operation jsonl file into aggregated products jsonl",
    description="Transforms Shopify bulk output of products and their associated objects (metafields, collections, variants, variants metafields) into a single aggregated Shopify product record."
  )
  
  parser.add_argument(
    "--input-file",
    help="File path of shopify bulk operation jsonl",
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
