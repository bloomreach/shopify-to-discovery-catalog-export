import gzip
import json
import jsonlines
import logging
from os import getenv

logger = logging.getLogger(__name__)


# TODO: transform to iteratively build file instead of in memory
def create_products(fp, pid_identifiers = None, vid_identifiers = None):
  products = []
  
  # stream over file and index each object in bulk output
  with gzip.open(fp, 'rb') as file:
    for line in file:
      products.append(create_product(json.loads(line), pid_identifiers, vid_identifiers))
  
  return products


def create_product(shopify_product, pid_identifiers = None, vid_identifiers = None):

    # elif "collections" in prop:

    # elif "metafields" in prop:
    #   for metafield in v:
    #     attributes["spm." + metafield["key"]] = metafield["value"]
    # else:
    #   attributes["sp." + prop] = v

  return {
    "id": shopify_product["id"].split('/')[-1], 
    "attributes": create_attributes(shopify_product, "sp"), 
    "variants": create_variants(shopify_product, identifiers=vid_identifiers)
    }


def create_id(shopify_object, identifiers = None):
  id = "NOIDENTIFIERFOUND"
  # setup default identifiers based on common Shopify patterns
  if identifiers is None:
    identifiers = ["id"]
  else:
    identifiers = identifiers.split(",")

  for identifier in identifiers:
    if identifier in shopify_object and shopify_object[identifier]:
      id = shopify_object[identifier]
      break
    elif "id" in shopify_object:
      # If `id`` isn't supplied as custom identifier, use `id` as it should always be present
      id = shopify_object["id"]

  return id


def create_variants(shopify_product, identifiers = None):
  variants = {}
  if "variants" in shopify_product and shopify_product["variants"]:
    for variant in shopify_product["variants"]:
      variant = create_variant(variant, identifiers)
      variants[variant["id"]] = {"attributes": variant["attributes"]}
  return variants


def create_variant(shopify_variant, identifiers = None):

  # attributes = {}
  # for k,v in shopify_variant.items():
  #   if "metafields" in k:
  #     for metafield in v:
  #       # each metafield added to attributes with svm. namespace to avoid collisions
  #       #   and identify it came from a shopify variant metafield
  #       attributes["svm." + metafield["key"]] = metafield["value"]
  #   else:
  #     # each variant property added as attribute with sv. namespace
  #     #   and to identify it came from a shopify variant property
  #     attributes["sv." + k] = v

  return {
    "id": create_id(shopify_variant, identifiers),
    "attributes": create_attributes(shopify_variant, "sv")
    }


def create_attributes(shopify_object, namespace):
  # TODO: Handle all the different types of metafield value types like arrays, etc
  # TODO: add namespace
  # https://shopify.dev/api/admin-graphql/2023-01/objects/metafield#field-metafield-type
  # https://shopify.dev/apps/custom-data/metafields/types
  attributes = {}
  for k,v in shopify_object.items():
    if "variants" in k:
      continue
    if "metafields" in k:
      for metafield in v:
        # each metafield key/value added to attributes with namespace
        attribute_name = namespace + "m." + metafield["namespace"] + "." + metafield["key"]
        # This is a hacky way of doing this to cover all the list use cases
        # however, more robust value type mapping should occur based on metafield["type"]
        if "list" in metafield["type"]:
          attributes[attribute_name] = json.loads(metafield["value"])
        else:
          attributes[attribute_name] = metafield["value"]
    elif "collections" in k:
      attributes["category_paths"] = create_category_paths(v)
    else:
      # each object property added as attribute with namespace
      attributes[namespace + "." + k] = v
  return attributes


# TODO: pass in id and name properties to override defaults
def create_category_paths(collections):
  paths = []
  for collection in collections:
    paths.append([{"id": collection["handle"], "name": collection["title"]}])
  
  return paths


def main(fp_in, fp_out, pid_props, vid_props):
  products = create_products(fp_in, pid_identifiers=pid_props, vid_identifiers=vid_props)

  with gzip.open(fp_out, 'wb') as out:
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
    description="transforms Shopify aggregated products into Bloomreach Product model with no reserved attribute mappings, apart from setting product and variant identifiers. The product and variant identifiers may be specified prior to running, however, they default to `handle` for the product identifier and `sku` for the variant identifier. All other shopify properties are prefixed with a namespace to prevent collisions with any Bloomreach reserved attributes. Product properties are prefixed with `sp.`, Product metafield properties are prefixed with `spm.`, Variant properties are prefixed with `sv.`, and Variant metafield properties are prefixed with `svm.`. This output may be loaded directly into a Bloomreach Discovery catalog as is."
  )
  
  parser.add_argument(
    "--input-file",
    help="File path of Generic Products jsonl",
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

  parser.add_argument(
    "--pid-props",
    help="Comma separated property names to use to resolve a shopify product property to Bloomreach product identifier. Usually set to the string 'handle'.",
    type=str,
    default="handle",
    required=False
  )

  parser.add_argument(
    "--vid-props",
    help="Comma separated property names to use to resolve a shopify variant property to Bloomreach variant identifier. Usually set to the string 'sku'.",
    type=str,
    default="sku",
    required=False)

  args = parser.parse_args()
  fp_in = args.input_file
  fp_out = args.output_file
  pid_props= args.pid_props
  vid_props= args.vid_props

  main(fp_in, fp_out, pid_props, vid_props)
