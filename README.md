# Overview

Python program to transform Shopify bulk output object set into a format suitable to load into a Bloomreach Discovery Catalog.

Example Usage:

```bash
python3 src/main.py \
    --shopify-url="stg-store.myshopify.com" \
    --shopify-pat="shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
    --br-environment="staging" \
    --br-account-id="6490" \
    --br-catalog-name="test_us_feed" \
    --br-api-token="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
    --output-dir="/Users/user/output"
```

The program will:
* Submit a Bulk Operation job via GraphQL to the shopify store using a PAT token that has sufficient privileges
  * If there is a current Bulk Operation job already running, the script will continue to retry until it can successfully submit a job
* Poll for the completion of the Bulk Operation job to retrieve the URL of the jsonl file that contains a dump of all product, variant, collection, and metafield data needed
* Transform that file into an additional file that aggregates in memory the individual product, variant, collection, and metafield data into a single product model
* Transform that single product model into an additional generic Bloomreach product model
* Transform the generic model into an additional specific business logic model
* Transform the final product modoel into an additional Bloomreach patch file that can be used as a patch for a full feed
* Submit the patch file as a full feed via the Discovery Feed API

Additonal details about the transform phases

1. transforms Shopify bulk output of products and their associated objects (metafields, collections, variants, variants metafields) into a single aggregated product record.

1. transforms Shopify aggregated products into Bloomreach Product model with no reserved attribute mappings, apart from setting product and variant identifiers. The product and variant identifiers may be specified prior to running, however, they default to `handle` for the product identifier and `sku` for the variant identifier. All other shopify properties are prefixed with a namespace to prevent collisions with any Bloomreach reserved attributes. Product properties are prefixed with `sp.`, Product metafield properties are prefixed with `spm.`, Variant properties are prefixed with `sv.`, and Variant metafield properties are prefixed with `svm.`. This output may be loaded directly into a Bloomreach Discovery catalog as is.

1. transforms generic products with custom logic specific to an individual catalog. This is more or less a place holder script to add any transformations necessary that need to be made on top of the generic product transforms. For instance, if shopify product tags are used in a special way, custom transforms can be created. Also, generic transforms can be overriden should it be necessary for a catalog specific behavior. The values of the shopify prefixed attributes should not be modified.

1. transforms bloomreach products into a Bloomreach Discovery catalog patch, where each patch operation is an `Add Product` operation. This patch can be used as a Full or Delta feed data source either directly in API request or SFTP.

## Requirements

### Shopify Access

You will need an access token from Shopify.

To get the access token, setup a custom app with required scope to read_products.

[Create and install a custom app](https://help.shopify.com/en/manual/apps/app-types/custom-apps#create-and-install-a-custom-app)
[Generate access tokens for custom apps in the Shopify admin](https://shopify.dev/docs/apps/auth/admin-app-access-tokens)

Exact steps:

1. Go to store admin interface
2. Click Settings in bottom of left nav
3. Click Apps and sales channels in the left nav
4. Click Develop apps
5. Click Create an app
6. Give it a name like `Bloomreach Test`
7. Click Configure Admin API scopes
8. Select `read_products` as a scope
9. Click Save
10. Click Install app and confirm
11. Click Reveal token once and use the value as the access token for this reference code.

### Setup project

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

* Python3 (3.8 or >)
    * jsonlines
    * ShopifyAPI
    * polling

To run tests and work with jsonl files:
* jq (https://stedolan.github.io/jq/)
* gron (https://github.com/tomnomnom/gron)

## Run Full Feed

There is a `template_env` file in the root dir that contains required environment variables.

Copy this file, rename it something ending in .env, and add in the empty variable values.

This file can be combined with the `run_feed.sh` bash script that will run the app with the given environment variables.

Don't check in your environment file as it contains sensitive keys.

If it has a suffix of `.env`, it will be ignored via the `.gitignore` file.

```bash
BASH_ENV=staging.env ./run_feed.sh
```

### Using Docker

```bash
# build image and give it a tag
docker build -t shopify-to-bloomreach .

# source in environment variables
. staging.env

# run image with a mounted volume (pass --rm if you want to auto cleanup)
docker run --env-file docker.env.list --env BR_OUTPUT_DIR=/feed_data --mount source=feed_data,target=/feed_data shopify-to-bloomreach
```

## Viewing output files

```bash
# view pretty formatted patch.jsonl
jq -C ' . ' patch.jsonl | less -R

# flatten out patch for easy grepping and analysis (memory intensive)
jq -s ' . ' patch.jsonl | gron | sort > patch.gron.sorted.txt
```

## Running comparisons

```bash
# vim diff is handy to do small, non-semantic difference check
vimdiff <(cat patch.expected.selected.jsonl | jq -s . | gron | sort) <(cat patch.selected.jsonl | jq -s . | gron | sort)
```

## Update full feed

The below commands assume you've already created environment files for each of the 4 environments based off the `template_env` file.

### Staging store into to test staging catalog

```bash
BASH_ENV=test_staging.env ./run_feed.sh
```


### Production store into to test production catalog

```bash
BASH_ENV=test_production.env ./run_feed.sh
```

### Staging store into to Bloomreach staging catalog

__WARNING, OPERATES ON REAL ACCOUNT__

```bash
BASH_ENV=staging.env ./run_feed.sh
```

### Production store into to Bloomreach production catalog

__WARNING, OPERATES ON REAL ACCOUNT__

```bash
BASH_ENV=production.env ./run_feed.sh
```

