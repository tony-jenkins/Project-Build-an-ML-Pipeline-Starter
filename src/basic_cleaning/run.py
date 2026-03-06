#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# DO NOT MODIFY
def go(args):

    run = wandb.init(job_type="basic_cleaning", group="cleaning", save_code=True)
    run.config.update(vars(args))

    # Download input artifact. This will also log that this script is using this
    
    # run = wandb.init(project="nyc_airbnb", group="cleaning", save_code=True)
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Step 6: TODO
    # Only implement this step when reaching Step 6: Pipeline Release and Updates
    # in the project.
    # Add longitude and latitude filter to allow test_proper_boundaries to pass
    # ENTER CODE HERE

    # Save the cleaned data
    df.to_csv('clean_sample.csv',index=False)

    # log the new data.
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
 )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")
  
    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Input W&B artifact path/name containing the raw dataset (e.g., sample.csv:latest)",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the cleaned output artifact to create in W&B (e.g., clean_sample.csv)",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="W&B artifact type for the cleaned dataset (e.g., clean_sample)",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description for the cleaned dataset artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum nightly price to keep (filter out rows below this value)",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum nightly price to keep (filter out rows above this value)",
        required=True
    )


    args = parser.parse_args()

    go(args)
