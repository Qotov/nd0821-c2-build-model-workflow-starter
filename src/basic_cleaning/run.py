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


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    try:
    	artifact_local_path = run.use_artifact(args.input_artifact).file()
    	logger.info('We got artifact {}'.format(args.input_artifact))
    except:
    	logger.error('We did not get artifact {}'.format(args.input_artifact))

    # read and drop outliers
    df = pd.read_csv(artifact_local_path)
    df = df[df['price'].between(args.min_price, args.max_price)].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # save dataframe
    path_to_clean_df = 'clean_sample.csv'
    df.to_csv(path_to_clean_df, index=False)
    logger.info('Cleaned dataframe saved as {}'.format(path_to_clean_df))

    out_artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    out_artifact.add_file(path_to_clean_df)

    run.log_artifact(out_artifact)
    logger.info('Dataframe uploaded in wandb')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Input artifact name',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Output artifact name',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type of artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Data after EDA',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Min price for outliers removing',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='Max price for outliers removing',
        required=True
    )


    args = parser.parse_args()

    go(args)
