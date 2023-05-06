import io
import json
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from logging import handlers

import boto3
import pandas as pd
import pinecone
from langchain.docstore.document import Document
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter

DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
CSV = "csv"
PINECONE = "pinecone"
schema = ["title", "topic"]
s3 = boto3.client("s3")
config = {}

logger = logging.getLogger()


def gather_news():
    logging.debug("Starting gather_news")
    paginator = s3.get_paginator("list_objects_v2")
    bucket = config["FROM_S3_BUCKET"]

    pages = paginator.paginate(Bucket=bucket, Prefix=DATE)
    for page in pages:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            # Check if the object is a JSON file
            if not obj["Key"].endswith(".json"):
                continue
            logging.debug("Found json file: {}".format(obj["Key"]))
            # Read the contents of the object and decode the JSON data
            json_data = json.loads(
                s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"]
                .read()
                .decode("utf-8")
            )
            yield json_data
    logging.debug("Finished gather_news")


def transform_news_to_csv(csv_buffer):
    """Transform the news data into a CSV file"""
    data = defaultdict(list)
    for article in gather_news():
        for key in schema:
            data[key].append(article[key])

    return pd.DataFrame(data).to_csv(csv_buffer, index=False)


def csvEtl():
    bucket = config["TO_S3_BUCKET"]
    file_name = f"{DATE}-news.csv"

    # Create a buffer to hold the transformed data
    csv_buffer = io.StringIO()
    transform_news_to_csv(csv_buffer)

    # Upload the file to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=file_name)


def pineconeEtl():
    """Transform the news data into embeddings and upload to Pinecone"""

    logging.debug("Starting pineconeEtl")

    pinecone.init(
        api_key=config["PINECONE_API_KEY"], environment=config["PINECONE_ENV"]
    )

    index = pinecone.Index(config["PINECONE_INDEX_NAME"])

    # Create LangChain documents from the news data
    docs = []
    for article in gather_news():
        m_data = deepcopy(article)
        del m_data["text"]
        doc = Document(page_content=article["text"], metadata=m_data)
        docs.append(doc)

    logging.debug("Created {} langchain documents".format(len(docs)))

    # Split and embed the documents
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=50)
    texts = text_splitter.split_documents(docs)

    embeddings = OpenAIEmbeddings(openai_api_key=config["OPEN_API_KEY"])
    embedding_list = embeddings.embed_documents([text.page_content for text in texts])
    chunk_size = 50
    pc_emb = []

    logging.debug("Created {} embeddings".format(len(embedding_list)))

    # Upload the embeddings to Pinecone

    from copy import copy

    for i, emb in enumerate(embedding_list):
        m_data = copy(texts[i].metadata)
        m_data["publisher"] = m_data["publisher"]["title"]
        pc_emb.append(("vec" + str(i), emb, m_data))

    for i in range(0, len(pc_emb), chunk_size):
        chunk = pc_emb[i : i + chunk_size]

        index.upsert(vectors=chunk, namespace="articles")

    logging.debug("Finished pineconeEtl")


def main():
    logging.debug("Starting main")
    if config["ETL"] == CSV:
        csvEtl()
    elif config["ETL"] == PINECONE:
        pineconeEtl()
    logging.debug("Finished main")


def lambda_handler(event, _):

    if "LOG_LEVEL" in event and event["LOG_LEVEL"].lower() in [
        "debug",
        "info",
        "warning",
        "error",
        "critical",
    ]:
        logger.setLevel(level=getattr(logging, event["LOG_LEVEL"].upper()))
    else:
        logger.setLevel(level=logging.INFO)

    config["FROM_S3_BUCKET"] = event["FROM_S3_BUCKET"]
    config["ETL"] = event["ETL"]

    if config["ETL"] not in [CSV, PINECONE]:
        raise ValueError(f"Invalid ETL type: {config['ETL']}")

    if config["ETL"] == CSV:
        config["TO_S3_BUCKET"] = event["TO_S3_BUCKET"]
    else:
        pineconeEtlConfigs = [
            "PINECONE_API_KEY",
            "PINECONE_ENV",
            "PINECONE_INDEX_NAME",
            "OPEN_API_KEY",
        ]
        for conf in pineconeEtlConfigs:
            config[conf] = event[conf]

    main()
