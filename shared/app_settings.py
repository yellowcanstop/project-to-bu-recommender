"""Defines the configuration settings for the Azure Functions application.

The variables are defined by environment variables configured in the from the `local.settings.json` file when running locally, and from the Azure Function App settings when running in Azure.
"""


import os
from configuration import Configuration
config = Configuration()

otel_exporter_otlp_endpoint = config.get_value(
    "OTEL_EXPORTER_OTLP_ENDPOINT", None)
azure_openai_endpoint = config.get_value("AZURE_OPENAI_ENDPOINT", None)
azure_openai_chat_deployment = config.get_value(
    "AZURE_OPENAI_CHAT_DEPLOYMENT", None)
azure_client_id = config.get_value("AZURE_CLIENT_ID", None)
azure_storage_account = config.get_value(
    "AZURE_STORAGE_ACCOUNT", None)
azure_storage_queues_connection_string = config.get_value(
    "AZURE_STORAGE_QUEUES_CONNECTION_STRING", None)

'''
import os


def get_setting(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# Blob storage
BLOB_ACCOUNT_URL = get_setting("BLOB_ACCOUNT_URL")
BLOB_CONTAINER = get_setting("BLOB_CONTAINER", "project-leads")
BCI_BLOB_NAME = get_setting("BCI_BLOB_NAME", "bci_leads.xlsx")
NON_BCI_BLOB_NAME = get_setting("NON_BCI_BLOB_NAME", "non_bci_leads.xlsx")

# Azure OpenAI
AZURE_OPENAI_ENDPOINT = get_setting("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_CHAT_DEPLOYMENT = get_setting("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = get_setting(
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-small"
)
'''