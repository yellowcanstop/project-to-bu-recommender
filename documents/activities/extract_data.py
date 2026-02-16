"""Recommends BUs for cleaned and deduplicated leads.

This module provides the blueprint for an Azure Function activity that cleans and deduplicates lead data from documents, which are then used for recommending BU for eligible leads.
"""

from __future__ import annotations
from pydantic import Field
from shared.workflows.base_request import BaseRequest
from shared.workflows.validation_result import ValidationResult
from storage.services.azure_storage_client_factory import AzureStorageClientFactory
import shared.identity as identity
from shared import app_settings
import azure.durable_functions as df
import logging
from typing import Dict, Optional

name = "ExtractData"
bp = df.Blueprint()
storage_factory = AzureStorageClientFactory(identity.default_credential)


@bp.function_name(name)
@bp.activity_trigger(input_name="input", activity=name)
def run(input: Request) -> Dict:
    """Cleans and deduplicates lead data from documents.

    :param input: The request containing the container name and blob name of the document.
    :return: The recommendations if successful; otherwise, None.
    """

    validation_result = input.validate()
    if not validation_result.is_valid:
        logging.error(f"Invalid input: {validation_result.to_str()}")
        return None

    blob_content = storage_factory.get_blob_content(
        app_settings.azure_storage_account, input.container_name, input.blob_name)

    # TODO
    data = {}

    return data


class Request(BaseRequest):
    """Defines the request payload for the activity."""

    container_name: str = Field(
        description="The name of the container within the storage account.")
    blob_name: str = Field(
        description="The name of the document blob to extract data from.")
    

    def validate(self) -> ValidationResult:
        result = ValidationResult()

        if not self.container_name:
            result.add_error("container_name is required")

        if not self.blob_name:
            result.add_error("blob_name is required")

        return result

    @staticmethod
    def to_json(obj: Request) -> str:
        """
        Convert the Request object to a JSON string.

        For more information on this serialization method for Azure Functions, see:
        https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-serialization-and-persistence?tabs=python
        """
        return obj.model_dump_json()

    @staticmethod
    def from_json(json_str: str) -> Request:
        """
        Convert a JSON string to an Request object.

        For more information on this serialization method for Azure Functions, see:
        https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-serialization-and-persistence?tabs=python
        """
        return Request.model_validate_json(json_str)