#-------------------------------------------------------------------------------
# Name:        VITEC_REST_API
# Purpose:     Implementation of Vitec REST API for data exchange
#
# Author:      kristjan.vilgo
#
# Created:     2022-03-09
# Copyright:   (c) kristjan.vilgo 2022
# Licence:     GPL2
#-------------------------------------------------------------------------------

from requests import Session
from requests_ntlm import HttpNtlmAuth
from cgi import parse_header
from uuid import uuid4
from pathlib import Path
import os
import sys
import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s",
                        stream=sys.stdout)


def get_filename(response):
    """Parse filename from requests response MIME header"""

    file_name = None
    logger.info("Parsing file name from response")
    content_disposition = response.headers.get('Content-Disposition')
    if content_disposition:
        value, params = parse_header(content_disposition)
        file_name = params.get('filename')

    logger.info(f"Retrieved - {file_name}")

    return file_name


class Client:

    def __init__(self, server, username, password):

        session = Session()
        session.auth = HttpNtlmAuth(username=username, password=password)
        response = session.get(server)
        logger.info(f"Created connection to {server} as {username} with status {response.status_code} {response.reason}")
        self.session = session
        self.base_url = server

    # Base functions
    def download(self):
        """Download single file"""

        logger.info("Downloading single file")
        response = self.session.get(f"{self.base_url}/FileTransfer/download")

        logger.debug(response.headers)
        logger.info(f"Response status {response.status_code} {response.reason}")

        return {"content": response.content, "reason": response.reason, "status": response.status_code, "file_name": get_filename(response)}

    def download_all(self):
        """
        Download all available files.
        All files in Aiolos export folder will be added to a zip file and then downloaded in one request
        """
        logger.info("Downloading single file")
        response = self.session.get(f"{self.base_url}/FileTransfer/downloadall")
        logger.debug(response.headers)
        logger.info(f"Response status {response.status_code} {response.reason}")

        return {"content": response.content, "reason": response.reason, "status": response.status_code, "file_name": get_filename(response)}

    def upload(self, file_object, file_name, folder=""):
        """
        Upload a file. file_object needs to be bytes
        By default it is uploaded to root folder, but you can define a specific folder
        """

        assert type(file_object) is bytes, "file_object needs to be bytes"

        logger.info(f"Uploading {file_name} to {folder}")

        query = f"{self.base_url}/FileTransfer/upload?filename={file_name}&dir={folder}"
        response = self.session.post(query, data=file_object, headers={"Content-Type": "multipart/form-data"})

        logger.debug(response.headers)
        logger.info(f"Response status {response.status_code} {response.reason}")
        return response

    # Helper functions
    def upload_from_path(self, glob_pattern="*", path=".",  upload_folder=""):
        """
        Upload all flies from a local folder matching glob pattern
        Optionally define upload folder
        """

        path = Path(path)

        if not path.exists():
            logger.error(f"Path does not exist - {path.absolute()}")
            return

        for file_path in path.glob(glob_pattern):
            logger.info(f"Pattern {glob_pattern} Uploading - {file_path.absolute()} to {upload_folder}")
            self.upload(file_object=file_path.open("rb").read(), file_name=str(file_path), folder=upload_folder)



    def download_and_save(self, path=".", download_all_files=False):
        """Download single file and save to a given path"""

        path = Path(path)
        
        if not path.exists():
            logger.error(f"Path does not exist - {path}")
            return

        if download_all_files:
            response = self.download_all()
        else:
            response = self.download()

        if response["status"] == 200:

            file_name = f"{uuid4()}_{response['file_name']}"

            # Save file
            full_path = os.path.join(path, file_name)

            with open(full_path, "wb") as file_object:
                file_object.write(response["content"])
                logger.info(f"Saved - {full_path}")

        elif response["status"] == 204:
            logger.warning(f"No files available {response}")
        else:
            logger.error(f"Could not download files {response}")


if __name__ == "__main__":

    # TEST
    base_url = "https://AiolosCustomerData.vitec.net"
    username = ""
    password = ""

    client = Client(base_url, username, password)
    
    client.download_and_save()
