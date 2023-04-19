from jina import DocumentArray, Executor, requests
from jina.logging.logger import JinaLogger
import requests as pyrequests
import jinja2 as j2
import subprocess
from typing import Dict, Optional
import tempfile
import time
import os
import uuid
import mimetypes

logger = JinaLogger('simple-file-server')


base_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(base_dir, "templates")

environment = j2.Environment(loader=j2.FileSystemLoader(template_dir))
template = environment.get_template("simple-docker-config.yml.jinja")


class ExecutorSimpleFileServer(Executor):
    """
    Executor which talk to a local docker container running a simple file server.
    The image and documentation is available at: 
    https://hub.docker.com/r/flaviostutz/simple-file-server
    
    A simple configuration for the docker container would be something like this:
    
    `docker-compose.yml`
    ```yaml
    version: '3.7'

    services:

        simple-file-server:
            image: flaviostutz/simple-file-server
            ports:
            - "4000:4000"
            environment:
            - WRITE_SHARED_KEY=
            - READ_SHARED_KEY=
            - LOCATION_BASE_URL=http://localhost:4000
            - LOG_LEVEL=debug
    ```
    
    The exector will check to see if the file server is up.
    If it is not up, it will automatically try to start it, based on a template of the configuration above.
    In the executor you can configure the following parameters:
        host: the host of the file server (do not specify the service or port, eg `localhost`)
        workspace: the local directory where the files will be stored
        external_base_url: the base url of the file server, if there is an external 
        service (for example, ngrok)
    
    For each document, the executor will:
        - check if the document has a tensor
        - check if the document has a blob
    
    For image tensors, the exceutor will assume that it is an image, and save it to the file server
    based on the assumed file extension (default is jpg).
    
    If the document has a blob, the executor will first see if there is a mime_type field.
    If no mime type is found, it will look for a tag with `file` in the key, and check 
    the extension of the key. If no extension is found, it will save it with no extension.
    
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 external_host: Optional[str] = None,
                 port: int = 4000,
                 teardown: bool = False,
                 set_as_tag: bool = True,
                 metas: Optional[Dict] = None, 
                 requests: Optional[Dict] = None, 
                 runtime_args: Optional[Dict] = None, 
                 workspace: Optional[str] = None, 
                 dynamic_batching: Optional[Dict] = None, 
                 **kwargs):
        super().__init__(metas, requests, runtime_args, workspace, dynamic_batching, **kwargs)
        self.host = host
        self.port = port
        self.base_url = f"http://{self.host}:{self.port}"
        self.external_host = external_host
        self.teardown = teardown
        self.set_as_tag = set_as_tag
        # make sure workspace exists and we have the absolute path
        if not os.path.isdir(workspace):
            os.makedirs(workspace)
            
        if not os.path.isabs(self.workspace):
            raise ValueError(f"workspace must be an absolute path, got {self.workspace}")
        
        self.base_template = template.render(
            base_url=self.base_url, 
            port=self.port,
            workspace=self.workspace
        )
        
        # check if file server is up
        try:
            pyrequests.get(self.base_url)
        except pyrequests.exceptions.ConnectionError as e:
            logger.info(f"connection error: {e}")
            logger.info('starting file server...')
            logger.info("using compose file:")
            logger.info(f"\n{self.base_template}")
            with open(f"{self.workspace}/docker-compose.yml", "w") as f:
                f.write(self.base_template)
            subprocess.run(["docker-compose", "-f", f"{self.workspace}/docker-compose.yml", "up", "-d"])
            logger.info('file server started, checking connection...')
            # sleep for 5 seconds to wait for the file server to start
            time.sleep(5)
            try:
                logger.debug(f"trying to connect to: {self.base_url}")
                pyrequests.get(self.base_url)
            except pyrequests.exceptions.ConnectionError as e:
                logger.critical(f"connection error: {e}")
                raise pyrequests.exceptions.ConnectionError("could not start file server")
                    
        
    
    
    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        for doc in docs:
            if doc.blob is not None:
                # check if there is a mime_type
                mtype = doc.mime_type
                ext = mimetypes.guess_extension(mtype)
                did = doc.id
                # save it to a temporary file
                with tempfile.TemporaryDirectory() as tmpdir:
                    uid = str(uuid.uuid4())
                    fn = f"{uid}{ext}"
                    fp = f"{tmpdir}/{fn}"
                    doc.save_blob_to_file(fp)
                    new_url = self._post_to_file_server(did, fp, mtype)
                    doc.uri = new_url
                    doc.mime_type = mtype
                    if self.set_as_tag:
                        doc.tags['file_url'] = new_url
                        
                    if self.external_host is not None:
                        doc.tags["external_url"] = doc.tags['file_url'].replace(self.base_url, self.external_host)
                    else:
                        doc.tags['external_url'] = None
                
                
                    
            if doc.tensor is not None:
                # we are going to assume that is a jpg
                mtype = "image/jpeg"
                # ext = mimetypes.guess_extension(mtype)
                ext = ".jpeg"
                did = doc.id
                # save to temp file
                with tempfile.TemporaryDirectory() as tmpdir:
                    uid = str(uuid.uuid4())
                    fn = f"{uid}{ext}"
                    fp = f"{tmpdir}/{fn}"
                    doc.save_image_tensor_to_file(fp, image_format='jpeg')
                    new_url = self._post_to_file_server(did, fp, mtype)
                    doc.uri = new_url
                    doc.mime_type = mtype
                    if self.set_as_tag:
                        doc.tags['file_url'] = new_url
                        
                    if self.external_host is not None:
                        doc.tags["external_url"] = doc.tags['file_url'].replace(self.base_url, self.external_host)
                    else:
                        doc.tags['external_url'] = None
                    
                    
                
    def _post_to_file_server(self, docid, filename, mimetype):
        
        h = {"Content-Type": mimetype}
        _, sp = os.path.split(filename)
        out_url = f"{self.base_url}/{docid}/{sp}"
        
        logger.debug(f"save to file server: {out_url}" )
        logger.debug(f"headers: {h}")
        logger.debug(f"file: {filename}")
        logger.debug(f"mimetype: {mimetype}")
        logger.debug(f"docid: {docid}")
        
        r = pyrequests.put(
            out_url,
            data=open(filename, "rb").read(),
            headers=h
        )
        # the resopnse from the server will be a string with the url
        if not r.ok:
            raise Exception(f"error posting to file server: {r.text}")
        elif r.ok:
            logger.debug(f"response from file server: {r.text}")
        
        # create the url and return it
        assert f"{self.base_url}{r.text}" == out_url
        return out_url
    
    def close(self):
        if self.teardown:
            logger.info("teardown is True, stopping file server")
            # stop file server
            logger.debug("stopping file server")
            cmd = ['docker-compose', '-f', f"{self.workspace}/docker-compose.yml", 'down']
            logger.debug(f"running command: {cmd}")
            subprocess.run(cmd)
            logger.info("file server stopped")
            # remove docker-compose.yml
            os.remove(f"{self.workspace}/docker-compose.yml")
        else:
            logger.info("teardown is False, not stopping file server")
        super().close()
        
        