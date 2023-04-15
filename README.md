# executor-simple-file-server

Simple executor to communicate with a local file server with RESTful API, [flaviostutz/simple-file-server:latest](https://hub.docker.com/r/flaviostutz/simple-file-server).

## Usage

Has a few paramters to configure:

- `host`: host, should be IP, domain, `localhost`, etc
- `port`: Port of the server
- `workspace`: directory to save files
- `teardown`: if `True`, will shut down the docker container after the execution
- `external_host`: can set an external host, will be used to generate the URL for a document, stored in the `doc.tags['external_url']` field.
