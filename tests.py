from executor import ExecutorSimpleFileServer
from docarray import DocumentArray, Document
import requests
import os
import shutil

wd = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '.tmp')
)


workdir = wd
print(f"workdir: {workdir}")
with ExecutorSimpleFileServer(workspace=workdir,port=4001,teardown=True) as fse:

    d = DocumentArray(
            [Document(blob=open('test_blob.txt', 'rb').read(), mime_type='text/plain')]
        )

    fse.index(
        d
    )

    # fse.close()

    print("file indexed...")
    for doc in d:
        print(f"uri: {doc.uri}")
        print(f"data from request: {requests.get(doc.uri).text}")
        print(f"mime type: {doc.mime_type}")
        print(f"external url: {doc.tags['external_url']}")


