""" A Simple file server for uploading and downloading files with MinIO"""
import http.client
import json
import mimetypes
import os
import urllib.parse
from argparse import ArgumentParser
from collections import defaultdict

import boto3
import botocore
from flask import Flask, request, abort, Response
from flask_compress import Compress
from flask_cors import CORS

from config import config
from utils import get_env_bool

log = config.logger(__file__)

app = Flask(__name__)
CORS(app, **config.get("fileserver.cors"))

if get_env_bool("CLEARML_COMPRESS_RESP", default=True):
    Compress(app)

app.config["SEND_FILE_MAX_AGE_DEFAULT"] = config.get(
    "fileserver.download.cache_timeout_sec", 5 * 60
)

minio_conf = config.get("fileserver.minio")
minio_endpoint = minio_conf.minio_endpoint
access_key = minio_conf.access_key
secret_key = minio_conf.secret_key
bucket_name = minio_conf.bucket_name
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)


def _is_path_existed(path):
    message = ""
    code = http.client.OK
    try:
        _ = s3_client.head_object(Bucket=bucket_name, Key=path)
        existed = True
    except botocore.exceptions.ClientError as e:
        message = e.response['Error']['Message']
        code = e.response['Error']['Code']
        log.error(f"Get S3 meta failed for object[{path}] :{e.response['Error']}")
        existed = False
    return existed, message, code


@app.route("/", methods=["GET"])
def ping():
    return "OK-minio", 200


@app.before_request
def before_request():
    if request.content_encoding:
        return f"Content encoding is not supported ({request.content_encoding})", 415


@app.after_request
def after_request(response):
    response.headers["server"] = config.get(
        "fileserver.response.headers.server", "clearml"
    )
    return response


@app.route("/", methods=["POST"])
def upload():
    log.info(f"[S] Upload files")
    results = []
    for filename, file in request.files.items():
        if not filename:
            continue
        file_path = filename.lstrip(os.sep)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=file,
        )
        log.info(f"{filename} uploaded to {bucket_name}.")
        results.append(file_path)

    log.info(f"[E] Upload {len(results)} files")
    return json.dumps(results), 200


@app.route("/<path:path>", methods=["GET"])
def download(path):
    log.info(f"[S] Download file {str(path)}")
    as_attachment = "download" in request.args

    basename = os.path.basename(path)
    _, encoding = mimetypes.guess_type(basename)
    mimetype = "application/octet-stream" if encoding == "gzip" else None

    response = s3_client.get_object(Bucket=bucket_name, Key=path)

    file_stream = response['Body']
    response = Response(
        file_stream,
        content_type=mimetype
    )
    if as_attachment:
        response.headers['Content-Disposition'] = f'attachment; filename={basename}'

    if config.get("fileserver.download.disable_browser_caching", False):
        headers = response.headers
        headers["Pragma-directive"] = "no-cache"
        headers["Cache-directive"] = "no-cache"
        headers["Cache-control"] = "no-cache"
        headers["Pragma"] = "no-cache"
        headers["Expires"] = "0"

    log.info(f"[E] Downloaded file {str(path)}")
    return response


@app.route("/<path:path>", methods=["DELETE"])
def delete(path):
    log.info(f"[S] Delete file {str(path)}")

    existed, message, code = _is_path_existed(path)
    if not existed:
        log.error(f"Error deleting file {str(path)}: {message}")
        abort(Response(f"{message}", code))

    # Delete the uploaded file
    s3_client.delete_object(Bucket=bucket_name, Key=path)
    log.info(f"{path} deleted from {bucket_name}.")

    log.info(f"[E] Delete file {str(path)}")
    return json.dumps(str(path)), 200


def batch_delete():
    log.info(f"[S] Delete files/folders")
    body = request.get_json(force=True, silent=False)
    if not body:
        abort(Response("Json payload is missing", 400))
    files = body.get("files")
    if not files:
        abort(Response("files are missing", 400))

    deleted = {}
    errors = defaultdict(list)
    log_errors = defaultdict(list)

    def record_error(msg: str, file_, path_):
        errors[msg].append(str(file_))
        log_errors[msg].append(str(path_))

    for file in files:
        path = urllib.parse.unquote_plus(file)
        if not path or not path.strip("/"):
            # empty path may result in deleting all company data. Too dangerous
            record_error("Empty path not allowed", file, path)
            continue

        existed, message, code = _is_path_existed(path)
        if not existed:
            record_error(message, file, path)
            continue

        try:
            s3_client.delete_object(Bucket=bucket_name, Key=path)
            log.info(f"{path} deleted from {bucket_name}.")
        except botocore.exceptions.ClientError as ex:
            record_error(ex.strerror, file, path)
            continue
        except Exception as ex:
            record_error(str(ex).replace(str(path), ""), file, path)
            continue

        deleted[file] = str(path)

    for error, paths in log_errors.items():
        log.error(f"{len(paths)} files/folders cannot be deleted due to the {error}")

    log.info(f"[E] Delete {len(deleted)} files/folders")
    return json.dumps({"deleted": deleted, "errors": errors}), 200


if config.get("fileserver.delete.allow_batch"):
    app.route("/delete_many", methods=["POST"])(batch_delete)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", "-p", type=int, default=8081, help="Port (default %(default)d)"
    )
    parser.add_argument(
        "--ip", "-i", type=str, default="0.0.0.0", help="Address (default %(default)s)"
    )
    parser.add_argument("--debug", action="store_true", default=True)
    args = parser.parse_args()

    app.run(debug=args.debug, host=args.ip, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
