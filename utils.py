import os
from urllib.parse import urlparse


def is_valid_url(path):
    try:
        result = urlparse(path)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def is_preload_needed(url):
    if url.startswith('upload') or url.startswith('/upload'):
        url = '/data' + ('' if url.startswith('/') else '/') + url

    is_uploaded_file = url.startswith('/data/upload')
    is_local_storage_file = url.startswith('/data/') and '?d=' in url
    is_cloud_storage_file = url.startswith('s3:') or url.startswith('gs:') or url.startswith('azure-blob:')
    path_exists = os.path.exists(url)

    return (
        is_uploaded_file
        or is_local_storage_file
        or is_cloud_storage_file
        or is_valid_url(url)
        or path_exists
    )


