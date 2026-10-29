"""Optional S3-compatible Akamai OSS upload; streaming TAR, bounded multipart buffers."""
import hashlib
import io
import os
from pathlib import Path
import queue
import tarfile
import threading
from urllib.parse import urlsplit


def upload_config():
    names = ['RMB_OSS_ENDPOINT','RMB_OSS_BUCKET','RMB_OSS_REGION','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY']
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise ValueError('OSS configuration is missing: '+', '.join(missing))
    if urlsplit(os.environ['RMB_OSS_ENDPOINT']).scheme!='https':
        raise ValueError('OSS endpoint must use HTTPS')
    return {'endpoint':os.environ['RMB_OSS_ENDPOINT'],'bucket':os.environ['RMB_OSS_BUCKET'],
            'region':os.environ['RMB_OSS_REGION'],'prefix':os.environ.get('RMB_OSS_PREFIX','rmb-youtube').strip('/')}


class TarStream(io.RawIOBase):
    """Forward-only reader fed by tarfile; queue limits memory and propagates producer errors."""
    def __init__(self, files, prefix):
        super().__init__()
        self.files = files;self.prefix = prefix
        self.queue = queue.Queue(maxsize=8)
        self.cancelled = threading.Event();self.buffer = bytearray();self.done = False
        self.digest = hashlib.sha256();self.bytes_read=0
        self.thread = threading.Thread(target=self.produce,name='tar-stream')
        self.thread.start()

    def readable(self):return True
    def seekable(self):return False

    def put(self, value):
        while not self.cancelled.is_set():
            try:self.queue.put(value,timeout=.2);return
            except queue.Full:pass
        raise BrokenPipeError('TAR upload cancelled')

    def produce(self):
        owner=self
        class Sink:
            def write(self, block):
                owner.put(bytes(block));return len(block)
        try:
            with tarfile.open(fileobj=Sink(),mode='w|',format=tarfile.PAX_FORMAT) as archive:
                for path, relative in self.files:
                    path=Path(path)
                    if path.is_symlink() or not path.is_file():raise ValueError('Unsafe TAR input')
                    if relative.startswith('/') or '..' in relative.split('/') or '\\' in relative:raise ValueError('Unsafe TAR member')
                    info=tarfile.TarInfo(self.prefix+'/'+relative)
                    info.size=path.stat().st_size;info.mode=0o644;info.mtime=0
                    with path.open('rb') as handle:archive.addfile(info,handle)
            self.put(None)
        except Exception as exc:
            if not self.cancelled.is_set():self.put(exc)

    def read(self,size=-1):
        if size is None or size<0:
            raise ValueError('Unbounded reads are disabled for streaming TAR')
        while len(self.buffer)<size and not self.done:
            value=self.queue.get()
            if isinstance(value,Exception):raise value
            if value is None:self.done=True;break
            self.buffer.extend(value)
        result=bytes(self.buffer[:size]);del self.buffer[:size]
        self.digest.update(result);self.bytes_read+=len(result)
        return result

    def close(self):
        self.cancelled.set();self.thread.join(timeout=10)
        super().close()


def stream_upload(files, delivery_name, key, config=None, client=None, fingerprint=None, transfer_config=None):
    config=config or upload_config()
    if client is None:
        import boto3
        from botocore.config import Config
        client=boto3.client('s3',endpoint_url=config['endpoint'],region_name=config['region'],
            config=Config(signature_version='s3v4',retries={'max_attempts':4,'mode':'standard'},
                          s3={'addressing_style':'path'},request_checksum_calculation='when_required',
                          response_checksum_validation='when_required'))
    try:
        existing=client.head_object(Bucket=config['bucket'],Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in {'404','NoSuchKey','NotFound'}:raise
    else:
        if fingerprint and existing.get('Metadata',{}).get('rmb_snapshot')==fingerprint and existing['ContentLength']>0:
            return {'bucket':config['bucket'],'key':key,'bytes':existing['ContentLength'],'reused_verified_object':True,'etag':existing.get('ETag')}
        raise ValueError('Object key already exists with different metadata; refusing overwrite')
    if transfer_config is None:
        from boto3.s3.transfer import TransferConfig
        transfer_config=TransferConfig(multipart_threshold=16*1024*1024,multipart_chunksize=16*1024*1024,
                                       max_concurrency=1,use_threads=False)
    stream=TarStream(files,delivery_name)
    try:
        client.upload_fileobj(stream,config['bucket'],key,
            ExtraArgs={'ContentType':'application/x-tar','Metadata':{'rmb_snapshot':fingerprint or ''}},
            Config=transfer_config)
        head=client.head_object(Bucket=config['bucket'],Key=key)
        if head['ContentLength']!=stream.bytes_read:
            raise ValueError('Uploaded object size does not match TAR stream')
        return {'bucket':config['bucket'],'key':key,'bytes':stream.bytes_read,
                'tar_sha256':stream.digest.hexdigest(),'etag':head.get('ETag')}
    finally:stream.close()
