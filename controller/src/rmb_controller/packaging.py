"""Build self-contained Linux upload packages from the exact published task contract."""
import gzip
import hashlib
import json
from pathlib import Path
import tarfile

from .artifacts import json_bytes, write_new

WORKER_FILES=('__init__.py','worker.py','rmb_contract.py','rmb_uploader.py','requirements.txt','Dockerfile','README.md','oss.env.example')


def make_archive(path,root,files):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as output:
        with gzip.GzipFile(filename='',mode='wb',fileobj=output,mtime=0) as compressed:
            with tarfile.open(fileobj=compressed,mode='w|',format=tarfile.PAX_FORMAT) as archive:
                for source in sorted(files,key=lambda p:p.relative_to(root).as_posix()):
                    if source.is_symlink() or not source.resolve().is_relative_to(root.resolve()):
                        raise ValueError('Unsafe package input')
                    info=tarfile.TarInfo(source.relative_to(root).as_posix())
                    info.size=source.stat().st_size;info.mode=0o644;info.mtime=0
                    with source.open('rb') as handle:archive.addfile(info,handle)
    digest=hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(1024*1024),b''):digest.update(block)
    return digest.hexdigest()


def package_worker_and_batches(staging):
    source_root=Path(__file__).resolve().parents[3]/'vm_worker'
    hashes={};worker=[]
    for filename in WORKER_FILES:
        target=staging/'worker'/filename
        hashes[target.relative_to(staging).as_posix()]=write_new(target,(source_root/filename).read_bytes())
        worker.append(target)
    bundle=staging/'vm_worker_bundle.tar.gz'
    hashes[bundle.name]=make_archive(bundle,staging,worker)
    packages={}
    for part in range(1,21):
        name=f'batch-{part:02d}'
        files=worker+[p for p in (staging/name).rglob('*') if p.is_file()]
        target=staging/'packages'/(name+'.tar.gz')
        digest=make_archive(target,staging,files)
        relative=target.relative_to(staging).as_posix();hashes[relative]=digest
        packages[name]={'path':relative,'sha256':digest,'bytes':target.stat().st_size,
                        'contains':[name+'/', 'worker/']}
    hashes['packages/manifest.json']=write_new(staging/'packages/manifest.json',json_bytes(packages))
    return hashes
