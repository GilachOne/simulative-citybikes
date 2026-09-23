"""Download the exact three station files used in this project."""
import hashlib
import json
from pathlib import Path
import requests

ROOT=Path(__file__).resolve().parent
out=ROOT/'data'
out.mkdir(exist_ok=True)
station_key='https://disk.yandex.ru/d/ON1QuYSYOwP_Xg'
network_key='https://disk.yandex.ru/d/uUs-x3NrkgQixg'
names=['stations_abu-dhabi-careem-bike.json','stations_bergen-bysykkel.json','stations_beryl-bcp.json']
session=requests.Session()
listing=session.get('https://cloud-api.yandex.net/v1/disk/public/resources',params={'public_key':station_key,'limit':1000},timeout=90)
listing.raise_for_status()
paths={v['name']:v['path'] for v in listing.json()['_embedded']['items']}
sources=[(network_key,None,'networks.json')]+[(station_key,paths[name],name) for name in names]
manifest=[]
for key,path,name in sources:
    params={'public_key':key}
    if path:params['path']=path
    response=session.get('https://cloud-api.yandex.net/v1/disk/public/resources/download',params=params,timeout=90)
    response.raise_for_status()
    download=session.get(response.json()['href'],timeout=180)
    download.raise_for_status()
    json.loads(download.content)
    (out/name).write_bytes(download.content)
    manifest.append({'file':name,'bytes':len(download.content),'sha256':hashlib.sha256(download.content).hexdigest(),'source':key})
    print('Downloaded',name)
(out/'source_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
