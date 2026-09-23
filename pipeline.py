"""CityBikes ELT stages. Raw JSON is preserved alongside typed columns."""
import hashlib
import json
import logging
import os
from pathlib import Path
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parent
log = logging.getLogger(__name__)

def pg():
    return psycopg2.connect(os.environ['PG_DSN'], connect_timeout=15)

def read_source():
    networks = json.loads((ROOT/'data/networks.json').read_text('utf-8'))['networks']
    stations = []
    for path in sorted((ROOT/'data').glob('stations_*.json')):
        network_id = path.stem.removeprefix('stations_')
        for original in json.loads(path.read_text('utf-8'))['stations']:
            row = dict(original)
            row['network_id'] = network_id
            extra = row.get('extra') or {}
            # Files have no station ID: use source UID within network, stable fallback otherwise.
            row['station_id'] = str(row.get('id') or extra.get('uid') or hashlib.sha256(
                json.dumps([row.get('name'),row.get('latitude'),row.get('longitude')],ensure_ascii=False).encode()).hexdigest())
            row['free_bikes'] = row.get('free_bikes',row.get('bikes'))
            row['empty_slots'] = row.get('empty_slots',row.get('free'))
            row['raw_payload'] = original
            stations.append(row)
    return networks, stations

def load_postgres():
    networks, stations = read_source()
    ids = {n['id'] for n in networks}
    if len(ids) != len(networks): raise ValueError('Duplicate network IDs')
    keys = {(s['network_id'],s['station_id']) for s in stations}
    if len(keys) != len(stations): raise ValueError('Duplicate station IDs')
    if any(s['network_id'] not in ids for s in stations): raise ValueError('Unknown network')
    def js(value): return None if value is None else Json(value)
    columns = ['id','name','location','href','company','gbfs_href','system','source','ebikes','scooters','instances','license']
    json_columns = {'location','company','instances','license'}
    network_rows = [tuple(js(n.get(k)) if k in json_columns else n.get(k) for k in columns)+(Json(n),) for n in networks]
    station_rows=[]
    for s in stations:
        timestamp = datetime.fromisoformat(s['timestamp'].replace('Z','+00:00'))
        if timestamp.tzinfo is None: raise ValueError('Station timestamp has no timezone')
        station_rows.append((s['station_id'],s['network_id'],s.get('name'),s.get('latitude'),s.get('longitude'),
            timestamp,s['free_bikes'],s['empty_slots'],js(s.get('extra')),Json(s['raw_payload'])))
    conn = pg()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute((ROOT/'sql/schema.sql').read_text('utf-8'))
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('gilach-citybikes',0))")
                # Complete snapshot replacement is atomic and removes obsolete records.
                cur.execute('DELETE FROM gilach_stations')
                cur.execute('DELETE FROM gilach_networks')
                execute_values(cur,'INSERT INTO gilach_networks VALUES %s',network_rows)
                execute_values(cur,'INSERT INTO gilach_stations VALUES %s',station_rows)
    finally: conn.close()
    log.info('Loaded networks=%s stations=%s station_networks=%s',len(networks),len(stations),len({s['network_id'] for s in stations}))
    return {'networks':len(networks),'stations':len(stations)}

def spark_session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master('local[2]').appName('citybikes-mart')
        .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','2')
        .config('spark.sql.session.timeZone','UTC').config('spark.ui.enabled','false')
        .config('spark.hadoop.dfs.replication','1')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())

def normalize_update_time(F, value):
    return F.when(value.rlike(r'^\d{13}$'),F.timestamp_seconds(value.cast('double')/1000))\
        .when(value.rlike(r'^\d{10}(\.\d+)?$'),F.timestamp_seconds(value.cast('double')))\
        .otherwise(F.to_timestamp(value))

def transform_hdfs():
    from pyspark.sql import functions as F, types as T
    conn = pg()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id,name,location::text,company::text,system FROM gilach_networks ORDER BY id')
            networks = cur.fetchall()
            cur.execute('SELECT network_id,station_id,name,latitude,longitude,timestamp::text,free_bikes,empty_slots,extra::text FROM gilach_stations ORDER BY network_id,station_id')
            stations = cur.fetchall()
    finally: conn.close()
    spark = spark_session()
    try:
        ns = 'network_id string, network_name string, location_json string, company_json string, system string'
        ss = 'network_id string, station_id string, station_name string, station_latitude double, station_longitude double, observed_at_raw string, free_bikes int, empty_slots int, extra_json string'
        n = spark.createDataFrame(networks,ns)
        s = spark.createDataFrame(stations,ss)
        n = n.withColumn('location',F.from_json('location_json','latitude double, longitude double, city string, country string'))
        n = n.withColumn('company',F.from_json('company_json',T.ArrayType(T.StringType())))
        extra_schema = T.StructType([T.StructField(k,t) for k,t in [
            ('renting',T.BooleanType()),('returning',T.BooleanType()),('last_updated',T.StringType()),
            ('slots',T.IntegerType()),('ebikes',T.IntegerType()),('has_ebikes',T.BooleanType()),
            ('payment',T.ArrayType(T.StringType())),('payment-terminal',T.BooleanType()),
            ('address',T.StringType()),('post_code',T.StringType())]])
        s = s.withColumn('extra',F.from_json('extra_json',extra_schema))
        last = F.col('extra.last_updated')
        last_time = normalize_update_time(F, last)
        joined = n.join(s,'network_id','left')
        mart = joined.select('network_id','network_name',F.col('location.latitude').alias('network_latitude'),
            F.col('location.longitude').alias('network_longitude'),F.col('location.city').alias('city'),
            F.col('location.country').alias('country'),'company','system',
            F.when(F.col('company').isNotNull(),F.size('company')).alias('companies_count'),
            'station_id','station_name','station_latitude','station_longitude',
            F.to_timestamp('observed_at_raw').alias('observed_at'),'free_bikes','empty_slots',
            F.col('extra.renting').alias('renting'),F.col('extra.returning').alias('returning'),
            last_time.alias('last_updated'),F.col('extra.slots').alias('slots'),F.col('extra.ebikes').alias('ebikes'),
            F.col('extra.has_ebikes').alias('has_ebikes'),F.col('extra.payment').alias('payment'),
            F.col('extra.payment-terminal').alias('payment_terminal'),F.col('extra.address').alias('address'),
            F.col('extra.post_code').alias('post_code'))
        for field in mart.schema.fields:
            if isinstance(field.dataType,T.StringType): mart=mart.withColumn(field.name,F.trim(F.col(field.name)))
        for name in ['company','payment']:
            mart=mart.withColumn(name,F.transform(name,lambda x:F.trim(x)))
        mart=mart.withColumn('network_station',F.when(F.col('station_id').isNotNull(),F.concat_ws('/','network_name','station_name')))
        if mart.filter(F.col('station_id').isNotNull() & F.col('observed_at').isNull()).count():
            raise ValueError('Station timestamp parsing failed')
        target=os.environ['HDFS_URI']
        staging=target+'_staging'
        mart.write.mode('overwrite').parquet(staging)
        loaded=spark.read.parquet(staging)
        count=loaded.count()
        assert loaded.select('network_id').distinct().count()==len(networks)
        assert loaded.filter(F.col('station_id').isNotNull()).count()==len(stations)
        fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        # Use target URI's filesystem, not the default local filesystem.
        PathJ=spark._jvm.org.apache.hadoop.fs.Path
        fs=PathJ(target).getFileSystem(spark._jsc.hadoopConfiguration())
        backup=target+'_previous'
        if fs.exists(PathJ(backup)): fs.delete(PathJ(backup),True)
        had_previous=fs.exists(PathJ(target))
        if had_previous and not fs.rename(PathJ(target),PathJ(backup)): raise RuntimeError('Cannot back up mart')
        if not fs.rename(PathJ(staging),PathJ(target)):
            if had_previous: fs.rename(PathJ(backup),PathJ(target))
            raise RuntimeError('Cannot publish mart')
        log.info('HDFS mart saved: rows=%s path=%s',count,target)
        return count
    finally: spark.stop()

def ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=os.environ['CH_HOST'],username=os.environ['CH_USER'],password=os.environ['CH_PASSWORD'],database='citybikes')

def load_clickhouse():
    spark=spark_session()
    client=ch()
    try:
        mart=spark.read.parquet(os.environ['HDFS_URI'])
        mapping={'StringType':'Nullable(String)','DoubleType':'Nullable(Float64)','IntegerType':'Nullable(Int32)',
                 'LongType':'Nullable(Int64)','BooleanType':'Nullable(UInt8)','TimestampType':"Nullable(DateTime64(6, 'UTC'))",'ArrayType':'Array(String)'}
        definitions=[]
        for field in mart.schema.fields:
            typ=mapping[type(field.dataType).__name__]
            if field.name=='network_id':typ='String'
            definitions.append('`'+field.name+'` '+typ)
        ddl=', '.join(definitions)
        client.command('DROP TABLE IF EXISTS citybikes.dm_loading')
        client.command(f'CREATE TABLE citybikes.dm_loading ({ddl}) ENGINE=MergeTree ORDER BY (network_id,ifNull(station_id,\'\'))')
        batch=[]
        arrays={i for i,f in enumerate(mart.schema.fields) if type(f.dataType).__name__=='ArrayType'}
        # Bounded batches; no full mart collect into the Python driver.
        for row in mart.toLocalIterator():
            values=list(row)
            for i,v in enumerate(values):
                if i in arrays and v is None: values[i]=[]
                elif isinstance(v,datetime): values[i]=v.replace(tzinfo=timezone.utc)
                elif isinstance(v,bool): values[i]=int(v)
            batch.append(values)
            if len(batch)>=500:
                client.insert('dm_loading',batch,column_names=mart.columns);batch=[]
        if batch:client.insert('dm_loading',batch,column_names=mart.columns)
        total=client.query('SELECT count() FROM dm_loading').first_row[0]
        assert total==mart.count()
        client.command(f'CREATE TABLE IF NOT EXISTS citybikes.dm ({ddl}) ENGINE=MergeTree ORDER BY (network_id,ifNull(station_id,\'\'))')
        client.command('EXCHANGE TABLES citybikes.dm AND citybikes.dm_loading')
        client.command('DROP TABLE citybikes.dm_loading')
        log.info('ClickHouse mart published: rows=%s',total)
        return total
    finally:
        client.close();spark.stop()

def verify():
    client=ch()
    try:
        row=client.query('SELECT count(),uniqExact(network_id),countIf(station_id IS NOT NULL),uniqExactIf(network_id,station_id IS NOT NULL),sum(free_bikes),sum(empty_slots) FROM dm').first_row
        networks,stations=read_source()
        assert row[1]==len(networks) and row[2]==len(stations)
        assert row[4]==sum(s['free_bikes'] or 0 for s in stations)
        assert row[5]==sum(s['empty_slots'] or 0 for s in stations)
        result=dict(zip(['mart_rows','networks','stations','networks_with_stations','free_bikes','empty_slots'],row))
        result['hdfs_uri']=os.environ['HDFS_URI']
        result['verified_at']=datetime.now(timezone.utc).isoformat()
        (ROOT/'evidence').mkdir(exist_ok=True)
        (ROOT/'evidence/verification.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result))
        return result
    finally:client.close()
