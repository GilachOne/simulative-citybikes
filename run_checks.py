import sys
import pendulum
sys.path.insert(0,'/opt/airflow/dags')
from airflow.www.app import create_app
from citybikes_dag import dag
from pipeline import verify, spark_session, normalize_update_time
from datetime import datetime
from pyspark.sql import functions as F

create_app()
run=dag.test(execution_date=pendulum.datetime(2025,5,13,tz='UTC'))
assert str(run.state)=='success',run.state
verify()
spark=spark_session()
try:
    values=[('1747056454',),('1747056454000',),('2025-05-12T13:27:34Z',),('2025-05-12T16:27:34+03:00',),(None,)]
    result=spark.createDataFrame(values,'raw string').select(normalize_update_time(F,F.col('raw')).alias('parsed')).collect()
    assert all(r.parsed==datetime(2025,5,12,13,27,34) for r in result[:4])
    assert result[4].parsed is None
    print('TIMESTAMP FORMATS VERIFIED')
finally:spark.stop()
print('CITYBIKES DAG VERIFIED')
