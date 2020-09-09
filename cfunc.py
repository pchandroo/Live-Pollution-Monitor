import base64
from google.cloud import storage
import pandas as pd
import dask.dataframe as dd
import sqlalchemy

connection_name = ""
db_name = ""
db_user = ""
db_password = ""
query_string = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})

def gcs_pull():
    
    engine = sqlalchemy.create_engine(
     sqlalchemy.engine.url.URL(
        drivername="postgres+pg8000",
        username=db_user,
        password=db_password,
        database=db_name,  
        query=query_string,),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )


    client = storage.Client()
    bucket = client.get_bucket("hourly_files_287823")
    blob = bucket.get_blob("file_tracker.txt")
    content = blob.download_as_string()
    content_str = content.decode('UTF-8')
    lines = content_str.split('\n')
    filename = str(lines[0]) 
    filepath = filename[:-1] + "/*.csv"

    df = dd.read_csv(filepath)
    df_pd = df.compute()


    postgreSQLConnection = engine.connect();
    postgreSQLConnection.execute("DROP TABLE IF EXISTS hourly_val_table;")

    try:
        frame = df_pd.to_sql("hourly_val_table", postgreSQLConnection, if_exists='fail');
    except ValueError as vx:
        print(vx)
    except Exception as ex:  
        print(ex)
    else:
        print("PostgreSQL Table has been created successfully.");
    finally:
        postgreSQLConnection.close();

    blob_old = bucket.blob("file_tracker.txt")
    blob_old.delete()
    blob_new = bucket.blob('file_tracker.txt')
    data_tofile = "\n".join(lines[1:])
    blob_new.upload_from_string(data_tofile)

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    gcs_pull()
