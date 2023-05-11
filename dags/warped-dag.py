#import libraries
import datetime
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from src.extract import get_artist_info, get_songs_from_playlist, get_song_audio_quality, authenitcate_api
from src.transform import transform_data, validate_data
from src.load import stage_tables, upload_to_database

today_str = datetime.today().strftime('%Y-%m-%d')
month=datetime.today().month
month_name=datetime.today().strftime("%B")
day_of_month=datetime.today().day
day_of_week=datetime.today().weekday()
day_name=datetime.today().strftime("%A")
quarter=(month -1) // 3 +1
#Create default argument for dag
default_args = {
    'owner': 'Ananta Moharana',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes= .5)
}

#Create dag instance
with DAG(
    'Summer_Warped_ETL',
    default_args = default_args,
    description = 'Gets data regarding the songs in my summer 2023 playlist',
    schedule = '@weekly'
    ) as dag:

    #Create start etl task
    start_etl = EmptyOperator(
        task_id = 'StartETL',  
        dag = dag
    )


    #Create the authentication operator
    authenticate=PythonOperator(
        task_id='AuthenticateAPI',
        python_callable=authenitcate_api,
        dag=dag

    )

    #Create the operataro to get the songs from the playlist
    get_songs=PythonOperator(
        task_id='GetSongsFromPlaylist',
        python_callable=get_songs_from_playlist,
        dag=dag

    )

    #Create the operatar to get the artists information from the playlist
    artist_info=PythonOperator(
        task_id='GetArtistInfo',
        python_callable=get_artist_info,
        dag=dag

    )

    #Create the operatar to get the audio features of each song
    audio_quality=PythonOperator(
        task_id='GetAudioQuality',
        python_callable=get_song_audio_quality,
        dag=dag

    )

    #Create the operatar to get the audio features of each song
    data_transformations=PythonOperator(
        task_id='TransformData',
        python_callable=transform_data,
        dag=dag

    )

####################
    #Validate and put Song Fact in S3
    with TaskGroup('ValidateAndLoadSongFact') as PrepareLoadSongFact:
        #Validate the song fact
        validate_song_fact=PythonOperator(
            task_id='ValidateSongFact',
            python_callable=validate_data,
            op_kwargs={
                'columns':['song_id','popularity','date'],
                "table": "song_fact",
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_fact') }}"
            },
            dag=dag
        )
        #Put the song fact in S3
        stage_song_fact=PythonOperator(
            task_id='SongFactToS3',
            python_callable=stage_tables,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_fact.csv',
                'bucketname':'spotify-warped',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_fact') }}"
            },
            dag=dag
        )
        validate_song_fact >> stage_song_fact
####################
    #Validate and put Song Dim in S3
    with TaskGroup('ValidateAndLoadSongDim') as PrepareLoadSongDim:
        #Validate the song dimenesion
        validate_song_dim=PythonOperator(
            task_id='ValidateSongDim',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_dim') }}",
                'columns':['song_name','song_id','explicit','acousticness', 'danceability','duration_ms','energy','instrumentalness','key','liveness','loudness','speechiness','tempo','valence','mode'],
                "table": "song_dim"
            },
            dag=dag
        )
        #Put the song dimension in S3
        stage_song_dim=PythonOperator(
            task_id='SongDimToS3',
            python_callable=stage_tables,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_dim.csv',
                'bucketname':'spotify-warped',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_dim') }}"
            },
            dag=dag
        )
        validate_song_dim >> stage_song_dim
####################
    #Validate and put Song Artist Bridge in S3
    with TaskGroup('ValidateAndLoadSongArtistBridge') as PrepareSongArtistBridge:
        #Validate the song dimenesion
        validate_song_artist_bridge=PythonOperator(
            task_id='ValidateSongArtistBridge',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}",
                'columns':['song_id','artist_id'],
                "table": "song_artist_bridge"
            },
            dag=dag
        )
        #Put the song artist brudge in S3
        stage_song_artist_bridge=PythonOperator(
            task_id='SongArtistBridgeToS3',
            python_callable=stage_tables,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_artist_bridge.csv',
                'bucketname':'spotify-warped',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}"
            },
            dag=dag
        )
        validate_song_artist_bridge >> stage_song_artist_bridge
####################
  #Validate and put Artist Fact in S3
    with TaskGroup('ValidateAndLoadArtistFact') as PrepareArtistFact:
        #Validate the artist fact
        validate_artist_fact=PythonOperator(
            task_id='ValidateArtistFact',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_fact') }}",
                'columns':['artist_id','followers','popularity','date'],
                "table": "artist_fact"
            },
            dag=dag
        )
        #Put the song artist fact in S3
        stage_artist_fact=PythonOperator(
            task_id='ArtistFactToS3',
            python_callable=stage_tables,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/artist_fact.csv',
                'bucketname':'spotify-warped',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_fact') }}"
            },
            dag=dag
        )
        validate_artist_fact >> stage_artist_fact

####################
    #Validate and put Artist Genre in S3
    with TaskGroup('ValidateAndLoadArtistDim') as PrepareArtistDim:
        #Validate the artist dim
        validate_artist_dim=PythonOperator(
            task_id='ValidateArtistDim',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_dim') }}",
                'columns':['artist_name','artist_id','genre'],
                "table": "artist_dim"
            },
            dag=dag
        )
        #Put the artist dim  in S3
        stage_artist_dim=PythonOperator(
            task_id='ArtistDimToS3',
            python_callable=stage_tables,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/artist_dim.csv',
                'bucketname':'spotify-warped',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_dim') }}"
            },
            dag=dag
        )
        validate_artist_dim  >> stage_artist_dim
####################
    #Validate and put Artist Genre in S3
    with TaskGroup('LoadData') as LoadDatabase:
        #Validate the artist dim
        song_dim=PythonOperator(
            task_id='LoadSongDim',
            python_callable=upload_to_database,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_dim.csv',
                'table':"song_dim",
                "s3_bucket":"spotify-warped",
                "primary_key":'song_id'
            },
            dag=dag
        )

        artist_dim=PythonOperator(
            task_id='LoadArtistDim',
            python_callable=upload_to_database,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/artist_dim.csv',
                'table':"artist_dim",
                "s3_bucket":"spotify-warped",
                "primary_key":"artist_id"
            },
            dag=dag
        )

        calendar = PostgresOperator(
            task_id='LoadCalendar',
            postgres_conn_id='postgres',
            sql="""
                INSERT INTO calendar (date, month, monthname,dayofmonth,dayofweek,dayname,quarter)
                VALUES ('{date}', {month}, '{month_name}',{day_of_month},{day_of_week},'{day_name}',{quarter})
                ON CONFLICT (date) DO NOTHING;
                 """.format(date=today_str,month=month,month_name=month_name,day_of_month=day_of_month,day_of_week=day_of_week,day_name=day_name,quarter=quarter),
            dag=dag
        )

        song_fact=PythonOperator(
            task_id='LoadSongFact',
            python_callable=upload_to_database,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_fact.csv',
                'table':"song_fact",
                "s3_bucket":"spotify-warped",
                "primary_key":"song_id,date"

            },
            dag=dag
        )

        song_artist_bridge=PythonOperator(
            task_id='LoadSongArtistBridge',
            python_callable=upload_to_database,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/song_artist_bridge.csv',
                'table':"song_artist_bridge",
                "s3_bucket":"spotify-warped",
                "primary_key":"song_id,artist_id"

            },
            dag=dag
        )

        artist_fact=PythonOperator(
            task_id='LoadArtistFact',
            python_callable=upload_to_database,
            op_kwargs={
                'key':datetime.today().strftime('%Y-%m')+'/artist_fact.csv',
                'table':"artist_fact",
                "s3_bucket":"spotify-warped",
                "primary_key":"artist_id,date"
                

            },
            dag=dag
        )

        song_dim >> artist_dim >> calendar >> song_artist_bridge >> song_fact >> artist_fact


    #Create end etl task
    finished = EmptyOperator(
        task_id = 'ETLDone',  
        dag = dag
    )

    
start_etl >>\
authenticate >> \
get_songs >>\
[artist_info, audio_quality] >>\
data_transformations >>\
[PrepareArtistFact,PrepareArtistDim,PrepareSongArtistBridge,PrepareLoadSongDim,PrepareLoadSongFact] >>\
LoadDatabase >>\
finished