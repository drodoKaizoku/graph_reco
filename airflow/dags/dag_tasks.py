import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from operators.aws_tracker_retrieve_operator import RetrieveFileAws
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from operators.tracker_to_neo4j_operator import trackerFormatData
from operators.user_retrieve_operator import UserRetrieveOperator
from operators.user_to_neo4j_operator import UserToNeoOperator
from operators.remove_user_csv_operator import RemoveUserCsv
from operators.aws_adn_retrieve_operator import AwsS3RetrieveAdn
from operators.adn_to_neo4_operator import adnToNeo4j

from operators.aws_date_tracker_operator import AwsS3RetrieveDateTracker
from operators.aws_date_adn_operator import AwsS3RetrieveDateAdn

from operators.similarity_operator import SimilarityNeo
from operators.ratio_operator import RatioOperator
from operators.relation_score_operator import RelationScoreOperator

from operators.pearson_similarity_operator import SimilarityPearsonNeo
from operators.relation_pearson_operator import RelationPearsonOperator

import neo_request

default_args={

    'owner': 'airflow_nj',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_date_tracker(**kwargs):
    value = kwargs['dag_run'].conf.get("date_tracker")
    print(value)
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key="date_tracker", value=value)

def get_date_adn(**kwargs):
    value = kwargs['dag_run'].conf.get("date_adn")
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key="date_adn", value=value)

def get_xcom(**kwargs):
    value = kwargs['ti'].xcom_pull(task_ids='START')
    print(value)

########################################## PIPELINE DATE ADN ##########################################
dag_adn = DAG(
    dag_id='DATE_ADN',
    default_args=default_args, 
    catchup=False, 
    schedule_interval='@daily'
)    

with dag_adn:
    get_date = PythonOperator(
        task_id='START_ADN',
        python_callable = get_date_adn,
        provide_context = True
    )

    date_adn = AwsS3RetrieveDateAdn(
        task_id='DATE_ADN',
        source="test",
        provide_context = True  
    )

    date_adn_to_neo4j = adnToNeo4j(
        task_id='ADN_DATE_TO_NEO4J',
        source="test", 
        provide_context=True,
        adn='adn_date'
    )

    delete_csv = RemoveUserCsv(
        task_id="DELETE_CSV",
        source="test",
        provide_context=True
    )

get_date >> date_adn >> date_adn_to_neo4j >> delete_csv
########################################## PIPELINE DATE TRACKER ##########################################
dag_tracker = DAG(
    dag_id='DATE_TRACKER',
    default_args=default_args, 
    catchup=False, 
    schedule_interval='@daily'
)

with dag_tracker:

    get_date = PythonOperator(
        task_id='START_TRACKER',
        python_callable = get_date_tracker,
        provide_context = True
    )

    date_tracker = AwsS3RetrieveDateTracker(
        task_id='DATE_TRACKER',
        source="test",
        provide_context = True  
    )
    tracker_date_to_neo = trackerFormatData(
        task_id="INSERT_TRACKER_DATE",
        source="test",
        provide_context=True
    )

    retrieve_user_from_postgres = UserRetrieveOperator(
        task_id="RETRIEVE_USER",
        source="test",
        provide_context=True
    )

    db_user_gender_to_neo = UserToNeoOperator(
        task_id="INSERT_USER",
        source="test",
        provide_context=True
    )

    # similarity = SimilarityNeo(
    #     task_id='SIMILARITY',
    #     source="test", 
    #     provide_context=True,
        
    # )

    delete_csv = RemoveUserCsv(
        task_id="DELETE_CSV",
        source="test",
        provide_context=True
    )

    ratio_csv = RatioOperator(
        task_id="RATIO_CSV",
        source="test",
        provide_context=True
    )

    score = RelationScoreOperator(
        task_id="SCORE_NEO",
        source="test",
        provide_context=True
    )

    pearson_neo = SimilarityPearsonNeo(
        task_id="PEARSON_NEO",
        source="test",
        provide_context=True
    )

    pearson_relation_neo = RelationPearsonOperator(
        task_id="RELATION_PEARSON_NEO",
        source="test",
        provide_context=True
    )


get_date >> date_tracker >> ratio_csv >> tracker_date_to_neo >> retrieve_user_from_postgres >> db_user_gender_to_neo >> score >> pearson_neo >> pearson_relation_neo >> delete_csv


########################################## PIPELINE RECO ##########################################
dag = DAG(
    dag_id='RECO_PIPELINE',
    default_args=default_args, 
    catchup=False, 
    schedule_interval='@daily'
)

with dag:


    retrieve_s3_tracker = RetrieveFileAws(
        task_id='RETRIEVE_TRACKER',
        source="test", 
        provide_context=True
    )

    tracker_to_neo = trackerFormatData(
        task_id="INSERT_TRACKER",
        source="test",
        provide_context=True
    )

    retrieve_from_postgres = UserRetrieveOperator(
        task_id="RETRIEVE_USER",
        source="test",
        provide_context=True
    )

    user_gender_to_neo = UserToNeoOperator(
        task_id="INSERT_USER",
        source="test",
        provide_context=True
    )

    delete_csv = RemoveUserCsv(
        task_id="DELETE_CSV",
        source="test",
        provide_context=True
    )

    retrieve_adn = AwsS3RetrieveAdn(
        task_id='RETRIEVE_ADN',
        source="test", 
        provide_context=True
    )

    adn_to_neo4j = adnToNeo4j(
        task_id='ADN_TO_NEO4J',
        source="test", 
        provide_context=True,
        adn='adn'
    )

    similarity_neo = SimilarityNeo(
        task_id='SIMILARITY',
        source="test", 
        provide_context=True,
        
    )

retrieve_s3_tracker >> retrieve_from_postgres >> tracker_to_neo >> user_gender_to_neo >> delete_csv  >> similarity_neo

retrieve_adn >> adn_to_neo4j >> delete_csv >> similarity_neo

########################################## PIPELINE TEST ##########################################

dag_test = DAG(
    dag_id='TEST_PIPELINE',
    default_args=default_args, 
    catchup=False, 
    schedule_interval='@daily'
)

with dag_test:

    similarity_test = SimilarityNeo(
        task_id='SIMILARITY',
        source="test", 
        provide_context=True,
        
    )

    similarity_test