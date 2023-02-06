from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer

import pandas as pd
import pymongo
from datetime import datetime
import re
import random
from queue import Queue


# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1)) as dag:

    uri = 'mongodb+srv://admin:uvsqawsgroupe17@cluster0.nkdni.mongodb.net/?retryWrites=true&w=majority'

    def get_list_id(ti):
        client = pymongo.MongoClient(uri)

        result = client["SOA"]["TWEETS"].find({}, {"_id": 0, "id": 1})
        
        list_id = [elem["id"] for elem in result]

        ti.xcom_push(key='list_id', value=list_id)

    def Recup_Auteur(ti):
        client = pymongo.MongoClient(uri)
        list_id = ti.xcom_pull(key='list_id', task_ids='get_list_id_task')
        auteurs = []
        for id_publication in list_id:
            auteurs.append(client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1}).get("author_id"))
        return auteurs

    def Recup_Hashtags(ti):
        client = pymongo.MongoClient(uri)
        list_id = ti.xcom_pull(key='list_id', task_ids='get_list_id_task')

        list_hashtags = []
        for id_publication in list_id:
            result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "authorid": 1})
            publication = result.get("text")
            Hashtags = re.findall(r"(#+[a-zA-Z0-9()]{1,})", publication)
            list_hashtags.append(Hashtags)
        print(list_hashtags)
        return list_hashtags

    def Identification_Topics(ti):
        client = pymongo.MongoClient(uri)
        list_id = ti.xcom_pull(key='list_id', task_ids='get_list_id_task')

        list_topics = []
        for id_publication in list_id:
            result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
            Topics = ["Politique", "Scientifique", "Finance", "Santé", "Culture", "People", "Sport"]
            Pics = random.sample(Topics, random.randint(1, 4))

            list_topics.append(Pics)
        return list_topics
    
    def Analyse_Sentiment(ti):
        client = pymongo.MongoClient(uri)
        list_id = ti.xcom_pull(key='list_id', task_ids='get_list_id_task')

        list_sentiments = []
        for id_publication in list_id:
            result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
            publication = result.get("text")
            blob = TextBlob(publication,pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

            list_sentiments.append(blob.sentiment[0])
        return list_sentiments

    def write_db(ti):
        client = pymongo.MongoClient(uri)

        auteurs = ti.xcom_pull(key='return_value', task_ids='Recup_Auteur_task')
        hashtags = ti.xcom_pull(key='return_value', task_ids='Recup_Hashtags_task')
        topics = ti.xcom_pull(key='return_value', task_ids='Identification_Topics_task')
        sentiments = ti.xcom_pull(key='return_value', task_ids='Analyse_Sentiment_task')
        list_id = ti.xcom_pull(key='list_id', task_ids='get_list_id_task')

        for ind,id_publication in enumerate(list_id):
            key = {"Publication": id_publication}
            data = {"Publication": id_publication, "Auteur": auteurs[ind], "Hashtags": hashtags[ind], "Sentiments": sentiments[ind],
                    "Topics": topics[ind]}
            client["SOA"]["Airflow_Tweets"].update_one(key, {"$setOnInsert": data}, upsert=True)

        return "Everything good"

    get_list_id_task = PythonOperator(task_id='get_list_id_task', python_callable=get_list_id,dag=dag)
    Recup_Auteur_task = PythonOperator(task_id='Recup_Auteur_task', python_callable=Recup_Auteur,dag=dag)
    Recup_Hashtags_task = PythonOperator(task_id='Recup_Hashtags_task', python_callable=Recup_Hashtags, dag=dag)
    Identification_Topics_task = PythonOperator(task_id='Identification_Topics_task', python_callable=Identification_Topics, dag=dag)
    Analyse_Sentiment_task = PythonOperator(task_id='Analyse_Sentiment_task', python_callable=Analyse_Sentiment, dag=dag)
    write_db_task = PythonOperator(task_id='write_db_task', python_callable=write_db, dag=dag)


    get_list_id_task >> [Recup_Auteur_task,Recup_Hashtags_task,Identification_Topics_task,Analyse_Sentiment_task] >> write_db_task