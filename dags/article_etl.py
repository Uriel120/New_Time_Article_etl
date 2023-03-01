"""
Ecriture du dag
"""
import os
import datetime
import re
import pandas as pd
import spacy
from pynytimes import NYTAPI

from spacytextblob.spacytextblob import SpacyTextBlob
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


nlp = spacy.load("en_core_web_sm")
nlp.add_pipe('spacytextblob')

KEY = Variable.get('KEY')
CHEMIN = Variable.get('chemin')


def extract(api_key,ti):
    """
    Cette fonction nous permet d'extraire des données des articles de the New york times
    En utilisant son API et une librairie de python

    :param api_key: Clé d'API pour accéder à l'API de the New York Times
    :type api_key: str

    :return: Liste de dictionnaires contenant les données des articles
    :rtype: list
    """
    print(api_key)
    # Instancier une instance de la classe NYTAPI avec la clé d'API spécifiée
    nyt_api = NYTAPI(api_key)

    # Définir les dates de début et de fin de la recherche
    today = datetime.date.today()
    begin_date = today
    end_date = today

    # Rechercher les articles dans la période spécifiée
    articles = nyt_api.article_search(
        results=300,
        dates={"begin": begin_date, "end": end_date},
        options={"sort": "oldest"}
    )

    # Extraire les données d'article et les stocker dans une liste
    article_data = []
    for article in articles:
        article_data.append({
            'titre': article['headline']['main'],
            'resume': article['abstract'],
            'date': article['pub_date'],
            'auteur': article['byline']['original'] if 'byline' in article else None,
            'categorie': article['news_desk'] if 'news_desk' in article else None,
            'url': article['web_url']
            })
        # Retourner la liste de données d'article
        #ti.xcom_push(key='article_data', value=article_data)
    return article_data


def get_sentiment(text):
    """
    Cette fonction nous permet d'évaluer la polarité d'un texte positif, négatif ou neutre
    en utilisant son score.

    :param text: Le texte pour lequel la polarité doit être évaluée
    :type text: str

    :return: Un tuple contenant le score de polarité et l'étiquette correspondante
    :rtype: tuple
    """
    doc = nlp(text)
    sentiment = doc._.blob.polarity
    sentiment = round(sentiment, 2)
    if sentiment > 0:
        sent_label = "Positive"
    elif sentiment == 0:
        sent_label = "Neutre"
    else:
        sent_label = "Negative"
    return sentiment, sent_label


def create_folder_structure(data,chemin):
    """
    Cette fonction nous permet de sauvegarder le dataframe dans notre système de fichiers
    selon une arborescence.
    """
    #data = ti.xcom_pull(task_ids=['transform_data'],key='processed_data')
    # Créer le dossier principal pour les données
    data_folder = chemin
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Récupérer la date de récupération des données
    retrieval_date = datetime.datetime.today().strftime("%Y-%m-%d")

    # Créer le dossier pour la date de récupération des données
    retrieval_folder = os.path.join(data_folder, retrieval_date)
    if not os.path.exists(retrieval_folder):
        os.makedirs(retrieval_folder)

    # Enregistrer le dataframe dans un fichier csv dans le dossier de récupération des données
    filename = "dataframe.csv"
    filepath = os.path.join(retrieval_folder, filename)
    data.to_csv(filepath, index=False)


def transform_load(ti,chemin):
    """
    Cette fonction nous permet de transformer le dataframe en appliquant
    des traitements mais aussi de l'enrichir
    """
    article_data = ti.xcom_pull(task_ids=['extract_data'])
    article_data = article_data[0]

    #articles = XCom.get_many(task_ids=['extract_data'])
    # Convertir la liste de dictionnaires en une chaîne JSON
    data = pd.DataFrame(columns=['titre', 'resume', 'date', 'auteur', 'categorie', 'url'])
    for i in range(len(article_data)):
        #print(len(articles))
        #print(len(articles))
        #print(type(articles))
        article = article_data[i]
        #print(article)
        # Extraire les champs pertinents pour le DataFrame
        data = data.append({
            'titre': article['titre'],
            'resume': article['resume'],
            'date': article['date'],
            'auteur': article['auteur'],
            'categorie': article['categorie'],
            'url': article['url']
        }, ignore_index=True)

    # Créer un DataFrame à partir de la liste de données d'article
    #print(data.shape)
    #print([x for x in data.columns])
    #data.to_csv('data/essai.csv')

    # Prétraiter le texte des colonnes "titre" et "resume"
    data["titre"] = data["titre"].apply(lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x))
    data["titre"] = data["titre"].apply(lambda x: re.sub(r"\s+", " ", x))
    data["titre"] = data["titre"].apply(lambda x: x.lower())

    data["resume"] = data["resume"].apply(lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x))
    data["resume"] = data["resume"].apply(lambda x: re.sub(r"\s+", " ", x))
    data["resume"] = data["resume"].apply(lambda x: x.lower())

    # Prétraiter la colonne "date"
    data["date"] = pd.to_datetime(data["date"])

    # Prétraiter la colonne "auteur"
    data["auteur"] = data["auteur"].fillna("Inconnu")

    # Prétraiter la colonne "categorie"
    data["categorie"] = data["categorie"].fillna("Inconnu")
    data["categorie"].replace('', 'inconnu', inplace=True)

    # Prétraiter la colonne "entities"
    data["entities"] = data["resume"].apply(lambda x:
                                        [(ent.text, ent.label_) for ent in nlp(x).ents]
                                        if len([(ent.text, ent.label_) for ent in nlp(x).ents]) > 0
                                        else "inconnu")

    # Ajouter une colonne "sentiment" et "sentiment_label"
    data[["sentiment", "sentiment_label"]] = data["resume"].apply(lambda x:
                                                                pd.Series(get_sentiment(x))
                                                                if isinstance(x, str) and len(x) > 0
                                                                else pd.Series([0, "Unknown"]))

    # Retourner le DataFrame
    create_folder_structure(data,chemin)
    print('tranform et load reussie')




default_args = {
    "owner": "toffe",
    'depends_on_past': False,
    'max_active_run':1,
}

article_dag = DAG("article_2",
                  default_args=default_args,
                  start_date=datetime.datetime(2023, 2, 28),
                  catchup = False,
                  schedule="30 19 * * *")

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract,
    op_kwargs={'api_key':KEY},
    dag=article_dag,
    #do_xcom_push = True
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_load,
    op_kwargs={'chemin':CHEMIN},
    #provide_context=True,
    dag=article_dag,
    do_xcom_push = True
)


extract_task>>transform_task
