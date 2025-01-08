import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.base import BaseHook

import tweepy
import pandas as pd
from datetime import datetime as dt, timedelta, timezone
import time
import os

class TwitterHook(BaseHook):
    """
    Hook para conectar na API do Twitter via Tweepy,
    usando apenas o Bearer Token.
    As credenciais vêm do Extra JSON da conexão 'my_twitter_conn'.
    """
    def __init__(self, conn_id="my_twitter_conn"):
        super().__init__()
        self.conn_id = conn_id
        self.client = None

    def get_conn(self):
        """
        Retorna um objeto client do Tweepy (se já estiver criado, reaproveita).
        """
        if self.client:
            return self.client

        # Lê a conexão a partir do Airflow
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson

        # Extrai somente o Bearer Token
        bearer_token = extras.get("bearer_token")
        if not bearer_token:
            raise ValueError("Faltou bearer_token no Extra JSON.")

        # Cria o client do Tweepy usando somente Bearer Token (Application-only auth)
        self.client = tweepy.Client(bearer_token=bearer_token)
        return self.client

    def search_tweets(self, query, start_time, end_time, max_results=10):
        """
        Busca tweets recentes usando Tweepy (API v2) e retorna a resposta.
        """
        client = self.get_conn()
        response = client.search_recent_tweets(
            query=query,
            max_results=max_results,
            start_time=start_time,
            end_time=end_time,
            tweet_fields=["id", "author_id", "conversation_id", "created_at", "public_metrics", "text"],
            user_fields=["id", "name", "username", "created_at"],
            expansions=["author_id"]
        )
        return response


def fetch_tweets_task(**context):
    """
    Função chamada pelo PythonOperator para buscar tweets
    sobre "Brasil" de ontem e salvar em JSON.
    """
    # Instancia o Hook que usa a conexão my_twitter_conn
    hook = TwitterHook(conn_id="my_twitter_conn")

    # Define o intervalo de tempo (ex: ontem)
    yesterday = dt.now(timezone.utc) - timedelta(days=1)
    start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    end_time = yesterday.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

    search_query = '"Brasil" -is:retweet'
    max_results = 10

    try:
        response = hook.search_tweets(
            query=search_query,
            start_time=start_time,
            end_time=end_time,
            max_results=max_results
        )

        if response and response.data:
            print(f"Tweets de {start_time} até {end_time}:\n")

            # Mapeia os usuários pelo ID
            users = {u["id"]: u for u in response.includes.get("users", [])}
            lista_tweets = []

            for tweet in response.data:
                print(f"Tweet ID: {tweet.id}")
                print(f"Autor ID: {tweet.author_id}")
                print(f"Thread (Conversation ID): {tweet.conversation_id}")
                print(f"Data de Criação: {tweet.created_at}")
                print(f"Texto: {tweet.text}")
                print(f"Métricas Públicas: {tweet.public_metrics}\n")

                user = users.get(tweet.author_id)
                if user:
                    print(f"Nome do Autor: {user.name}")
                    print(f"Username do Autor: {user.username}")
                    print(f"Data de Criação do Usuário: {user.created_at}\n")

                tweet_info = {
                    "tweet_id": tweet.id,
                    "autor_id": tweet.author_id,
                    "conversation_id": tweet.conversation_id,
                    "data_criacao_tweet": tweet.created_at.isoformat(),
                    "texto": tweet.text,
                    "likes": tweet.public_metrics["like_count"],
                    "retweets": tweet.public_metrics["retweet_count"]
                }

                if user:
                    tweet_info["nome_autor"] = user.name
                    tweet_info["username_autor"] = user.username
                    tweet_info["data_criacao_autor"] = user.created_at.isoformat()

                lista_tweets.append(tweet_info)

            # Converte a lista de dicionários em DataFrame
            df = pd.DataFrame(lista_tweets)

            # Timestamp para nome de arquivo
            now = dt.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")

            # Nome do arquivo JSON
            nome_arquivo = f"tweets_brasil_{timestamp_str}.json"

            # Salva em JSON (indentado)
            df.to_json(nome_arquivo, orient="records", force_ascii=False, indent=4)

            print(f"Dados salvos em {nome_arquivo} com sucesso.")
        else:
            print(f"Nenhum tweet encontrado entre {start_time} e {end_time}.")

    except tweepy.errors.TooManyRequests as e:
        print("Limite de requisições atingido. Tente novamente mais tarde.")
        reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 900))
        wait_time = reset_time - int(time.time())
        print(f"Tempo para resetar: {wait_time} segundos")

    except tweepy.errors.BadRequest as e:
        print(f"Erro de requisição: {e}")


# Definição da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2023, 1, 1),  # Ajuste conforme precisar
    "retries": 0,
}

with DAG(
    dag_id="my_twitter_dag",
    default_args=default_args,
    schedule_interval="0 9 * * *",  # roda todo dia às 9h
    catchup=False
) as dag:

    task_fetch_tweets = PythonOperator(
        task_id="fetch_tweets",
        python_callable=fetch_tweets_task,
        provide_context=True
    )

    task_fetch_tweets
