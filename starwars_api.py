import requests
import mysql.connector
import configparser
import os
import pandas as pd
from tabulate import tabulate
from sqlalchemy import create_engine
from time import sleep

class ApiStarWars():

    def __init__(self, url):
        self.url = url

        config = configparser.ConfigParser()
        path = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
        path = os.path.join(path, 'config.ini')
        config.read(path)
        self.user_mysql = config['MYSQL']['user']
        self.password_mysql = config['MYSQL']['password']
        self.port = config['MYSQL']['port']
        self.host = config['MYSQL']['host']

    @staticmethod
    def pegar_dados_api(url, ultimo_id, tentativas):
        df = []
        contador = 0
        item = ultimo_id
        while True:

            link = f'{url}{item}/'

            res = requests.get(link)

            if res.status_code == 200:
                valor = res.json()
                valor_ajustado = {}
                for x in valor.keys():
                    if type(valor[x]) == list:
                        valor_ajustado[x] = ','.join(valor[x])
                    else:
                        valor_ajustado[x] = valor[x]
                

                df.append(valor_ajustado)
                item += 1
                ultimo_id = item
                contador = 0
                print(f'{link}: Encontrado')
            else:
                if contador == tentativas:
                    break
                else:
                    print(f'{link}: Não encontrado')
                    contador += 1
                    item += 1
        
        return pd.DataFrame(df), ultimo_id

    @staticmethod
    def valida_database(database, conexao):

        with conexao.cursor() as cursor:
            # Criação do banco de dados se não existir
            query_databases = 'SHOW DATABASES'
            cursor.execute(query_databases)
            databases = cursor.fetchall()
            banco_encontrado = any(banco[0] == database for banco in databases)

            if banco_encontrado:
                print(f'Database {database} já existe.')
            else:
                create_database_query = f"""CREATE DATABASE IF NOT EXISTS {database}
                                            CHARACTER SET utf8mb4
                                            COLLATE utf8mb4_general_ci;
                                            """

                cursor.execute(create_database_query)
                print(f'Database {database} criado.')
            

    @staticmethod
    def pega_ultimos_log(conexao, database, tabela):    
        query_create = f"""
                CREATE TABLE IF NOT EXISTS {database}.log_star_wars (
                tabela VARCHAR(100)
                ,ultimo_id int
                )
                """
        
        query_select = f"""select ultimo_id from {database}.log_star_wars where tabela = '{tabela}'"""
        
        with conexao.cursor() as cursor:
            cursor.execute(query_create)
            cursor.execute(query_select)
            try:
                id = cursor.fetchall()[0][0]
                return id
            except:
                return 0
        
            
    @staticmethod
    def atualizar_log(conexao, database, tabela, valor):
        query_delete = f"""
                DELETE FROM {database}.log_star_wars WHERE tabela = '{tabela}';
                """
        query_insert = f"""
                INSERT INTO {database}.log_star_wars VALUES('{tabela}', {valor});
                """
        with conexao.cursor() as cursor:
            cursor.execute(query_delete)
            conexao.commit()
            cursor.execute(query_insert)
            conexao.commit()
        
            

    def testar_conexao(self):
        
        db_config = {
            'user': self.user_mysql,
            'password': self.password_mysql,
            'host': self.host,
            'port': self.port
        }

        while True:
            try:
                conn = mysql.connector.connect(**db_config)
                conn.close()                
                print('Conectado ao banco')
                break
            except:
                print('Tentando conectar no banco.')
                sleep(1)

    def listar_links(self):
        # Batendo na API
        api = requests.get(self.url)

        # Pegando os dados
        valores = api.json()
        self.links = valores

    def popular_banco(self, database, tabelas=[], tentativas=15):

        db_config = {
            'user': self.user_mysql,
            'password': self.password_mysql,
            'host': self.host,
            'port': self.port
        }

        conn = mysql.connector.connect(**db_config)
        ApiStarWars.valida_database(database, conn)

        db_config['database'] = database

        # Engine sqlalchemy
        engine = create_engine(f"mysql+mysqlconnector://{self.user_mysql}:{self.password_mysql}@{self.host}:{self.port}/{database}")

        df_log = []

        if len(tabelas) == 0:
            tabelas = list(self.links.keys())
        
        for tabela in tabelas:

            conn = mysql.connector.connect(**db_config)

            ultimo_item = ApiStarWars.pega_ultimos_log(conn, database, tabela)
            ultimo_item +=1
            
            print(f'Iniciando etapa de carga de {tabela}')
            df, id = ApiStarWars.pegar_dados_api(self.links[tabela], ultimo_item, tentativas)

            if len(df) > 0:
                print(f'Criado df {tabela}, com {len(df)} dados. Iniciando carga na tabela {database}.{tabela}')
                df.to_sql(tabela, con=engine, if_exists='append', index=False)
                print('Carga concluida.')
                ApiStarWars.atualizar_log(conn, database, tabela, id)
                print('Tabela de log atualizada.')
            else:
                print(f'Sem novos dados para a tabela {database}.{tabela}')
                id -=1
            
            df_log.append({'tabela': tabela, 'itens': len(df), 'ultimo_id': id})
            

            print('\n'*5)
        
        df_log = pd.DataFrame(df_log)
        print('Itens adicionados:')
        print(tabulate(df_log, headers='keys', tablefmt='grid', showindex=False))

        conn.close()




if __name__ == '__main__':
    print('Esperando subir o banco.')
    url_api = 'https://swapi.py4e.com/api'
    api = ApiStarWars(url_api)
    api.testar_conexao()
    api.listar_links()
    api.popular_banco('star_wars', tentativas=10)