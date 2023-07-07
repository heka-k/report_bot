import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import pandas as pd
import pandahouse
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf
    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)
        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-koteljanets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 7),
}

#cron выражение
schedule_interval = '0 11 * * *'


TOKEN = '6219304136:AAHUr8dQC9QVzK8FWYJzCBIdLoL7gVKULeY'
bot = telegram.Bot(token = TOKEN) 
chat_id = '-938659451'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_bot_kotelyanets():
    
    @task()
    def extract_yesterday_info():
        # Запрос для DAU, CTR, просмотров и лайков
        q = '''
        SELECT COUNT(DISTINCT user_id) AS DAU,
               countIf(user_id, action = 'view') AS views,
               countIf(user_id, action = 'like') AS likes,
               ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 2) AS CTR
        FROM simulator_20230520.feed_actions
        WHERE toDate(time) = yesterday()
        '''
        df = Getch(q).df
        return df
    
    @task()
    def report_message(df):
        msg = f'Лента новостей\n\
-------------------\n\
Отчет за {(datetime.today() - timedelta(days = 1)).strftime("%d.%m.%Y")}\n\
-------------------\n\
DAU: {df.DAU[0]}\n\
Просмотры: {df.views[0]}\n\
Лайки: {df.likes[0]}\n\
CTR: {df.CTR[0]}'                           # Собщение для отчета
        bot.sendMessage(chat_id = chat_id, text = msg)

    @task() 
    def extract_7days_info():
        q_plot = '''
        SELECT COUNT(DISTINCT user_id) AS DAU,
               countIf(user_id, action = 'view') AS views,
               countIf(user_id, action = 'like') AS likes,
               ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 3) AS CTR,
               toDate(time) AS date
        FROM simulator_20230520.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()
        GROUP BY date
        '''
        df_7 = Getch(q_plot).df
        return df_7
    
    @task()
    def DAU_plot(df_7):
        sns.lineplot(x = df_7.date, y = df_7.DAU)
        plt.style.use('bmh')
        plt.title('DAU')
        plt.xlabel('Date')
        plt.ylabel('Users')
        sns.set(rc={'figure.figsize':(11.7,8.27)})
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task()
    def views_plot(df_7):
        sns.lineplot(x = df_7.date, y = df_7.views)
        plt.style.use('bmh')
        plt.title('Просмотры')
        plt.xlabel('Date')
        plt.ylabel('Views')
        sns.set(rc={'figure.figsize':(11.7,8.27)})
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'views_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task()
    def likes_plot(df_7):
        sns.lineplot(x = df_7.date, y = df_7.likes)
        plt.style.use('bmh')
        plt.title('Лайки')
        plt.xlabel('Date')
        plt.ylabel('likes')
        sns.set(rc={'figure.figsize':(11.7,8.27)})
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'likes_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task()
    def CTR_plot(df_7):
        sns.lineplot(x = df_7.date, y = df_7.CTR)
        plt.style.use('bmh')
        plt.title('CTR')
        plt.xlabel('Date')
        plt.ylabel('')
        sns.set(rc={'figure.figsize':(11.7,8.27)})
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'CTR_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    #исполнение
    df = extract_yesterday_info()
    report_message = report_message(df)
    df_7 = extract_7days_info()
    DAU_plot = DAU_plot(df_7)
    views_plot = views_plot(df_7)
    like_plot = likes_plot(df_7)
    CTR_plot = CTR_plot(df_7)
    
report_bot_kotelyanets = report_bot_kotelyanets()