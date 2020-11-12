#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 12:52:42 2020

@author: kritwishmondal
"""

import sqlite3
import pandas as pd
import warnings
import glob
warnings.filterwarnings("ignore")

import tweepy
import time
from keys import API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_SECRET_TOKEN #Edit the key file with your generated keys

consumer_key = API_KEY
consumer_secret = API_SECRET_KEY
access_token = ACCESS_TOKEN
access_token_secret = ACCESS_SECRET_TOKEN

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

db_files = glob.glob('data/*.db')

data = pd.DataFrame()
for db_file in db_files:
    conn = sqlite3.connect(db_file)

    c = conn.cursor()

    temp = pd.read_sql("SELECT tweet_id FROM geo", conn)

    data = data.append(temp)

data.reset_index(drop=True,inplace=True)

tweet_data = pd.DataFrame()
last_save = 0
for i in range(len(data)):
    tweet_id = data.iloc[i][0]

    try:
        start = time.time()
        tweet = api.get_status(id=tweet_id,tweet_mode='extended')

        tweet_data = tweet_data.append({'tweet_id':int(tweet_id),
                                        'created_at':tweet.created_at,
                                        'username':tweet.user.screen_name,
                                        'name':tweet.user.name,
                                        'text':tweet.full_text,
                                        'likes':tweet.favorite_count,
                                        'following':tweet.user.friends_count,
                                        'followers':tweet.user.followers_count,
                                        'status_count':tweet.user.statuses_count,
                                        'city':tweet.place.name,
                                        'country':tweet.place.country},ignore_index=True)

        tweet_data['tweet_id'] = tweet_data['tweet_id'].astype('int64')

        print('\nTry i:',i)
        print(tweet_id,tweet.user.name)
        print("Last Saved at:",last_save)
        end = time.time()

        time.sleep(1-(end-start))

    except:
        pass

    if i==last_save+900:
        tweet_data.to_csv('COVID19_IND_Tweets2.csv',encoding="utf-8",index=False)
        print('Tweet Data CSV updated at i='+str(i))
        last_save = i

tweet_data.to_csv('COVID19_IND_Tweets2.csv',encoding="utf-8",index=False)
