from datetime import datetime
from flask import Flask, render_template, request, url_for, redirect,flash
from pymongo import MongoClient
from markupsafe import escape
from bson.objectid import ObjectId
import time
import pandas as pd
import os
import mysql.connector
import redis
import math
import json

USER = "root"
PASSWORD = "YOUR-PASSWORD"

db_sql=mysql.connector.connect(host="localhost",user=USER,password=PASSWORD,database="Twitter_user_data")
mycursor=db_sql.cursor(buffered=True)

app = Flask(__name__)

app.config['SECRET_KEY'] = os.urandom(24)

client = MongoClient("mongodb://localhost:27017/") 
db = client.TweetsDB
tweets = db.tweets_coll

# Cache functionality

r = redis.StrictRedis(host='localhost', port = 6379, db = 0)
p = redis.StrictRedis(host='localhost', port = 6379, db = 1)

class Cache():
    
    def __init__(self,r,p):
        self.r = r
        self.p = p

    def data_modifier(self, data):
        res = {'res': data}
        json_res = json.dumps(res,ensure_ascii=False).encode('utf8')
        return json_res

    def convert_to_num(self, s):
        if s[-1] == 'K':
            return (float(s[0:s.find('.')]) + float(s[s.find('.'):s.find('K')]))/1000
        return float(s[0:s.find('.')]) + float(s[s.find('.'):s.find('M')])
        

    def clear_20_percent(self, cur_mem):
        print('CLEARING')
        # First sorting searches
        keys, idle_times = [], []
        for key in self.r.scan_iter("*"):
            idle = self.r.object("idletime", key)
            keys.append(key)
            idle_times.append(idle)

        keys = [k for i,k in sorted(zip(idle_times, keys))]
        target = math.ceil(len(keys)*0.2)

        # deleting oldest 20% of keys
        for i in range(target):
            self.r.delete(keys.pop())

        # if current cache memory still more than 10 mb [only a safety check]
        cur_mem = self.convert_to_num(self.r.info()['used_memory_human'])
        while(cur_mem > 10):
            self.r.delete(keys.pop())
            cur_mem = self.convert_to_num(self.r.info()['used_memory_human'])

    def push_to_dynamic_cache(self, key, data):
        data = self.data_modifier(data)
        cur_mem = self.convert_to_num(self.r.info()['used_memory_human'])
        if cur_mem < 10:
            self.r.set(key, data)
        else:
            self.clear_20_percent(cur_mem)
            self.r.set(key, data)

    def get_from_dynamic_cache(self, key):
        try:
            return json.loads(self.r.get(key).decode())['res']
        except:
            return -1

    def push_to_top10_cache(self, key, data):
        data = self.data_modifier(data)
        self.p.set(key, data)

    def get_from_top10_cache(self, key):
        try:
            return json.loads(self.p.get(key).decode())['res']
        except:
            return -1

cache = Cache(r,p)

def text_search(search_term,sorting_var = "Retweet_Count"):
    agg_result = db.tweets_coll.aggregate(
        [
           {"$match":{
               "$and":[
                   {"$text":{"$search":"\""+search_term+"\""}},
                   {"Retweet_ID": 0}
               ]}},
           {"$sort": {sorting_var:-1}},
        ]
    )
    output = list(agg_result)
    return output
    
def author_search(search_term,sorting_var = "Retweet_Count"):
    agg_result = db.tweets_coll.aggregate(
        [
           {"$match":{"UserID": search_term}},
           {"$sort": {sorting_var:-1}},
        ]
    )
    output = list(agg_result)
    return output

def hashtag_search(search_term,sorting_var = "Retweet_Count"):
    agg_result = db.tweets_coll.aggregate([
     {
      "$match":{
        "$and":[
              {'Hashtag': { '$elemMatch': { '$eq': search_term} }},
              {"Retweet_ID": 0}
        ]
       }},
        {"$sort": {sorting_var:-1}},
    ])
    output = list(agg_result)
    return output

#Used for Retweet Drill-down
def find_retweets(tweet_id):
    agg_result = db.tweets_coll.aggregate([
     {
      "$match":{"Retweet_ID_str": tweet_id}
      },
        {"$sort": {"created_at":-1}},
    ])
    output = list(agg_result)
    return output

def search_by_screen_name(search_term):
    search_query="""SELECT * FROM user WHERE screen_name='{}';""".format(search_term)
    mycursor.execute(search_query)
    return mycursor.fetchall()

def search_by_user_id(search_term):
    search_query="""SELECT * FROM user WHERE Id={};""".format(search_term)
    mycursor.execute(search_query)
    return mycursor.fetchall()

#Used for Top 10 Users
def users_10():
    mycursor.execute("SELECT * FROM user ORDER BY followers_count DESC LIMIT 10;")
    return mycursor.fetchall()

#Used for Top 10 Tweets
def tweets_10():
    agg_result = db.tweets_coll.aggregate([
        {"$sort": {"Retweet_Count":-1}},
        {"$limit": 10}
    ])
    output = list(agg_result)
    return output

def get_date(d):
    return d['created_at']

def get_retweet(d):
    return d['Retweet_Count']

#Home Page
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method=='POST':
        search_term = request.form['search']
        search_by = request.form['search_by']
        start_date = "2000-01-01"
        end_date = "2030-01-01"
        if request.form['date_from']:
            start_date = request.form['date_from']
        if request.form['date_to']:
            end_date = request.form['date_to']
        
        return redirect(url_for('results',search_by = search_by, search_term = search_term, start_date = start_date, end_date = end_date))
    return render_template('index.html')

#Results Page. Receives imputs from the form on the front page and first checks the cache to see if it's in there
#If not in the cache, then the corresponding queries are performed and the data is displayed on the Results page
@app.route('/results/<search_by>/<search_term>/<start_date>/<end_date>', methods=['GET','POST'])
def results(search_by,search_term, start_date = "2000-01-01", end_date = "2030-01-01"):
    start = time.time()
    search_key = "search " + search_term + " category " + search_by
    cache_results = cache.get_from_dynamic_cache(search_key)
    if cache_results != -1:
        search_results = cache_results
    else:
        if search_by == 'text':
            search_results = text_search(search_term)
            for doc in search_results:
                user_info = search_by_user_id(doc['UserID'])
                del doc['_id']
                try:
                    doc['name'] = user_info[0][2]
                    doc['screen_name'] = user_info[0][3]
                except:
                    continue
                    # search_results.remove(doc)
        elif search_by == "author":
            user_info = search_by_screen_name(search_term)
            if user_info:
                search_results = author_search(user_info[0][0])
                for doc in search_results:
                    doc['name'] = user_info[0][2]
                    doc['screen_name'] = user_info[0][3] 
                    del doc['_id']
            else:
                search_results = []
        elif search_by =="hashtag":
            search_results = hashtag_search(search_term)
            for doc in search_results:
                user_info = search_by_user_id(doc['UserID'])
                del doc['_id']
                try:
                    doc['name'] = user_info[0][2]
                    doc['screen_name'] = user_info[0][3]
                except:
                    continue
                    # search_results.remove(doc)
                
        cache.push_to_dynamic_cache(search_key,search_results)
    end = time.time()
    for doc in search_results:
        if datetime.strptime(doc['created_at'][:10],"%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d") or datetime.strptime(doc['created_at'][:10],"%Y-%m-%d") > datetime.strptime(end_date,"%Y-%m-%d"):
            search_results.remove(doc)
    if request.method =='POST':
        sort_by = request.form['sort_by']
        if sort_by == "relevance":
            search_results.sort(key=get_retweet,reverse=True)
        elif sort_by == "recent":
            search_results.sort(key=get_date, reverse=True)
        elif sort_by == "old":
            search_results.sort(key=get_date)
        return render_template('results.html',results = search_results,search_time = end-start)
    print(len(search_results))
    if len(search_results) == 0:
        flash('NO RESULTS FOUND')
        return redirect(url_for('index'))
    else:
        return render_template('results.html', results = search_results, search_time = end-start)

#Page for User Drill-down, accepts a user_id
@app.route('/user/<user_id>')
def user(user_id):
    user_info = search_by_user_id(user_id)
    if not user_info:
        flash('USER NOT FOUND')
        return redirect(url_for('index'))
    search_results = author_search(user_info[0][0], sorting_var = "created_at")
    return render_template('users.html', user_info = user_info, results = search_results)

#Page for retweet Drill-down, accepts a tweet_id
@app.route('/retweets/<tweet_id>')
def retweets(tweet_id):
    rt_info = find_retweets(tweet_id)
    for doc in rt_info:
            user_info = search_by_user_id(doc['UserID'])
            doc['name'] = user_info[0][2]
            doc['screen_name'] = user_info[0][3]
    tweet = db.tweets_coll.find({'Id_str':tweet_id})
    return render_template('retweets.html', tweet = tweet, retweets = rt_info)

#Page for top 10 users by follower count
@app.route('/top10users')
def top_users():
    start = time.time()
    cache_results = cache.get_from_top10_cache('top10users')
    if  cache_results == -1:
        top_users_ = users_10()
        top_users=[]
        for doc in top_users_:
            doc = list(doc)
            doc[9] = doc[9].strftime("%Y-%m-%d")
            top_users.append(doc)
        cache.push_to_top10_cache('top10users',top_users)
    else:
        top_users = cache_results
    end = time.time()
    return render_template('top10users.html', top_users = top_users, search_time = end-start)

#Page for top 10 tweets by retweet count
@app.route('/top10tweets')
def top_tweets():
    start = time.time()
    cache_results = cache.get_from_top10_cache('top10tweets')
    if cache_results == -1:
        top_tweets = tweets_10()
        for doc in top_tweets:
            del doc['_id']
            user_info = search_by_user_id(doc['UserID'])
            try:
                doc['name'] = user_info[0][2]
                doc['screen_name'] = user_info[0][3]
            except:
                print(doc['UserID'])
        cache.push_to_top10_cache('top10tweets', top_tweets)
    else:
        top_tweets = cache_results
    end = time.time()
    return render_template('top10tweets.html', top_tweets = top_tweets, search_time = end-start)
