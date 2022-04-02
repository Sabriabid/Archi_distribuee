import time
import tweepy 
import pandas as pd
import numpy as np
from time import strftime
from kafka import KafkaProducer
import json


consumer_key = "MWi2uTim4iPJll4QRpiAJtp0w"
consumer_secret = "sgvmgD9GIgQMPcXdBHIbgrxbCGqfkDszhKCUOps77644UBAhiL"

access_key= "1502361924746596356-1w7sBv1JdgjY77b6ycGZHPJrL3J6yS"
access_secret = "7Y5A8FFBVazCceIxHzsQKJdDiCFtrigqJExn7iTUnBnAR"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth)

p = KafkaProducer(bootstrap_servers=['localhost:9092'])

#user= tweepy.API.me(api)          #recuperer mes tweet
user=api.get_user('CVEannounce')

print("User id : "+ user.id_str)
print(user.name)
print("Description: "+ user.description)
print("Account created at : "+str(user.created_at))
print("Location: "+user.location)
print("Number of tweet: "+ str(user.statuses_count))
print("Number of followers: "+ str(user.followers_count))
print("Following: "+ str(user.friends_count))
print("A member of: "+str(user.listed_count)+"lists.")

statuses = api.user_timeline(id= user.id, count=200)

for status in statuses:
    print("***")
    print("Tweet id : "+ status.id_str)
    t = status.text
    print(t)
    print("retweet count : "+ str(status.retweet_count))
    print("Favorite count: "+str(status.favorite_count))
    d = status.created_at
    dat=d.strftime("%d/%m/%Y")
    print(dat)
    l=[{"text":t,"date":dat}]
    print("Status place: "+str(status.place))
    print("Source : "+ status.source)
    print("Coordinates : "+ str(status.coordinates))
    print(l)
    time.sleep(1)
    
    p.send('test5', json.dumps(l).encode('utf-8'))
    p.flush()
    time.sleep(2)