import pandas as pd
from kafka import KafkaConsumer
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType
import streamlit as st
import csv 
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly
import folium
import time
import matplotlib.pyplot as plt
import numpy as np


# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('twitter')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

c = KafkaConsumer('test5', bootstrap_servers=['kafka:9093'], api_version=(2,6,0))


cSchema = StructType([StructField("text", StringType())\
                     ,StructField("date", StringType())])

for msg in c:
    df = pd.DataFrame(json.loads(msg.value))    #charger le message dans un Data-Frame df
    #df1=df[~df['vitesse'].isna()]               #suprimmer les vitesses null
    print(df)    
    dfs = spark.createDataFrame(df,schema=cSchema)    #charger la Data-Frame df dans un Data-Frame SPARK dfs 
    print(dfs.show())                                  #affichage de la dfs
    #print(dfs.agg({'vitesse' : 'mean'}).show())        #calcul et affichage de la moyenne des vitesses
    #print("le nombre d'avions en vol actuellement est de : " , dfs.count())

st.write(
    """
    # TEST:fire:
    """
)

st.dataframe(df)
#plt.savefig('PieChart01.png')
df = dfs.toPandas()


pos = df.sentiment.str.count("Positive").sum()
neg = df.sentiment.str.count("Negative").sum()

x = np.array([pos, neg])
mylabels = [ 'Positive', 'Negative']
fig = plt.figure(figsize=(10, 4))
plt.pie(x, labels = mylabels)
st.pyplot(fig)