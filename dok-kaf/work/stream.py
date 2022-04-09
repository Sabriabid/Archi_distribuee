import pandas as pd
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
from streamlit_autorefresh import st_autorefresh
import json 
import plotly.graph_objects as go
import plotly.express as px

# Titre 
st.write(
    """
    # E-reputation:fire:
    """
)

# Search bar
name = st.text_input("Enter Your name", "Type Here ...") 

# ecriture sur le fichier JSON
if(st.button('Submit')):
    result = name.title() 
    y={"recherche":result}
    st.success(result)
    jsonFile = open("data.json", "w")
    json.dump(y, jsonFile)


#lecture de notre fichier csv et sa transformation en df pendas
df = pd.read_csv('streamlit.csv',	 sep=",")

#affichage du contenu de notre df
st.dataframe(df)


#ici on somme le nombre de Positif et de Negatif pour la repartition sur nos visualisations
pos = df.Sentiment.str.count("Positive").sum()
neg = df.Sentiment.str.count("Negative").sum()

x = np.array([pos, neg])
mylabels = [ 'Positive', 'Negative']

labels = [ 'Positive', 'Negative']
values = x

#Creation de notre Pie
fig3 = px.pie(mylabels, values=values, names=labels,
color_discrete_map={'Sendo':'cyan', 'Tiki':'royalblue','Shopee':'darkblue'})
fig3.update_layout(title="<b>Pourcentage de tweet positifs et negatifs</b>")
st.plotly_chart(fig3)

#Creation de notre histogramme
fig = px.bar(df, labels, y= x , barmode='group', height=400)
st.plotly_chart(fig)

#permet de rafraichir notre affiche automatiquement pour visualiser les ajouts de tweet en temps réel
count = st_autorefresh(interval=3500)
