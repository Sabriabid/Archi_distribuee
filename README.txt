Projet Architecture distribuée avec Rayane Haroun,Rayan Ramdani,Sabri Abid

1/ Lancer l'image docker fournie dans le dossier dok-kaf

2/ rentrer dans le container docker pyspark notebook ici on pourra executer le ficher install.py
pour ne pas avoir de probleme lors de l'execution des prochains scripts. 

3/ executer le fichier prediction.py dans ce meme container

4/ En local executer le Producer.py 

5/ Executer dans le container consumer.py

6/ Dans le meme container executer le fichier stream.py avec la commande streamlit run stream.py Pour avoir notre affichage ! 

Nos données sont stockées dans mongo sous forme de documents. 
Elles sont aussi stockées dans le csv streamlit.csv celui ci nous est utile pour la visualisation streamlit.