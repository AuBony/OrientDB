# -*- coding: utf-8 -*-
"""
@title : OrientDB 
@authors : Audrey Bony ; Anne-Victoire de Croutte ; Oriane David ; Léa Pautrel ; Junyi Zhao
"""

######################
### Importations nécessaires
######################
# Libraries
# Installation de pyorient : mettre dans la console ceci -> pip install pyorient --user
# code magique de Oriane -> pip install --upgrade git+https://github.com/OpenConjecture/pyorient.git
import pyorient    # Lien Python et OrientDB
import json        # Importation du fichier de données json

# Données
data_lotr = json.load(open('./donnees/data_tolkien.json', 'r'))
    
######################
### Connexion a la BDD
######################

# Demande à l'utilisateur les informations nécessaires
# (possiblement à changer pour rendre le document final)

userpassword = input("Quel est le mot de passe choisi pour la base de données ? --> ")

# Initialisation client
client = pyorient.OrientDB("localhost", 2424)
client.set_session_token(True)
session_id = client.connect("root", userpassword)

# On supprime la BDD tolkien si elle existe déjà pour la recréer avec la suite du script
if client.db_exists("tolkien"):
    client.db_drop("tolkien")
    print("Suppression de la base de donnée 'tolkien' pour la recréer")

# On créé une BDD tolkien
client.db_create("tolkien")
print("Base de données 'tolkien' crée.")
print("\n----- Bases de données actuelles de l'utilisateur -----")
print(client.db_list())

# On se connecte à la base de données
client.db_open("tolkien", "root", userpassword)
print("\n----- Connexion à la base de données 'tolkien' -----")

######################
### Création des clusters de la BDD
######################
# Ancienne version à ne pas supprimer au cas où ça bugue avec la nouvelle version de list_clusters
#    list_clusters = ["internal", "index","manindex", # ID 0 à 2
#                     "default", "orole", "ouser", # 3 à 5
#                     "ofunction", "oschedule", "orids", # 6 à 8
#                     "v","e","creature", # 9 à 11
#                     "location","event","begets", # 12 à 14
#                     "loves","hassibling", "_studio", # 15 à 17
#                     "osecuritypolicy"]# 18

list_clusters = ["CREATURE", "LOCATION","EVENT","BEGETS", "LOVES","HASSIBLING"]

for clust in list_clusters:
    client.data_cluster_add(clust, pyorient.CLUSTER_TYPE_PHYSICAL)
    print('Cluster physique créé : %s' % clust)

######################
### Création des records dans la BDD
######################
    
for k in data_lotr.get("records"):
#    print(k) 
    
    # Choix de l'ID en fonction du nom du cluster
    if k.get("@class") == "CREATURE":
        id_cluster = 18
    elif k.get("@class") == "LOCATION":
        id_cluster = 19
    elif k.get("@class") == "EVENT":
        id_cluster = 20
    elif k.get("@class") == "BEGETS":
        id_cluster = 21
    elif k.get("@class") == "LOVES":
        id_cluster = 22
    elif k.get("@class") == "HASSIBLING":
        id_cluster = 23
        
    # Sélection des records pour lesquels @class in list_clusters
    if k.get("@class") in list_clusters:
        print(k)



######################
### Déconnexion de la BDD
######################
# client.shutdown('root', userpassword)
