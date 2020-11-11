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
print("Base de données 'tolkien' créée.")
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

list_clusters = ["Creature", "Location","Event","BEGETS", "LOVES","HASSIBLING"]

print("\n----- Création des cluster -----")
for clust in list_clusters:
    client.data_cluster_add(clust, pyorient.CLUSTER_TYPE_PHYSICAL)
    print('Cluster physique créé : %s' % clust)


######################
### Création des classes
######################
    
print("\n----- Création des classes -----")
# Vertex Classes
client.command("CREATE CLASS AbstractName EXTENDS V")
client.command("CREATE CLASS Event EXTENDS V")
client.command("CREATE CLASS Creature EXTENDS AbstractName")
client.command("CREATE CLASS Location EXTENDS AbstractName")

# Edge Classes
client.command("CREATE CLASS BEGETS EXTENDS E")
client.command("CREATE CLASS HASSIBLING EXTENDS E")
client.command("CREATE CLASS LOVES EXTENDS E")
print("Classes créées")

######################
### Création des records dans la BDD
######################
    
print("\n----- Insertion des records dans la base de données -----")

for k in data_lotr.get("records"):
#    print(k) 
    id_cluster = 0
    
    # Classe Creature
    if k.get("@class") == "Creature":
        record_k = {"searchname": k.get("searchname"),
                    "uniquename": k.get("uniquename"),
                    "gender": k.get("gender"),
                    "race": k.get("race"),
                    "gatewaylink": k.get("gatewaylink"),
                    "born": k.get("born"),
                    "altname": k.get("altname"),
                    "in_BEGETS": k.get("in_BEGETS"),
            		"died": k.get("died"),
            		"significance": k.get("significance"),
            		"out_BEGETS": k.get("out_BEGETS"),
            		"name": k.get("name"),
            		"location": k.get("location"),
            		"in_LOVES": k.get("in_LOVES"),
            		"illustrator": k.get("illustrator"),
                    }
        id_cluster = 18
    elif k.get("@class") == "Location":
        record_k = {"significance":  k.get("significance"),
                    "area": k.get("area"),
                    "searchname": k.get("searchname"),
                    "uniquename": k.get("uniquename"),
                    "gender": k.get("gender"),
                    "race": k.get("race"),
                    "gatewaylink": k.get("gatewaylink"),
            		"name": k.get("name"),
                    "altname": k.get("altname"),
                    "type": k.get("type"),
            		"age": k.get("age"),
            		"canon": k.get("canon"),
            		"illustrator": k.get("illustrator"),
                    }
        id_cluster = 19
    elif k.get("@class") == "Event":
        record_k = {"uniquename": k.get("uniquename"),
            		"name": k.get("name"),
                    "description": k.get("description"),
            		"illustrator": k.get("illustrator"),
                    }
        id_cluster = 20
    elif k.get("@class") == "BEGETS":
        record_k = {"in": k.get("in"),
            		"out": k.get("out")
                    }
        id_cluster = 21
    elif k.get("@class") == "LOVES":
        record_k = {"in": k.get("in"),
            		"out": k.get("out")
                    }
        id_cluster = 22
    elif k.get("@class") == "HASSIBLING":
        record_k = {"in": k.get("in"),
            		"out": k.get("out")
                    }
        id_cluster = 23
    
    # Insertion dans la BDD du record
    if k.get("@class") in list_clusters:
        client.record_create(id_cluster, record_k)



######################
### Déconnexion de la BDD
######################
# client.shutdown('root', userpassword)
