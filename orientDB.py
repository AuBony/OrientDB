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
import pyorient
from pyorient.ogm import Graph, Config
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

# Importation de la BDD qui marche pas
#client.command("IMPORT DATABASE D:/Agrocampus/M2/UE4-ComputerScienceforBigData/Projet/OrientDB/donnees/data_tolkien.json")

# On créé une BDD tolkien
client.db_create("tolkien", pyorient.DB_TYPE_GRAPH)
print("Base de données 'tolkien' créée.")
print("\n----- Bases de données actuelles de l'utilisateur -----")
print(client.db_list())

# On se connecte à la base de données
client.db_open("tolkien", "root", userpassword)
print("\n----- Connexion à la base de données 'tolkien' -----")

######################
### Création des clusters de la BDD
######################

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
client.command("CREATE CLASS Event EXTENDS V") # 13
client.command("CREATE CLASS Creature EXTENDS AbstractName") # 11
client.command("CREATE CLASS Location EXTENDS AbstractName") # 12

# Edge Classes
client.command("CREATE CLASS BEGETS EXTENDS E") # 14
client.command("CREATE CLASS HASSIBLING EXTENDS E") # 16
client.command("CREATE CLASS LOVES EXTENDS E") # 15
print("Classes créées")

######################
### Création des pripriétés des classes
######################
print("\n----- Création des propriétés des classes (vertex) -----")

# Properties Creature
## Properties EMBEDDEDLIST STRING
emb_str_cr = ["altname", "location"]
for i in range(0, len(emb_str_cr)):
    query = "CREATE PROPERTY Creature.%s EMBEDDEDLIST STRING" % emb_str_cr[i]
    client.command(query)
## Properties STRING
str_cr = ["born","died", "gatewaylink", "gender", "illustrator", "name", "race", "searchname", "significance", "uniquename"]
for i in range(0, len(str_cr)):
    query = "CREATE PROPERTY Creature.%s STRING" % str_cr[i]
    client.command(query)
print("Propriétés de Creature créées")

# Properties Location
## Properties EMBEDDEDLIST STRING
emb_str_lo = ["altname", "area"]
for i in range(0, len(emb_str_lo)):
    query = "CREATE PROPERTY Location.%s EMBEDDEDLIST STRING" % emb_str_lo[i]
    client.command(query)
## Properties STRING
str_lo = ["age","canon", "gatewaylink", "illustrator", "searchname", "significance", "type", "uniquename"]
for i in range(0, len(str_lo)):
    query = "CREATE PROPERTY Location.%s STRING" % str_lo[i]
    client.command(query)
print("Propriétés de Location créées")

# Properties Event
str_ev = ["description", "illustrator", "name", "uniquename"]
for i in range(0, len(str_ev)):
    query = "CREATE PROPERTY Event.%s STRING" % str_ev[i]
    client.command(query)
print("Propriétés de Event créées")
print("Toutes les propriétés ont été créées")

######################
### Création des records dans la BDD
######################
    
print("\n----- Insertion des records dans la base de données -----")

for k in data_lotr.get("records"):
    
    # Classe Creature
    if k.get("@class") == "Creature":
        out_beget_tmp = k.get("out_BEGETS", [])
        for i in range(0, len(out_beget_tmp)):
            out_beget_tmp[i] = "#21"+str(out_beget_tmp[i][3:len(out_beget_tmp[i])])

        in_beget_tmp = k.get("in_BEGETS", [])
        for i in range(0, len(in_beget_tmp)):
            in_beget_tmp[i] = "#21"+str(in_beget_tmp[i][3:len(in_beget_tmp[i])])
            
        in_loves_tmp = k.get("in_LOVES", [])
        for i in range(0, len(in_loves_tmp)):
            in_loves_tmp[i] = "#22"+str(in_loves_tmp[i][3:len(in_loves_tmp[i])])
            
        in_hassibling_tmp = k.get("in_HASSIBLING", [])
        for i in range(0, len(in_hassibling_tmp)):
            in_hassibling_tmp[i] = "#23"+str(in_hassibling_tmp[i][3:len(in_hassibling_tmp[i])])
        
        out_loves_tmp = k.get("out_LOVES", [])
        for i in range(0, len(out_loves_tmp)):
            out_loves_tmp[i] = "#22"+str(out_loves_tmp[i][3:len(out_loves_tmp[i])])
            
        out_hassibling_tmp = k.get("out_HASSIBLING", [])
        for i in range(0, len(out_hassibling_tmp)):
            out_hassibling_tmp[i] = "#23"+str(out_hassibling_tmp[i][3:len(out_hassibling_tmp[i])])
            
        record_k = {"searchname": k.get("searchname"),
                    "uniquename": k.get("uniquename"),
                    "gender": k.get("gender"),
                    "race": k.get("race"),
                    "gatewaylink": k.get("gatewaylink"),
                    "born": k.get("born"),
                    "altname": k.get("altname"),
            		"died": k.get("died"),
            		"significance": k.get("significance"),
                    "name": k.get("name"),
            		"location": k.get("location"),
                    "in_HASSIBLING": in_hassibling_tmp,
                    "out_HASSIBLING": out_hassibling_tmp,
                    "in_BEGETS": in_beget_tmp, # ID BEGETS 14 -> 21
            		"out_BEGETS": out_beget_tmp, # ID BEGETS 14 -> 21
            		"in_LOVES": in_loves_tmp, # ID LOVE 15 -> 22
                    "out_LOVES": out_loves_tmp, 
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
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])

        record_k = {"in": in_tmp, 
            		"out": out_tmp
                    }
        id_cluster = 21
        
    elif k.get("@class") == "LOVES":
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])
            
        record_k = {"in": in_tmp, 
            		"out": out_tmp
                    }
        id_cluster = 22
        
    elif k.get("@class") == "HASSIBLING":
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])
            
        record_k = {"in": in_tmp, 
            		"out": out_tmp
                    }
        id_cluster = 23
    
    # Insertion dans la BDD du record
    if k.get("@class") in list_clusters:
        client.record_create(id_cluster, record_k)
        
print("Insertion des records terminée")


######################
### Déconnexion de la BDD
######################
client.db_close()
