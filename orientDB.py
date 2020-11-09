# -*- coding: utf-8 -*-
"""
@title : OrientDB 
@authors : Audrey Bony ; Anne-Victoire de Croutte ; Oriane David ; Léa Pautrel ; Junyi Zhao
"""

######################
### Importations nécessaires
######################
# Installation de pyorient : mettre dans la console ceci -> pip install pyorient --user
import pyorient

######################
### Connexion a la BDD
######################

# Demande à l'utilisateur les informations nécessaires
# (possiblement à changer pour rendre le document final)
userpassword = input("Quel est le mot de passe choisi pour la base de données ? --> ")
db_name = input("Quel est le nom de la base de données à laquelle se connecter ? --> ")

# Initialisation client
client = pyorient.OrientDB("localhost", 2424)
session_id = client.connect("root", userpassword)

# Liste des BDD (pour vérifier si la connexion fonctionne)
client.db_list()

#    # Connexion a la BDD
#    client.db_open( db_name, "admin", userpassword)
#    # Création de la BDD
#    client.db_create( db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )

######################
### Déconnexion de la BDD
######################
client.db_close()
