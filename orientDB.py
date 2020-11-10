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
# code magique de Oriane -> pip install --upgrade git+https://github.com/OpenConjecture/pyorient.git

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

#On affiche les bases de doonees de l'utilisateur pour vérifier la bonne connexion au serveur
print(client.db_list())

#On se connecte à la base de données
db_name='Tolkien-Arda'
client.db_open(db_name, "root", userpassword)


 

# Création de la BDD
# client.db_create( db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )

######################
### Déconnexion de la BDD
######################

client.shutdown()
