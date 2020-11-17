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
data_lotr = json.load(open('./donnees/data_tolkien.json', 'r', encoding='utf-8'))
    
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
id_clusters = [18, 19, 20, 21, 22, 23]
list_clusters_vertex = ["Creature", "Location","Event"]
list_clusters_edge = ["BEGETS", "LOVES","HASSIBLING"]

print("\n----- Création des cluster -----")
for i in range(len(list_clusters)):
    query = ("CREATE CLUSTER %s ID %s" % (list_clusters[i], id_clusters[i]))
    client.command(query)
    print('Cluster physique créé : %s' % (list_clusters[i]))


######################
### Création des classes
######################
    
print("\n----- Création des classes -----")
# Vertex Classes
client.command("CREATE CLASS AbstractName EXTENDS V")
client.command("CREATE CLASS Event EXTENDS V") # 13 -> 20
client.command("CREATE CLASS Creature EXTENDS AbstractName") # 11 -> 18
client.command("CREATE CLASS Location EXTENDS AbstractName") # 12 -> 19

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
### Création des records USER dans la BDD
######################
print("\n----- Insertion des records dans la base de données -----")

for k in data_lotr.get("records"):
    # Classe Creature
    if k.get("@class") == "Creature":
        # altname et location : remplacement des " en '
        location = str(k.get('location')).replace('"', "'")
        altname = str(k.get('altname')).replace('"', "'")

        query = ('CREATE VERTEX Creature SET searchname = "' + str(k.get('searchname')) +
                    '", uniquename = "' + str(k.get('uniquename')) +
                    '", gender = "'+ str(k.get('gender')) +
                    '", race = "' + str(k.get('race')) +
                    '", gatewaylink = "' + str(k.get('gatewaylink')) +
                    '", born = "' + str(k.get('born')) +
                    '", altname = "' + str(altname) +
            		'", died = "' + str(k.get('died')) +
            		'", significance = "' + str(k.get('significance')) +
                    '", name = "' + str(k.get('name')) +
            		'", location = "' + str(location) +
            		'", illustrator = "' + str(k.get('illustrator')) + '"')
        client.command(query)
        
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
        client.record_create(id_cluster, record_k)
        
    elif k.get("@class") == "Event":
        record_k = {"uniquename": k.get("uniquename"),
            		"name": k.get("name"),
                    "description": k.get("description"),
            		"illustrator": k.get("illustrator"),
                    }
        id_cluster = 20
        client.record_create(id_cluster, record_k)
        
print("Insertion des records des vertex terminée")


for k in data_lotr.get("records"):
    if k.get("@class") == "BEGETS":
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE BEGETS FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)
        
    elif k.get("@class") == "LOVES":
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE LOVES FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)
        
    elif k.get("@class") == "HASSIBLING":
        in_tmp = "#18"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#18"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE HASSIBLING FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)

print("Insertion des edge terminée")


######################
### Requetes
######################

print("\n\n\n----- Requêtes sur la base de donnée -----\n")

# Requête introductive
print("→ Requête introductive : Quand est né Frodon Saquet et quand est-il mort ?")
requete = "SELECT * FROM Creature WHERE name = 'Frodo Baggins'"
print("Requête demandée : " + requete)
data = client.query(requete)
print("Frodon est né le " + str(data[0].born) + " et est mort le " + str(data[0].died) )

# Premiere requete
print("\n→ Requête 1 : Combien de couples y a-t-il en tout ?")
query1 = client.query("select count(*) from Loves")
print("Il y a "+ str(query1[0].count)+ " couples.")

# Deuxième requête
print("\n→ Requête 2 : Quelles sont les régions les plus peuplées ?")
query2 = client.query("select location, count(*) as regioncount from Creature group by location order by regioncount DESC limit 4 ")
print("Les trois régions les plus peuplées sont:")
for i in range(len(query2)):
    if i==0:
        res = ("Il y a "+ str(query2[i].regioncount)+" personnes dont on ne connait pas la région.")
    else:
        res = ("La région "+ str(query2[i].location)+" est la "+ str(i+1) +"ème plus grande région, et elle héberge "+str(query2[i].regioncount)+" personnes")
    print(res)
    
# Troisième requete
print("\n→ Requête 3 : Combien d'enfants a Samwise Gamgee ?")
query3 = client.query("MATCH {Class: Creature, as: Father, where: (name='Samwise Gamgee')}-BEGETS->{Class: Creature, as: Children} RETURN Children")
print("Samwise a "+str(len(query3))+" enfants.")

#Quatrième requete
print("\n→ Requête 4 : Combien y a t-il de triangle amoureux et qui en fait partis ?")
query4= client.query("MATCH {Class: Creature, as: C1}-LOVES->{Class: Creature, as: C2}-LOVES->{Class: Creature, as: C3} RETURN C1.name as C1, C2.name as C2, C3.name as C3")
print("Il y a "+str(len(query4))+" couples")
for i in range(len(query4)):
    print("Les membres du "+str(i+1)+"ème triangle amoureux sont: "+query4[i].C1+" , "+query4[i].C2+" et "+query4[i].C3)

#Cinquième requête
print("\n→ Requête 5 : Combien de générations directes séparent Isildur et Aragorn II ?")
query5= client.query("SELECT count(*) FROM (SELECT expand(path) FROM (SELECT shortestPath($from, $to) AS path LET  $from = (SELECT FROM Creature WHERE name='Aragorn II'), $to = (SELECT FROM Creature WHERE name='Isildur') UNWIND path))")
print(str(query5[0].count-2)+ " générations séparent Aragorn et Isildur")

#Sixième requête
query6= client.query("SELECT name FROM (SELECT expand(path) FROM (SELECT shortestPath($from, $to) AS path LET  $from = (SELECT FROM Creature WHERE name='Aragorn II'), $to = (SELECT FROM Creature WHERE name='Isildur') UNWIND path) limit 100)")
print(len(query6))
arbre= ""

for i in range(len(query6)):
    print(query6[i].name)
print(arbre)

######################
### Déconnexion de la BDD
######################
client.db_close()
print("\n----- Déconnexion de la base de données 'tolkien' -----\n\n")