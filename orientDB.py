# -*- coding: utf-8 -*-
"""
@title : OrientDB
@subtitle : Création d'une base de données, requêtes et comparaison avec Neo4j
@authors : Audrey Bony ; Anne-Victoire de Croutte ; Oriane David ; Léa Pautrel ; Junyi Zhao
"""

message = """
----- Message de lancement du script -----

Avant de continuer ce script, vérifiez que :
→ Dans le dossier où ce script est, vous avez bien un dossier 'donnees' avec le document 'data_tolkien.json'
→ Vous avez bien lancé le 'server.bat' de OrientDB
→ Vous avez bien créé une base de données vide locale Neo4j avec Neo4j Desktop, ayant pour user "neo4j" et en password "Neo4J"
→ Vous avez bien fait Start pour cette base de données sur Neo4j Desktop
→ Vous avez bien installé pyorient
    - Dans la console : pip install pyorient
    - Puis mise à jour avec : pip install --upgrade git+https://github.com/OpenConjecture/pyorient.git
→ Vous avez bien installé py2neo (dans la console: pip install py2neo)
    
"""
print(message)

######################
### Importations nécessaires
######################
# Libraries
# Installation de pyorient : mettre dans la console ceci -> pip install pyorient --user
# code magique post-installation -> pip install --upgrade git+https://github.com/OpenConjecture/pyorient.git
import pyorient
from py2neo import Graph, Node, Relationship
import json   # Importation du fichier de données json
import timeit # Comparaison des temps de calculs
import matplotlib.pyplot as plt
import pandas as pd

# Données
data_lotr = json.load(open('./donnees/data_tolkien.json', 'r', encoding='utf-8'))
    

######################
### Fonctions
######################
# Création des fonctions de temps de calcul
def query_neo4j(requete_neo):
    ''' 
    prend en entrée une requête Neo4j sous forme de chaîne caractères
    renvoie une liste contenant les objets py2neo
    '''
    data = list(graph.run(requete_neo))
    return(data)

def query_orientdb(requete_orientdb):
    ''' 
    prend en entrée une requête OrientDB sous forme de chaîne caractères
    renvoie un objet pyorient
    '''
    data = client.query(requete_orientdb)
    return(data)
    
    
######################
### Connexion a la BDD
######################

# Demande à l'utilisateur les informations nécessaires
# (possiblement à changer pour rendre le document final)

userpassword = input("Quel est le mot de passe choisi pour la base de données (mot de passe entré lors de l'installation de OrientDB) ? --> ")

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
id_clusters = [35,36,37,38,39,40]
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
        id_cluster = 36
        client.record_create(id_cluster, record_k)
        
    elif k.get("@class") == "Event":
        record_k = {"uniquename": k.get("uniquename"),
            		"name": k.get("name"),
                    "description": k.get("description"),
            		"illustrator": k.get("illustrator"),
                    }
        id_cluster = 37
        client.record_create(id_cluster, record_k)
        
print("Insertion des records des vertex terminée")


for k in data_lotr.get("records"):
    if k.get("@class") == "BEGETS":
        in_tmp = "#35"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#35"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE BEGETS FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)
        
    elif k.get("@class") == "LOVES":
        in_tmp = "#35"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#35"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE LOVES FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)
        
    elif k.get("@class") == "HASSIBLING":
        in_tmp = "#35"+str(k.get("in")[3:len(k.get("in"))])
        out_tmp = "#35"+str(k.get("out")[3:len(k.get("out"))])
        query = "CREATE EDGE HASSIBLING FROM (SELECT FROM Creature WHERE @rid = '" + in_tmp + "' ) TO ( SELECT FROM Creature WHERE @rid = '" + out_tmp + "' )"
        client.command(query)

print("Insertion des edge terminée")
print("\n----- La base de données a bien été créée dans OrientDB. -----")



##################################################################################################################
######################
### Création de la base de données Neo4J
######################

print("\n\n----- Création de la même base de données dans Neo4j, pour comparaison des vitesses d'exécution. -----")

graph = Graph("bolt://localhost", auth=("neo4j", "Neo4J"))
graph.delete_all()

## DICO DES RECORDS

reco_crea = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Creature':
        reco_crea[k.get("@rid")] = k
print("Il y a ",len(reco_crea)," créatures.")

reco_loc = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Location':
        reco_loc[k.get("@rid")] = k
print("Il y a ",len(reco_loc)," locations.")

reco_ev = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Event':
        reco_ev[k.get("@rid")] = k
print("Il y a ",len(reco_ev)," évenements.")

reco_rel = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'LOVES' or k.get('@class') == 'BEGETS' or k.get('@class') == 'HASSIBLING':
        reco_rel[k.get("@rid")] = k


## NOEUDS
# TABLE CREATURE

print("\n----- Création des noeuds -----")

a = 0
crea = {}

for k in reco_crea.items() :
    d = k[1]
    if d.get("@class") == "Creature":
        a += 1
        crea[k[0]] = Node("Creature", rid = d.get("@rid"), searchname = d.get("searchname"), uniquename = d.get("uniquename"), gender = d.get("gender"), race = d.get("race"), gatewaylink = d.get("gatewaylink"), born = d.get("born"), altname = d.get("altname"), died = d.get("died"), significance = d.get("significance"), name = d.get("name"), location = d.get("location"), illustrator = d.get("illustrator"))
        graph.create(crea[k[0]])
print("On a bien importé",a," noeuds creature (",len(reco_crea)," créatures comptées plus haut)")

# TABLE LOCATION

a = 0
loc = {}

for k in reco_loc.items() :
    d = k[1]
    if d.get("@class") == "Location":
        a += 1
        loc[k[0]] = Node("Location", rid = d.get("@rid"), significance = d.get("significance"), area = d.get("area"), searchname = d.get("searchname"), uniquename = d.get("uniquename"), gatewaylink = d.get("gatewaylink"), name = d.get("name"), altname = d.get("altname"), type = d.get("type"), age = d.get("age"), canon = d.get("canon"), illustrator = d.get("illustrator"))
        graph.create(loc[k[0]])
print("On a bien importé",a," noeuds location (",len(reco_loc)," locations comptées plus haut)")

# TABLE EVENT

a = 0
ev = {}

for k in reco_ev.items() :
    d = k[1]
    if d.get("@class") == "Event":
        a += 1
        ev[k[0]] = Node("Event", rid = d.get("@rid"), uniquename = d.get("uniquename"), name = d.get("name"), description = d.get("description"), illustrator = d.get("illustrator"))
        graph.create(ev[k[0]])
print("On a bien importé",a,"noeuds event (",len(reco_ev)," events comptés plus haut)")


## RELATIONS

print("\n----- Création des relations -----")

rel = []
for k in reco_rel.items():
    a = k[1]
    fr = crea[a.get('out')]
    to = crea[a.get('in')]
    rel.append(Relationship(fr,str(a.get('@class')),to))

for r in rel :
    graph.create(r)
print("Création des relations terminée pour la base Neo4J.")



##################################################################################################################
######################
### Requetes
######################
print("\n\n\n-------------------- REQUETES --------------------\n")
print("\n----- Requêtes sur la base de données OrientDB -----\n")

# Requête introductive
print("→ Requête introductive : Quand est né Frodon Saquet et quand est-il mort ?")
query0 = "SELECT * FROM Creature WHERE name = 'Frodo Baggins'"
print("Requête demandée : " + query0)
data = client.query(query0)
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
        res = ("La région "+ str(query2[i].location)[4:len(str(query2[i].location))-4]+" est la "+ str(i+1) +"ème plus grande région, et elle héberge "+str(query2[i].regioncount)+" personnes")
    print(res)
    
# Troisième requete
print("\n→ Requête 3 : Combien d'enfants a Samwise Gamgee ?")
query3 = client.query("MATCH {Class: Creature, as: Father, where: (name='Samwise Gamgee')}<-BEGETS-{Class: Creature, as: Children} RETURN Children")
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
print("\n→ Requête 6: Quels sont ces ancêtres entre Isildur et Aragorn II ?")
query6 = client.query("SELECT name FROM (SELECT expand(path) FROM (SELECT shortestPath($from, $to) AS path LET  $from = (SELECT FROM Creature WHERE name='Aragorn II'), $to = (SELECT FROM Creature WHERE name='Isildur') UNWIND path) limit 100)")
arbre= ""
for i in range(len(query6)):
    arbre+=query6[i].name+" "
print(arbre)

print('\n')
input("Appuyez sur Entrée pour continuer : ")
######################
### Comparaison requetes OrientDB Neo4j
######################
print("\n----- Comparaison des temps de calcul pour les requêtes entre OrientDB et Neo4j -----\n")

# Initialisation des listes récupérant les temps et requêtes
tps_requetes = []
tps_orientdb = []
tps_neo4j = []

# Requête introductive 
tps_requetes.append("\n→ Requête 0 : Quand est né Frodon Saquet et quand est-il mort ?")
print(tps_requetes[0])
requete_odb = "SELECT * FROM Creature WHERE name = 'Frodo Baggins'"
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH (c:Creature) WHERE c.name = 'Frodo Baggins' RETURN c"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))

# Requête 1 
tps_requetes.append("\n→ Requête 1 : Combien de couples y a-t-il en tout ?")
print(tps_requetes[1])
requete_odb = "SELECT COUNT(*) FROM LOVES"
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH ()-[l:LOVES]->() RETURN count(l)"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))

# Requête 2
tps_requetes.append("\n→ Requête 2 : Quelles sont les régions les plus peuplées ?")
print(tps_requetes[2])
requete_odb = ("select location, count(*) as regioncount from Creature group by location order by regioncount DESC limit 4 ")
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH (c:Creature) WITH c, c.location as p RETURN p,count(c) ORDER BY count(c) DESC LIMIT 4"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))



# Requête 3
tps_requetes.append("\n→ Requête 3 : Combien d'enfants a Samwise Gamgee ?")
print(tps_requetes[3])
requete_odb = ("MATCH {Class: Creature, as: Father, where: (name='Samwise Gamgee')}-BEGETS->{Class: Creature, as: Children} RETURN Children")
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH (c:Creature)-[:BEGETS]->(e:Creature) WHERE c.name='Samwise Gamgee' RETURN e"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))

# Requête 4
tps_requetes.append("\n→ Requête 4 : Combien y a t-il de triangle amoureux et qui en fait partis ?")
print(tps_requetes[4])
requete_odb = ("MATCH {Class: Creature, as: C1}-LOVES->{Class: Creature, as: C2}-LOVES->{Class: Creature, as: C3} RETURN C1.name as C1, C2.name as C2, C3.name as C3")
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH (c1:Creature)-[:LOVES]->(c2:Creature)-[:LOVES]->(c3:Creature) RETURN c1,c2,c3"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))

# Requête 5
tps_requetes.append("\n→ Requête 5 : Combien de générations directes séparent Isildur et Aragorn II ?")
print(tps_requetes[5])
requete_odb = ("SELECT count(*) FROM (SELECT expand(path) FROM (SELECT shortestPath($from, $to) AS path LET  $from = (SELECT FROM Creature WHERE name='Aragorn II'), $to = (SELECT FROM Creature WHERE name='Isildur') UNWIND path))")
print("   Requête OrientDB : " + requete_odb)
requete_neo = "MATCH (s:Creature {name:'Aragorn II'}), (e:Creature {name:'Isildur'}), p = shortestPath((s)-[*]-(e)) RETURN p"
print("   Requête Neo4j    : " + requete_neo)

tps_orientdb.append(timeit.timeit('query_orientdb(requete_odb)', globals = globals(), number = 200))
tps_neo4j.append(timeit.timeit('query_neo4j(requete_neo)', globals = globals(), number = 200))

print('\n')
input("Appuyez sur Entrée pour afficher le premier graphique : ")
# Graphique lignes
print("\nEvolution des temps de calculs par requête selon la base de données utilisée")
tps_comp = pd.DataFrame(list(zip(range(0,5), tps_requetes, tps_orientdb, tps_neo4j)), 
               columns =['NumRequete', 'Requete', 'OrientDB', 'Neo4j'])

plt.rcParams.update({'font.size': 16})
plt.figure(figsize=(9,5))  
ax = plt.gca()
tps_comp.plot(kind='line',x='NumRequete',y='OrientDB',ax=ax)
tps_comp.plot(kind='line',x='NumRequete',y='Neo4j', color='red', ax=ax)
plt.show()

print('\n')
input("Appuyez sur Entrée pour afficher le deuxième graphique : ")

# Boxplot récapitulatif
print("\nBoxplot des temps de calculs selon la base de données utilisée")
tps_comp.boxplot(column=['OrientDB', 'Neo4j'], fontsize = 16, grid = False, figsize = (9,5))
plt.show()

print('\n')
input("Appuyez sur Entrée pour fermer la base : ")

######################
### Déconnexion de la BDD
######################
client.db_close()
print("\n\n----- Déconnexion de la base de données 'tolkien' de OrientDB effectuée -----\n\n")
print("\nPour se déconnecter de la base de données de Neo4j, aller dans Neo4j Desktop et cliquer sur Stop.")