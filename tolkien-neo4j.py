from py2neo import Graph, Node, Relationship
import json

graph = Graph("bolt://localhost", auth=("neo4j", "Neo4J"))
graph.delete_all()

data_lotr = json.load(open('C:/Users/HP/Documents/GitHub/OrientDB/Donnees/data_tolkien.json', 'r'))

## DICO DES RECORDS

reco_crea = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Creature':
        reco_crea[k.get("@rid")] = k
print("Il y a ",len(reco_crea)," creatures.")

reco_loc = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Location':
        reco_loc[k.get("@rid")] = k
print("Il y a ",len(reco_loc)," locations.")

reco_ev = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'Event':
        reco_ev[k.get("@rid")] = k
print("Il y a ",len(reco_ev)," events.")

reco_rel = {}
for k in data_lotr.get("records"):
    if k.get('@class') == 'LOVES' or k.get('@class') == 'BEGETS' or k.get('@class') == 'HASSIBLING':
        reco_rel[k.get("@rid")] = k


## NOEUDS

# TABLE CREATURE

a = 0
crea = {}

for k in reco_crea.items() :
    d = k[1]
    if d.get("@class") == "Creature":
        a += 1
        crea[k[0]] = Node("Creature", rid = d.get("@rid"), searchname = d.get("searchname"), uniquename = d.get("uniquename"), gender = d.get("gender"), race = d.get("race"), gatewaylink = d.get("gatewaylink"), born = d.get("born"), altname = d.get("altname"), died = d.get("died"), significance = d.get("significance"), name = d.get("name"), location = d.get("location"), illustrator = d.get("illustrator"))
        graph.create(crea[k[0]])
        print(d.get("searchname"))
print("On a bien ",a," noeuds creature (",len(reco_crea)," creatures comptees plus haut)")

# TABLE LOCATION

a = 0
loc = {}

for k in reco_loc.items() :
    d = k[1]
    if d.get("@class") == "Location":
        a += 1
        loc[k[0]] = Node("Location", rid = d.get("@rid"), significance = d.get("significance"), area = d.get("area"), searchname = d.get("searchname"), uniquename = d.get("uniquename"), gatewaylink = d.get("gatewaylink"), name = d.get("name"), altname = d.get("altname"), type = d.get("type"), age = d.get("age"), canon = d.get("canon"), illustrator = d.get("illustrator"))
        graph.create(loc[k[0]])
        print(d.get("searchname"))
print("On a bien ",a," noeuds location (",len(reco_loc)," locations comptees plus haut)")

# TABLE EVENT

a = 0
ev = {}

for k in reco_ev.items() :
    d = k[1]
    if d.get("@class") == "Event":
        a += 1
        ev[k[0]] = Node("Event", rid = d.get("@rid"), uniquename = d.get("uniquename"), name = d.get("name"), description = d.get("description"), illustrator = d.get("illustrator"))
        graph.create(ev[k[0]])
        print(d.get("name"))
print("On a bien ",a," noeuds event (",len(reco_ev)," events comptees plus haut)")


## RELATIONS

rel = []

for k in reco_rel.items():
    a = k[1]
    fr = crea[a.get('in')]
    to = crea[a.get('out')]
    rel.append(Relationship(fr,str(a.get('@class')),to))

for r in rel :
    graph.create(r)

## REQUETE

a = list(graph.run("MATCH (n:Creature) WHERE n.race='Hobbit' RETURN n"))

