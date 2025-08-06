from pymongo import MongoClient  #  Import MongoClient to connect to MongoDB
import pymongo


client=MongoClient('mongodb://localhost:27017/')  # Connect to MongoDB

db=client.Avis # Access the avis client database  

print("-------------      Creation et Insertion document terminé    -------------") 


avis_client = [ 
    {"client_id": "C001", "product_id": "P001", "rating": 5, "comment": "Excellent !", "data": "2024-01-01"},
    {"client_id": "C002", "product_id": "P002", "rating": 3, "comment": "Averge product.", "data": "2024-01-02"},
    {"client_id": "C003", "product_id": "P003", "rating": 4, "comment": "Very good !", "data": "2024-01-03"},
]

#res=db.avis_client.insert_many(avis_client)  # Insert multiple documents into the 'avis_client' collection



print("-------------       Recuperer les donne depui MongoDb et convetir el liste de tuple     -------------") 

result=db.avis_client.find({},{"_id":0})

#print(result)


t=[]  # list we gonne convert it to tuple 
li=[]  #list 

# On store les result dans liste li de tuple t

for i in result:
    
    for j in i.values():
        t.append(j)
        
    k=tuple(t)
    
    li.append(k) 
    t=[]



print(li)

# 3- Manipulation et analyse des donnees 

print("------------1-Extraire Le poduit qui a la meilleur note moyenne -------------")

pipe=[
    {'$group' : {"_id":"$product_id", "note moyenne" :{'$avg': '$rating' }  }},  # Group by product_id and calculate average rating
    {'$limit' : 1}
]



re=db.avis_client.aggregate(pipe)
for i in re :
    print("Le produit ",i["_id"] + " a la meilleure note moyenne " ,  i['note moyenne'])  


print("------------2-Extraire Le client qui a laissé le plus d'avis  -------------")


pipie2=[

    {'$group': {'_id':'$client_id', "a laissé le plus avis": {"$sum": 1}  }} , # Group by client_id and count the number of reviews
    {'$limit' :1  } # Limit the result to the top client with the most reviews
]

re1=db.avis_client.aggregate(pipie2)
for i in re1:
    print("Le client ",i["_id"] + " a laissé le plus avis " ,  i['a laissé le plus avis'])  



print("------------3- Calcul Nombre total d'avis sur un produit -------------")

pipe3=[

    {'$group': {'_id': '$product_id' , 'nombre total d avis':{ '$sum': 1}   }}  # Group by product_id and count the total number of reviews


]

re3=db.avis_client.aggregate(pipe3)

for i in re3:
    print("Le produit ",i["_id"] + " a un nombre total d'avis de " ,  i['nombre total d avis'])  


print("------------4- Extraire les avis avec une note supérieure à 4 -------------")

re4=db.avis_client.find({'rating' :{ '$gt' : 4}})

for i in re4:
    print(i)



print("----- 5- -------------------------"  )
re5=db.avis_client.find({'$or' : [{"product_id":"P001"}, {"client_id":"C001"}]})

for i in re5:
    print(i)
                
 


client.close()  # Close the connection to MongoDB