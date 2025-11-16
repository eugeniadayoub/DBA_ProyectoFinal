import pymongo
import csv

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["local"]

ms = db["microsoft_pdet_filtered"]

pipeline = [
    {"$group": {
        "_id": "$mpio",
        "total_buildings": {"$sum": 1}
    }}
]

results = list(ms.aggregate(pipeline))

with open("entrega4/pdet_microsoft_counts.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["mpio", "total_buildings"])
    for row in results:
        writer.writerow([row["_id"], row["total_buildings"]])

print("Archivo generado: entrega4/pdet_microsoft_counts.csv")
