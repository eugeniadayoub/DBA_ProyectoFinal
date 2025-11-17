import pymongo
import csv

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["local"]  # O el nombre donde guardaron todo

google = db["google_pdet_filtered"]

pipeline = [
    {"$group": {
        "_id": "$mpio",
        "total_buildings": {"$sum": 1},
        "total_area_m2": {"$sum": "$area"}
    }}
]

results = list(google.aggregate(pipeline))

with open("entrega4/pdet_google_counts.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["mpio", "total_buildings", "total_area_m2"])
    for row in results:
        writer.writerow([row["_id"], row["total_buildings"], row["total_area_m2"]])

print("Archivo generado: entrega4/pdet_google_counts.csv")
