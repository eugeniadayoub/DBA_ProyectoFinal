print("Creando / validando índices...");

db.buildings_google.createIndex({ geometry: "2dsphere" });
db.buildings_microsoft.createIndex({ geometry: "2dsphere" });
db.municipalities_pdet.createIndex({ mpio: 1 });

print("Índices listos.");
