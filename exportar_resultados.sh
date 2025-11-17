#!/bin/bash

echo "Exportando resultados a CSV..."

docker exec -it mongo-proyecto-upme mongosh --quiet dba_proyectofinal --eval '
// Google
var googleData = db.buildings_google.aggregate([
  {$group: {
    _id: "$codigo_municipio",
    total_edificios: {$sum: 1},
    area_total_m2: {$sum: "$area_m2"}
  }},
  {$sort: {total_edificios: -1}}
]).toArray()

print("codigo_municipio,fuente,total_edificios,area_total_m2,area_total_ha")
googleData.forEach(function(doc) {
  print(doc._id + ",Google," + doc.total_edificios + "," + doc.area_total_m2.toFixed(2) + "," + (doc.area_total_m2/10000).toFixed(2))
})

// Microsoft
var msData = db.buildings_microsoft.aggregate([
  {$group: {
    _id: "$codigo_municipio",
    total_edificios: {$sum: 1},
    area_total_m2: {$sum: "$area_m2"}
  }},
  {$sort: {total_edificios: -1}}
]).toArray()

msData.forEach(function(doc) {
  print(doc._id + ",Microsoft," + doc.total_edificios + "," + doc.area_total_m2.toFixed(2) + "," + (doc.area_total_m2/10000).toFixed(2))
})
' > resultados_pdet_por_municipio.csv

echo "✓ Archivo generado: resultados_pdet_por_municipio.csv"
