#!/bin/bash

echo "=========================================="
echo "VERIFICACIÓN COMPLETA - ENTREGA 4"
echo "=========================================="
echo ""

docker exec -it mongo-proyecto-upme mongosh --quiet --eval '
use dba_proyectofinal

print("📊 COLECCIONES EN LA BASE DE DATOS:")
print("════════════════════════════════════")
db.getCollectionNames().forEach(col => print("  -", col))

print("\n📈 CONTEO DE DOCUMENTOS:")
print("═══════════════════════════")
var mpios = db.municipalities.countDocuments({})
var pdet = db.mgn_municipios_pdet.countDocuments({})
var google = db.buildings_google.countDocuments({})
var microsoft = db.buildings_microsoft.countDocuments({})

print("Municipios totales:        ", mpios.toLocaleString())
print("Municipios PDET:           ", pdet.toLocaleString())
print("Edificios Google (PDET):   ", google.toLocaleString())
print("Edificios Microsoft (PDET):", microsoft.toLocaleString())

print("\n✅ VERIFICACIÓN CRÍTICA - codigo_municipio:")
print("═══════════════════════════════════════════")
var googleNull = db.buildings_google.countDocuments({codigo_municipio: null})
var googleNoNull = db.buildings_google.countDocuments({codigo_municipio: {$ne: null}})
var msNull = db.buildings_microsoft.countDocuments({codigo_municipio: null})
var msNoNull = db.buildings_microsoft.countDocuments({codigo_municipio: {$ne: null}})

print("Google:")
print("  - CON código:    ", googleNoNull.toLocaleString(), googleNull === 0 ? "✓✓✓ PERFECTO" : "")
print("  - SIN código:    ", googleNull.toLocaleString(), googleNull === 0 ? "✓ OK" : "✗ ERROR")

print("\nMicrosoft:")
print("  - CON código:    ", msNoNull.toLocaleString(), msNull === 0 ? "✓✓✓ PERFECTO" : "")
print("  - SIN código:    ", msNull.toLocaleString(), msNull === 0 ? "✓ OK" : "✗ ERROR")

if (google > 0) {
  print("\n📍 TOP 10 MUNICIPIOS PDET (Google):")
  print("═══════════════════════════════════")
  db.buildings_google.aggregate([
    {$group: {
      _id: "$codigo_municipio",
      count: {$sum: 1},
      area_total: {$sum: "$area_m2"}
    }},
    {$sort: {count: -1}},
    {$limit: 10}
  ]).forEach(doc => {
    print("  ", doc._id, "│", doc.count.toLocaleString().padStart(10), "edificios │", (doc.area_total/10000).toFixed(2).padStart(12), "hectáreas")
  })
}

if (microsoft > 0) {
  print("\n📍 TOP 10 MUNICIPIOS PDET (Microsoft):")
  print("══════════════════════════════════════")
  db.buildings_microsoft.aggregate([
    {$group: {
      _id: "$codigo_municipio",
      count: {$sum: 1},
      area_total: {$sum: "$area_m2"}
    }},
    {$sort: {count: -1}},
    {$limit: 10}
  ]).forEach(doc => {
    print("  ", doc._id, "│", doc.count.toLocaleString().padStart(10), "edificios │", (doc.area_total/10000).toFixed(2).padStart(12), "hectáreas")
  })
}

print("\n✨ ÍNDICES ESPACIALES:")
print("════════════════════════")
print("Google:")
db.buildings_google.getIndexes().forEach(idx => {
  if (idx.name !== "_id_") print("  -", idx.name)
})

print("\nMicrosoft:")
db.buildings_microsoft.getIndexes().forEach(idx => {
  if (idx.name !== "_id_") print("  -", idx.name)
})

print("\n🎯 MUESTRA DE DATOS (Google):")
print("═════════════════════════════")
var sample = db.buildings_google.findOne({codigo_municipio: {$ne: null}})
if (sample) {
  print("Building ID:       ", sample.building_id)
  print("Fuente:            ", sample.fuente)
  print("Código Municipio:  ", sample.codigo_municipio)
  print("Área (m²):         ", sample.area_m2.toFixed(2))
  print("Geometry type:     ", sample.geometry.type)
}

print("\n🎯 MUESTRA DE DATOS (Microsoft):")
print("══════════════════════════════════")
var sampleMs = db.buildings_microsoft.findOne({codigo_municipio: {$ne: null}})
if (sampleMs) {
  print("Building ID:       ", sampleMs.building_id)
  print("Fuente:            ", sampleMs.fuente)
  print("Código Municipio:  ", sampleMs.codigo_municipio)
  print("Área (m²):         ", sampleMs.area_m2.toFixed(2))
  print("Geometry type:     ", sampleMs.geometry.type)
}

print("\n========================================")
print("VERIFICACIÓN COMPLETADA")
print("========================================")
'
