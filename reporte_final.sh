#!/bin/bash

echo "=============================================="
echo "   REPORTE FINAL - ENTREGA 4"
echo "   Análisis Geoespacial Municipios PDET"
echo "=============================================="
echo ""
date
echo ""

docker exec -it mongo-proyecto-upme mongosh --quiet dba_proyectofinal --eval '
print("📊 RESUMEN EJECUTIVO")
print("════════════════════════════════════════════════")
print("Municipios totales (MGN):       ", db.municipalities.countDocuments({}).toLocaleString())
print("Municipios PDET analizados:     ", db.mgn_municipios_pdet.countDocuments({}))
print("Edificios Google en PDET:       ", db.buildings_google.countDocuments({}).toLocaleString())
print("Edificios Microsoft en PDET:    ", db.buildings_microsoft.countDocuments({}).toLocaleString())
print("TOTAL edificios PDET:           ", (db.buildings_google.countDocuments({}) + db.buildings_microsoft.countDocuments({})).toLocaleString())

print("\n✅ VALIDACIÓN DE CALIDAD DE DATOS")
print("════════════════════════════════════════════════")
var gNull = db.buildings_google.countDocuments({codigo_municipio: null})
var mNull = db.buildings_microsoft.countDocuments({codigo_municipio: null})
print("Google sin codigo_municipio:    ", gNull, gNull === 0 ? "✓ CORRECTO" : "✗ ERROR")
print("Microsoft sin codigo_municipio: ", mNull, mNull === 0 ? "✓ CORRECTO" : "✗ ERROR")

print("\n📍 TOP 10 MUNICIPIOS PDET - GOOGLE OPEN BUILDINGS")
print("════════════════════════════════════════════════")
print("Código  │ Edificios  │ Área Total (ha)")
print("────────┼────────────┼─────────────────")
db.buildings_google.aggregate([
  {$group: {_id: "$codigo_municipio", count: {$sum: 1}, area: {$sum: "$area_m2"}}},
  {$sort: {count: -1}},
  {$limit: 10}
]).forEach(function(doc) {
  var areaHa = (doc.area / 10000).toFixed(2)
  print(doc._id, " │", doc.count.toString().padStart(10), "│", areaHa.padStart(16))
})

print("\n📍 TOP 10 MUNICIPIOS PDET - MICROSOFT FOOTPRINTS")
print("════════════════════════════════════════════════")
print("Código  │ Edificios  │ Área Total (ha)")
print("────────┼────────────┼─────────────────")
db.buildings_microsoft.aggregate([
  {$group: {_id: "$codigo_municipio", count: {$sum: 1}, area: {$sum: "$area_m2"}}},
  {$sort: {count: -1}},
  {$limit: 10}
]).forEach(function(doc) {
  var areaHa = (doc.area / 10000).toFixed(2)
  print(doc._id, " │", doc.count.toString().padStart(10), "│", areaHa.padStart(16))
})

print("\n📊 ESTADÍSTICAS POR FUENTE")
print("════════════════════════════════════════════════")
var statsG = db.buildings_google.aggregate([
  {$group: {
    _id: null,
    total: {$sum: 1},
    area_total: {$sum: "$area_m2"},
    area_min: {$min: "$area_m2"},
    area_max: {$max: "$area_m2"},
    area_avg: {$avg: "$area_m2"}
  }}
]).toArray()[0]

print("GOOGLE OPEN BUILDINGS:")
print("  Total edificios:    ", statsG.total.toLocaleString())
print("  Área total:         ", (statsG.area_total/10000).toFixed(2), "hectáreas")
print("  Área promedio:      ", statsG.area_avg.toFixed(2), "m²")
print("  Área mínima:        ", statsG.area_min.toFixed(2), "m²")
print("  Área máxima:        ", statsG.area_max.toFixed(2), "m²")

var statsM = db.buildings_microsoft.aggregate([
  {$group: {
    _id: null,
    total: {$sum: 1},
    area_total: {$sum: "$area_m2"},
    area_min: {$min: "$area_m2"},
    area_max: {$max: "$area_m2"},
    area_avg: {$avg: "$area_m2"}
  }}
]).toArray()[0]

print("\nMICROSOFT BUILDING FOOTPRINTS:")
print("  Total edificios:    ", statsM.total.toLocaleString())
print("  Área total:         ", (statsM.area_total/10000).toFixed(2), "hectáreas")
print("  Área promedio:      ", statsM.area_avg.toFixed(2), "m²")
print("  Área mínima:        ", statsM.area_min.toFixed(2), "m²")
print("  Área máxima:        ", statsM.area_max.toFixed(2), "m²")

print("\n🗺️  COBERTURA GEOGRÁFICA")
print("════════════════════════════════════════════════")
print("Municipios PDET con datos Google:   ", db.buildings_google.distinct("codigo_municipio").length)
print("Municipios PDET con datos Microsoft:", db.buildings_microsoft.distinct("codigo_municipio").length)

print("\n✨ ÍNDICES ESPACIALES CREADOS")
print("════════════════════════════════════════════════")
print("Google:")
db.buildings_google.getIndexes().forEach(function(idx) {
  if (idx.name !== "_id_") print("  ✓", idx.name)
})
print("\nMicrosoft:")
db.buildings_microsoft.getIndexes().forEach(function(idx) {
  if (idx.name !== "_id_") print("  ✓", idx.name)
})

print("\n============================================")
print("REPORTE GENERADO EXITOSAMENTE")
print("============================================")
'
