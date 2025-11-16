print("=== Filtrando Footprints por Municipios PDET ===");

// Crear índices si no existen
db.buildings_google.createIndex({ geometry: "2dsphere" });
db.buildings_microsoft.createIndex({ geometry: "2dsphere" });
db.municipalities_pdet.createIndex({ mpio: 1 });

// Filtrar Google
db.buildings_google.aggregate([
  {
    $lookup: {
      from: "municipalities_pdet",
      localField: "mpio",
      foreignField: "mpio",
      as: "pdet"
    }
  },
  { $match: { pdet: { $ne: [] } } },
  {
    $out: "google_pdet_filtered"
  }
]);

// Filtrar Microsoft
db.buildings_microsoft.aggregate([
  {
    $lookup: {
      from: "municipalities_pdet",
      localField: "mpio",
      foreignField: "mpio",
      as: "pdet"
    }
  },
  { $match: { pdet: { $ne: [] } } },
  {
    $out: "microsoft_pdet_filtered"
  }
]);

print("=== Filtrado completado. Nuevas colecciones creadas: google_pdet_filtered, microsoft_pdet_filtered ===");
