// MongoDB init script — Pipeline Hurtos Medellín
db = db.getSiblingDB("hurtos_medellin");

db.createCollection("eventos_raw");
db.createCollection("agregados");

// Índices para consultas eficientes
db.eventos_raw.createIndex({ "comuna": 1 });
db.eventos_raw.createIndex({ "fecha_hecho": 1 });
db.eventos_raw.createIndex({ "turno_dia": 1 });
db.eventos_raw.createIndex({ "modalidad": 1 });
db.eventos_raw.createIndex({ "latitud": 1, "longitud": 1 });

print("✓ Base de datos 'hurtos_medellin' inicializada con índices.");
