package com.aws.lambda_inventario;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class lambda_colecciones implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName = "tabla-colecciones";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        try {
            if (request == null) {
                context.getLogger().log("ERROR: La solicitud recibida es NULL");
                return createResponse(500, "Error: La solicitud recibida es NULL");
            }
            String httpMethod = "UNKNOWN";
            if (request.getRequestContext() != null &&
                    request.getRequestContext().getHttp() != null &&
                    request.getRequestContext().getHttp().getMethod() != null) {
                httpMethod = request.getRequestContext().getHttp().getMethod();
            }
            context.getLogger().log("Método HTTP recibido: " + httpMethod);
            switch (httpMethod) {
                case "POST":
                    return createColeccion(request.getBody(), context);
                case "GET":
                    return getAllColecciones(context);
                case "DELETE":
                    return deleteColeccionbyID(request.getBody(), context);
                case "PUT":
                    return modifyColeccionbyID(request.getBody(), context);
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent modifyColeccionbyID(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en PUT: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }

            // Convertimos el JSON a un Map
            Map<String, Object> rawMap = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});

            String id_coleccion = (String) rawMap.get("id_coleccion");
            if (id_coleccion == null || id_coleccion.trim().isEmpty()) {
                return createResponse(400, "Falta el campo 'id_coleccion'.");
            }

            // Construimos itemKey solo con la clave primaria
            Map<String, AttributeValue> itemKey = new HashMap<>();
            itemKey.put("id_coleccion", AttributeValue.builder().s(id_coleccion).build());

            // Construimos la expresión de actualización
            StringBuilder updateExpression = new StringBuilder("SET ");
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            Map<String, String> expressionAttributeNames = new HashMap<>();

            int count = 0;
            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                String key = entry.getKey();
                if (!key.equals("id_coleccion")) {  // Excluimos la clave primaria
                    count++;
                    String fieldKey = "#field" + count;
                    String valueKey = ":val" + count;

                    updateExpression.append(count > 1 ? ", " : "").append(fieldKey).append(" = ").append(valueKey);
                    expressionAttributeNames.put(fieldKey, key);

                    if (entry.getValue() instanceof String) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().s((String) entry.getValue()).build());
                    } else if (entry.getValue() instanceof Number) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().n(entry.getValue().toString()).build());
                    } else if (entry.getValue() instanceof Boolean) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().bool((Boolean) entry.getValue()).build());
                    }
                }
            }

            if (count == 0) {
                return createResponse(400, "No hay campos válidos para actualizar.");
            }

            // Construimos la solicitud UpdateItemRequest
            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();

            dynamoDbClient.updateItem(request);
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("id_coleccion", id_coleccion);
            responseMap.put("mensaje", "Coleccion actualizado correctamente.");
            return createResponse(200, objectMapper.writeValueAsString(responseMap));

        } catch (Exception e) {
            context.getLogger().log("ERROR en modifyColeccionbyID: " + e.getMessage());
            return createResponse(500, "Error al actualizar coleccion: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createColeccion(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en POST: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }
            Coleccion coleccion = objectMapper.readValue(body, Coleccion.class);
            if (coleccion.getNombre() == null || coleccion.getNombre().trim().isEmpty()) {
                return createResponse(400, "Falta el campo 'nombre'.");
            }
            if (coleccion.getId_coleccion() == null || coleccion.getId_coleccion().trim().isEmpty()) {
                coleccion.setId_coleccion(UUID.randomUUID().toString());
            }
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id_coleccion", AttributeValue.builder().s(coleccion.getId_coleccion()).build());
            item.put("nombre", AttributeValue.builder().s(coleccion.getNombre()).build());
            if (coleccion.getResponsable() != null) {
                item.put("responsable", AttributeValue.builder().s(coleccion.getResponsable()).build());
            }

            dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
            context.getLogger().log("Coleccion creada con ID: " + coleccion.getId_coleccion());
            return createResponse(200, objectMapper.writeValueAsString(coleccion));
        } catch (Exception e) {
            context.getLogger().log("ERROR en createColeccion: " + e.getMessage());
            return createResponse(500, "Error al crear coleccion: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent getAllColecciones(Context context) {
        try {
            context.getLogger().log("Consultando todas las colecciones en la tabla: " + tableName);
            ScanResponse scanResponse = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).build());
            List<Map<String, AttributeValue>> items = scanResponse.items();
            if (items == null || items.isEmpty()) {
                return createResponse(200, "[]");
            }
            List<Map<String, Object>> convertedItems = items.stream().map(item -> {
                Map<String, Object> map = new HashMap<>();
                item.forEach((key, attributeValue) -> {
                    if (attributeValue.s() != null) {
                        map.put(key, attributeValue.s());
                    } else if (attributeValue.n() != null) {
                        map.put(key, attributeValue.n());
                    } else if (attributeValue.ss() != null && !attributeValue.ss().isEmpty()) {
                        map.put(key, attributeValue.ss());
                    }
                });
                return map;
            }).collect(Collectors.toList());
            String json = objectMapper.writeValueAsString(convertedItems);
            return createResponse(200, json);
        } catch (Exception e) {
            context.getLogger().log("ERROR en getAllColecciones: " + e.getMessage());
            return createResponse(500, "Error al obtener colecciones: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*"); // Opcional, para CORS
        headers.put("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(body);
    }

    private APIGatewayProxyResponseEvent deleteColeccionbyID(String body, Context context){
        try {
            context.getLogger().log("Cuerpo recibido en DELETE" + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }
            String id_coleccion = "";
            Map<String, Object> bodyMap = objectMapper.readValue(body, new TypeReference<>(){});
            if(body.contains("id_coleccion")){
                id_coleccion = (String) bodyMap.get("id_coleccion");
            }else{
                context.getLogger().log("No se ha identificado el ID: " + body);
                System.out.println("No se ha identificado el ID: " + body);
                return createResponse(500, "Error al eliminar coleccion con id" + id_coleccion);
            }

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id_coleccion", AttributeValue.builder().s(id_coleccion).build());

            Object objeto = dynamoDbClient.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key).returnValues(ReturnValue.ALL_OLD).build());
            context.getLogger().log("Objeto eliminado: " + objeto.toString());
            return createResponse(200, objectMapper.writeValueAsString(bodyMap));
        }catch (Exception e){
            context.getLogger().log("ERROR en deleteColeccionbyID: " + e.getMessage());
            return createResponse(500, "Error al eliminar coleccion: " + e.getMessage());
        }
    }

    public static class Coleccion {
        private String id_coleccion;
        private String nombre;
        private String responsable;

        public String getId_coleccion() {
            return id_coleccion;
        }

        public void setId_coleccion(String id_coleccion) {
            this.id_coleccion = id_coleccion;
        }

        public String getNombre() {
            return nombre;
        }

        public void setNombre(String nombre) {
            this.nombre = nombre;
        }

        public String getResponsable() {
            return responsable;
        }

        public void setResponsable(String responsable) {
            this.responsable = responsable;
        }
    }
}
