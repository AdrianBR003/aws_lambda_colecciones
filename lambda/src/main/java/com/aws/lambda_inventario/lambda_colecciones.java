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
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
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
            if (coleccion.getDescripcion() != null) {
                item.put("descripcion", AttributeValue.builder().s(coleccion.getDescripcion()).build());
            }
            if (coleccion.getTipo_coleccion() != null) {
                item.put("tipo_coleccion", AttributeValue.builder().s(coleccion.getTipo_coleccion()).build());
            }
            if (coleccion.getId_padre() != null) {
                item.put("id_padrecoleccion", AttributeValue.builder().s(coleccion.getId_padre()).build());
            }
            if (coleccion.getUbicacion() != null) {
                item.put("ubicacion", AttributeValue.builder().s(coleccion.getUbicacion()).build());
            }
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
        return new APIGatewayProxyResponseEvent().withStatusCode(statusCode).withBody(body);
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
            key.put("id_coleccion",AttributeValue.fromN(id_coleccion));

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
        private String tipo_coleccion;
        private String id_padre;
        private String ubicacion;
        private String responsable;
        private String descripcion;

        public String getDescripcion() {
            return descripcion;
        }

        public void setDescripcion(String descripcion) {
            this.descripcion = descripcion;
        }

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

        public String getTipo_coleccion() {
            return tipo_coleccion;
        }

        public void setTipo_coleccion(String tipo_coleccion) {
            this.tipo_coleccion = tipo_coleccion;
        }

        public String getId_padre() {
            return id_padre;
        }

        public void setId_padre(String id_padre) {
            this.id_padre = id_padre;
        }

        public String getUbicacion() {
            return ubicacion;
        }

        public void setUbicacion(String ubicacion) {
            this.ubicacion = ubicacion;
        }

        public String getResponsable() {
            return responsable;
        }

        public void setResponsable(String responsable) {
            this.responsable = responsable;
        }
    }
}
