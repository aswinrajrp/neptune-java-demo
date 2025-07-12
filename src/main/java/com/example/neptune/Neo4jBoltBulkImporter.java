package com.example.neptune;


import com.opencsv.CSVReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.*;

import java.io.FileReader;
import java.time.OffsetDateTime;
import java.util.*;

public class Neo4jBoltBulkImporter {
  private static final Logger logger = LogManager.getLogger(Neo4jBoltBulkImporter.class);

  public static void main(String[] args) throws Exception {

    /*String vertexCsv = "C:\\GraphDB\\Dheeraj\\data\\vertices.csv";
    String edgeCsv = "C:\\GraphDB\\Dheeraj\\data\\edges.csv";*/

    String vertexCsv = "C:\\GraphDB\\Neptune\\Test Data\\1752104578160_small\\vertices_clean.csv";
    String edgeCsv = "C:\\GraphDB\\Neptune\\Test Data\\1752104578160_small\\edges.csv";


    try (Driver driver = getNeo4jBoltDriver();
         Session session = driver.session()) {

      Map<String, List<Map<String, Object>>> nodesByLabel = readVertices(vertexCsv);
      Map<String, List<Map<String, Object>>> relsByType = readEdges(edgeCsv);
      Map<Integer, String> nodeIdMap = new HashMap<>();

      // Bulk insert nodes per label using UNWIND, collect CSV ID → Neo4j ID mapping
      for (String label : nodesByLabel.keySet()) {
        List<Map<String, Object>> records = new ArrayList<>();
        for (Map<String, Object> props : nodesByLabel.get(label)) {
          if(null == (props.get("id")))
            continue;

          Integer csvId = Integer.parseInt(props.get("id").toString());
          Map<String, Object> row = new HashMap<>();
          row.put("id", csvId);
          row.put("props", props);
          records.add(row);
        }

        String cypher = """
            UNWIND $rows AS row
            CREATE (n:`%s`)
            SET n = row.props
            RETURN row.id AS csvId, id(n) AS sysId
            """.formatted(label);

        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("rows", records);

        //List<org.neo4j.driver.Record> results = session.run(cypher, parameters).list();
        var results = session.run(cypher, parameters);
        while (results.hasNext()) {
          var r = results.next();
          String sysId = r.get("sysId").toString();
          sysId = sysId.substring(1, sysId.length() - 1);
          nodeIdMap.put(Integer.parseInt(r.get("csvId").toString()), sysId);
        }
      }

      // Transform relationship rows using mapped Neo4j IDs
      for (String relType : relsByType.keySet()) {
        List<Map<String, Object>> transformed = new ArrayList<>();
        for (Map<String, Object> relRow : relsByType.get(relType)) {
          Integer from = (Integer) relRow.get("from");
          Integer to = (Integer) relRow.get("to");
          Map<String, Object> props = (Map<String, Object>) relRow.get("props");

          String fromId = nodeIdMap.get(from);
          String toId = nodeIdMap.get(to);
          if (fromId != null && toId != null) {
            Map<String, Object> transformedRow = new HashMap<>();
            transformedRow.put("fromId", fromId);
            transformedRow.put("toId", toId);
            transformedRow.put("props", props);
            transformed.add(transformedRow);
          }

         /* Map<String, Object> transformedRow = new HashMap<>();
          transformedRow.put("fromId", from);
          transformedRow.put("toId", to);
          transformedRow.put("props", props);
          transformed.add(transformedRow);*/
        }

        HashMap<String, Object> parametersRel = new HashMap<>();
        parametersRel.put("rows", transformed);

        String cypher = """
            UNWIND $rows AS row
            MATCH (a), (b)
            WHERE id(a) = row.fromId AND id(b) = row.toId
            CREATE (a)-[r:`%s`]->(b)
            SET r = row.props
            """.formatted(relType);

        /*String cypher = """
            UNWIND $rows AS row
            MATCH (a), (b)
            WHERE a.id = row.fromId AND b.id = row.toId
            CREATE (a)-[r:`%s`]->(b)
            SET r = row.props
            """.formatted(relType);*/

        session.run(cypher, parametersRel);
      }

      System.out.println("✅ Bulk insert with UNWIND completed.");
    }
  }

  static Map<String, List<Map<String, Object>>> readVertices(String csvPath) throws Exception {
    Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
    try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
      String[] header = reader.readNext();
      String[] keys = new String[header.length];
      String[] types = new String[header.length];

      for (int i = 0; i < header.length; i++) {
        String rawHeader = header[i];
        String[] parts = rawHeader.split(":", 2);
        String key = parts[0].replaceFirst("^~", "");
        if (rawHeader.startsWith("~id")) key = "id";
        keys[i] = key;
        types[i] = parts.length > 1 ? parts[1] : "string";
      }

      String[] row;
      while ((row = reader.readNext()) != null) {
        Map<String, Object> props = new HashMap<>();
        String label = "Unknown";

        for (int i = 0; i < row.length; i++) {
          String key = keys[i];
          String type = types[i];
          String value = row[i];

          if ("label".equalsIgnoreCase(key)) {
            label = value;
            continue;
          }

          Object typedValue = parseValue(value, type);
          if (typedValue != null) {
            if("id".equalsIgnoreCase(key)) {
              props.put(key, Integer.parseInt(typedValue.toString()));
            } else {
              props.put(key, typedValue);
            }

          }
        }

        grouped.computeIfAbsent(label, k -> new ArrayList<>()).add(props);
      }
    }
    return grouped;
  }

  static Map<String, List<Map<String, Object>>> readEdges(String csvPath) throws Exception {
    Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
    try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
      String[] header = reader.readNext();
      String[] keys = new String[header.length];
      String[] types = new String[header.length];

      for (int i = 0; i < header.length; i++) {
        String[] parts = header[i].replaceFirst("^~", "").split(":", 2);
        keys[i] = parts[0];
        types[i] = parts.length > 1 ? parts[1] : "string";
      }

      String[] row;
      while ((row = reader.readNext()) != null) {
        String relType = "RELATED";
        Map<String, Object> props = new HashMap<>();
        Integer from = null, to = null;

        for (int i = 0; i < row.length; i++) {
          String key = keys[i];
          String type = types[i];
          String value = row[i];

          switch (key) {
            case "from" -> from = Integer.parseInt(value);
            case "to" -> to = Integer.parseInt(value);
            case "label" -> relType = value;
            default -> {
              Object typedValue = parseValue(value, type);
              if (typedValue != null) props.put(key, typedValue);
            }
          }
        }

        Map<String, Object> full = new HashMap<>();
        full.put("from", from);
        full.put("to", to);
        full.put("props", props);

        grouped.computeIfAbsent(relType, k -> new ArrayList<>()).add(full);
      }
    }
    return grouped;
  }

  static Object parseValue(String value, String type) {
    if (value == null || value.isEmpty()) return null;
    try {
      return switch (type.toLowerCase()) {
        case "int", "integer" -> Integer.parseInt(value);
        case "short" -> Short.parseShort(value);
        case "double" -> Double.parseDouble(value);
        case "boolean" -> Boolean.parseBoolean(value);
        case "date" -> value; //Looks like Date data type is not supported in Neptune, using string
        default -> value;
      };
    } catch (Exception e) {
      System.err.println("⚠️ Could not parse \"" + value + "\" as " + type + " — storing as string.");
      return value;
    }
  }

  private static Driver getNeo4jBoltDriver() {
    NeptuneConfig config = NeptuneConfig.fromProperties();
    logger.info("Connecting to Neptune at: {}", config.getBoltUri());

    AuthToken authToken = config.isIamAuth() ?
        new NeptuneAuthToken(config.getRegion(), config.getHttpsUri(), config.getCredentialsProvider())
            .toAuthToken() :
        AuthTokens.none();


    // Create driver instance
    var driver = GraphDatabase.driver(config.getBoltUri(), authToken,
        Config.builder().withEncryption()
            .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
            .build());

    logger.info("Successfully created Bolt driver for URI: {}", config.getBoltUri());

    return driver;
  }

/*
  public static void main2(String[] args) throws Exception {
    try (Driver driver = getNeo4jBoltDriver();
         Session session = driver.session()) {

      Map<String, Object> parameters = new HashMap<>();

      Map<String, Object> row1 = new HashMap<>();
      row1.put("name", "Dheeraj");
      row1.put("id", 1);

      Map<String, Object> row2 = new HashMap<>();
      row2.put("name", "Aswin");
      row2.put("id", 2);

      Map<String, Object> row3 = new HashMap<>();
      row3.put("name", "Said");
      row3.put("id", 3);

      List<Map<String, Object>> rows = new ArrayList<>();
      rows.add(row1);
      rows.add(row2);
      rows.add(row3);

      parameters.put("rows", rows);

      String cypher = "UNWIND $rows as row CREATE (n:`Person`) SET n = row RETURN n";
      var res = session.run(cypher, parameters);
      System.out.println("Done");
    }
  }*/
}
