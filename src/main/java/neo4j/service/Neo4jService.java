package neo4j.service;

import org.neo4j.driver.v1.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

@Service
public class Neo4jService {

    private static final Set<String> IPS = new HashSet<>();
    private static final String CREATE_NODE_QUERY = "CREATE (ip:Ip { ip: $ip, port: $port, start: $start })";
    private static final String CREATE_RELATIONSHIP_QUERY = "MATCH (n), (m) WHERE n.ip = $sourceIp AND m.ip = $targetIp " +
            "CREATE (n)-[:#{PRTCL} {protocol: $protocol, packets: $packets, bytes: $bytes, start: $start, duration: $duration} ]->(m)";

    @Autowired
    private Session session;

    public void ingest(String line) {
        String[] data = line.split("->");

        String[] sourceData = data[0].split("\t");
        String start = sourceData[0];
        String duration = sourceData[1];
        String protocol = sourceData[2].replace("/", "_").replace("-", "_");
        String sourceIp = sourceData[3].split(":")[0];
        String sourcePort = null;
        if (sourceData[3].split(":").length == 2) {
            sourcePort = sourceData[3].split(":")[1];
        }

        String[] targetData = data[1].split("\t");
        String targetIp = targetData[1].split(":")[0];
        String packets = targetData[4];
        String bytes = targetData[5];
        String targetPort = null;
        if (targetData[1].split(":").length == 2) {
            targetPort = targetData[1].split(":")[1];
        }


        Boolean sourceNodeExists = IPS.contains(sourceIp);
        Boolean targetNodeExists = IPS.contains(targetIp);

        if (targetNodeExists && sourceNodeExists) {
            createRelationship(protocol, sourceIp, targetIp, packets, bytes, duration, start);
        } else if (sourceNodeExists) {
            createNode(targetIp, targetPort, start);
            createRelationship(protocol, sourceIp, targetIp, packets, bytes, duration, start);
            IPS.add(targetIp);
        } else if (targetNodeExists) {
            createNode(sourceIp, sourcePort, start);
            createRelationship(protocol, sourceIp, targetIp, packets, bytes, duration, start);
            IPS.add(sourceIp);
        } else {
            createNode(sourceIp, sourcePort, start);
            createNode(targetIp, targetPort, start);
            createRelationship(protocol, sourceIp, targetIp, packets, bytes, duration, start);
            IPS.add(targetIp);
            IPS.add(sourceIp);
        }
    }

    private void createNode(String ip, String port, String start) {
        session.run(CREATE_NODE_QUERY, parameters("ip", ip, "port", port, "start", start));
    }

    private void createRelationship(String protocol, String sourceIp, String targetIp, String packets, String bytes, String duration, String start) {
        session.run(
                CREATE_RELATIONSHIP_QUERY.replace("#{PRTCL}", protocol),
                parameters("sourceIp", sourceIp, "targetIp", targetIp, "protocol", protocol, "packets", packets, "bytes", bytes, "duration", duration, "start", start)
        );
    }

}
