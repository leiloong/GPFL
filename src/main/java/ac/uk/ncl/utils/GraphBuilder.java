package ac.uk.ncl.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GraphBuilder {

    private static Map<String, Long> map = new HashMap<>();

    public static void populateGraphFromTriples(String graphHome, File singleFile) {
        System.out.println("# GPFL System - Neo4j Graph Database Construction: ");

        GraphDatabaseService graph = createEmptyGraph(graphHome);
        Set<Triple> triples = new HashSet<>(readTriples(singleFile));
        System.out.println("# Triple Size: " + triples.size());
        writeToGraph(graph, triples, true);
    }

    public static void writeToGraph(GraphDatabaseService graph, Set<Triple> triples, boolean singleProperty) {
        long s = System.currentTimeMillis();
        try(Transaction tx = graph.beginTx()) {
            triples.forEach( triple -> {
                Node headNode = null, tailNode = null;
                if(map.containsKey(triple.head)) headNode = graph.getNodeById(map.get(triple.head));
                if(map.containsKey(triple.tail)) tailNode = graph.getNodeById(map.get(triple.tail));

                if(headNode == null) {
                    headNode = graph.createNode();
                    if(singleProperty) headNode.setProperty("name", triple.head);
                    else setProperties(triple.headProperties, headNode);
                    if(triple.headLabels.isEmpty()) headNode.addLabel(Label.label("Entity"));
                    else for (String label : triple.headLabels) headNode.addLabel(Label.label(label));
                    map.put(triple.head, headNode.getId());
                }
                if(tailNode == null) {
                    tailNode = graph.createNode();
                    if(singleProperty) tailNode.setProperty("name", triple.tail);
                    else setProperties(triple.tailProperties, tailNode);
                    if(triple.tailLabels.isEmpty()) tailNode.addLabel(Label.label("Entity"));
                    else for (String label : triple.tailLabels) tailNode.addLabel(Label.label(label));
                    map.put(triple.tail, tailNode.getId());
                }

                Relationship relationship = headNode.createRelationshipTo(tailNode, RelationshipType.withName(triple.relation));
                if(!singleProperty) setProperties(triple.relationProperties, relationship);
            });

            System.out.println("# Entities: " + graph.getAllNodes().stream().count());
            System.out.println("# Relations: " + graph.getAllRelationships().stream().count());
            System.out.println("# Relation Types: " + graph.getAllRelationshipTypes().stream().count());

            tx.success();
        }
        System.out.println("# Graph Population: " + (System.currentTimeMillis() - s) / 1000d + "s");
    }

    private static void setProperties(Map<String, Object> propertyMap, Entity entity) {
        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
            if(entry.getValue() instanceof Collection) {
                entity.setProperty(entry.getKey(), ((Collection) entry.getValue()).toArray(new String[0]));
            } else
                entity.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public static GraphDatabaseService createEmptyGraph(String home) {
        System.out.println("# Created New Neo4J Graph at: " + new File(home, "databases/graph.db").getAbsolutePath());
        deleteDirectory(new File(home, "databases"));
        return loadGraph(home);
    }

    private static boolean deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if(contents != null) {
            for (File content : contents) {
                deleteDirectory(content);
            }
        }
        return file.delete();
    }

    /**
     * Load existing graph if there is any.
     * @param home
     * @return
     */
    public static GraphDatabaseService loadGraph(String home) {
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static Set<Triple> readTriples(File file) {
        Set<Triple> triples = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                String[] words = processLine(l.nextLine());
                String head = words[0];
                String relation;
                if(words[1].toCharArray()[0] == '_') {
                    relation = words[1].substring(1).toUpperCase();
                } else relation = words[1];
                String tail = words[2];
                triples.add(new Triple(head, relation, tail));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return triples;
    }

    private static String[] processLine(String line) {
        String[] words = line.split("[\\s]");
        String[] results = new String[3];
        int count = 0;
        try {
            for (String word : words) if (word.length() != 0) {
                word = word.replaceFirst("[(|)]", "");
                results[count++] = word;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Error: Triple entry should be in the form of (sub pred obj)");
            e.printStackTrace();
            System.exit(-1);
        }
        return results;
    }

    public static class Triple {
        String head;
        String relation;
        String tail;

        Map<String, Object> headProperties;
        Map<String, Object> relationProperties;
        Map<String, Object> tailProperties;

        List<String> headLabels = new ArrayList<>();
        List<String> tailLabels = new ArrayList<>();

        Triple(String h, String r, String t) {
            head = h; relation = r; tail = t;
        }

        @Override
        public String toString() {
            return head + "\t" + relation + "\t" + tail;
        }

        @Override
        public int hashCode() {
            return head.hashCode() * 5
                    + relation.hashCode() * 2
                    + tail.hashCode() * 5;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Triple) {
                Triple other = (Triple) obj;
                return other.head.equals(head) && other.relation.equals(relation) && other.tail.equals(tail);
            }
            return false;
        }

        public static Set<String> getNodes(Collection<Triple> triples) {
            Set<String> nodes = new HashSet<>();
            triples.forEach( triple -> {
                nodes.add(triple.head);
                nodes.add(triple.tail);
            });
            return nodes;
        }

        public static Set<String> getRelationshipTypes(Collection<Triple> triples) {
            return triples.stream().map(triple -> triple.relation).collect(Collectors.toSet());
        }
    }
}
