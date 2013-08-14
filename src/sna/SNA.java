package sna;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;
import org.xml.sax.InputSource;

public class SNA {

    public static final String BASE_URI = "https://api.vk.com/method/";
    private static final int MAX_LEVEL = 2;
    private static final String DB_PATH = "D:\\neo4j\\data\\graph.db"; //windows
//    private static final String DB_PATH = "/home/oleg/neo4j_DB"; //linux
    private static final String FRIEND_LIST_KEY = "friend_list";
    private static final GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
    private static Index<Node> nodeIndex;

    private static enum RelTypes implements RelationshipType {

        FRIEND
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public static String requestGET(String address) {
        String result = "";
        try {
            URL url = new URL(address);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("User-Agent", "Java bot");
            conn.connect();
            int code = conn.getResponseCode();
            if (code == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        conn.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    result += inputLine;
                }
                in.close();
            }
            conn.disconnect();
            conn = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String[] getPersonFriends(String uid) {
        String response = requestGET("https://api.vk.com/method/friends.get?uid=" + uid);
        int start = response.indexOf('[');
        int end = response.indexOf(']');
        String uidsStr = response.substring(start + 1, end);
        return uidsStr.split(",");
    }

    public static String getPersonXMLData(String id) {
        StringBuilder request = new StringBuilder(BASE_URI);
        request = request.append("users.get.xml?uid=");
        request = request.append(id);
        request = request.append("&fields=");
        request = request.append("sex");
        request = request.append(",bdate");
        request = request.append(",city");
        request = request.append(",can_post");
        request = request.append(",status");
        request = request.append(",relation");
        request = request.append(",nickname");
        request = request.append(",relatives");
        request = request.append(",activities");
        request = request.append(",interests");
        request = request.append(",movies");
        request = request.append(",tv");
        request = request.append(",books");
        request = request.append(",games");
        request = request.append(",about");
        request = request.append(",personal");
        String response = requestGET(request.toString());
        return response;
    }

    public static String getOneField(String name, Element e) {
        if (e.getChild(name) != null) {
            String temp = e.getChild(name).getText().replaceAll("\'", "");
            if (!temp.equals("")) {
                return temp;
            }
        }
        return "?";
    }

    public static HashMap<String, Object> parsePersonXML(String xml) {
        HashMap<String, Object> personData = new HashMap<String, Object>();
        try {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(new InputSource(new StringReader(xml)));
            Element root = doc.getRootElement();
            Element user = root.getChild("user");

            personData.put("uid", getOneField("uid", user));
            personData.put("first_name", getOneField("first_name", user));
            personData.put("last_name", getOneField("last_name", user));
            personData.put("sex", getOneField("sex", user));
            personData.put("bdate", getOneField("bdate", user));
            personData.put("city", getOneField("city", user));
            personData.put("can_post", getOneField("can_post", user));
            personData.put("status", getOneField("status", user));
            personData.put("relation", getOneField("relation", user));
            personData.put("nickname", getOneField("nickname", user));
            personData.put("interests", getOneField("interests", user));
            personData.put("movies", getOneField("movies", user));
            personData.put("tv", getOneField("tv", user));
            personData.put("books", getOneField("books", user));
            personData.put("games", getOneField("games", user));
            personData.put("about", getOneField("about", user));
        } catch (JDOMException ex) {
            Logger.getLogger(SNA.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SNA.class.getName()).log(Level.SEVERE, null, ex);
        }
        return personData;
    }

    public static Node addToDB(HashMap<String, Object> personProperties) {
        Node tempPerson = null;
        Transaction tx = graphDb.beginTx();
        try {
            tempPerson = graphDb.createNode();
            Iterator<String> keySetIterator = personProperties.keySet().iterator();
            while (keySetIterator.hasNext()) {
                String key = keySetIterator.next();
                tempPerson.setProperty(key, personProperties.get(key));
            }
            nodeIndex.add(tempPerson, "uid", personProperties.get("uid"));
            tx.success();
        } finally {
            tx.finish();
        }
        return tempPerson;
    }

    public static Node addToDB(HashMap<String, Object> personProperties, String parentUid) {
        Node tempPerson = addToDB(personProperties);
        Node parentPerson = nodeIndex.get("uid", parentUid).getSingle();
        Transaction tx = graphDb.beginTx();
        try {
            tempPerson.createRelationshipTo(parentPerson, RelTypes.FRIEND);
            tx.success();
        } finally {
            tx.finish();
        }
        return tempPerson;
    }

    public static void dowloadData(String beginUid) {
        HashMap<String, Object> personData = parsePersonXML(getPersonXMLData(beginUid));
        String[] friendUids = getPersonFriends(beginUid);
        personData.put(FRIEND_LIST_KEY, friendUids);
        addToDB(personData);
        for (String tempFriend : friendUids) {
            recDownloadData(tempFriend, beginUid, 1);
        }
    }

    public static void recDownloadData(String tempUid, String parentUid, int lvl) {
        if (lvl == MAX_LEVEL) {
            return;
        }
        System.out.println("Parsing tempUid = " + tempUid + " parentUid = " + parentUid + " lvl = " + lvl);
        HashMap<String, Object> personData = parsePersonXML(getPersonXMLData(tempUid));
        String[] friendsUids = getPersonFriends(tempUid);
        personData.put(FRIEND_LIST_KEY, friendsUids);
        addToDB(personData, parentUid);
        for (String tempFriend : friendsUids) {
            recDownloadData(tempFriend, tempUid, lvl + 1);
        }
    }

    public static void setAllRelations() {
        Transaction tx = graphDb.beginTx();
        try {
            GlobalGraphOperations glOper = GlobalGraphOperations.at(graphDb);
            Iterator<Node> allNodes = glOper.getAllNodes().iterator();
            while (allNodes.hasNext()) {
                setOneNodeRelations(allNodes.next());
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public static void setOneNodeRelations(Node tempNode) {
        if (tempNode.hasProperty(FRIEND_LIST_KEY)) {
            String[] friendsUidList = (String[]) tempNode.getProperty(FRIEND_LIST_KEY);
            for (String tempFriendUid : friendsUidList) {
                Node tempFriend = nodeIndex.get("uid", tempFriendUid).getSingle();
                if (tempFriend == null) {
                    continue;
                }
                Iterable<Relationship> relationships = tempFriend.getRelationships(Direction.OUTGOING);
                boolean relationAlreadyExist = false;
                for (Relationship tempRel : relationships) {
                    if (tempRel.getEndNode().getId() == tempNode.getId()) {
                        relationAlreadyExist = true;
                        break;
                    }
                }
                if (!relationAlreadyExist) {
                    tempNode.createRelationshipTo(tempFriend, RelTypes.FRIEND);
                }
            }
        }
    }

    public static void main(String[] args) {
        registerShutdownHook(graphDb);
        nodeIndex = graphDb.index().forNodes("uids");
//        dowloadData("55827129");
        setAllRelations();

    }
}