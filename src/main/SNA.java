package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.xml.sax.InputSource;

public class SNA {

	private static final String DB_PATH = "C:\\Users\\123\\workspace\\SNA";
	private static final int maxLevel = 3;
	static int count = 0;
	static int errorCount = 0;
	static ArrayList<String> idList = new ArrayList<String>();
	static ArrayList<String> errIdList = new ArrayList<String>();
	static GraphDatabaseService neoDB = new GraphDatabaseFactory()
			.newEmbeddedDatabase(DB_PATH);

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
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

	public static String getField(String name, Element e) {
		if (e.getChild(name) != null)
			return e.getChild(name).getText();
		return "";
	}

	public static void addToDB(String xml, String id) throws Exception {
		SAXBuilder builder = new SAXBuilder();
		Document doc = builder.build(new InputSource(new StringReader(xml)));
		Element root = doc.getRootElement();
		Element user = root.getChild("user");

		String uid = getField("uid", user);
		String firstName = getField("first_name", user);
		String lastName = getField("last_name", user);
		String sex = getField("sex", user);
		String bDate = getField("bdate", user);
		String city = getField("city", user);
		String canPost = getField("can_post", user);
		String status = getField("status", user);
		String relation = getField("relation", user);
		String nickname = getField("nickname", user);
		String interests = getField("interests", user);
		String movies = getField("movies", user);
		String tv = getField("tv", user);
		String books = getField("books", user);
		String games = getField("games", user);
		String about = getField("about", user);

		ExecutionEngine engine = new ExecutionEngine(neoDB);
		Transaction tx = neoDB.beginTx();
		try {
			ExecutionResult result = engine
					.execute("start m=node(*) where m.uid?=" + id
							+ "create n={uid:\'" + uid + "\', firstName:\'"
							+ firstName + "\', lastName:\'" + lastName
							+ "\', sex:\'" + sex + "\', bDate:\'" + bDate
							+ "\', city:\'" + city + "\', canPost:\'" + canPost
							+ "\', relation:\'" + relation + "\', nickname:\'"
							+ nickname + "\', interests:\'" + interests
							+ "\', movies:\'" + movies + "\', tv:\'" + tv
							+ "\', books:\'" + books + "\', games:\'" + games
							+ "\', about:\'" + about + "\'}, n-[:FRIEND]-m");
			tx.success();
		} finally {
			tx.finish();
		}
	}

	public static String getPersonData(String id) {
		String response = requestGET("https://api.vk.com/method/users.get.xml?uid="
				+ id
				+ "&fields=sex,bdate,city,can_post,status,relation,counters,nickname,relatives,activities,interests,movies,tv,books,games,about,personal"
				+ "&access_token=3bf38361ee194107e45bb79a19dbbc5a7f93d25e23e2a23a96768e8a00b63ef2f76c657d4b64ff6cd64ce");
		return response;
	}

	static public void readFriends(int level, String startId)
			throws InterruptedException, FileNotFoundException {
		if (level <= maxLevel) {
			Thread.sleep(500);
			String response = requestGET("https://api.vk.com/method/friends.get?uid="
					+ startId
					+ "&access_token=3bf38361ee194107e45bb79a19dbbc5a7f93d25e23e2a23a96768e8a00b63ef2f76c657d4b64ff6cd64ce");
			int start = response.indexOf('[');
			int end = response.indexOf(']');
			String id_block = response.substring(start + 1, end);
			String[] ids = id_block.split(",");
			for (String id : ids) {
				if (idList.contains(id)) {
					continue;
				}
				idList.add(id);
				Thread.sleep(200);
				System.out.println(++count + " " + "lvl" + level + " " + id
						+ "err-" + errorCount);
				try {
					addToDB(getPersonData(id), startId);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					Thread.sleep(1000);
					++errorCount;
					errIdList.add(id);
					PrintStream st = new PrintStream(new FileOutputStream(
							"errOutput.txt"));
					System.setErr(st);
					e.printStackTrace();
					continue;
				}
			}
			for (String id : ids) {
				readFriends(level + 1, id);
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		registerShutdownHook(neoDB);
		// TODO Auto-generated method stub
		try {
			// ExecutionEngine engine = new ExecutionEngine(neoDB);
			// ExecutionResult result =
			// engine.execute("create n={uid:\"146143804\"}");
			readFriends(0, "146143804");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			for (int i = 0; i < errIdList.size(); ++i)
				System.out.print(errIdList.get(i));
		}
	}
}
