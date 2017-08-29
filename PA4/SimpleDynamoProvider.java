package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.content.Context;
import android.util.Log;
import android.content.ContentResolver;
import java.io.BufferedReader;
import android.database.MatrixCursor;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.io.*;

public class SimpleDynamoProvider extends ContentProvider {


	public static Context context;
	public static  Uri myUri;
	public static ContentResolver myContentResolver;

	public static String[] allPorts = {"11108", "11112", "11116", "11120", "11124"};
	public static TreeMap<String, String> ring = new TreeMap<String, String>();
	public static String myPort, successor1, successor2, predecessor1, predecessor2;
	public static String originPort;

	public static HashMap<String, String> CPHash = new HashMap<String, String>();
	public static HashMap<String,String> results = new HashMap<String, String>();
	public static MatrixCursor matrixCursor = null;

	public static boolean shouldSend = false;
	public static boolean isForwadedQuery = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String fileName = selection;
		Log.i("test", "Delete fileName :: " + fileName);

		if("*".equals(fileName)){

            /*
            * get messages from all the avds present in the ring
            * */

			deleteGlobally(uri);
            /*
            * wait till you get results from all the avds
            * */


		}else if("@".equals(fileName)){

            /*
            * get messages from the local avd
            * */

			deleteLocally(uri);


		}else{

			//find where the key is stored and delete it along with its replica
			String key = selection;
			String hashKey = "";
			try {
				hashKey = genHash(key);
			}catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}

			String storePort = getStorePort(hashKey);


			Log.i("test", "Key and Store Port :: " + key + " -- " + storePort);

			Log.i("test", "Key and hashkey and ring :: " + key + " -- " + hashKey + " -- " + ring);

			if(myPort.equals(storePort)){

				deleteMessage(fileName);

				deleteFromSucc(fileName, successor1);
				deleteFromSucc(fileName, successor2);

			}else{

				forwardDelete(fileName, storePort);
			}

			
		}
		return 0;
	}

	private void forwardDelete(String fileName, String storePort) {

		Log.i("test", "Message to be deleted stored on :: " + storePort + " -- " + fileName);
		Message nm = new Message();
		nm.msgType = "deleteFirst";
		nm.toPort = storePort;
		nm.fileName = fileName;

		ClientThread ct = new ClientThread(nm);
		Thread t = new Thread(ct);
		t.run();
	}

	private void deleteFromSucc(String fileName, String successor) {

		Message nm = new Message();
		nm.msgType = "delete";
		nm.fileName = fileName;
		nm.toPort = successor;

		ClientThread ct = new ClientThread(nm);
		Thread t = new Thread(ct);
		t.start();

	}

	private void deleteMessage(String fileName){

		Log.i("test", "Deleting message::" + fileName);
		File f = new File(context.getFilesDir().getAbsolutePath() + "/" + fileName);
		f.delete();

		Log.i("test", "File Deleted!");

	}

	private void deleteLocally(Uri uri){

		Log.i("test", "Local Delete:: All files");
		File file = new File(context.getFilesDir().getAbsolutePath());
		for (File f : file.listFiles()) {

			String fileName = f.getName();
			Log.i("test", "Deleting ::" + fileName);
			f.delete();

		}

	}


	private void deleteGlobally(Uri uri) {

		Log.i("test", "Global Delete :: All files");
		File file = new File(context.getFilesDir().getAbsolutePath());
		for (File f : file.listFiles()) {

			String fileName = f.getName();
			Log.i("test", "Deleting ::" + fileName);
			f.delete();

		}

        /*
        * delete from other avds too
        * */

		Message nm = new Message();
		nm.msgType = "dumpdelete";
		nm.toPort = successor1;
		nm.myPort = myPort;
		nm.uri = uri.toString();
		nm.originPort = originPort;

		ClientThread clientThread = new ClientThread(nm);
		Thread thread = new Thread(clientThread);
		thread.start();

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		/*
		* check which partition the key should be in
		* if it's this avd, then insert directly and forward it to its 2 successors (chain replication)
		* else forward the key to it's intended node
		* */

		String key = values.getAsString("key");
		String hashKey = "";
		try {
			 hashKey = genHash(key);
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		String storePort = getStorePort(hashKey);


		Log.i("test", "Key and Store Port :: " + key + " -- " + storePort);

		Log.i("test", "Key and hashkey and ring :: " + key + " -- " + hashKey + " -- " + ring);

		if(myPort.equals(storePort)){

			//insert
			insertMessage(values);
			//replicate to two other next ports
			replicateMessage(uri, values);

		}else{

			//forward to storePort
			forwardMessage(uri, values, storePort);
		}

		return null;
	}

	private void forwardMessage(Uri uri, ContentValues values, String storePort) {

		Log.i("test", "Message should be forwarded to port:: " + storePort);
		Message nm = new Message();
		nm.msgType = "insert";
		nm.toPort = storePort;
		nm.myPort = storePort;
		String[] arr = {values.getAsString("key"), values.getAsString("value")};
		nm.values = arr;

		Log.i("test", "Forwarding message to desired port " + storePort);
		ClientThread ct = new ClientThread(nm);
		Thread t = new Thread(ct);
		t.start();
	}

	private void replicateMessage(Uri uri, ContentValues values) {

		//forward message to succ1 and succ2 of myPort
		Message nm = new Message();
		nm.msgType = "replicateToSucc";
		nm.uri = uri.toString();
		nm.succ1 = successor1;
		nm.succ2 = successor2;
		nm.toPort = successor1;
		String[] arr = {values.getAsString("key"), values.getAsString("value")};
		nm.values = arr;

		Log.i("test", "Sending message to succ1 for replication");
		ClientThread ct = new ClientThread(nm);
		Thread t = new Thread(ct);
		t.start();



	}

	public void insertMessage(ContentValues values){

		String fileName = values.getAsString("key");
		String fileContent = values.getAsString("value");

		CPHash.put(fileName, fileContent);

		Log.i("test", "File write on port -- fn-fc-p ::" + fileName + " :: " + fileContent + " :: " + myPort);

		try {
			File f = new File(context.getFilesDir().getAbsolutePath(), fileName);
			FileWriter fileWriter = new FileWriter(f);
			fileWriter.write(fileContent);
			fileWriter.close();

			Log.i("test", "Done writing!");
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub


		//get predecessors and successor for this node

		context = getContext();
		myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		myContentResolver = context.getContentResolver();

		myPort = getPort();

		makeRing();
		assignPredSucc();

		ServerThread server = new ServerThread();
		Thread serverThread = new Thread(server);
		serverThread.start();


		return false;
	}

	private void makeRing() {


		ring = new TreeMap<String, String>(new Comparator<String>() {
			@Override
			public int compare(String port1, String port2) {
				//Log.i("test", "compare to:" + port1 + " -- " + port2);
				try {
					port1 = genHash(String.valueOf(Integer.parseInt(port1) / 2));
					port2 = genHash(String.valueOf(Integer.parseInt(port2) / 2));
					//Log.i("test", "compare to:" + port1 + " -- " + port2);

				}catch (NoSuchAlgorithmException e){
					e.printStackTrace();
				}

				BigInteger p1 = new BigInteger(port1, 16);
				BigInteger p2 = new BigInteger(port2, 16);
				//Log.i("test", "BigInts::" + p1 + " -- " + p2);

				return p1.compareTo(p2);
			}
		});

		// ring is fixed, has all 5 avds
		try {
			int i;
			for (i = 0; i < allPorts.length; i++) {

				String p = allPorts[i];
				ring.put(p, genHash(String.valueOf((Integer.parseInt(p) / 2))));
			}

			Log.i("test", "Ring formed :: " + ring);

		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

	}

	private void assignPredSucc() {

		if(myPort.equals("11108")){

			successor1 = "11116";
			successor2 = "11120";
			predecessor1 = "11112";
			predecessor2 = "11124";

		}else if (myPort.equals("11116")){

			successor1 = "11120";
			successor2 = "11124";
			predecessor1 = "11108";
			predecessor2 = "11112";

		}else if (myPort.equals("11120")){

			successor1 = "11124";
			successor2 = "11112";
			predecessor1 = "11116";
			predecessor2 = "11108";

		}else if (myPort.equals("11124")){

			successor1 = "11112";
			successor2 = "11108";
			predecessor1 = "11120";
			predecessor2 = "11116";

		}else if (myPort.equals("11112")){

			successor1 = "11108";
			successor2 = "11116";
			predecessor1 = "11124";
			predecessor2 = "11120";

		}
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub


		String fileName = selection;

		Log.i("test", "SDP :: query for :: " + fileName);

		String[] columns = {"key", "value"};
		matrixCursor = new MatrixCursor(columns);

		if("@".equals(fileName)){

			getLDump(uri);

		}else if("*".equals(fileName)){

			getGDump(uri);

		}else{

			//find where this key is stored

			String hashKey = "";
			try {
				hashKey = genHash(fileName);
			}catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}

			String storePort = getStorePort(hashKey);

			//find the 2nd successor directly...(as read from tail)
			String retrievePort = find2ndSuccessor(storePort);

			//get the result from this port
			forwardQuery(uri, fileName, retrievePort);

			try {
				Thread.sleep(1000);
			}catch (InterruptedException e){
				e.printStackTrace();
			}

		}


		Log.i("test", "SDP :: Returning Matrix Cursor -- count :: " + matrixCursor.getCount());

		return matrixCursor;
	}

	private void getGDump(Uri uri) {

		HashMap<String, String> results = new HashMap<String, String>();
		Log.i("test", "GDump :: " + context.getFilesDir().getAbsolutePath() + " :: " + myPort);
		File f = new File(context.getFilesDir().getAbsolutePath());
		for(File tf : f.listFiles()){
			String fileName = tf.getName();
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(f + "/" + fileName));
				String fileContent = bufferedReader.readLine();
				String[] row = {fileName, fileContent};
				matrixCursor.addRow(row);
				results.put(fileName, fileContent);

			}catch (FileNotFoundException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
		}

        /*
        * ask other avds
        * */

		Message nm = new Message();
		nm.msgType = "dump";
		nm.myPort = myPort;
		nm.results = results;
		nm.uri = uri.toString();
		nm.toPort = successor1;
		nm.originPort = myPort;

		ClientThread clientThread = new ClientThread(nm);
		Thread thread = new Thread(clientThread);
		thread.start();

		while (!shouldSend) {

		}
		shouldSend = false;
	}

	private void forwardQuery(Uri uri, String fileName, String retrievePort) {

		Message nm = new Message();
		nm.msgType = "query";
		nm.toPort = retrievePort;
		nm.originPort = myPort;
		nm.fileName = fileName;
		nm.myPort = retrievePort;
		nm.uri = uri.toString();

		ClientThread ct = new ClientThread(nm);
		Thread t = new Thread(ct);
		t.start();
	}

	private String find2ndSuccessor(String storePort) {

		if("11108".equals(storePort)){
			return "11120";
		}else if("11116".equals(storePort)){
			return "11124";
		}else if("11120".equals(storePort)){
			return "11112";
		}else if("11124".equals(storePort)){
			return "11108";
		}else{
			return "11116";
		}
	}

	private void getLDump(Uri uri) {

		Log.i("test", "SDP :: LDump :: " + context.getFilesDir().getAbsolutePath() + " :: " + myPort + " :: size: " + matrixCursor.getCount());

		File f = new File(context.getFilesDir().getAbsolutePath());

		for(File tf : f.listFiles()){

			String fileName = tf.getName();
			Log.i("test", "Getting file ::" + fileName);
			try {

				BufferedReader bufferedReader = new BufferedReader(new FileReader(f + "/" + fileName));
				String fileContent = bufferedReader.readLine();
				String[] row = {fileName, fileContent};
				matrixCursor.addRow(row);
				results.put(fileName, fileContent);

				Log.i("test", "File retrieved!");

			}catch (FileNotFoundException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
		}

		/*
        * ask other avds, in case of *
        * */

		Log.i("test", "LDump Is Forwarded:" + isForwadedQuery);

		if(isForwadedQuery) {
			Message nm = new Message();
			nm.msgType = "dump";
			nm.myPort = myPort;
			nm.results = results;
			nm.uri = uri.toString();
			nm.originPort = originPort;
			nm.toPort = successor1;

			ClientThread clientThread = new ClientThread(nm);
			Thread thread = new Thread(clientThread);
			thread.start();
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	public String getPort(){

		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String avdNo = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPort = String.valueOf((Integer.parseInt(avdNo) * 2));

		return myPort;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public String getStorePort(String hashKey) {

		/*
		* iterate over the ring to find where the key needs to be stored
		* */

		String storePort = myPort;

		Log.i("test", "hashkey :: " + hashKey);
		Log.i("test", "ring :: " + ring);

		if(hashKey.compareTo(ring.get("11108")) > 0 && hashKey.compareTo(ring.get("11116")) < 0){
			storePort = "11116";
		}else if(hashKey.compareTo(ring.get("11116")) > 0 && hashKey.compareTo(ring.get("11120")) < 0){
			storePort = "11120";
		}else if(hashKey.compareTo(ring.get("11124")) > 0 && hashKey.compareTo(ring.get("11112")) < 0){
			storePort = "11112";
		}else if(hashKey.compareTo(ring.get("11112")) > 0 && hashKey.compareTo(ring.get("11108")) < 0){
			storePort = "11108";
		}else if(hashKey.compareTo(ring.get("11120")) > 0 ){
			storePort = "11124";
		}else if(hashKey.compareTo(ring.get("11124")) < 0){
			storePort = "11124";
		}

		return storePort;
	}
}


