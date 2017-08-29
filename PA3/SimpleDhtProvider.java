package edu.buffalo.cse.cse486586.simpledht;

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
import java.util.TreeMap;
import java.io.*;
import android.os.AsyncTask;


public class SimpleDhtProvider extends ContentProvider {

    public static TreeMap<String,String> chordRing = new TreeMap<String, String>();
    public static HashMap<String,String> results = new HashMap<String, String>();

    public static String myPort;
    public static String myID;
    public static String predecessor;
    public static String successor;
    public static String predID;
    public static String originPort;
    public static String smallestPort;
    public static String largestPort;
    public static String smallestID;
    public static String largestID;

    public static MatrixCursor matrixCursor = null;
    public static BufferedReader bufferedReader = null;

    public static Context context;
    public static  Uri myUri;
    public static ContentResolver myContentResolver;

    public static boolean isForwadedQuery = false;
    public static boolean send = false;
    public static boolean onCurrent = true;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public void insertMessage(ContentValues values){

        String fileName = values.getAsString("key");
        String fileContent = values.getAsString("value");

        Log.i("test", "file write::" + fileName + " :: " + fileContent + " :: " + myPort);
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

    public void forwardMessage(Uri uri, ContentValues values, String successor){

        Message nm = new Message();
        nm.msgType = "insert";
        nm.toPort = successor;
        nm.myPort = myPort;
        nm.uri = uri.toString();
        String[] arr = {values.getAsString("key"), values.getAsString("value")};
        nm.values = arr;

        ClientThread clientThread = new ClientThread(nm);
        Thread thread = new Thread(clientThread);
        thread.start();

        Log.i("test", "Message forwarded! to port::" + values.getAsString("key") + " ::::: " + nm.toPort);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        /*
        * get hash of the key
        * compare with my.id and pred.id
        * */

        Log.i("test", "onCurrent::" + onCurrent);
        if(onCurrent == false) {
            Log.i("test", "in insert::" + myPort + " -p- " + predecessor + " -s- " + successor);
            Log.i("test", "in insert:: s & l::" + smallestPort + " -- " + largestPort);
            Log.i("test", "values::" + values.getAsString("key") + " :: " + values.getAsString("value"));
            try {
                String hashKey = genHash(values.getAsString("key"));
                myID = genHash(String.valueOf((Integer.parseInt(myPort) / 2)));
                predID = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));
                smallestID = genHash(String.valueOf((Integer.parseInt(smallestPort) / 2)));
                largestID = genHash(String.valueOf((Integer.parseInt(largestPort) / 2)));

                Log.i("test", "hashes: " + hashKey + " :: " + myID + " :: " + predID);

                if (hashKey.compareTo(myID) < 0 && hashKey.compareTo(predID) > 0) {

                    Log.i("test", "hashkey inbetween myPort and pred, so insert");
                    insertMessage(values);

                }else if (myID.equals(smallestID) && hashKey.compareTo(largestID) > 0) {

                    Log.i("test", "On smallest and hash > largest, so insert on smallest node, i.e. current node");
                    insertMessage(values);

                } else if (hashKey.compareTo(myID) > 0 && !successor.equals(myPort)) {

                    Log.i("test", "HashKey greater than myPort, so forward it to successor");
                    forwardMessage(uri, values, successor);

                } else if (predecessor.equals(myPort)) {

                    Log.i("test", "Same pred and myPort, so insert");
                    insertMessage(values);

                } else if (hashKey.compareTo(myID) < 0 && hashKey.compareTo(predID) < 0 && !myID.equals(smallestID)) {

                    Log.i("test", "Hash key less than myport and pred, so forward it to predecessor");
                    forwardMessage(uri, values, predecessor);

                }  else if (myID.equals(smallestID) && hashKey.compareTo(myID) < 0) {

                    Log.i("test", "On smallest and hash < myID, so insert on smallest node, i.e. current node");
                    insertMessage(values);

                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }else{
            Log.i("test", "onCurrent = true");
            insertMessage(values);
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        context = getContext();
        myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        myContentResolver = context.getContentResolver();

        myPort = getPort();

        predecessor = myPort;
        successor = myPort;
        smallestPort = myPort;
        largestPort = myPort;

        //start server
        ServerThread server = new ServerThread();
        Thread serverThread = new Thread(server);
        serverThread.start();


        /*
        * send join message to 11108, if that is where you are not
        * */

//        try {
//            Thread.sleep(10000);
//        }catch (InterruptedException e){
//            e.printStackTrace();
//        }

        if(!("11108".equals(myPort))){

            Message m = new Message();
            m.msgType = "join";
            m.toPort = "11108";
            m.myPort = myPort;

            new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,m);
            ClientThread joiningThread = new ClientThread(m);
            Thread clientThread = new Thread(joiningThread);
            clientThread.start();

        }else{

            /*
            * make a ring with port and hash of avd value
            * */
            try{
                chordRing = new TreeMap<String, String>(new Comparator<String>() {
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
                        Log.i("test", "BigInts::" + p1 + " -- " + p2);



                        return p1.compareTo(p2);
                    }
                });
                chordRing.put(myPort, genHash(String.valueOf((Integer.parseInt(myPort) / 2))));

            }catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }
        }


        try {
            Thread.sleep(10000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }



        Log.i("test", "in main:" + myPort + " -- " + predecessor + " -- " + successor + " -s- " + smallestPort + " -l- " + largestPort);

        return true;
    }

    public Cursor retrieveMessage(String fileName){

        Log.i("test", "in retrieveMessage");
        File f = new File(context.getFilesDir().getAbsolutePath());
        try {

            bufferedReader = new BufferedReader(new FileReader(f + "/" + fileName));
            String fileContent = bufferedReader.readLine();
            String[] result = {fileName, fileContent};
            matrixCursor.addRow(result);

            Log.i("test", "Message Retrieved!" + " :::: " + fileContent);
            bufferedReader.close();

            if(isForwadedQuery){

                Message nm = new Message();
                nm.msgType = "reply";
                nm.toPort = originPort;
                nm.values = result;
                nm.myPort = myPort;

                ClientThread clientThread = new ClientThread(nm);
                Thread thread = new Thread(clientThread);
                thread.start();

            }
            isForwadedQuery = false;


        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

        return matrixCursor;
    }

    public void forwardQuery(Uri uri, String fileName, String successor){

        Message nm = new Message();
        nm.msgType = "query";
        nm.toPort = successor;
        nm.myPort = myPort;
        nm.uri = uri.toString();
        nm.fileName = fileName;
        nm.originPort = originPort;


        ClientThread clientThread = new ClientThread(nm);
        Thread thread = new Thread(clientThread);
        thread.start();

        Log.i("test", "Key forwarded to port::" + nm.toPort + " with origin port::" + nm.originPort);

    }

    public void getGDump(Uri uri){

        Log.i("test", "GDump::" + context.getFilesDir().getAbsolutePath() + " :: " + myPort);
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
        nm.succPort = successor;
        nm.predPort = predecessor;
        nm.results = results;
        nm.uri = uri.toString();

        ClientThread clientThread = new ClientThread(nm);
        Thread thread = new Thread(clientThread);
        thread.start();
    }

    public void getLDump(Uri uri){

        Log.i("test", "LDump::" + context.getFilesDir().getAbsolutePath() + " :: " + myPort);
        File f = new File(context.getFilesDir().getAbsolutePath());
        for(File tf : f.listFiles()){
            String fileName = tf.getName();
            Log.i("test", "getting file::" + fileName);
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
        * ask other avds
        * */

        if(isForwadedQuery) {
            Message nm = new Message();
            nm.msgType = "dump";
            nm.myPort = myPort;
            nm.succPort = successor;
            nm.predPort = predecessor;
            nm.results = results;
            nm.uri = uri.toString();
            nm.originPort = originPort;
            nm.toPort = originPort;

            ClientThread clientThread = new ClientThread(nm);
            Thread thread = new Thread(clientThread);
            thread.start();
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub


        String fileName = selection;
        Log.i("test", "query for::" + fileName);

        String[] columns = {"key", "value"};
        matrixCursor = new MatrixCursor(columns);

        if("*".equals(fileName)){

            /*
            * get messages from all the avds present in the ring
            * */

            getGDump(uri);
            /*
            * wait till you get results from all the avds
            * */


        }else if("@".equals(fileName)){

            /*
            * get messages from the local avd
            * */

            getLDump(uri);


        }else{

            /*
            * retrieve message with the given key
            * */

            try {
                String hashKey = genHash(fileName);
                myID = genHash(String.valueOf((Integer.parseInt(myPort) / 2)));
                predID = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));

                if(!isForwadedQuery){
                    originPort = myPort;
                }else{
                    originPort = sortOrder;
                }

                Log.i("test", "myport::pred && origin--" + myPort + " -- " + predecessor + " --o--" + originPort);
                Log.i("test", "query hashes::" + hashKey + " :: " + myID + " :: " + predID);


                if(hashKey.compareTo(myID) < 0 && hashKey.compareTo(predID) > 0){
                    return retrieveMessage(fileName);
                }else if(hashKey.compareTo(myID) > 0 && !successor.equals(myPort)){
                    forwardQuery(uri, fileName, successor);
                }else if(predecessor.equals(myPort)){
                    return retrieveMessage(fileName);
                }


            }catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }

        }



        isForwadedQuery = false;

        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public String getPort(){

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String avdNo = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(avdNo) * 2));

        return myPort;
    }
}
