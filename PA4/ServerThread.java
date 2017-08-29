package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import android.content.Context;
import android.content.ContentValues;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
/**
 * Created by deeptichavan on 4/28/17.
 */

public class ServerThread implements Runnable {


    static final int SERVER_PORT = 10000;
    public static ArrayList<String> msgs = new ArrayList<String>();

    public static String predPort = "";
    public static String succPort = "";

    @Override
    public void run() {

        ServerSocket ss;
        Context context = SimpleDynamoProvider.context;

        try {
            ss = new ServerSocket(SERVER_PORT);

            Message m;

            while (true) {

                Socket socket = ss.accept();

                //reads what client has to send
                ObjectInputStream inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                m = (Message) inputStream.readObject();

                ObjectOutputStream outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                Log.i("test", "Server :: msgType ::" + m.msgType + " -mp- " + m.myPort + " -tp- " + m.toPort + " -sp- " + m.succ1 + " -sp- " + m.succ2);

                if(m.msgType.equals("insert")){

                    Log.i("test", "Server :: First insert on port " + m.toPort);
                    ContentValues contentValues = new ContentValues();
                    contentValues.put("key", m.values[0]);
                    contentValues.put("value", m.values[1]);

                    insertMessage(contentValues);

                    Log.i("test", "Server :: Finding succ1 and succ2 of this port");
                    m.myPort = SimpleDynamoProvider.myPort;
                    m.succ1 = SimpleDynamoProvider.successor1;
                    m.succ2 = SimpleDynamoProvider.successor2;

                    Log.i("test", "Server :: Succ1 and Succ2 of :: " + m.myPort + " -- " + m.succ1 + " -- " + m.succ2);

                    Log.i("test", "Server :: Replicating this msg to succ1");
                    m.msgType = "replicateToSucc";
                    m.toPort = m.succ1;

                    forwardMessage(m);

                }else if(m.msgType.equals("replicateToSucc")){

                    //first successor got the msg, store and send it to next successor
                    Log.i("test", "Server :: first successor got the msg: " + m.succ1);
                    ContentValues contentValues = new ContentValues();
                    contentValues.put("key", m.values[0]);
                    contentValues.put("value", m.values[1]);
                    //SimpleDynamoProvider.context.getContentResolver().insert(Uri.parse(m.uri), contentValues);
                    insertMessage(contentValues);

                    //change the msgType
                    m.msgType = "replicateToSucc2";
                    m.toPort = m.succ2;

                    //now send it to next successor
                    forwardMessage(m);

                }else if(m.msgType.equals("replicateToSucc2")){

                    Log.i("test", "Server :: Second successor got the msg: " + m.succ2);
                    ContentValues contentValues = new ContentValues();
                    contentValues.put("key", m.values[0]);
                    contentValues.put("value", m.values[1]);
                    insertMessage(contentValues);

                }else if(m.msgType.equals("query")){

                    Log.i("test", "Server :: Return query result from this to the origin port: " + m.myPort + " -- " + m.originPort);
                    //context.getContentResolver().query(Uri.parse(m.uri), null, m.fileName, null, m.originPort);

                    //send result back to origin port
                    Message nm = new Message();
                    nm.msgType = "queryReply";
                    nm.values = fetchQueryResult(m.fileName);
                    nm.toPort = m.originPort;

                    ClientThread ct = new ClientThread(nm);
                    Thread t = new Thread(ct);
                    t.start();

                }else if("queryReply".equals(m.msgType)){

                    Log.i("test", "Server :: Return the retrieved query result: " + m.values[0]);
                    String[] columns = {"key", "value"};
                    MatrixCursor cr = new MatrixCursor(columns);
                    cr.addRow(m.values);
                    SimpleDynamoProvider.matrixCursor = cr;

                    Log.i("test", "Server :: MatrixCursor Set! :: " + SimpleDynamoProvider.matrixCursor.getCount());

                }else if("dump".equals(m.msgType)){

                    /*
                    * copy the result in a cursor and pass it on if not on the same port
                    * */
                    String[] columns = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);

                    Log.i("test", "Server :: gdump :: " + m.originPort + " :: " + m.myPort);

                    if (!(m.originPort.equals(SimpleDynamoProvider.myPort))) {

                        Log.i("test", "Done taking a round...");
                        SimpleDynamoProvider.isForwadedQuery = true;
                        SimpleDynamoProvider.originPort = m.originPort;
                        SimpleDynamoProvider.results = m.results;

                        context.getContentResolver().query(Uri.parse(m.uri), null, "@", null, m.originPort);

                    }else{
                        for(String k : m.results.keySet()){
                            String fileName = k;
                            String fileContent = m.results.get(k);
                            String[] row = {fileName, fileContent};
                            matrixCursor.addRow(row);
                        }
                        SimpleDynamoProvider.matrixCursor = matrixCursor;
                        SimpleDynamoProvider.shouldSend = true;
                    }

                }else if("dumpdelete".equals(m.msgType)){

                    if (!(m.originPort.equals(SimpleDynamoProvider.myPort))) {

                        Log.i("test", "Server :: Done taking a round");
                        SimpleDynamoProvider.isForwadedQuery = true;
                        SimpleDynamoProvider.originPort = m.originPort;

                        context.getContentResolver().delete(Uri.parse(m.uri), "@", null);

                    }

                }else if("delete".equals(m.msgType)){

                    Log.i("test", "Server :: Deleting message::" + m.fileName);
                    File f = new File(context.getFilesDir().getAbsolutePath() + "/" + m.fileName);
                    f.delete();

                    Log.i("test", "Server :: File Deleted!");

                }else if("deleteFirst".equals(m.msgType)){

                    Log.i("test", "Server :: Delete request received -- Deleting message::" + m.fileName);
                    File f = new File(context.getFilesDir().getAbsolutePath() + "/" + m.fileName);
                    f.delete();

                    Log.i("test", "Server :: File Deleted!");

                    //delete the replicas
                    Log.i("test", "Server :: Deleting from Successor 1 -- " + SimpleDynamoProvider.successor1);
                    forwardDelete(m.fileName, SimpleDynamoProvider.successor1);

                    Log.i("test", "Server :: Deleting from Successor 2 -- " + SimpleDynamoProvider.successor2);
                    forwardDelete(m.fileName, SimpleDynamoProvider.successor2);

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void forwardDelete(String fileName, String successor) {

        Log.i("test", "Server :: Forwarding delete request to successor ... " + successor + " -- " + fileName);
        Message nm = new Message();
        nm.msgType = "delete";
        nm.toPort = successor;
        nm.fileName = fileName;

        ClientThread ct = new ClientThread(nm);
        Thread t = new Thread(ct);
        t.start();
    }

    private String[] fetchQueryResult(String fileName) {

        File f = new File(SimpleDynamoProvider.context.getFilesDir().getAbsolutePath());
        String[] arr = new String[2];
        try {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(f + "/" + fileName));
            String fileContent = bufferedReader.readLine();
            arr[0] = fileName;
            arr[1] = fileContent;

            Log.i("test", "Message Retrieved!" + " :::: " + fileName + " :::: " + fileContent);
            bufferedReader.close();

        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

        return arr;

    }

    private void forwardMessage(Message m) {

        Log.i("test", "Server:: forwarding msg to successor ::" + m.toPort);


        ClientThread ct = new ClientThread(m);
        Thread t = new Thread(ct);
        t.start();
    }


    private synchronized void insertMessage(ContentValues values){

        String fileName = values.getAsString("key");
        String fileContent = values.getAsString("value");

        SimpleDynamoProvider.CPHash.put(fileName, fileContent);

        Log.i("test", "File write on port -- fn-fc-p ::" + fileName + " :: " + fileContent);

        try {
            File f = new File(SimpleDynamoProvider.context.getFilesDir().getAbsolutePath(), fileName);
            FileWriter fileWriter = new FileWriter(f);
            fileWriter.write(fileContent);
            fileWriter.close();

            Log.i("test", "Done writing!");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
