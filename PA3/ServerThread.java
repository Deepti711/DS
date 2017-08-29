package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;

import android.content.ContentValues;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

/**
 * Created by deeptichavan on 4/7/17.
 */

public class ServerThread implements Runnable{

    static final int SERVER_PORT = 10000;
    public static ArrayList<String> msgs = new ArrayList<String>();

    public static String predPort = "";
    public static String succPort = "";

    @Override
    public void run() {

        ServerSocket ss;

        try {
            ss = new ServerSocket(SERVER_PORT);

            Message m = new Message();

            while (true){

                Socket socket = new Socket();
                socket = ss.accept();

                ObjectInputStream inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                m = (Message)inputStream.readObject();

                Log.i("test", "Server: " + m.msgType + " -- " + m.myPort + " -- " + m.toPort);

                try {
                    if ("join".equals(m.msgType)) {
                        SimpleDhtProvider.chordRing.put(m.myPort, SimpleDhtProvider.genHash(String.valueOf((Integer.parseInt(m.myPort) / 2))));
                    }
                }catch (NoSuchAlgorithmException e){
                    e.printStackTrace();
                }

                /*
                * decide the predecessor and successor for this port/avd
                * next one is succ
                * prev one is pred
                * */

                int i;
                if("join".equals(m.msgType)) {
                    /*
                    * iterate over the ring and decide pred and succ for each avd, as the ring changes when a new avd is added
                    * */

                    Log.i("test", "Server-ChordRing- " + SimpleDhtProvider.chordRing);

                    String temp="";
                    SimpleDhtProvider.onCurrent = false;
                    ArrayList<Message> messages = new ArrayList<Message>();
                    String[] arr = SimpleDhtProvider.chordRing.keySet().toArray(new String[SimpleDhtProvider.chordRing.size()]);

                    Log.i("test", "arr:" + Arrays.toString(arr));
                    Log.i("test", "size:" + SimpleDhtProvider.chordRing.size());
                    Log.i("test", "myport:" + m.myPort);

                    /*
                    * find the smallest and the largest node
                    * */

                    String smallestPort = arr[0];
                    String largestPort = arr[arr.length-1];

                    Log.i("test", "smallest and largest" + smallestPort + " :: " + largestPort);

                    for (i = 0; i < arr.length; i++) {

                        Message nm = new Message();

                        String forPort = arr[i];


                        if (i - 1 >= 0) {
                            predPort = arr[i - 1];
                        } else {
                            predPort = arr[arr.length - 1];
                        }

                        if (i + 1 < arr.length) {
                            succPort = arr[i + 1];
                        } else {
                            succPort = arr[0];
                        }

                        Log.i("test", "forPort--pred--succ-->>>>" + forPort + " -p- " + predPort + " -s- " + succPort);

                        if (!(forPort.equals(SimpleDhtProvider.myPort))) {
                            nm.msgType = "update";
                            nm.predPort = predPort;
                            nm.succPort = succPort;
                            nm.toPort = forPort;
                            nm.myPort = m.myPort;
                            nm.smallestPort = smallestPort;
                            nm.largestPort = largestPort;
                            messages.add(nm);
                        } else {
                            //isSame = true;
                            Log.i("test", "On same avd, so set pred & succ directly ");
                            SimpleDhtProvider.predecessor = predPort;
                            SimpleDhtProvider.successor = succPort;
                            SimpleDhtProvider.smallestPort = smallestPort;
                            SimpleDhtProvider.largestPort = largestPort;

                        }

                    }

                    /*
                    * send the messages across
                    * */
                    for (i = 0; i < messages.size(); i++) {
                        ClientThread ct = new ClientThread(messages.get(i));
                        Thread thread = new Thread(ct);
                        thread.start();
                    }


                }else if("update".equals(m.msgType)){

                    Log.i("test", "in update:: " + m.smallestPort + " :: " + m.largestPort);

                    SimpleDhtProvider.onCurrent = false;
                    SimpleDhtProvider.successor = m.succPort;
                    SimpleDhtProvider.predecessor = m.predPort;
                    SimpleDhtProvider.smallestPort = m.smallestPort;
                    SimpleDhtProvider.largestPort = m.largestPort;

                }else if("insert".equals(m.msgType)){

                    Log.i("test", "Server gets forwarded message");
                    ContentValues contentValues = new ContentValues();
                    contentValues.put("key", m.values[0]);
                    contentValues.put("value", m.values[1]);
                    SimpleDhtProvider.context.getContentResolver().insert(Uri.parse(m.uri), contentValues);

                }else if("query".equals(m.msgType)){

                    SimpleDhtProvider.isForwadedQuery = true;
                    Log.i("test", "Server gets forwarded key");
                    SimpleDhtProvider.context.getContentResolver().query(Uri.parse(m.uri), null, m.fileName, null, m.originPort);

                }else if("reply".equals(m.msgType)){

                    SimpleDhtProvider.send = true;
                    Log.i("test", "Server sending back the result");
                    String[] columns = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);
                    matrixCursor.addRow(m.values);
                    SimpleDhtProvider.matrixCursor = matrixCursor;
                    Log.i("test", "Result sent back!");

                }else if("dump".equals(m.msgType)){

                    /*
                    * copy the result in a cursor and pass it on if not on the same port
                    * */
                    String[] columns = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);

                    Log.i("test", "gdump::" + m.originPort + " :: " + m.myPort);

                    if (!(m.originPort.equals(SimpleDhtProvider.myPort))) {

                        SimpleDhtProvider.isForwadedQuery = true;
                        SimpleDhtProvider.originPort = m.originPort;
                        SimpleDhtProvider.results = m.results;

                        SimpleDhtProvider.context.getContentResolver().query(Uri.parse(m.uri), null, "@", null, m.originPort);

                    }else{
                        for(String k : m.results.keySet()){
                            String fileName = k;
                            String fileContent = m.results.get(k);
                            String[] row = {fileName, fileContent};
                            matrixCursor.addRow(row);
                        }
                        SimpleDhtProvider.matrixCursor = matrixCursor;
                    }



                }


            }

        }catch (IOException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }
}
