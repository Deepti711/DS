package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by deeptichavan on 4/28/17.
 */


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import android.util.Log;

public class ClientThread implements Runnable {

    public Message m;

    public ClientThread(Message m){
        this.m = m;
    }

    @Override
    public void run() {

        Socket socket = null;

        try {
            Log.i("test", "Client::" + m.toPort);
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(m.toPort));

            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeObject(m);
            out.flush();

            Log.i("test", "Client:: msg sent to server");

//            ObjectInputStream inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
//            String ack = inputStream.readObject().toString();
//            Log.i("test", "Client received ack::" + ack);
//
//            if("ACK".equals(ack)) {
//
//                socket.close();
//                Log.i("test", "Client socket closed!");
//            }

            socket.close();
        }catch (UnknownHostException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }



    }
}

