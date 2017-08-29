package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.ContentValues;
import android.net.Uri;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by deeptichavan on 4/28/17.
 */

public class Message implements Serializable {

    private static final long serialVersionUID = 7526471155622776147L;
    public String msgType;
    public String myPort;
    public String succ1;
    public String succ2;
    public String toPort;
    public String originPort;
    public String[] values;
    public String uri;
    public String fileName;
    public HashMap<String, String>results;
}

