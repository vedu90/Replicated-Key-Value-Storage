package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int NUMBER_OF_AVDS = 5;
    static final int NUM_OF_REPLICATED_NODES = 3;
    static final int SERVER_PORT = 10000;
    static int myPort;
    //	static String queryKeyResult="";
    static int successorPorts[] = new int[2];
    static int predecessorPorts[] = new int[2];
    static String selfNode;
    ArrayList<portNodeMap> sortedNodes = new ArrayList<portNodeMap>(5);
    ArrayList<String> queryResults = new ArrayList<String>(8);
    static ReentrantLock queryLock = new ReentrantLock();
   // Map<Integer,ArrayList<String>> recoverMessages = new HashMap<Integer,ArrayList<String>>();
    ArrayList<String> recoverMessages = new ArrayList<String>();
    public class sortedNodesComparator implements Comparator<portNodeMap> {
        public int compare(portNodeMap m1, portNodeMap m2) {
            return new String(m1.node).compareTo(new String(m2.node));
        }}
    class portNodeMap{

        String node;
        int port;

        void Set(String n, int p)
        {
            node = n;
            port = p;
        }

    }


    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {

        Log.v("SimpleDynamo", "Delete called "+selection);


        if(selection.equals("@") || selection.equals("*")){
            DeleteLocalFiles();
            if(selection.equals("*")){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"D_ALL");
            }
        }
        else{

            int ind = FindCorrectNodeIndex(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"D_KEY",selection,Integer.toString(ind));
        }
        return 0;
    }

    public void DeleteLocalFiles()
    {
        Log.v("SimpleDynamo", "Deleting local files " );
        Context currentContext = getContext();
        String[] keyFiles = currentContext.fileList();
        for(String file:keyFiles) {
            try {
                if(currentContext.deleteFile(file))
                {
                    Log.v("SimpleDynamo", "File deleted " + file);

                }
                else
                {
                    Log.v("SimpleDynamo", "File not found to delete " + file);
                }
            } catch (Exception e) {
                Log.v("SimpleDynamo", "Delete File open failed " + e);
            }

        }
        return ;
    }

    public boolean DeleteFile(String Key)
    {
        Context currentContext = getContext();

        try {
            if(currentContext.deleteFile(Key))
            {
                Log.v("SimpleDynamo", "File deleted " + Key);
                return true;
            }
            Log.v("SimpleDynamo", "File not found to delete " + Key);
            return false;
        } catch (Exception e) {
            Log.v("SimpleDynamo", "To be deleted File not found " + e);
            return false;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD);

        int ind = FindCorrectNodeIndex(key);

        if(sortedNodes.get(ind).port == myPort){
            Log.v("SimpleDynamo", "Inserting Locally "+ind);
            String portNum = Integer.toString(myPort);
            insertMessage(key,val,portNum);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"I",Integer.toString(ind),key,val,"0");
        }
        else
        {
            Log.v("SimpleDynamo", "The node index to be inserted to is "+ind);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"I",Integer.toString(ind),key,val,"1");
        }

        return null;
    }

    public void insertMessage(String key, String val,String port)
    {
        FileOutputStream outputStream;
        Context currentContext = getContext();
        //     val+="|";
        //     val+=port;
        Log.v("SimpleDynamo", "Inserting key:"+key+" , val:"+val);
        try {
            outputStream =  currentContext.openFileOutput(key,Context.MODE_PRIVATE);
            outputStream.write(val.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.v("SimpleDynamo", "File write failed in insert"+e);
        }
    }
    public int FindCorrectNodeIndex(String key)
    {
        try
        {
            String hashKey = genHash(key);
            Log.v("SimpleDynamo", "Hash key for key is "+hashKey);
            String prevKey = sortedNodes.get(0).node;
            for(int ind = 1 ; ind < NUMBER_OF_AVDS ; ind++)
            {
                String currKey = sortedNodes.get(ind).node;
                if(currKey.compareTo(hashKey) >0 && hashKey.compareTo(prevKey) > 0)
                {
                    Log.v("SimpleDynamo", "FindCorrectNodeIndex Index is "+ind);
                    return ind;
                }
            }
            Log.v("SimpleDynamo", "FindCorrectNodeIndex Index is 0");
            return 0;
        }
        catch (Exception e)
        {
            Log.v("SimpleDynamo", "Exception caught in FindCorrectNodeIndex "+e);
            return 0;
        }

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        try{
            //   DeleteLocalFiles();
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            int avd = (Integer.parseInt(portStr));
            myPort = avd*2;
            Log.v("SimpleDynamo", "New approach On create called in provider "+myPort+" avd "+portStr);
            selfNode = genHash(portStr);
            FindTwoSuccessors();
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.v("SimpleDynamo", "On create called in provider, serverSocket created ");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);

            RecoverMessages();

        }catch(Exception e){
            Log.v("SimpleDynamo", "Exception caught in OnCreate "+e);
        }

        return false;
    }

    public void FindTwoSuccessors()
    {
        try
        {
            int nodeNum = 11108;
            for(int i = 0 ; i < NUMBER_OF_AVDS ; i++)
            {
                int nodePort = nodeNum/2;
                String n = genHash(Integer.toString(nodePort));
                portNodeMap obj = new portNodeMap();
                obj.Set(n,nodeNum);
                sortedNodes.add(obj);
                nodeNum+=4;
            }
            Collections.sort(sortedNodes,new sortedNodesComparator());
            for(portNodeMap obj:sortedNodes)
            {
                Log.v("SimpleDynamo", "Sorted nodes are "+obj.node+","+obj.port);
            }
            int ind = 0;
            for(portNodeMap obj:sortedNodes)
            {
                if(obj.port == myPort)
                {
                    break;
                }
                ind++;
            }

            successorPorts[0] = sortedNodes.get((ind+1)%NUMBER_OF_AVDS).port;
            successorPorts[1] = sortedNodes.get((ind+2)%NUMBER_OF_AVDS).port;
            predecessorPorts[0] = sortedNodes.get((ind+3)%NUMBER_OF_AVDS).port;
            predecessorPorts[1] = sortedNodes.get((ind+4)%NUMBER_OF_AVDS).port;

            Log.v("SimpleDynamo", "Successor ports for "+myPort+" are "+successorPorts[0]+","+successorPorts[1]);

        }
        catch (Exception e)
        {
            Log.v("SimpleDynamo", "Exception caught in FindTwoSuccessors "+e);
        }

    }

    public void RecoverMessages(){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVER_SUCC");
   //     new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVER_PRED");
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {

        MatrixCursor matrixCursor = new MatrixCursor(new String[] { KEY_FIELD, VALUE_FIELD });
        Log.v("SimpleDynamo", "Query called "+selection);

        try
        {
            if(selection.equals("@") || selection.equals("*")){
                ArrayList<String> res = GetLocalFiles();
                for(int i = 0 ; i < res.size() ; i+=2)
                {
                    Log.v("SimpleDynamo", "Adding local query result "+res.get(i)+","+res.get(i+1));
                    matrixCursor.addRow(new Object[] { res.get(i), res.get(i+1) });
                }
                if(selection.equals("*"))
                {

                    Log.v("SimpleDynamo", "Querying for *, so put to sleep ");

                    ArrayList<String> tempList = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"Q_ALL").get();

                    for(int i = 0 ; i < queryResults.size() ; i+=2)
                    {
                        Log.v("SimpleDynamo", "Adding query result "+queryResults.get(i)+","+queryResults.get(i+1));
                        matrixCursor.addRow(new Object[] { queryResults.get(i), queryResults.get(i+1) });
                    }
                    queryResults.clear();
                }
            }
            else
            {
                Log.v("SimpleDynamo", "Single query called for key:"+selection);
                String res = GetQueriedFile(selection);
			/*	if(!res.equals(""))
				{
					Log.v("SimpleDynamo", "Queried file "+selection+" is present locally");
					Log.v("SimpleDynamo", "Key value found "+res);
					matrixCursor.addRow(new Object[] { selection, res });
				}*/
                //else
                {
                    int ind = FindCorrectNodeIndex(selection);
                    ind+=2;
                    ind=ind%NUMBER_OF_AVDS;
                    ArrayList<String> tempList = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"Q_KEY",selection,Integer.toString(ind)).get();
                    Log.v("SimpleDynamo", "Querying for "+selection+", so put to sleep ");



                    if(queryResults.size() == 1){
                        matrixCursor.addRow(new Object[] { selection, queryResults.get(0) });
                        Log.v("SimpleDynamo", "Key value found "+queryResults.get(0)+" for key "+selection+" l ");
                    }
                    else if(queryResults.size() == 2){
                        matrixCursor.addRow(new Object[] { selection, queryResults.get(1) });
                        Log.v("SimpleDynamo", "Key value found "+queryResults.get(1)+" for key "+selection+" l ");
                    }
                    else{
                        if(queryResults.get(0).equals(queryResults.get(1))){
                            matrixCursor.addRow(new Object[] { selection, queryResults.get(1) });
                            Log.v("SimpleDynamo", "Key value found "+queryResults.get(0)+" for key "+selection+" l ");
                        }
                        else if(queryResults.get(0).equals(queryResults.get(2))){
                            matrixCursor.addRow(new Object[] { selection, queryResults.get(0) });
                            Log.v("SimpleDynamo", "Key value found "+queryResults.get(0)+" for key "+selection+" l ");
                        }
                        else if(queryResults.get(1).equals(queryResults.get(2))){
                            matrixCursor.addRow(new Object[] { selection, queryResults.get(1) });
                            Log.v("SimpleDynamo", "Key value found "+queryResults.get(1)+" for key "+selection+" l ");
                        }
                        else{
                            matrixCursor.addRow(new Object[] { selection, queryResults.get(2) });
                            Log.v("SimpleDynamo", "Key value found "+queryResults.get(2)+" for key "+selection+" l ");
                        }
                    }
                    queryResults.clear();
                }
            }
            return matrixCursor;
        }
        catch (Exception e)
        {
            queryResults.clear();
            //	queryLock.unlock();
            Log.v("SimpleDynamo", "Exception caught in query "+e);
            return null;
        }

    }

    public String GetQueriedFile(String Key)
    {
        FileInputStream inputStream;
        Context currentContext = getContext();

        StringBuilder value = new StringBuilder();
        try {
            inputStream = currentContext.openFileInput(Key);

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                //     String[] temp = line.split("\\|");
                value.append(line);
            }
            inputStreamReader.close();
            inputStream.close();

            return value.toString();
        } catch (Exception e) {
            Log.v("SimpleDynamo", "Exception Queried File not found " + e);
            return "";
        }

    }


    public ArrayList<String> GetLocalFiles()
    {
        FileInputStream inputStream;
        Context currentContext = getContext();
        String[] keyFiles = currentContext.fileList();
        ArrayList<String> result = new ArrayList<String>();
        for(String file:keyFiles) {
            StringBuilder value = new StringBuilder();
            try {
                inputStream = currentContext.openFileInput(file);

                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    //   String[] temp = line.split("\\|");
                    value.append(line);
                }
                inputStreamReader.close();
                inputStream.close();
            } catch (Exception e) {
                Log.v("SimpleDynamo", "Query File open failed " + e);
            }
            Log.v("SimpleDynamo", "Adding Key " + file + " value " + value);
            result.add(file);
            result.add(value.toString());
        }
        return result;
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

    public String GetAllKeyValueForAVD(String port){
        Log.v("SimpleDynamo", " GetAllKeyValueForAVD called");
        FileInputStream inputStream;
        Context currentContext = getContext();
        String[] keyFiles = currentContext.fileList();
        String result="";
        int p = Integer.parseInt(port);
        try{
           // if(recoverMessages.containsKey(p)){
                for(String res : recoverMessages){
                    result+=(res+"|");
                }
                recoverMessages.clear();
         //   }
            Log.v("SimpleDynamo", " Recovered messages "+result);
        }
        catch (Exception e){
            Log.v("SimpleDynamo", "Exception in GetAllKeyValueForAVD " + e);
        }

        /*
        for(String file:keyFiles) {
            //StringBuilder value = new StringBuilder();
            try {
                inputStream = currentContext.openFileInput(file);

                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] temp = line.split("\\|");
                    if(temp[1].equals(port)){
                        result+=file;
                        result+="|";
                        result+=temp[0];
                        result+="|";
                    }
                    //value.append(temp[0]);
                }
                inputStreamReader.close();
                inputStream.close();
            } catch (Exception e) {
                Log.v("SimpleDynamo", "Exception in GetAllKeyValueForAVD " + e);
            }
        }*/


        return result;
    }

    class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.v("SimpleDynamo", "Server task initiated");
            try {
                while (true) {
                    Log.v("SimpleDynamo", "Modified Waiting for message on server side");
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader msgFromClient
                            = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                    Log.v("SimpleDynamo", "New Connection established with server");

                    String message = msgFromClient.readLine();
                    if (message == null) {
                        Log.v("SimpleDynamo", "Null message on Server Side");
                        msgFromClient.close();
                        clientSocket.close();
                        continue;
                    }
                    Log.v("SimpleDynamo", "Message received on server "+message);

                    String[] splitMessage = message.split("\\|");

                    if(splitMessage[0].equals("I"))
                    {
                        Log.v("SimpleDynamo", "Insert message received ");
                        insertMessage(splitMessage[2],splitMessage[3],splitMessage[4]);

                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println("ACK");
                        pw.flush();
                    }
                    else if(splitMessage[0].equals("Q_ALL")){
                        Log.v("SimpleDynamo", "Query all message received from  "+splitMessage[1]);

                        ArrayList<String> tempList;
                        tempList = GetLocalFiles();

                        String msg ="";
                        for(String str:tempList)
                        {
                            msg+=str;
                            msg+="|";
                        }
                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(msg);
                        Thread.sleep(100);
                        pw.flush();
                    }
                    else if(splitMessage[0].equals("Q_KEY")){
                        Log.v("SimpleDynamo", "Query Key message received from  "+splitMessage[1]+"for key "+splitMessage[2]);

                        String qResult = GetQueriedFile(splitMessage[2]);
                        Log.v("SimpleDynamo", "Sending query key result to "+splitMessage[1]+"with result "+qResult);
                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(qResult);
                        Thread.sleep(100);
                        pw.flush();
                    }
                    else if(splitMessage[0].equals("D_ALL")){
                        Log.v("SimpleDynamo", "Delete all messages received from  "+splitMessage[1]);
                        DeleteLocalFiles();

                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println("ACK");
                        pw.flush();
                    }
                    else if(splitMessage[0].equals("D_KEY")){
                        Log.v("SimpleDynamo", "Delete message with key " +splitMessage[2]+" received from  "+splitMessage[1]);
                        DeleteFile(splitMessage[2]);

                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println("ACK");
                        pw.flush();
                    }
                    else if(splitMessage[0].equals("RECOVER")){
                        Log.v("SimpleDynamo", "Recover message received for port num "+splitMessage[1]);
                        String msg = GetAllKeyValueForAVD(splitMessage[1]);
                        OutputStream os;
                        PrintWriter pw;
                        os= clientSocket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(msg);
                        Thread.sleep(150);
                        Log.v("SimpleDynamo", "Recover message sent ");
                        pw.flush();
                    }
                    clientSocket.close();
                }

            } catch (Exception e) {
                Log.v("SimpleDynamo", "Exception caught in Server "+e);
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, ArrayList<String>> {

        @Override
        protected ArrayList<String> doInBackground(String... msgs) {
            try {
                if(msgs[0].equals("I"))
                {
                    int ind = Integer.parseInt(msgs[1]);
                    String message = "I";
                    message+="|";
                    message+=myPort;
                    message+="|";
                    message+=msgs[2];
                    message+="|";
                    message+=msgs[3];
                    int n = NUM_OF_REPLICATED_NODES;
                    if(msgs[4].equals("0"))
                    {
                        message+="|";
                        message+=Integer.toString(myPort);
                        Log.v("SimpleDynamo", "Sending Local type Insert message");
                        ind++;
                        n--;
                    }
                    else
                    {
                        message+="|";
                        message+=Integer.toString(sortedNodes.get(ind%NUMBER_OF_AVDS).port);
                    }

                    for(int i = 0 ; i < n ; i++)
                    {
                        int receiverPort = sortedNodes.get(ind%NUMBER_OF_AVDS).port;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                receiverPort);
                        Log.v("SimpleDynamo", "Sending Insert message to  "+receiverPort+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        socket.getInputStream();
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        //Thread.sleep(80);
                        pw.flush();

                        BufferedReader msgFromClient
                                = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        String msg = msgFromClient.readLine();
                        if (msg == null) {
                            Log.v("SimpleDynamo", "Null message on Client Side");
                            String backupMsg = "I|"+msgs[2]+"|"+msgs[3];
                            ArrayList<String> arrayMsg = new ArrayList<String>();
                            Log.v("SimpleDynamo", "Putting in map "+sortedNodes.get(ind%NUMBER_OF_AVDS).port+","+message);
                            recoverMessages.add(backupMsg);
                        }
                        else
                        {
                            Log.v("SimpleDynamo", "Acknowledgement message on Client Side "+msg);
                        }
                        msgFromClient.close();
                        socket.close();
                        ind++;
                    }
                }
                else if(msgs[0].equals("Q_ALL"))
                {
                    String message = "Q_ALL";
                    message+="|";
                    message+=Integer.toString(myPort);
                    Log.v("SimpleDynamo", "Q_ALL initiated from client");
                    for(portNodeMap obj : sortedNodes)
                    {
                        if(obj.port != myPort){
                            GetQueryResult(obj.port,message);
                        }
                    }
                    return queryResults;
                }
                else if(msgs[0].equals("Q_KEY")){
                    Log.v("SimpleDynamo", "Q_KEY initiated from client");
                    int ind = Integer.parseInt(msgs[2]);
                    String message = "Q_KEY";
                    message+="|";
                    message+=Integer.toString(myPort);
                    message+="|";
                    message+=msgs[1];
                    for(int k = 0 ; k < NUM_OF_REPLICATED_NODES; k++){
                        if(GetQueryResult(sortedNodes.get((ind+5)%NUMBER_OF_AVDS).port,message))
                        {
                         //   return queryResults;
                        }
                        ind--;
                    }

                    return queryResults;
                }
                else if(msgs[0].equals("D_ALL"))
                {
                    String message = "D_ALL";
                    message+="|";
                    message+=Integer.toString(myPort);
                    Log.v("SimpleDynamo", "D_ALL initiated from client");
                    for(portNodeMap obj : sortedNodes)
                    {
                        if(obj.port != myPort){
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    obj.port);
                            Log.v("SimpleDynamo", "Sending Delete All message to  "+obj.port+message);
                            OutputStream os;
                            PrintWriter pw;
                            os= socket.getOutputStream();
                            pw= new PrintWriter(os,true);
                            pw.println(message);
                            //Thread.sleep(100);
                            pw.flush();

                            BufferedReader msgFromClient
                                    = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                            String msg = msgFromClient.readLine();
                            if (msg == null) {
                                Log.v("SimpleDynamo", "Null message on Client Side");
                            }
                            else
                            {
                                Log.v("SimpleDynamo", "Acknowledgement message on Client Side "+msg);
                            }
                            msgFromClient.close();
                        }
                    }
                }
                else if(msgs[0].equals("D_KEY")){
                    String message = "D_KEY";
                    message+="|";
                    message+=Integer.toString(myPort);
                    message+="|";
                    message+=msgs[1];
                    Log.v("SimpleDynamo", "D_Key initiated from client");

                    int ind = Integer.parseInt(msgs[2]);
                    for(int i = 0 ; i < NUM_OF_REPLICATED_NODES ; i++)
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                sortedNodes.get(ind%NUMBER_OF_AVDS).port);
                        Log.v("SimpleDynamo", "Sending Delete key message to  "+sortedNodes.get(ind%NUMBER_OF_AVDS).port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        socket.getInputStream();
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        //Thread.sleep(100);
                        pw.flush();


                        BufferedReader msgFromClient
                                = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        String msg = msgFromClient.readLine();
                        if (msg == null) {
                            Log.v("SimpleDynamo", "Null message on Client Side");
                        }
                        else
                        {
                            Log.v("SimpleDynamo", "Acknowledgement message on Client Side "+msg);
                        }
                        msgFromClient.close();

                        socket.close();
                        ind++;
                    }

                }
                else if(msgs[0].equals("RECOVER_SUCC")){

                    String message = "RECOVER";
                    message+="|";
                    message+=Integer.toString(myPort);
                    Log.v("SimpleDynamo", "Trying to recover from successors");
                    for(portNodeMap p : sortedNodes){

                        if(p.port != myPort){
                            if(GetRecoveryResult(p.port,message,Integer.toString(myPort))){
                             //   break;
                            }
                        }
                    }
                }
            /*    else if(msgs[0].equals("RECOVER_PRED")){
                    for(int i = 0 ; i < NUM_OF_REPLICATED_NODES-1 ; i++){
                        String message = "RECOVER";
                        message+="|";
                        message+=Integer.toString(predecessorPorts[i]);
                        Log.v("SimpleDynamo", "Trying to recover from predecessors");
                        GetRecoveryResult(predecessorPorts[i],message,Integer.toString(predecessorPorts[i]));
                    }
                }*/

            }
            catch (Exception e)
            {
                Log.v("SimpleDynamo", "Exception caught in Client Task "+e);
            }
            return null;
        }

        boolean GetRecoveryResult(int port,String message,String origPort){
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port);
                Log.v("SimpleDynamo", "Sending Recovery message to  "+port+ message);
                OutputStream os;
                PrintWriter pw;
                os= socket.getOutputStream();
                pw= new PrintWriter(os,true);
                pw.println(message);
                //Thread.sleep(80);
                pw.flush();

                BufferedReader msgFromClient
                        = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String msg = msgFromClient.readLine();
                if (msg == null || msg.equals("")) {
                    Log.v("SimpleDynamo", "Null message on Client Side from port "+port);
                    msgFromClient.close();
                    socket.close();
                    return false;
                }

                String[]splitMsg = msg.split("\\|");
                Log.v("SimpleDynamo", "Received msg for Recovery all  with len "+splitMsg.length+"for message "+message);
                for(int k = 0 ; k < splitMsg.length ; k+=3)
                {
                    Log.v("SimpleDynamo", "Received msg key and val is "+splitMsg[k+1]+","+splitMsg[k+2]);
                    insertMessage(splitMsg[k+1],splitMsg[k+2],origPort);
                }
                socket.close();
                return true;
            }
            catch (Exception e){
                Log.v("SimpleDynamo", "Exception in GetRecoveryResult");
                return false;
            }
        }

        boolean GetQueryResult(int port,String message)
        {
            try
            {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port);
                Log.v("SimpleDynamo", "Sending Query All message to  "+port+ message);
                OutputStream os;
                PrintWriter pw;
                os= socket.getOutputStream();
                pw= new PrintWriter(os,true);
                pw.println(message);
                //Thread.sleep(100);
                pw.flush();

                BufferedReader msgFromClient
                        = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String msg = msgFromClient.readLine();
                if (msg == null || msg.equals("")) {
                    Log.v("SimpleDynamo", "Null message on Client Side");
                    msgFromClient.close();
                    socket.close();
                    return false;
                }

                String[]splitMsg = msg.split("\\|");
                Log.v("SimpleDynamo", "Received msg for query all  for message "+message+","+msg);
                for(int k = 0 ; k < splitMsg.length ; k++)
                {
                    queryResults.add(splitMsg[k]);
                }
                socket.close();
                return true;
            }
            catch (Exception e) {
                Log.v("SimpleDynamo", "Exception caught in GetQueryResult "+e);
                return false;
            }
        }
    }
}
