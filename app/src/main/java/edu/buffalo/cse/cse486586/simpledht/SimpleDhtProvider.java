package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SimpleDhtProvider extends ContentProvider {

    private Database database;
    private String nodeId;
    private String myPort;
    private String previous;
    private String next;
    private NavigableMap<String, String> joined = new TreeMap<String, String>();
    static final int SERVER_PORT = 10000;
    static final String MASTER_PORT = "11108";
    static String PORT;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("*")) {
            database.deleteAll();
        } else {
            database.deleteAll();
        }

        return 1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            String key = genHash((String) values.get("key"));
            if (next.equals(previous) || key.compareTo(previous) > 0 && key.compareTo(nodeId) <= 0 || (nodeId.equals(joined.firstKey()) && (key.compareTo(joined.lastKey()) > 0 || key.compareTo(nodeId) < 0))) {
                insertDB(values, key);
            } else {
                passForward(values, key);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    private void passForward(ContentValues values, String key) {
        Message message = new Message();
        message.setHashCode(key);
        message.setKey((String) values.get("key"));
        message.setValue((String) values.get("value"));
        message.setStatus(MessageStatus.INSERT);
        new ClientInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    public void insertDB(ContentValues values, String key) {
        database.delete(values);
        database.insert(values);
        Log.v("insert", values.toString());
    }

    @Override
    public boolean onCreate() {
        database = new Database(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        PORT = portStr;
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            nodeId = genHash(portStr);
            joined.put(nodeId, myPort);
            updateLinks();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        create();
        if (!myPort.equals(MASTER_PORT)) {
            join();
        }
        return true;
    }

    private void create() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        if (selection.equals("*")) {
            return getAll();
        } else if (selection.equals("@")) {
            return database.getAllMessages();
        } else {
            String key = null;
            try {
                key = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if (nodeId.equals(joined.firstKey())) {
                if (key.compareTo(joined.lastKey()) > 0 || key.compareTo(joined.firstKey()) <= 0) {
                    return database.query(selection);
                }
            } else {

            }
            return database.query(selection);
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    public Cursor getAll() {
        return database.getAllMessages();
    }

    public void deleteAll() {

    }

    public void join() {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    private void updateLinks() {
        next = joined.higherKey(nodeId);
        previous = joined.lowerKey(nodeId);
        if (next == null) {
            next = joined.firstKey();
        }
        if (previous == null) {
            previous = joined.lastKey();
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    try {
                        Message message = (Message) objectInputStream.readObject();
                        if (message.getStatus().equals(MessageStatus.JOIN)) {
                            joined.put(message.getNodeHash(), message.getPort());
                            updateLinks();
                            message.setJoined(joined);
                            message.setStatus(MessageStatus.ACK);
                            writeToAll(message);

                        } else if (message.getStatus().equals(MessageStatus.ACK)) {
                            joined = message.getJoined();
                            updateLinks();
                        } else if (message.getStatus().equals(MessageStatus.INSERT)) {
                            if ((nodeId.equals(joined.firstKey()) && (message.getHashCode().compareTo(joined.lastKey()) > 0 || message.getHashCode().compareTo(nodeId) < 0)) || message.getHashCode().compareTo(previous) > 0 && message.getHashCode().compareTo(nodeId) <= 0) {
                                insertNew(message);
                            } else {
                                new ClientInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                            }
                        }
                        objectInputStream.close();
                        socket.close();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        private void insertNew(Message message) {
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", message.getKey());
            contentValues.put("value", message.getValue());
            insertDB(contentValues, message.getHashCode());
        }

        private void writeToAll(Message message) throws IOException {
            for (String key : joined.keySet()) {
                String port = joined.get(key);
                if (port.equals(MASTER_PORT)) continue;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.valueOf(port));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... strings) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.valueOf(MASTER_PORT));
                Message message = new Message();
                message.setNodeHash(nodeId);
                message.setPort(myPort);
                message.setStatus(MessageStatus.JOIN);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientInsertTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... messages) {
            Message message = messages[0];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(joined.get(next)));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}
