package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
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
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
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
    Message replyMessage = null;
    MatrixCursor matrixCursor = null;

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
            if (next.equals(nodeId) || key.compareTo(previous) > 0 && key.compareTo(nodeId) <= 0 || (nodeId.equals(joined.firstKey()) && (key.compareTo(joined.lastKey()) > 0 || key.compareTo(nodeId) < 0))) {
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
            if (next.equals(nodeId)) {
                return database.getAllMessages();
            } else {
                return getAll();
            }
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
            }
            if (key.compareTo(previous) > 0 && key.compareTo(nodeId) <= 0) {
                return database.query(selection);
            }

            Message message = new Message();
            message.setStatus(MessageStatus.QUERY);
            message.setKey(selection);
            message.setHashCode(key);
            message.setPort(myPort);
            new ClientQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            while (true) {
                if (replyMessage != null) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new String[]{replyMessage.getKey(), replyMessage.getValue()});
                    replyMessage = null;
                    return cursor;
                }
            }

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
        Message message = new Message();
        database.getAllMessages();
        message.setPort(myPort);
        message.setStatus(MessageStatus.QAll);
        new ClientQueryAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        while (true) {
            if (matrixCursor != null) {
                Cursor cursor = matrixCursor;
                matrixCursor = null;
                return cursor;
            }
        }
    }

    public void deleteAll() {

    }

    public List<String[]> getListFromCursor(Cursor cursor) {
        List<String[]> list = new ArrayList<String[]>();
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            String[] s = new String[]{cursor.getString(0), cursor.getString(1)};
            list.add(s);
            cursor.moveToNext();
        }
        return list;
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
        System.out.println(nodeId);
        System.out.println(next);
        System.out.println(previous);
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
                            objectInputStream.close();
                            socket.close();
                        } else if (message.getStatus().equals(MessageStatus.ACK)) {
                            joined = message.getJoined();
                            updateLinks();
                        } else if (message.getStatus().equals(MessageStatus.INSERT)) {
                            if ((nodeId.equals(joined.firstKey()) && (message.getHashCode().compareTo(joined.lastKey()) > 0 || message.getHashCode().compareTo(nodeId) < 0)) || message.getHashCode().compareTo(previous) > 0 && message.getHashCode().compareTo(nodeId) <= 0) {
                                insertNew(message);
                            } else {
                                new ClientInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                            }
                        } else if (message.getStatus().equals(MessageStatus.QUERY)) {
                            if ((nodeId.equals(joined.firstKey()) && (message.getHashCode().compareTo(joined.lastKey()) > 0 || message.getHashCode().compareTo(nodeId) < 0)) || message.getHashCode().compareTo(previous) > 0 && message.getHashCode().compareTo(nodeId) <= 0) {
                                writeBackQuery(database.query(message.getKey()), message);
                            } else {
                                new ClientQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                            }
                        } else if (message.getStatus().equals(MessageStatus.QREPLY)) {
                            replyMessage = message;
                        } else if (message.getStatus().equals(MessageStatus.QAll)) {
                            if (message.getPort().equals(myPort)) {
                                List<String[]> list = getListFromCursor(database.getAllMessages());
                                list.addAll(message.getKeyValuePairs());
                                MatrixCursor m = new MatrixCursor(new String[]{"key", "value"});
                                for (String[] s : list) {
                                    m.addRow(new String[]{s[0], s[1]});
                                }
                                matrixCursor = m;
                            } else {
                                Cursor cursor = database.getAllMessages();
                                message.getKeyValuePairs().addAll(getListFromCursor(cursor));
                                new ClientQueryAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
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

        private void writeBackQuery(Cursor cursor, Message message) {
            cursor.moveToFirst();
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.valueOf(message.getPort()));
                message.setStatus(MessageStatus.QREPLY);
                message.setValue(cursor.getString(1));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    private class ClientQueryTask extends AsyncTask<Message, Void, Void> {

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

    private class ClientQueryAllTask extends AsyncTask<Message, Void, Void> {
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

