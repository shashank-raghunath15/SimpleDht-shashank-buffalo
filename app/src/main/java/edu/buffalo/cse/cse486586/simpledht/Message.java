package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by shash on 3/31/2018.
 */

public class Message implements Serializable {
    private String nodeHash;
    private String hashCode;
    private String port;
    private String key;
    private String value;
    private MessageStatus status;
    private NavigableMap<String, String> joined = new TreeMap<String, String>();

    public String getNodeHash() {
        return nodeHash;
    }

    public void setNodeHash(String nodeHash) {
        this.nodeHash = nodeHash;
    }

    public NavigableMap<String, String> getJoined() {
        return joined;
    }

    public void setJoined(NavigableMap<String, String> joined) {
        this.joined = joined;
    }

    public String getHashCode() {
        return hashCode;
    }

    public void setHashCode(String hashCode) {
        this.hashCode = hashCode;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public MessageStatus getStatus() {
        return status;
    }

    public void setStatus(MessageStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Message{" +
                "nodeHash='" + nodeHash + '\'' +
                ", hashCode='" + hashCode + '\'' +
                ", port='" + port + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", status=" + status +
                ", joined=" + joined +
                '}';
    }
}
