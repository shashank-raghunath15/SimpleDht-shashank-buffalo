package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by shash on 2/22/2018.
 */

public class Database extends SQLiteOpenHelper {

    public static final String DB_NAME = "messages.db";
    public static final String TABLE_NAME = "msg";
    public static final String KEY = "key";
    public static final String VAL = "value";

    public Database(Context context) {
        super(context, DB_NAME, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("create table " + TABLE_NAME + " ( "
                + KEY + " TEXT, "
                + VAL + " TEXT "
                + " , CONSTRAINT unique_key UNIQUE (" + KEY + "))");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(sqLiteDatabase);
    }

    public boolean insert(ContentValues contentValues) {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        return sqLiteDatabase.insert(TABLE_NAME, null, contentValues) == -1 ? false : true;
    }

    public Cursor getAllMessages() {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        return sqLiteDatabase.rawQuery("select * from " + TABLE_NAME, null);
    }

    public Cursor query(String selectionArg) {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sqLiteDatabase.rawQuery("select * from " + TABLE_NAME + " WHERE " + KEY + " = '" + selectionArg + "'", null);
        return cursor;
    }

    public void delete(ContentValues contentValues) {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        sqLiteDatabase.execSQL("DELETE FROM " + TABLE_NAME + " WHERE " + KEY + " = '" + contentValues.get(KEY) + "'");
    }

    public void deleteAll(){
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        sqLiteDatabase.execSQL("DELETE FROM " + TABLE_NAME);
    }
}
