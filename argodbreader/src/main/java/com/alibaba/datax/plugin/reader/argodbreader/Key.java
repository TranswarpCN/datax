package com.alibaba.datax.plugin.reader.argodbreader;

import io.transwarp.holodesk.connector.utils.RowSetsGroup;

public class Key {
    public final static String SHIVA_MASTER_GROUP="ngmrShivaMastergroup";
    public final static String TABLE ="tableName";
    // columnName1:ColumnType1,columnName2:ColumnType2...
    public final static String SCHEMA ="schema";
    public final static String NEED_COLUMN ="needColumn";
    public final static String CAN_DO_BLOCK_REMOTE_READ ="blockRemoteRead";



    // task config
    // RowSetsGroup config
    public final static String SPLIT_CONTEXT = "splitContext";
    public final static String HOSTS = "hosts";

    // ReadOptions config
    public final static String READ_OPTIONS = "readOptions";


}
