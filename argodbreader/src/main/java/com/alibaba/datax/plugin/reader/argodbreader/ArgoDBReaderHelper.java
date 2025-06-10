package com.alibaba.datax.plugin.reader.argodbreader;

import com.alibaba.datax.common.util.Configuration;
import io.transwarp.holodesk.common.Table;
import io.transwarp.holodesk.common.TableUtilities;
import io.transwarp.holodesk.connector.utils.RowSetsGroup;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.shiva.client.ShivaClient;
import io.transwarp.shiva.common.Options;
import io.transwarp.shiva.exception.ShivaException;
import io.transwarp.shiva.holo.HoloClient;
import io.transwarp.shiva.holo.SplitContext;
import io.transwarp.slipstream.util.Bytes;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ArgoDBReaderHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ArgoDBReaderHelper.class);


  public static void checkConfig(Configuration config) {
    if (config.getString(Key.NEED_COLUMN) == null ||
      config.getString(Key.NEED_COLUMN).trim().equalsIgnoreCase("")) {
      throw new RuntimeException(String.format("Please set %s property in argodb reader properties!", Key.NEED_COLUMN));
    }
  }

  public static  List<Configuration> splitJob(Configuration config,
                                              RowSetsGroup[] rowSetsGroups) throws IOException {
    List<Configuration> configs = new ArrayList<>();
    for (RowSetsGroup rowSetsGroup : rowSetsGroups) {
      if (rowSetsGroup.splitContexts().length != 1) {
        throw new RuntimeException("SplitContexts length should be 1");
      }
      SplitContext splitContext = rowSetsGroup.splitContexts()[0];
      Configuration conf = config.clone();
      conf.set(Key.SPLIT_CONTEXT, splitContextToString(splitContext));

      configs.add(conf);
    }
    return configs;
  }

  public static String splitContextToString(SplitContext splitContext) throws IOException {
    FastByteArrayOutputStream outputStream = new FastByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    objectOutputStream.writeObject(splitContext);
    ByteBuffer buffer = ByteBuffer.allocate(outputStream.length);
    buffer.put(outputStream.array, 0, outputStream.length);

    byte[] splitContextBytes = buffer.array();
    return Bytes.toStringBinary(splitContextBytes);
  }

  public static SplitContext stringToSplitContext(String splitContextStr) throws IOException, ClassNotFoundException {
    byte[] splitContextBytes = Bytes.toBytesBinary(splitContextStr);

    FastByteArrayInputStream inputStream = new FastByteArrayInputStream(splitContextBytes);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    SplitContext splitContext = (SplitContext) objectInputStream.readObject();
    return splitContext;
  }

  public static String readOptionsToString(ReadOptions readOptions) throws IOException {
    FastByteArrayOutputStream outputStream = new FastByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    objectOutputStream.writeObject(readOptions);
    ByteBuffer buffer = ByteBuffer.allocate(outputStream.length);
    buffer.put(outputStream.array, 0, outputStream.length);

    byte[] splitContextBytes = buffer.array();
    return Bytes.toStringBinary(splitContextBytes);
  }

  public static ReadOptions stringToReadOptions(String readOptionsStr) throws IOException, ClassNotFoundException {
    byte[] splitContextBytes = Bytes.toBytesBinary(readOptionsStr);

    FastByteArrayInputStream inputStream = new FastByteArrayInputStream(splitContextBytes);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    ReadOptions readOptions = (ReadOptions) objectInputStream.readObject();
    return readOptions;
  }

  public static ArrayList<DataType> getDataTypes(String schemaStr) {
    if (schemaStr == null) throw new RuntimeException("Schema is null!");
    ArrayList<DataType> dataTypes = new ArrayList<>();
    String[] column2Types = schemaStr.split(",");
    for (String column2Type : column2Types) {
      String dataTypeStr = column2Type.split(":")[1].toUpperCase().trim();
      dataTypes.add(DataType.genDataType(dataTypeStr));
    }
    return dataTypes;
  }

  public static int[] getNeedColumnIds(Table holodeskTable, String needColumnStr) {
    if (needColumnStr == null) throw new RuntimeException("schema is null or needColumnStr is null!");
    if (needColumnStr.trim().equalsIgnoreCase("*")) return holodeskTable.getGlobalColumnIds();
    String[] needColumns = needColumnStr.split(",");
    int[] needColumnIds = new int[needColumns.length];
    for (int i = 0; i < needColumns.length; i++) {
      String currentNeedColumn = needColumns[i].toUpperCase().trim();
      if (holodeskTable.getColumn2Id().contains(currentNeedColumn)) {
        needColumnIds[i] = (int) holodeskTable.getColumn2Id().get(currentNeedColumn).get();
      } else {
        throw new RuntimeException("Column: " + currentNeedColumn + " not exists in table!");
      }
    }
    return needColumnIds;
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, ShivaException {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setGlobalColumnIds(new int[]{0, 1, 2})
      .setNeedColumnIds(new int[]{0,2});

    String readOptionsStr = readOptionsToString(readOptions);

    ReadOptions newReadOptions = stringToReadOptions(readOptionsStr);

    System.out.println("Except global ids: 0 1 2");
    System.out.println("Actual: ");
    for (int id : newReadOptions.getGlobalColumnIds()) {
      System.out.print(id + " ");
    }
    System.out.println();

    System.out.println("Except need ids: 0 2");
    System.out.println("Actual: ");
    for (int id : newReadOptions.getNeedColumnIds()) {
      System.out.print(id + " ");
    }
    System.out.println();

    String shivaGroup = "172.26.0.69:9360,172.26.0.71:9360,172.26.0.72:9360";
    Options options = new Options();
    options.masterGroup = shivaGroup;
    ShivaClient shivaClient = ShivaClient.getInstance();
    shivaClient.start(options);
    HoloClient holoClient = shivaClient.newHoloClient();
    String tableName = "default.t1_186e2277-8a37-4ebe-afe6-806005e11e8f";
    Table holodeskTable = TableUtilities.getHolodeskTable(holoClient.openTable(tableName));
    int[] needColumnIDs = getNeedColumnIds(holodeskTable, "name");
    for (int i : needColumnIDs) {
      System.out.print(i + " ");
    }
  }
}
