package com.alibaba.datax.plugin.reader.argodbreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import io.transwarp.holodesk.common.Table;
import io.transwarp.holodesk.common.TableUtilities;
import io.transwarp.holodesk.connector.serde.HolodeskCoreFastSerde;
import io.transwarp.holodesk.connector.utils.HolodeskCoreReadOptionsHelper;
import io.transwarp.holodesk.connector.utils.RowSetUtils;
import io.transwarp.holodesk.connector.utils.RowSetsGroup;
import io.transwarp.holodesk.core.common.ReadMode;
import io.transwarp.holodesk.core.common.ScannerIteratorType;
import io.transwarp.holodesk.core.iterator.RowResultIterator;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.holodesk.core.result.RowResult;
import io.transwarp.holodesk.transaction.TransactionUtils;
import io.transwarp.shiva.bulk.Transaction;
import io.transwarp.shiva.client.ShivaClient;
import io.transwarp.shiva.common.Options;
import io.transwarp.shiva.holo.Distribution;
import io.transwarp.shiva.holo.HoloClient;
import io.transwarp.shiva.holo.RowSet;
import io.transwarp.shiva.holo.SplitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class ArgoDBReader extends Reader {

  public static class Job extends Reader.Job {
    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    private Configuration jobConfig = null;
    private ShivaClient shivaClient = null;
    private HoloClient holoClient = null;
    private Transaction transaction = null;

    @Override
    public void init() {
      this.jobConfig = super.getPluginJobConf();

      try {
        ArgoDBReaderHelper.checkConfig(jobConfig);
        // "172.26.0.71:9630,172.26.0.72:9630,172.26.0.76:9630";
        String shivaGroup = jobConfig.getString(Key.SHIVA_MASTER_GROUP);
        Options options = new Options();
        options.masterGroup = shivaGroup;
        shivaClient = ShivaClient.getInstance();
        shivaClient.start(options);
        holoClient = shivaClient.newHoloClient();

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<Configuration> split(int adviceNumber) {
      try {
        String tableName = jobConfig.getString(Key.TABLE);
        boolean tableIsExist = tableName != null && shivaClient.newTableExister(tableName).isExist();
        if (!tableIsExist) {
          throw DataXException.asDataXException(ArgoDBReaderErrorCode.BAD_CONFIG_VALUE, Key.TABLE);
        }
        Table holodeskTable = TableUtilities.getHolodeskTable(holoClient.openTable(tableName));

        transaction = TransactionUtils.beginTransaction(shivaClient, true);
        Distribution distribution = TransactionUtils.getReadTransactionHandler(transaction, tableName, tableName)
          .getDistribution();
        Iterator<RowSet> rowSetIterator = distribution.getRowSetIterator();

        RowSetsGroup[] rowSetsGroups = ArgoRowSetSplitHelper.splitRowSetsToFiles(distribution, rowSetIterator);
        ReadOptions readOptions = HolodeskCoreReadOptionsHelper.getDefaultReadOptions(holodeskTable);
        readOptions.setPreferReadMode(ReadMode.RowMode);
        int[] needColumnIds = ArgoDBReaderHelper.getNeedColumnIds(holodeskTable, jobConfig.getString(Key.NEED_COLUMN));
        readOptions.setNeedColumnIds(needColumnIds);
        StringBuilder sb = new StringBuilder();
        for (int needId : needColumnIds) {
          sb.append(needId).append(" ");
        }
        LOG.info("[NeedColumnID] " + sb.toString());
//        String canDoRemoteRead = jobConfig.getString(Key.CAN_DO_BLOCK_REMOTE_READ);
//        readOptions.setCanDoRowBlockScan(canDoRemoteRead != null && canDoRemoteRead.toUpperCase().equalsIgnoreCase("TRUE"));
        String readOptionsStr = ArgoDBReaderHelper.readOptionsToString(readOptions);
        jobConfig.set(Key.READ_OPTIONS, readOptionsStr);

        return ArgoDBReaderHelper.splitJob(jobConfig, rowSetsGroups);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public void destroy() {
      try {
        TransactionUtils.commitTransaction(transaction);
      } catch (Exception e) {
        throw new RuntimeException("Commit transaction failed: " + e);
      }
    }
  }

  public static class Task extends Reader.Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private Configuration taskConfig;
    private ShivaClient shivaClient = null;
    private HoloClient holoClient = null;

    // currnet Task splitContext
    private SplitContext[] splitContexts = null;
    private ReadOptions readOptions = null;
    private ArrayList<DataType> dataTypes;

    @Override
    public void init() {
      this.taskConfig = super.getPluginJobConf();
      try {
        // "172.26.0.71:9630,172.26.0.72:9630,172.26.0.76:9630";
        String shivaGroup = taskConfig.getString(Key.SHIVA_MASTER_GROUP);
        Options options = new Options();
        options.masterGroup = shivaGroup;
        shivaClient = ShivaClient.getInstance();
        shivaClient.start(options);
        holoClient = shivaClient.newHoloClient();

        String splitContextStr = taskConfig.getString(Key.SPLIT_CONTEXT);
        splitContexts = new SplitContext[1];
        splitContexts[0] = ArgoDBReaderHelper.stringToSplitContext(splitContextStr);

        String readOptionsStr = taskConfig.getString(Key.READ_OPTIONS);
        readOptions = ArgoDBReaderHelper.stringToReadOptions(readOptionsStr);

        String schemaStr = taskConfig.getString(Key.SCHEMA);
        dataTypes = ArgoDBReaderHelper.getDataTypes(schemaStr);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void startRead(RecordSender recordSender) {
      try {
        RowSet[] rowSets = RowSetUtils.getRowSetsFromSplitContexts(shivaClient, splitContexts, new HashSet<Integer>());
        if (rowSets.length != 1) {
          throw new RuntimeException("RowSet number has changed before scan, current rowsets:${rowSets.length}, expected rowsets:${split.rowSetNum}");
        }
        readOptions.setScannerIteratorType(ScannerIteratorType.scan);
        readOptions.setNeedCheckNum(false);
        String tableName = taskConfig.getString(Key.TABLE);

        Table holodeskTable = TableUtilities.getHolodeskTable(holoClient.openTable(tableName));
        int[] holodeskColumnTypes = holodeskTable.getColumnDataTypes();
        HolodeskCoreFastSerde holodeskCoreFastSerde = new HolodeskCoreFastSerde(dataTypes.size(), holodeskColumnTypes, null);
        RowResultIterator rowResultIterator = ScanClient.scan(shivaClient, holoClient, tableName, readOptions, rowSets, null);
        int count = 0;
        while (rowResultIterator.hasNext()) {
          RowResult rowResult = rowResultIterator.next();
          Record record = recordSender.createRecord();
          RowResultUtils.rowResultToRecord(holodeskCoreFastSerde, rowResult, record, dataTypes, readOptions);
          count++;
          recordSender.sendToWriter(record);
        }
        LOG.info(String.format("[================ Task Read %s rows =================]", count));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void destroy() {

    }
  }
}
