package com.alibaba.datax.plugin.reader.argodbreader;

import com.alibaba.datax.common.element.*;
import io.transwarp.holodesk.connector.serde.HolodeskCoreFastSerde;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.holodesk.core.result.ByteArrayColumnResult;
import io.transwarp.holodesk.core.result.ColumnResult;
import io.transwarp.holodesk.core.result.RowResult;
import io.transwarp.holodesk.core.utils.DecimalUtil;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

import java.math.BigDecimal;
import java.util.ArrayList;

public class RowResultUtils {

  public static void batchRowResultToRecord(HolodeskCoreFastSerde holodeskCoreFastSerde,
                                            RowResult rowResult,
                                            Record record,
                                            ArrayList<DataType> dataTypes,
                                            int rowIdx) {
    if (rowIdx > rowResult.getBatchSize()) {
      throw new RuntimeException("RowIdx must be less than bachsize!");
    }
  }

  public static void rowResultToRecord(HolodeskCoreFastSerde holodeskCoreFastSerde,
                                       RowResult rowResult,
                                       Record record,
                                       ArrayList<DataType> dataTypes,
                                       ReadOptions readOptions) {
    if (rowResult.getBatchSize() != 1) {
      throw new RuntimeException("Not support batch mode now");
    }

    ColumnResult[] columnResults = rowResult.getColumns();
    for (int colIdx = 0; colIdx < columnResults.length; colIdx++) {
      if (columnResults[colIdx] == null) {
        // means need column is not contains this column
        continue;
      }
      ByteArrayColumnResult cell = (ByteArrayColumnResult) columnResults[colIdx];
      boolean isNUll = cell.isNull();

      if (isNUll) {
        record.addColumn(new StringColumn());
      } else {
        String columnValue = null;
        switch (dataTypes.get(colIdx)) {
          case BOOLEAN:
            columnValue = holodeskCoreFastSerde.getValue(cell, colIdx, 0).toString();
            record.addColumn(new BoolColumn(columnValue));
            break;
          case BYTE:
            columnValue = holodeskCoreFastSerde.getValue(cell, colIdx, 0).toString();
            record.addColumn(new StringColumn(columnValue));
            break;
          case SHORT:
          case INT:
          case LONG:
            columnValue = holodeskCoreFastSerde.getValue(cell, colIdx, 0).toString();
            record.addColumn(new LongColumn(columnValue));
            break;
          case FLOAT:
          case DOUBLE:
          case DECIMAL:
            byte[] decimalByteArray = (byte[]) holodeskCoreFastSerde.getValue(cell, colIdx, 0);
            BigDecimal bigDecimal = DecimalUtil.getBigDecimal(decimalByteArray, 0);
            record.addColumn(new DoubleColumn(bigDecimal));
            break;
          case CHAR:
          case VARCHAR:
          case VARCHAR2:
          case STRING:
            columnValue = holodeskCoreFastSerde.getValue(cell, colIdx, 0).toString();
            record.addColumn(new StringColumn(columnValue));
            break;
          case TIMESTAMP:
            byte[] tempArray = (byte[]) holodeskCoreFastSerde.getValue(cell, colIdx, 0);
            TimestampWritable av = new TimestampWritable();
            av.setBinarySortable(tempArray, 0);

            columnValue = av.getTimestamp().toString();
            record.addColumn(new StringColumn(columnValue));
            break;
          case DATE:
            long dataLongV = (long) holodeskCoreFastSerde.getValue(cell, colIdx, 0);
//            DateWritable date = new DateWritable();
//            date.set(dataLongV);
//            columnValue = date.get().toString();
            record.addColumn(new DateColumn(dataLongV));
            break;
          default:
            throw new RuntimeException("Not support datatype");
        }
      }
    }
  }
}
