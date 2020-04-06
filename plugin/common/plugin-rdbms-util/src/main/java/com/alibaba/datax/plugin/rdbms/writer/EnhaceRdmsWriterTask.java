package com.alibaba.datax.plugin.rdbms.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

public class EnhaceRdmsWriterTask extends CommonRdbmsWriter.Task{
	
	private static final String VALUE_HOLDER = "?";
	protected static String FIND_EXIST_SQL;
	protected String findSQL;
	protected String updateKey;
	protected String updateSQL;

	public EnhaceRdmsWriterTask() {
	}
	
	@Override
	public void init(Configuration writerSliceConfig) {
		super.init(writerSliceConfig);
		
		FIND_EXIST_SQL = writerSliceConfig.getString("findExistSql");
		updateKey = writerSliceConfig.getString("updateKey");
	}

	public void startReplaceWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection) {
        this.taskPluginCollector = taskPluginCollector;

        // 用于写入数据的时候的类型根据目的表字段类型转换
        this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                this.table, StringUtils.join(this.columns, ","));
        // 写数据库的SQL语句
        calcWriteRecordSql();
        
        Pattern pattern = Pattern.compile(":\\w+");
        Matcher matcher = pattern.matcher(FIND_EXIST_SQL);
        
        List<Integer> paramIndex = new ArrayList<Integer>();
        while (matcher.find()) {
			String index = matcher.group();
			paramIndex.add(Integer.parseInt(index.substring(1)));
		}
        this.findSQL = matcher.replaceAll(VALUE_HOLDER);
        
        List<String> valueHolders = new ArrayList<String>(columnNumber);
        for (int i = 0; i < columns.size(); i++) {
            String type = resultSetMetaData.getRight().get(i);
            valueHolders.add(calcValueHolder(type));
        }
        this.updateSQL = updateSQLTemplate(this.columns, valueHolders, dataBaseType);
        this.updateSQL = String.format(this.updateSQL, this.table);

        List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
        int writeBufferBytes = 0;
        List<Record> updateBuffer = new ArrayList<Record>(this.batchSize);
        int updateBufferBytes = 0;
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != this.columnNumber) {
                    // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.CONF_ERROR,
                                    String.format(
                                            "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                            record.getColumnNumber(),
                                            this.columnNumber));
                }
                
                connection.setAutoCommit(false);
                PreparedStatement preparedStatement = connection.prepareStatement(this.findSQL);
                fillFindExistPreparedStatement(preparedStatement, record, paramIndex);
                ResultSet resultSet = preparedStatement.executeQuery();
                boolean existFlag = false;
                if (resultSet.next()) {
                	int total = resultSet.getInt(1);
                	if (total > 1) {
						existFlag = true;
					}
				}
                DBUtil.closeDBResources(resultSet, preparedStatement, null);
                
                if (!existFlag) {
                	writeBuffer.add(record);
                	writeBufferBytes += record.getMemorySize();
				}else {
					updateBuffer.add(record);
					updateBufferBytes += record.getMemorySize();
				}

                if (writeBuffer.size() >= batchSize || writeBufferBytes >= batchByteSize) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                    writeBufferBytes = 0;
                }
                if (updateBuffer.size() >= batchSize || updateBufferBytes >= batchByteSize) {
                	doBatchUpdate(connection, writeBuffer);
                	updateBuffer.clear();
                	updateBufferBytes = 0;
                }
            }
            if (!writeBuffer.isEmpty()) {
                doBatchInsert(connection, writeBuffer);
                writeBuffer.clear();
                writeBufferBytes = 0;
            }
            if (!updateBuffer.isEmpty()) {
            	doBatchUpdate(connection, writeBuffer);
            	updateBuffer.clear();
            	updateBufferBytes = 0;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            writeBuffer.clear();
            writeBufferBytes = 0;
            DBUtil.closeDBResources(null, null, connection);
        }
    }
	
	public void startReplace(RecordReceiver recordReceiver,
	            Configuration writerSliceConfig,
	            TaskPluginCollector taskPluginCollector) {
//		Connection connection = DBUtil.getConnection(this.dataBaseType,
//		 this.jdbcUrl, username, password);
		Connection connection = null;
		if (dataSource != null) {
			try {
				connection = dataSource.getConnection();
			} catch (SQLException e1) {
				throw new RuntimeException(e1);
			}
		}else {
			connection = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);
		}
		DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
		 this.dataBaseType, BASIC_MESSAGE);
		startReplaceWithConnection(recordReceiver, taskPluginCollector, connection);
	}
	
	private void calcWriteRecordSql() {
        if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
            List<String> valueHolders = new ArrayList<String>(columnNumber);
            for (int i = 0; i < columns.size(); i++) {
                String type = resultSetMetaData.getRight().get(i);
                valueHolders.add(calcValueHolder(type));
            }

            boolean forceUseUpdate = false;
            //ob10的处理
            if (dataBaseType != null && dataBaseType == DataBaseType.MySql && OriginalConfPretreatmentUtil.isOB10(jdbcUrl)) {
                forceUseUpdate = true;
            }

            INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, valueHolders, "INSERT", dataBaseType, forceUseUpdate);
            writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
        }
    }

	private PreparedStatement fillFindExistPreparedStatement(PreparedStatement preparedStatement, Record record, List<Integer> columnIndex) throws SQLException {
		for (Integer i : columnIndex) {
			int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
			preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
		}
		return preparedStatement;
	}
	
	private PreparedStatement fillUpdatePreparedStatement(PreparedStatement preparedStatement, Record record, String updateKey) throws SQLException {
		List<String> columns = this.resultSetMetaData.getLeft();
		List<Integer> columnIndex = new ArrayList<Integer>();
		int updateKeyIndex = -1;
		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			if (column.equalsIgnoreCase(updateKey)) {
				updateKeyIndex = i;
			}
			columnIndex.add(i);
		}
		columnIndex.add(updateKeyIndex);
		
		for (Integer i : columnIndex) {
			int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
			preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
		}
		return preparedStatement;
	}
	
	protected void doBatchUpdate(Connection connection, List<Record> buffer) throws SQLException {
		PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(this.updateSQL);

            for (Record record : buffer) {
                preparedStatement = fillUpdatePreparedStatement(
                        preparedStatement, record, this.updateKey);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
            connection.rollback();
            doOneUpdate(connection, buffer);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(preparedStatement, null);
        }
	}
	
	protected void doOneUpdate(Connection connection, List<Record> buffer) {
        PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(true);
            preparedStatement = connection
                    .prepareStatement(this.updateSQL);

            for (Record record : buffer) {
                try {
                    preparedStatement = fillUpdatePreparedStatement(
                            preparedStatement, record, this.updateKey);
                    preparedStatement.execute();
                } catch (SQLException e) {
                    LOG.debug(e.toString());

                    this.taskPluginCollector.collectDirtyRecord(record, e);
                } finally {
                    // 最后不要忘了关闭 preparedStatement
                    preparedStatement.clearParameters();
                }
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(preparedStatement, null);
        }
    }
	
	protected String updateSQLTemplate(List<String> columnHolders, List<String> valueHolders, DataBaseType dataBaseType) {
		StringBuilder updateSqlTemplate = new StringBuilder();
		updateSqlTemplate.append("UPDATE %s SET ");
		for (int i = 0; i < columnHolders.size(); i++) {
			String column = columnHolders.get(i);
			if (column.equalsIgnoreCase(updateKey)) {
				continue;
			}
			updateSqlTemplate.append(column).append(" = ").append(valueHolders.get(i));
			if (i+1 < columnHolders.size()) {
				updateSqlTemplate.append(",");
			}
		}
		updateSqlTemplate.append(" WHERE ").append(updateKey).append(" = ").append(VALUE_HOLDER);
		return updateSqlTemplate.toString();
	}
}
