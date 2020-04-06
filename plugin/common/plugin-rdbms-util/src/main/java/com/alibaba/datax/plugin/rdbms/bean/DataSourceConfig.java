package com.alibaba.datax.plugin.rdbms.bean;

import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

public class DataSourceConfig {

	private String jdbcUrl;
	private String userName;
	private String passWord;
	
	private DataBaseType dataBaseType;
	
	public DataSourceConfig() {}
	
	public DataSourceConfig(String jdbcUrl, String userName, String passWord) {
		super();
		this.jdbcUrl = jdbcUrl;
		this.userName = userName;
		this.passWord = passWord;
	}
	public String getJdbcUrl() {
		return jdbcUrl;
	}
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassWord() {
		return passWord;
	}
	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}
	public DataBaseType getDataBaseType() {
		return dataBaseType;
	}
	public void setDataBaseType(DataBaseType dataBaseType) {
		this.dataBaseType = dataBaseType;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataBaseType == null) ? 0 : dataBaseType.hashCode());
		result = prime * result + ((jdbcUrl == null) ? 0 : jdbcUrl.hashCode());
		result = prime * result + ((passWord == null) ? 0 : passWord.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataSourceConfig other = (DataSourceConfig) obj;
		if (dataBaseType != other.dataBaseType)
			return false;
		if (jdbcUrl == null) {
			if (other.jdbcUrl != null)
				return false;
		} else if (!jdbcUrl.equals(other.jdbcUrl))
			return false;
		if (passWord == null) {
			if (other.passWord != null)
				return false;
		} else if (!passWord.equals(other.passWord))
			return false;
		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName))
			return false;
		return true;
	}

}
