package org.apache.pig.newplan.optimizer;


import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

  @PersistenceCapable(table="repository")
	public interface Repository{
	@PrimaryKey
	String getHashcode();
	void setHashcode(String hashcode);
	String getFilename();
	void setFilename(String filename);
	int getFrequency();
	void setFrequency(int frequency);
	String getLast_access();
	void setLast_access(String last_access);
	}
