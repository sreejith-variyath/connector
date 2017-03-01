package com.infodnasolutions.connector.vo;

import java.util.Date;

public class DocumentMetaInfo {

	private Integer id;
	private String name;
	private Date createdDate;
	private Date modifiedDate;
	private Long fileSize;

	
	public DocumentMetaInfo(){
		
	}
	public DocumentMetaInfo(Integer id, String fileName, Date fileCreatedDate, Date fileModifiedDate, Long fileSize) {
		this.id = id;
		this.name = fileName;
		this.createdDate = fileCreatedDate;
		this.modifiedDate = fileModifiedDate;
		this.fileSize = fileSize;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getId() {
		return id;
	}

	public String getFileName() {
		return name;
	}

	public void setFileName(String fileName) {
		this.name = fileName;
	}

	public Date getFileCreatedDate() {
		return createdDate;
	}

	public void setFileCreatedDate(Date fileCreatedDate) {
		this.createdDate = fileCreatedDate;
	}

	public Date getFileModifiedDate() {
		return modifiedDate;
	}

	public void setFileModifiedDate(Date fileModifiedDate) {
		this.modifiedDate = fileModifiedDate;
	}

	public Long getFileSize() {
		return fileSize;
	}

	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}

	@Override public String toString() {
		return "DocumentMetaInfo{" +
				"id=" + id +
				", fileName='" + name + '\'' +
				", FileCreatedDate=" + createdDate +
				", fileModifiedDate=" + modifiedDate +
				", fileSize=" + fileSize +
				'}';
	}
}
