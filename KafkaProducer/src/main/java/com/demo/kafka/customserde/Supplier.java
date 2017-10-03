package com.demo.kafka.customserde;

import java.util.Date;

public class Supplier 
{
	private int id;
	private String name;
	private Date startDate;

	public Supplier(int id, String name, Date startDate) {
		super();
		this.id = id;
		this.name = name;
		this.startDate = startDate;
	}


	public int getId() {
		return id;
	}
	public String getName() {
		return name;
	}
	public Date getStartDate() {
		return startDate;
	}

}
