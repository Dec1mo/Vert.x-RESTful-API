package manager;

import java.util.concurrent.atomic.AtomicInteger;

public class UserManager {
	private static final AtomicInteger COUNTER = new AtomicInteger();
	private final int id;
	private String name;
	private int year;
	
	public UserManager(String name, int year) {
		this.id = COUNTER.getAndIncrement();
		this.name = name;
		this.year = year;
	}
	
	public UserManager() {
		this.id = COUNTER.getAndIncrement();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getId() {
		return id;
	}


}
