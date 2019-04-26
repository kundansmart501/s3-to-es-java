package uk.co.vis360.constant;

public enum University {
	
	BIRMINGHAM_CITY_UNIVERSITY("Birmingham City University"),
	UNIVERSITY_OF_BIRMINGHAM("University of Birmingham"),
	NEWCASTLE_UNIVERSITY("Newcastle University"),
	UNIVERSITY_OF_SUSSEX("University of Sussex"),
	ASTON_UNIVERSITY("Aston University"),
	UNIVERSITY_OF_ABERDEEN("University of Aberdeen"),
	
	THE_UNIVERSITY_OF_EDINBURGH("The University of Edinburgh"),
	QUEENS_UNIVERSITY_BELFAST("Queen's University Belfast"),
	STAFFOXFORDUNIVERSITYVARIOUSDEPARTMENTS1("StaffOxfordUniversityVariousDepartments1"),
	
	IMPERIAL_COLLEGE_LONDON("Imperial College London"),
	
	UNIVERSITY_OF_LEICESTER("University of Leicester");
	
	private String value;
	
	University(String text){
		value = text;
	}
	
	public String toString() {
		return value;
	}
	
}
