package uk.co.vis360.constant;

public enum WatsonCategory {
	
	HOME_AND_GARDEN("home and garden"),
	ART_AND_ENTERTAINMENT("art and entertainment"),
	TECHNOLOGY_AND_COMPUTING("technology and computing"),
	SHOPPING("shopping"),
	STYLE_AND_FASHION("style and fashion"),
	SPORTS("sports");
	private String value;
	
	WatsonCategory(String text){
		value = text;
	}
	
	public String toString() {
		return value;
	}
}
