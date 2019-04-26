package uk.co.vis360.watson;

import java.util.logging.Logger;

import uk.co.vis360.mongo.NewReaperData;

public class KonferMain {
	static Logger log = Logger.getLogger(KonferMain.class.getName());
	
	public static void main(String []arg) {
		log.info("In Konfer main");
		//PublicationData publicationDataUtil = new PublicationData();
		
		//publicationDataUtil.updateKonferPublication();
		//publicationDataUtil.getDataFromES();
		//publicationDataUtil.executeMultiSearchRequest();
		NewReaperData newReaperData = new NewReaperData();
		
		int skip = 0,limit = 5000;
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
		skip = skip + 5000;
		newReaperData = new NewReaperData();
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
		skip = skip + 5000;
		newReaperData = new NewReaperData();
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
		skip = skip + 5000;
		newReaperData = new NewReaperData();
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
		skip = skip + 5000;
		newReaperData = new NewReaperData();
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
		skip = skip + 5000;
		newReaperData = new NewReaperData();
		newReaperData.processData(skip,limit);
		newReaperData = null;
		
	}
}
