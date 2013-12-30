package com.nrelate.lookup;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;


public class RonLookup {
    InputStream inputStream = null;
	public Properties rpcOverrideProperties = null;
	public Properties cpcProperties = null;
	public Properties domainidProperties = null;
	public Properties widgetProperties = null;
	public Properties widgetProperties2 = null;


    public RonLookup(String rpcOverrideFile,String cpcFile,String domainidFile,String widgetFile,String widgetFile2){
		rpcOverrideProperties = new Properties();
    		cpcProperties = new Properties();
		domainidProperties = new Properties();
		widgetProperties = new Properties();
		widgetProperties2 = new Properties();
		try {
			// Have to discuss about using the Hashmap for RonLookup
			inputStream = getClass().getResourceAsStream(cpcFile);			 
			cpcProperties.load(inputStream);      
			inputStream = getClass().getResourceAsStream(domainidFile);			 			
			domainidProperties.load(inputStream);  
			inputStream = getClass().getResourceAsStream(widgetFile);			 			
			widgetProperties.load(inputStream);   
			inputStream = getClass().getResourceAsStream(widgetFile2);			 			
			widgetProperties2.load(inputStream);   
			inputStream = getClass().getResourceAsStream(rpcOverrideFile);
			rpcOverrideProperties.load(inputStream);  
			//inputStream.close();
   
		} catch (IOException e) {
			 System.out.println("Error loading properties...");
		}	
	}
	public RonLookup(String rpcOverrideFile){
		cpcProperties = new Properties();
		try {
			inputStream = getClass().getResourceAsStream(rpcOverrideFile);
			// Have to discuss about using the Hashmap for RonLookup
			cpcProperties.load(inputStream);  
   
		} catch (IOException e) {
			 System.out.println("Error loading properties...");
		}	
	}
    
    public String getRPC(String property){             
      return rpcOverrideProperties.getProperty(property);
    }
    public String getDomainid(String property){             
      return domainidProperties.getProperty(property);
    }
    public String getCPC(String property){             
      return cpcProperties.getProperty(property);
    }
    public String getWidgetConfiguration(String property){             
      return widgetProperties.getProperty(property);
    } 
    public String getWidgetConfigurationWithoutWidgetid(String property){             
      return widgetProperties2.getProperty(property);
    }    
	
	public static void main(String args[]){
          System.out.println(new RonLookup("campaignRun").getCPC("2013-01-0210714"));
    }
}
