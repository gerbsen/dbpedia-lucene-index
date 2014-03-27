package de.aksw;
import java.util.HashSet;
import java.util.Set;


/**
 * 
 */

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 *
 */
public class IndexDocumentLight {

    private String uri = "";
    private String label = "";
    private Set<String> types = new HashSet<String>();
    private Set<String> surfaceForms = new HashSet<String>();
    private String shortAbstract = "";
    private String longAbstract = "";
    
	/**
     * @return the uri
     */
    public String getUri() {
    
        return uri;
    }
    
    /**
     * @param uri the uri to set
     */
    public void setUri(String uri) {
    
        this.uri = uri;
    }
    
    /**
     * @return the label
     */
    public String getLabel() {
    
        return label;
    }
    
    /**
     * @param label the label to set
     */
    public void setLabel(String label) {
    
        this.label = label;
    }
    
    /**
     * @return the types
     */
    public Set<String> getTypes() {
    
        return types;
    }
    
    /**
     * @param types the types to set
     */
    public void setTypes(Set<String> types) {
    
        this.types = types;
    }
    
    /**
     * @return the surfaceForms
     */
    public Set<String> getSurfaceForms() {
    
        return surfaceForms;
    }
    
    /**
     * @param surfaceForms the surfaceForms to set
     */
    public void setSurfaceForms(Set<String> surfaceForms) {
    
        this.surfaceForms = surfaceForms;
    }
    
    /**
     * @return the shortAbstract
     */
    public String getShortAbstract() {
    
        return shortAbstract;
    }
    
    /**
     * @param shortAbstract the shortAbstract to set
     */
    public void setShortAbstract(String shortAbstract) {
    
        this.shortAbstract = shortAbstract;
    }
    
    /**
	 * @return the longAbstract
	 */
	public String getLongAbstract() {
		return longAbstract;
	}
	
	/**
	 * @param longAbstract the longAbstract to set
	 */
	public void setLongAbstract(String longAbstract) {
		this.longAbstract = longAbstract;
	}
    
}
