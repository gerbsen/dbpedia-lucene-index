package de.aksw;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import de.danielgerber.file.BufferedFileWriter;
import de.danielgerber.file.FileUtil;
import de.danielgerber.file.BufferedFileWriter.WRITER_WRITE_MODE;
import de.danielgerber.rdf.NtripleUtil;

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 */
public class DBpediaSpotlightSurfaceFormGenerator {
    
    private static final Logger logger = Logger.getLogger("DBpediaSpotlightSurfaceFormGenerator");
    
    private static final int MAXIMUM_SURFACE_FORM_LENGHT    = 50;

    private static List<String> LOWERCASE_STOPWORDS         = null;
    private static final List<String> STOPWORDS             = Arrays.asList("but", "i", "a", "about", "an", "and", "are", "as", "at", "be", "by", "com", "for", "from", "how", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "what", "when", "where", "who", "will", "with", "the", "www", "before", ",", "after", ";", "like", "and", "such");
    
    /**
     * 
     * @return
     * @throws UnsupportedEncodingException 
     */
    private Set<String> createConceptUris() throws UnsupportedEncodingException {
        
        Set<String> conceptUris = new HashSet<String>();
        Set<String> badUris = new HashSet<String>();
        badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBpediaLuceneIndexGenerator.DBPEDIA_REDIRECTS_FILE, ""));
        logger.info("Finished reading redirect file for bad uri detection!");
        badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBpediaLuceneIndexGenerator.DBPEDIA_DISAMBIGUATIONS_FILE, ""));
        logger.info("Finished reading disambiguations file for bad uri detection!");
        
        // every uri which looks like a good uri and is not in the disambiguations or redirect files is a concept uri
        NxParser n3Parser = NtripleUtil.openNxParser(DBpediaLuceneIndexGenerator.FILTERED_LABELS_FILE);
        while (n3Parser.hasNext()) {
            
            Node[] node = n3Parser.next();
            String subjectUri = node[0].toString();
            
            String subjectUriWihtoutPrefix = subjectUri.substring(subjectUri.lastIndexOf("/") + 1);
            
            if ( isGoodUri(subjectUriWihtoutPrefix) && !badUris.contains(subjectUri) ) conceptUris.add(subjectUri);
        }
        logger.info("Concept Uris construction complete! Total of: " + conceptUris.size() + " concept URIs found!");
        return conceptUris;
    }
    
    private void initStopwords() {
        
        List<String> lowerCaseStopWords = new ArrayList<String>();
        for ( String stopword : STOPWORDS ) {
            
            lowerCaseStopWords.add(stopword.toLowerCase());
        }
        LOWERCASE_STOPWORDS = lowerCaseStopWords;
        logger.info("There were " + LOWERCASE_STOPWORDS.size() + " lowercase stopwords found.");
    }
    
    public Map<String,Set<String>> createOrReadSurfaceForms() throws UnsupportedEncodingException {
        
        if ( new File(DBpediaLuceneIndexGenerator.SURFACE_FORMS_FILE).exists() ) return this.initializeSurfaceFormsFromFile();
        
        initStopwords();
        Set<String> conceptUris = createConceptUris();
        Map<String,Set<String>> surfaceForms = new HashMap<String,Set<String>>();
        
        // first add all uris of the concept uris
        for ( String uri : conceptUris ) 
            addSurfaceForm(surfaceForms, uri, uri.substring(uri.lastIndexOf("/") + 1));
                

        logger.info("Finished adding all conceptUris: " + surfaceForms.size());
        
        List<String[]> subjectToObject = NtripleUtil.getSubjectAndObjectsFromNTriple(DBpediaLuceneIndexGenerator.DBPEDIA_DISAMBIGUATIONS_FILE, "");
        subjectToObject.addAll(NtripleUtil.getSubjectAndObjectsFromNTriple(DBpediaLuceneIndexGenerator.DBPEDIA_REDIRECTS_FILE, ""));
        
        for ( String[] subjectAndObject : subjectToObject ) {
            
            String object = subjectAndObject[1];
            String subject = subjectAndObject[0];
            
            if ( conceptUris.contains(object) && !object.contains("%") ) 
                addSurfaceForm(surfaceForms, object, subject.substring(subject.lastIndexOf("/") + 1));
        }
        logger.info("Finished generation of surface forms.");
        
        // write the file
        BufferedFileWriter writer = FileUtil.openWriter(DBpediaLuceneIndexGenerator.SURFACE_FORMS_FILE, "UTF-8", WRITER_WRITE_MODE.OVERRIDE);
        for (Map.Entry<String, Set<String>> entry : surfaceForms.entrySet()) 
            writer.write(entry.getKey() + "\t" + StringUtils.join(addNonAccentVersion(entry.getValue()), "\t"));
        
        writer.close();
        logger.info("Finished writing of surface forms to disk.");
        
        return surfaceForms;
    }
    
    private static Set<String> addNonAccentVersion(Set<String> surfaceForms) {
    	
    	// remove all the accents in the surface forms and add that new label
        Set<String> normalizedLabels = new HashSet<String>();
        for ( String surfaceForm : surfaceForms) {
        	
    	    String normalized = Normalizer.normalize(surfaceForm, Normalizer.Form.NFD);
    	    normalized = normalized.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    	    if ( !normalized.equals(surfaceForm) ) normalizedLabels.add(normalized);
        }
        surfaceForms.addAll(normalizedLabels);
        
		return surfaceForms;
	}
    
	private static void addSurfaceForm(Map<String, Set<String>> surfaceForms, String key, String value) {

        // clean and URL decode, whitespace removal
        String newSurfaceForm =  createCleanSurfaceForm(value);
        if ( newSurfaceForm != null ) {
            
            if ( surfaceForms.containsKey(key) ) surfaceForms.get(key).add(newSurfaceForm);
            else {
                
                Set<String> sfList = new HashSet<String>();
                sfList.add(newSurfaceForm);
                surfaceForms.put(key, sfList);
            }
        }
    }

    private static String createCleanSurfaceForm(String label) {

        try {
            
            String newLabel = URLDecoder.decode(label, "UTF-8");
            newLabel = newLabel.replaceAll("_", " ").replaceAll(" +", " ").trim();
            newLabel = newLabel.replaceAll(" \\(.+?\\)$", "");
            
            return isGoodSurfaceForm(newLabel) ? newLabel : null;
        }
        catch (IllegalArgumentException e) {
            
            String error = "Could not decode label: " + label + " with URLDecoder.";
            System.out.println(error);
            return null;
        }
        catch (UnsupportedEncodingException e) {
            
            e.printStackTrace();
            String error = "Could not decode label: " + label + " with encoding UTF-8";
            System.out.println(error);
            throw new RuntimeException(error, e);
        }
    }

    /**
     * 
     * @param uri
     * @return
     */
    private static boolean isGoodUri(String uri) {

        if ( uri.contains("Liste_") || uri.contains("(Begriffsklärung)") || uri.contains("List_of_") || uri.contains("(Disambiguation)") || uri.contains("/") || uri.contains("%23") || uri.matches("^[\\W\\d]+$") ) {
            
            logger.log(Level.FINEST, "Uri: <" + uri + "> is not a good uri! / or %23 or regex");
            return false;
        }
        return true;
    }
    
    private static boolean isGoodSurfaceForm(String surfaceForm) {
        
        if ( surfaceForm.length() > MAXIMUM_SURFACE_FORM_LENGHT 
                || surfaceForm.matches("^[\\W\\d]+$")) {
            
            logger.log(Level.FINEST, "Surfaceform: " + surfaceForm +  " is not a good surface form because its too long or regex match.");
            return false;
        }
        
        int i = 0;
        for ( String token : surfaceForm.toLowerCase().split(" ") ) {
            
            // current token is not a stopword
            if ( !LOWERCASE_STOPWORDS.contains(token) ) i++;
        }
        // at least one non stop word found
        if ( i > 0 ) return true;
        else {
            
            logger.log(Level.FINEST, "Surfaceform: " + surfaceForm +  " is not a good surface form because it contains only stop words.");
            return false;
        }
    }
    
    private Map<String,Set<String>> initializeSurfaceFormsFromFile() {
        
        logger.info("Intializing surface forms from file...");
        
        List<String> surfaceForms    = FileUtil.readFileInList(DBpediaLuceneIndexGenerator.SURFACE_FORMS_FILE, "UTF-8");
        Map<String,Set<String>> urisToLabels = new HashMap<String,Set<String>>(); 
        
        // initialize the surface forms from dbpedia spotlight 
        for ( String line : surfaceForms ) {
            
            String[] lineParts = line.split("\t");
            String[] surfaceFormsPart = Arrays.copyOfRange(lineParts, 1, lineParts.length);
            Set<String> filteredSurfaceForms = new HashSet<String>();
            
            for ( String surfaceForm : surfaceFormsPart) {
                
                if ( surfaceForm.length() <= MAXIMUM_SURFACE_FORM_LENGHT ) filteredSurfaceForms.add(surfaceForm);
            }
            urisToLabels.put(lineParts[0], filteredSurfaceForms);
            urisToLabels.put(lineParts[0].replace("http://en.", "http://"), filteredSurfaceForms);
        }
        logger.info("Finished intializing surface forms! Found " + urisToLabels.size() + " dbpedia spotlight surfaceforms in file");
        
        return urisToLabels;
    }
}
