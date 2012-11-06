import java.util.HashSet;
import java.util.Set;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import de.danielgerber.file.BufferedFileWriter;
import de.danielgerber.file.FileUtil;
import de.danielgerber.file.BufferedFileWriter.WRITER_WRITE_MODE;
import de.danielgerber.rdf.NtripleUtil;

/**
 * 
 */

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 *
 */
public class FilterBadUris {

    public static final String FILTERED_LABELS_FILE = "labels_en_filtered.nt";
    
    /**
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("Starting to filter labels_en.uri!");
        Set<String> badUris = new HashSet<String>();
        badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBpediaSpotlightSurfaceFormGenerator.DBPEDIA_REDIRECTS_FILE, ""));
        System.out.println("Finished reading bad redirect uris!");
        badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBpediaSpotlightSurfaceFormGenerator.DBPEDIA_DISAMBIGUATIONS_FILE, ""));
        System.out.println("Finished reading bad disambiguations uris!");
        
        // write the file
        BufferedFileWriter writer = FileUtil.openWriter(FILTERED_LABELS_FILE, "UTF-8", WRITER_WRITE_MODE.OVERRIDE);
        System.out.println("Writing filtered labels file: " + FILTERED_LABELS_FILE);
        NxParser n3Parser = NtripleUtil.openNxParser(DBpediaSpotlightSurfaceFormGenerator.DBPEDIA_LABELS_FILE);
        while (n3Parser.hasNext()) {
            
            Node[] node = n3Parser.next();
            String subjectUri = node[0].toString();
            
            if ( !badUris.contains(subjectUri) ) {
                
                writer.write(node[0].toN3() + " " + node[1].toN3() + " " + node[2].toN3() + " .");
            }
        }
        
        writer.close();
    }
}
