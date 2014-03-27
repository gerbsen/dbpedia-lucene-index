package de.aksw;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

import de.danielgerber.file.BufferedFileWriter;
import de.danielgerber.file.BufferedFileWriter.WRITER_WRITE_MODE;
import de.danielgerber.file.FileUtil;
import de.danielgerber.rdf.NtripleUtil;

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 */
public class DBpediaLuceneIndexGeneratorLight {

	// default values, should get overwritten with main args
    private static String GRAPH                 = "http://dbpedia.org";
    private static double RAM_BUFFER_MAX_SIZE   = 1024;
    private static boolean OVERWRITE_INDEX      = true;
    private static Boolean FILTER_SURFACE_FORMS = false;
    public static String DIRECTORY              = "";
    public static String LANGUAGE 				= null;
    private static String INDEX_DIRECTORY       = "";
    private static String SPARQL_ENDPOINT       = "http://localhost:8890/sparql";
    private static IndexWriter writer;
    
    public static String DBPEDIA_REDIRECTS_FILE       = null;
    public static String DBPEDIA_LABELS_FILE          = null;
    public static String DBPEDIA_DISAMBIGUATIONS_FILE = null;
    public static String SURFACE_FORMS_FILE           = null;
    public static String FILTERED_LABELS_FILE		  = null;
    
    /**
     * @param args
     * @throws IOException 
     * @throws CorruptIndexException 
     */
    public static void main(String[] args) throws CorruptIndexException, IOException {
        
        for (int i = 0; i < args.length ; i = i + 2) {
            
            if ( args[i].equals("-o") ) OVERWRITE_INDEX     	= Boolean.valueOf(args[i+1]);
            if ( args[i].equals("-b") ) RAM_BUFFER_MAX_SIZE 	= Double.valueOf(args[i+1]);
            if ( args[i].equals("-d") ) DIRECTORY           	= args[i+1];
            if ( args[i].equals("-i") ) INDEX_DIRECTORY     	= args[i+1];
            if ( args[i].equals("-s") ) SPARQL_ENDPOINT     	= args[i+1];
            if ( args[i].equals("-g") ) GRAPH               	= args[i+1];
            if ( args[i].equals("-l") ) LANGUAGE            	= args[i+1];
			if ( args[i].equals("-f") ) FILTER_SURFACE_FORMS	= new Boolean(args[i+1]);
            
            DBPEDIA_REDIRECTS_FILE       = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "redirects_" + LANGUAGE + ".ttl";
            DBPEDIA_LABELS_FILE          = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "labels_" + LANGUAGE + ".ttl";
            DBPEDIA_DISAMBIGUATIONS_FILE = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "disambiguations_" + LANGUAGE + ".ttl";
            SURFACE_FORMS_FILE           = DBpediaLuceneIndexGeneratorLight.DIRECTORY + LANGUAGE + "_surface_forms.tsv";
            FILTERED_LABELS_FILE		 = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "labels_" + LANGUAGE + "_filtered.ttl";
            
            DBpediaLuceneIndexGenerator.DBPEDIA_REDIRECTS_FILE       = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "redirects_" + LANGUAGE + ".ttl";
            DBpediaLuceneIndexGenerator.DBPEDIA_LABELS_FILE          = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "labels_" + LANGUAGE + ".ttl";
            DBpediaLuceneIndexGenerator.DBPEDIA_DISAMBIGUATIONS_FILE = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "disambiguations_" + LANGUAGE + ".ttl";
            DBpediaLuceneIndexGenerator.SURFACE_FORMS_FILE           = DBpediaLuceneIndexGeneratorLight.DIRECTORY + LANGUAGE + "_surface_forms.tsv";
            DBpediaLuceneIndexGenerator.FILTERED_LABELS_FILE		 = DBpediaLuceneIndexGeneratorLight.DIRECTORY + "labels_" + LANGUAGE + "_filtered.ttl";
        }
        
        DBpediaSpotlightSurfaceFormGenerator surfaceFormGenerator = new DBpediaSpotlightSurfaceFormGenerator();
        
        // we need to break here, because after the step we need to import the stuff to virtuoso
        if ( FILTER_SURFACE_FORMS ) {
            
        	System.out.println("Starting to filter labels_" + LANGUAGE + ".uri!");
            Set<String> badUris = new HashSet<String>();
            badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBPEDIA_REDIRECTS_FILE, ""));
            System.out.println("Finished reading bad redirect uris!");
            badUris.addAll(NtripleUtil.getSubjectsFromNTriple(DBPEDIA_DISAMBIGUATIONS_FILE, ""));
            System.out.println("Finished reading bad disambiguations uris!");
            
            // write the file
            BufferedFileWriter writer = FileUtil.openWriter(FILTERED_LABELS_FILE, "UTF-8", WRITER_WRITE_MODE.OVERRIDE);
            System.out.println("Writing filtered labels file: " + FILTERED_LABELS_FILE);
            NxParser n3Parser = NtripleUtil.openNxParser(DBPEDIA_LABELS_FILE);
            while (n3Parser.hasNext()) {
                
                Node[] node = n3Parser.next();
                String subjectUri = node[0].toString();
                
                if ( !badUris.contains(subjectUri) ) {
                    
                    writer.write(node[0].toN3() + " " + node[1].toN3() + " " + node[2].toN3() + " .");
                }
            }
            
            writer.close();
            
            // generate the surface forms (and save them to the file) or load them from a file
            surfaceFormGenerator.createOrReadSurfaceForms();
            
            return;
        }
        
        
        System.out.println("Override-Index: " + OVERWRITE_INDEX);
        System.out.println("RAM-Buffer-Max-Size: " + RAM_BUFFER_MAX_SIZE);
        System.out.println("Index-Directory: " + INDEX_DIRECTORY);
        System.out.println("SPARQL-Endpoint: " + SPARQL_ENDPOINT);
        System.out.println("GRAPH: " + GRAPH);
        
        DBpediaLuceneIndexGeneratorLight indexGenerator = new DBpediaLuceneIndexGeneratorLight();
        
        // create the index writer configuration and create a new index writer
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_40, new StandardAnalyzer(Version.LUCENE_40));
        indexWriterConfig.setRAMBufferSizeMB(RAM_BUFFER_MAX_SIZE);
        indexWriterConfig.setOpenMode(OVERWRITE_INDEX || !indexGenerator.isIndexExisting(INDEX_DIRECTORY) ? OpenMode.CREATE : OpenMode.APPEND);
        writer = indexGenerator.createIndex(INDEX_DIRECTORY, indexWriterConfig);

        Map<String,Set<String>> surfaceForms = surfaceFormGenerator.createOrReadSurfaceForms();
        
        Set<IndexDocumentLight> indexDocuments = new HashSet<IndexDocumentLight>();
        
        // time measurements
        int counter = 0;
        long start = System.currentTimeMillis();
        int total = surfaceForms.size();
        
        Iterator<Map.Entry<String,Set<String>>> surfaceFormIterator = surfaceForms.entrySet().iterator();
        while ( surfaceFormIterator.hasNext() ) {
            
            try {
				Map.Entry<String,Set<String>> entry = surfaceFormIterator.next();
				indexDocuments.add(indexGenerator.queryAttributesForUri(entry.getKey(), entry.getValue()));
				
				// improve speed through batch save
				if ( ++counter % 10000 == 0 ) {

				    indexGenerator.addIndexDocuments(indexDocuments);
				    System.out.println("Done: " + counter + "/" + total + " " + MessageFormat.format("{0,number,#.##%}", (double) counter / (double) total) + " in " + (System.currentTimeMillis() - start) + "ms" );
				    start = System.currentTimeMillis();
				    indexDocuments = new HashSet<IndexDocumentLight>();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        // write the last few items
        indexGenerator.addIndexDocuments(indexDocuments);
        
        writer.close();
    }

	/**
     * Adds a set of index documents in batch mode to the index
     * Uris and image urls as well as type Uris are not analyzed.
     * Termvectors are not stored for anything. Everything else is
     * analyzed using a standard analyzer. 
     * 
     * @param indexDocuments - the documents to be indexed
     * @throws CorruptIndexException - index corrupted
     * @throws IOException - error
     */
    private void addIndexDocuments(Set<IndexDocumentLight> indexDocuments) throws CorruptIndexException, IOException {

        Set<Document> luceneDocuments = new HashSet<Document>();
        FieldType stringType = new FieldType(StringField.TYPE_STORED);
        stringType.setStoreTermVectors(false);
        FieldType textType = new FieldType(TextField.TYPE_STORED);
        textType.setStoreTermVectors(false);
        for ( IndexDocumentLight indexDocument : indexDocuments ) {
            
            Document luceneDocument = new Document();
            luceneDocument.add(new Field("uri", indexDocument.getUri(), stringType));
            luceneDocument.add(new Field("label", indexDocument.getLabel(), textType));
            luceneDocument.add(new Field("short-abstract", indexDocument.getShortAbstract(), textType));
            luceneDocument.add(new Field("long-abstract", indexDocument.getLongAbstract(), textType));
            for ( String type : indexDocument.getTypes() )
                luceneDocument.add(new Field("types", type, stringType));
            for ( String surfaceForm : indexDocument.getSurfaceForms() )
                luceneDocument.add(new Field("surfaceForms", surfaceForm, stringType));
                    
            luceneDocuments.add(luceneDocument);
        }
        writer.addDocuments(luceneDocuments);
    }
    
    /**
     * Queries a configured sparql endpoint for all information a document needs
     *  - rank
     *  - label
     *  - uri
     *  - imageUrl
     *  - abstract 
     *  - types
     * 
     * @param uri the uri of the reosurce
     * @param surfaceForms the surface forms of this resource
     * @param language2dbpediaLinks 
     * @return a document ready to be indexed
     * @throws Exception 
     */
    private IndexDocumentLight queryAttributesForUri(String uri, Set<String> surfaceForms) throws Exception  {
    	String query =
		        String.format(
		        "SELECT ?label ?short_abstract ?long_abstract ?types " +
		        "FROM <%s> " +
		        "WHERE { " +
		        "   OPTIONAL { <%s> <http://www.w3.org/2000/01/rdf-schema#label> ?label . } " +
		        "   OPTIONAL { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?types . } " +
		        "   OPTIONAL { <%s> <http://www.w3.org/2000/01/rdf-schema#comment> ?short_abstract . } " +
		        "   OPTIONAL { <%s> <http://dbpedia.org/ontology/abstract> ?long_abstract . } " +
		        "}", GRAPH, uri, uri, uri, uri);
        // execute the query
		IndexDocumentLight document;
		try {
			document = new IndexDocumentLight();
			QueryEngineHTTP qexec = new QueryEngineHTTP(SPARQL_ENDPOINT, query);
			ResultSet result = qexec.execSelect();
			
			while (result.hasNext()) {
			    
			    QuerySolution solution = result.next();
			    
			    // those values do get repeated, we need them to set only one time
			    if ( document.getUri().isEmpty() ) {
			        
			        document.setUri(URLDecoder.decode(uri, "UTF-8"));
			        document.setLabel(solution.get("label").asLiteral().getLexicalForm());
			        document.setShortAbstract(solution.get("short_abstract") != null ? solution.get("short_abstract").asLiteral().getLexicalForm() : "");
			        document.setLongAbstract(solution.get("long_abstract") != null ? solution.get("long_abstract").asLiteral().getLexicalForm() : "");
			        
			    }
			    // there might be different types
			    if (solution.get("types") != null) document.getTypes().add(solution.get("types").toString());
			}
			document.setSurfaceForms(surfaceForms == null ? new HashSet<String>() : surfaceForms);
			return document;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(query);
			throw e;
		}
    }
    
    /**
     * Create a new filesystem lucene index
     * 
     * @param absoluteFilePath - the path where to create/append the index
     * @param indexWriterConfig - the index write configuration
     * @return
     */
    private IndexWriter createIndex(String absoluteFilePath, IndexWriterConfig indexWriterConfig) {

        try {
            
            return new IndexWriter(FSDirectory.open(new File(absoluteFilePath)), indexWriterConfig);
        }
        catch (CorruptIndexException e) {
            
            e.printStackTrace();
            throw new RuntimeException("Could not create index", e);
        }
        catch (LockObtainFailedException e) {
            
            e.printStackTrace();
            throw new RuntimeException("Could not create index", e);
        }
        catch (IOException e) {
            
            e.printStackTrace();
            throw new RuntimeException("Could not create index", e);
        }
    }
    
    /**
     * Checks if an index exists at the given location.
     * 
     * @param indexDirectory - the directory of the index to be checked
     * @return true if the index exists, false otherwise
     */
    public boolean isIndexExisting(String indexDirectory) {
        
        try {
            
            return DirectoryReader.indexExists(FSDirectory.open(new File(indexDirectory)));
        }
        catch (IOException e) {
            
            e.printStackTrace();
            String error = "Check if index exists failed!";
            throw new RuntimeException(error, e);
        }
    }
}
