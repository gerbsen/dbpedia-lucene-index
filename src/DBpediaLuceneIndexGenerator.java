import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
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

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 */
public class DBpediaLuceneIndexGenerator {

    // default values, should get overwritten with main args
    private static String GRAPH                 = "http://dbpedia.org";
    private static double RAM_BUFFER_MAX_SIZE   = 1024;
    private static boolean OVERWRITE_INDEX      = true;
    public static String DIRECTORY              = "";
    private static String INDEX_DIRECTORY       = "";
    private static String SPARQL_ENDPOINT       = "http://localhost:8890/sparql";
    private static IndexWriter writer;
    
    /**
     * @param args
     * @throws IOException 
     * @throws CorruptIndexException 
     */
    public static void main(String[] args) throws CorruptIndexException, IOException {
        
        for (int i = 0; i < args.length ; i = i + 2) {
            
            if ( args[i].equals("-o") ) OVERWRITE_INDEX     = Boolean.valueOf(args[i+1]);
            if ( args[i].equals("-b") ) RAM_BUFFER_MAX_SIZE = Double.valueOf(args[i+1]);
            if ( args[i].equals("-d") ) DIRECTORY           = args[i+1];
            if ( args[i].equals("-i") ) INDEX_DIRECTORY     = args[i+1];
            if ( args[i].equals("-s") ) SPARQL_ENDPOINT     = args[i+1];
            if ( args[i].equals("-g") ) GRAPH               = args[i+1];

            // we need to break here, because after the step we need to import the stuff to virtuoso
            if ( args[i].equals("-f") && Boolean.valueOf(args[i+1]) ) {
                
                FilterBadUris.main(null);
                return;
            }
        }
        
        System.out.println("Override-Index: " + OVERWRITE_INDEX);
        System.out.println("RAM-Buffer-Max-Size: " + RAM_BUFFER_MAX_SIZE);
        System.out.println("Index-Directory: " + INDEX_DIRECTORY);
        System.out.println("SPARQL-Endpoint: " + SPARQL_ENDPOINT);
        System.out.println("GRAPH: " + GRAPH);
        
        DBpediaLuceneIndexGenerator indexGenerator = new DBpediaLuceneIndexGenerator();
        
        // create the index writer configuration and create a new index writer
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_40, new StandardAnalyzer(Version.LUCENE_40));
        indexWriterConfig.setRAMBufferSizeMB(RAM_BUFFER_MAX_SIZE);
        indexWriterConfig.setOpenMode(OVERWRITE_INDEX || !indexGenerator.isIndexExisting(INDEX_DIRECTORY) ? OpenMode.CREATE : OpenMode.APPEND);
        writer = indexGenerator.createIndex(INDEX_DIRECTORY, indexWriterConfig);

        // generate the surface forms (and save them to the file) or load them from a file
        DBpediaSpotlightSurfaceFormGenerator surfaceFormGenerator = new DBpediaSpotlightSurfaceFormGenerator();
        Map<String,Set<String>> surfaceForms = surfaceFormGenerator.createSurfaceForms();

        Set<IndexDocument> indexDocuments = new HashSet<IndexDocument>();
        
        // time measurements
        int counter = 0;
        long start = System.currentTimeMillis();
        int total = surfaceForms.size();
        
        Iterator<Map.Entry<String,Set<String>>> surfaceFormIterator = surfaceForms.entrySet().iterator();
        while ( surfaceFormIterator.hasNext() ) {
            
            Map.Entry<String,Set<String>> entry = surfaceFormIterator.next();
            indexDocuments.add(indexGenerator.queryAttributesForUri(entry.getKey(), entry.getValue()));
            
            // improve speed through batch save
            if ( ++counter % 10000 == 0 ) {

                indexGenerator.addIndexDocuments(indexDocuments);
                System.out.println("Done: " + counter + "/" + total + " " + MessageFormat.format("{0,number,#.##%}", (double) counter / (double) total) + " in " + (System.currentTimeMillis() - start) + "ms" );
                start = System.currentTimeMillis();
                indexDocuments = new HashSet<IndexDocument>();
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
    private void addIndexDocuments(Set<IndexDocument> indexDocuments) throws CorruptIndexException, IOException {

        Set<Document> luceneDocuments = new HashSet<Document>();
        FieldType stringType = new FieldType(StringField.TYPE_STORED);
        stringType.setStoreTermVectors(false);
        FieldType textType = new FieldType(TextField.TYPE_STORED);
        textType.setStoreTermVectors(false);
        for ( IndexDocument indexDocument : indexDocuments ) {
            
            Document luceneDocument = new Document();
            luceneDocument.add(new Field("uri", indexDocument.getUri(), stringType));
            luceneDocument.add(new Field("label", indexDocument.getLabel(), textType));
            luceneDocument.add(new Field("comment", indexDocument.getShortAbstract(), textType));
            luceneDocument.add(new Field("imageURL", indexDocument.getImageUri(), stringType));
            luceneDocument.add(new IntField("pagerank", indexDocument.getPageRank(), Field.Store.YES));
            luceneDocument.add(new DoubleField("disambiguationScore", indexDocument.getDisambiguationScore(), Field.Store.YES));
            for ( String type : indexDocument.getTypes() )
                luceneDocument.add(new Field("types", type, stringType));
            for ( String surfaceForm : indexDocument.getSurfaceForms() )
                luceneDocument.add(new Field("surfaceForms", surfaceForm, textType));
                    
            luceneDocuments.add(luceneDocument);
        }
        writer.addDocuments(luceneDocuments);
    }
    
    public double getAprioriScore1(String uri, String endpoint, String graph) {

        String query = "SELECT (COUNT(?s) AS ?cnt) WHERE {?s ?p <"+uri+">}";
        Query sparqlQuery = QueryFactory.create(query);
        QueryExecution qexec;
        if (graph != null) {
            qexec = QueryExecutionFactory.sparqlService(endpoint, sparqlQuery, graph);
        } else {
            qexec = QueryExecutionFactory.sparqlService(endpoint, sparqlQuery);
        }
        ResultSet results = qexec.execSelect();
        int count = 0;
        try {
            while (results.hasNext()) {
                QuerySolution soln = results.nextSolution();
                count = soln.getLiteral("cnt").getInt();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        //logger.info(uri+" -> "+Math.log(count+1));
        return Math.log(count+1);
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
     * @return a document ready to be indexed
     */
    private IndexDocument queryAttributesForUri(String uri, Set<String> surfaceForms) {

        String query =
                String.format(
                "SELECT (<LONG::IRI_RANK> (<%s>)) as ?rank ?label ?imageUrl ?abstract ?types " +
                "FROM <%s> " +
                "WHERE { " +
                "   OPTIONAL { <%s> <http://www.w3.org/2000/01/rdf-schema#label> ?label . } " +
                "   OPTIONAL { <%s> <http://dbpedia.org/ontology/thumbnail> ?imageUrl . } " +
                "   OPTIONAL { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?types . } " +
                "   OPTIONAL { <%s> <http://www.w3.org/2000/01/rdf-schema#comment> ?abstract . } " +
                "}", uri, GRAPH, uri, uri, uri, uri);
        
        // execute the query
        IndexDocument document = new IndexDocument();
        QueryEngineHTTP qexec = new QueryEngineHTTP(SPARQL_ENDPOINT, query);
        ResultSet result = qexec.execSelect();
        
        while (result.hasNext()) {
            
            QuerySolution solution = result.next();
            
            // those values do get repeated, we need them to set only one time
            if ( document.getUri().isEmpty() ) {
                
                document.setUri(uri);
                document.setLabel(solution.get("label").asLiteral().getLexicalForm());
                document.setPageRank(solution.get("rank") != null ? Integer.valueOf(solution.get("rank").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", "")) : 0);
                document.setImageUri(solution.get("imageUrl") != null ? solution.get("imageUrl").toString() : "");
                document.setShortAbstract(solution.get("abstract") != null ? solution.get("abstract").asLiteral().getLexicalForm() : "");
                
                try {
					double disambiguationScore = getAprioriScore1(uri, SPARQL_ENDPOINT, GRAPH);
					document.setDisambiguationScore(disambiguationScore);
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
            // there might be different types
            if (solution.get("types") != null) document.getTypes().add(solution.get("types").toString());
        }
        document.setSurfaceForms(surfaceForms == null ? new HashSet<String>() : surfaceForms);
        
        return document;
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
