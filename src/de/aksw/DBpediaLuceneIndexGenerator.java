package de.aksw;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.HashMap;
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
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import de.danielgerber.file.BufferedFileWriter;
import de.danielgerber.file.BufferedFileWriter.WRITER_WRITE_MODE;
import de.danielgerber.file.FileUtil;
import de.danielgerber.rdf.NtripleUtil;

/**
 * @author Daniel Gerber <dgerber@informatik.uni-leipzig.de>
 */
public class DBpediaLuceneIndexGenerator {

	private static final int BATCH_SIZE = 10000;
	private static final int maxNrOfTries = 10;
	private static final int DELAY_IN_MS = 5;
	// default values, should get overwritten with main args
	private static String GRAPH                 = "http://dbpedia.org";
	private static double RAM_BUFFER_MAX_SIZE   = 128;
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
	public static String INTER_LANGUAGE_LINKS_FILE	  = null;

	/**
	 * @param args
	 * @throws IOException 
	 * @throws CorruptIndexException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws CorruptIndexException, IOException, InterruptedException {
		for (int i = 0; i < args.length ; i = i + 2) {

			if ( args[i].equals("-o") ) OVERWRITE_INDEX     	= Boolean.valueOf(args[i+1]);
			if ( args[i].equals("-b") ) RAM_BUFFER_MAX_SIZE 	= Double.valueOf(args[i+1]);
			if ( args[i].equals("-d") ) DIRECTORY           	= args[i+1];
			if ( args[i].equals("-i") ) INDEX_DIRECTORY     	= args[i+1];
			if ( args[i].equals("-s") ) SPARQL_ENDPOINT     	= args[i+1];
			if ( args[i].equals("-g") ) GRAPH               	= args[i+1];
			if ( args[i].equals("-l") ) LANGUAGE            	= args[i+1];
			if ( args[i].equals("-f") ) FILTER_SURFACE_FORMS	= new Boolean(args[i+1]);

			DBPEDIA_REDIRECTS_FILE       = DBpediaLuceneIndexGenerator.DIRECTORY + "redirects_" + LANGUAGE + ".ttl";
			DBPEDIA_LABELS_FILE          = DBpediaLuceneIndexGenerator.DIRECTORY + "labels_" + LANGUAGE + ".ttl";
			DBPEDIA_DISAMBIGUATIONS_FILE = DBpediaLuceneIndexGenerator.DIRECTORY + "disambiguations_" + LANGUAGE + ".ttl";
			SURFACE_FORMS_FILE           = DBpediaLuceneIndexGenerator.DIRECTORY + LANGUAGE + "_surface_forms.tsv";
			FILTERED_LABELS_FILE		 = DBpediaLuceneIndexGenerator.DIRECTORY + "labels_" + LANGUAGE + "_filtered.ttl";
			INTER_LANGUAGE_LINKS_FILE    = DBpediaLuceneIndexGenerator.DIRECTORY + "interlanguage_links_" + LANGUAGE + ".ttl";
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

		DBpediaLuceneIndexGenerator indexGenerator = new DBpediaLuceneIndexGenerator();

		// create the index writer configuration and create a new index writer
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_46, new StandardAnalyzer(Version.LUCENE_46));
		indexWriterConfig.setRAMBufferSizeMB(RAM_BUFFER_MAX_SIZE);
		indexWriterConfig.setOpenMode(OVERWRITE_INDEX || !indexGenerator.isIndexExisting(INDEX_DIRECTORY) ? OpenMode.CREATE : OpenMode.APPEND);
		writer = indexGenerator.createIndex(INDEX_DIRECTORY, indexWriterConfig);

		Map<String,Set<String>> surfaceForms = surfaceFormGenerator.createOrReadSurfaceForms();
		Map<String,String> language2dbpediaLinks = createInterLanguageLinks();

		Set<IndexDocument> indexDocuments = new HashSet<IndexDocument>((int)(BATCH_SIZE*1.4));

		// time measurements
		int counter = 0;
		int noLabelCounter = 0;
		int http404Counter = 0;
		long start = System.currentTimeMillis();
		int total = surfaceForms.size();
		Set<String> surfaceFormValues;

		Iterator<Map.Entry<String,Set<String>>> surfaceFormIterator = surfaceForms.entrySet().iterator();
		while ( surfaceFormIterator.hasNext() ) {

			Map.Entry<String,Set<String>> entry = surfaceFormIterator.next();
			String uri = entry.getKey();
			//            if(uri.startsWith(""))
			try
			{
				indexDocuments.add(indexGenerator.queryAttributesForUri("http://dbpedia.org/resource/"+entry.getKey(), entry.getValue(), language2dbpediaLinks));
			}
			catch(NoLabelException e)
			{
				if(noLabelCounter++%100==0)
				{System.out.println(e.getMessage()+" "+noLabelCounter+"/"+(counter+1)+" labels faulty ("+100*noLabelCounter/counter+"%)");}
			}
			// don't know which one it is
			//          catch(org.openjena.atlas.web.HttpException|org.apache.jena.atlas.web.HttpException e)
			catch(QueryExceptionHTTP e)
			{System.out.println("Waiting for 10 seconds"+e.getMessage());Thread.sleep(10000);}

			// improve speed through batch save
			if ( ++counter % BATCH_SIZE == 0 ) {

				indexGenerator.addIndexDocuments(indexDocuments);
				System.out.println("Done: " + counter + "/" + total + " " + MessageFormat.format("{0,number,#.##%}", (double) counter / (double) total) + " in " + (System.currentTimeMillis() - start) + "ms" );
				start = System.currentTimeMillis();
				indexDocuments = new HashSet<IndexDocument>((int)(BATCH_SIZE*1.4));

			}


		}
		// write the last few items
		indexGenerator.addIndexDocuments(indexDocuments);
		writer.commit();
		writer.close();
	}

	private static Map<String, String> createInterLanguageLinks() {

		Map<String,String> languageToDbpediaUris = new HashMap<String, String>();

		if ( LANGUAGE.equals("en") ) return languageToDbpediaUris;

		NxParser n3Parser = NtripleUtil.openNxParser(DBpediaLuceneIndexGenerator.INTER_LANGUAGE_LINKS_FILE);
		while (n3Parser.hasNext()) {

			Node[] node = n3Parser.next();
			String subjectUri = node[0].toString();
			String objectUri = node[2].toString();

			if ( objectUri.startsWith("http://dbpedia.org") ) languageToDbpediaUris.put(subjectUri, objectUri);
		}

		return languageToDbpediaUris;
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
		Document luceneDocument;
		for ( IndexDocument indexDocument : indexDocuments ) {

			luceneDocument = new Document();
			luceneDocument.add(new Field("uri", indexDocument.getUri(), stringType));
			luceneDocument.add(new Field("dbpediaUri", indexDocument.getCanonicalDBpediaUri(), stringType));
			luceneDocument.add(new Field("label", indexDocument.getLabel(), textType));
			luceneDocument.add(new Field("comment", indexDocument.getShortAbstract(), textType));
			luceneDocument.add(new Field("imageURL", indexDocument.getImageUri(), stringType));
			luceneDocument.add(new IntField("pagerank", indexDocument.getPageRank(), Field.Store.YES));
			luceneDocument.add(new DoubleField("disambiguationScore", indexDocument.getDisambiguationScore(), Field.Store.YES));
			for ( String type : indexDocument.getTypes() )
				luceneDocument.add(new Field("types", type, stringType));
			for ( String surfaceForm : indexDocument.getSurfaceForms() )
				luceneDocument.add(new Field("surfaceForms", surfaceForm, stringType));

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

	private class NoLabelException extends Exception
	{
		public NoLabelException(String uri)
		{
			super("No label found for uri "+uri);
		}
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
	 * @param uri the URI of the resource
	 * @param surfaceForms the surface forms of this resource
	 * @param language2dbpediaLinks 
	 * @return a document ready to be indexed
	 * @throws UnsupportedEncodingException 
	 * @throws NoLabelException 
	 */
	private IndexDocument queryAttributesForUri(String uri, Set<String> surfaceForms, Map<String, String> language2dbpediaLinks) throws UnsupportedEncodingException, NoLabelException {

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
		int nrOfTries = 0;
		ResultSet result = null;
		while(result == null && nrOfTries++ <= maxNrOfTries){
			try {
				result = qexec.execSelect();
			} catch (Exception e1) {
				System.err.println("An error occured while executing SPARQL query\n" + query + "\nRetrying...");
				e1.printStackTrace();
				try {
					Thread.sleep(DELAY_IN_MS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		if(result == null){
			return null;
		}

		while (result.hasNext()) {

			QuerySolution solution = result.next();

			// those values do get repeated, we need them to set only one time
			if ( document.getUri().isEmpty() ) {

				document.setUri(URLDecoder.decode(uri, "UTF-8"));                
				RDFNode label = solution.get("label");
				if(label==null) throw new NoLabelException(uri);
				document.setLabel(label.asLiteral().getLexicalForm());

				document.setPageRank(solution.get("rank") != null ? Integer.valueOf(solution.get("rank").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", "")) : 0);
				document.setImageUri(solution.get("imageUrl") != null ? solution.get("imageUrl").toString() : "");
				document.setShortAbstract(solution.get("abstract") != null ? solution.get("abstract").asLiteral().getLexicalForm() : "");
				document.setCanonicalDBpediaUri(language2dbpediaLinks.containsKey(uri) ? language2dbpediaLinks.get(uri) : "");

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
