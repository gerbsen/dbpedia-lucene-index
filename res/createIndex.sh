#!/bin/bash

# before you run this script make sure the you have faceted browser vad activated
# Either use the web interface (in conductor: System Admin -> Packages, select fct)
# or isql command line: vad_install ('fct_dav.vad', 0);
# Also you need to have wget and bunzip2 installed and it would be very
# helpful if virtuoso is running and java is on PATH

virt_isql="/usr/local/bin/isql"
virt_port=1111
virt_userName="dba"
virt_passWord="dba"
virt_graphName="http://de.dbpedia.org"
virt_sparql="http://localhost:8890/sparql"

fctVad="/usr/local/Cellar/virtuoso/6.1.5/share/virtuoso/vad/fct_dav.vad"

createGraph=true
importData=true
rankData=true
language=de

indexDir="/Users/gerb/Development/workspaces/experimental/solr_4/dbpedia_resources_38_score_accent/index/index	"

# splits files and imports them into virtuoso
function importFile() {
    
    echo "importing $1 ..."
    load_source="$1"
    load_target=`mktemp -t XXXX`
    cp $load_source $load_target
    load_query="EXEC=TTLP_MT(file_to_string_output('$load_target'), '', '$virt_graphName', 255)"
    $virt_isql "$virt_port" "$virt_userName" "$virt_passWord" "$load_query"
}

# downloads and unzippes the file only if it is not 
# already available
function download() {
    
    # only download the files if they are not already there
    if [ ! -f $1 ]; then
        wget "http://downloads.dbpedia.org/3.8/$language/$1.bz2"
        bunzip2 $1".bz2"
    fi
}
    
download instance_types_$language.ttl
download mappingbased_properties_$language.ttl
download images_$language.ttl
download short_abstracts_$language.ttl
download labels_$language.ttl
download disambiguations_$language.ttl
download redirects_$language.ttl
download interlanguage_links_$language.ttl

if $createGraph ; then
    # create the graph where the files get imported into
    createGraphStmt="EXEC=SPARQL CREATE SILENT GRAPH <$virt_graphName>"
    echo $virt_isql "$virt_port" "$virt_userName" "$virt_passWord" "$createGraphStmt"
    $virt_isql "$virt_port" "$virt_userName" "$virt_passWord" "$createGraphStmt"
fi 

# we need to remove all the redirect and disambiguation uris
if [ ! -f labels_$language_filtered.ttl ]; then
    # -f: filter the bad uri's, no need for them to be in the index
	java -jar -Xmx4G indexCreator.jar -d "`pwd`/" -f true -l $language
fi

if $importData ; then
    
    # load data: filtered labels
    importFile "labels_" $language "_filtered.ttl"

    # load data: images 
    importFile "images_$language.ttl"

    # load data: rdf:type statements    
    importFile "instance_types_$language.ttl"

    # load data: owl:comment statements
    importFile "short_abstracts_$language.ttl"

    # load data: links between resources, needed for page-rank
    importFile "mappingbased_properties_$language.ttl"
fi

if $rankData ; then
    # create the page rank and install the necessary tool 
    $virt_isql $virt_port $virt_userName $virt_passWord "EXEC=vad_install ('$fctVad', 0);"
    $virt_isql $virt_port $virt_userName $virt_passWord "EXEC=s_rank();"
fi           
           
# create the index
# -o: override index [true || false]
# -b: max buffer size for lucene in megabyte [max 2000]
# -d: path to index dir, needs trailing slash
# -s: sparql endpoint, should be local because we make 2,7mio queries :)
# -g: graph name  
java -jar -Xmx10G indexCreator.jar -o true -b 1024 -d "`pwd`/" -i $indexDir -s $virt_sparql -g $virt_graphName -l $language
