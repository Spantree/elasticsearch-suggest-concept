package net.spantree.elasticsearch.experiments

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms

class BuildSuggestIndex {
    static TEMP_INDEX_NAME = "temp"
    static TEMP_TYPE = "suggest"
    static TEMP_SUGGEST_FIELD = "suggest"

    static SOURCE_INDEX_NAME = "products"
    static SOURCE_SUGGESTION_FIELD = "tags"
    static SOURCE_DEPARTMENT_FIELD = "product_type.name"

    static SUGGEST_INDEX_NAME = "suggestions"
    static SUGGEST_TYPE = "suggestion"
    static SUGGEST_FACET_NAME = "suggestions"
    static SUGGEST_DEPARTMENT_FACET_NAME = "department"

    static SUGGEST_FIELD = "suggest_string"
    static SUGGEST_DEPARTMENT_FIELD = "department"

    static void main(String[] args) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("esdemo.local", 9300))

        // Delete the existing temporary index, if it exists
        try {
            client.admin().indices().delete(new DeleteIndexRequest(TEMP_INDEX_NAME)).actionGet()
        } catch (IndexMissingException ime) {
            println "Index $TEMP_INDEX_NAME missing"
        }

        // Create a temporary index
        client.admin()
            .indices()
            .prepareCreate(TEMP_INDEX_NAME)
            .setSettings(
                [
                    analysis : [
                        analyzer : [
                            /*
                                When indexing, suggestions will be
                                lowercase and turned into ascii letters.
                                Most importantly, they will be turned into
                                bi-grams and tri-grams which we can later
                                make into suggestions.
                             */
                            suggester_index : [
                                type : 'custom',
                                tokenizer : 'standard',
                                filter : [
                                        'lowercase',
                                        'asciifolding',
                                        'shingle_filter'
                                ]
                            ],
                            suggester_search : [
                                type : 'custom',
                                tokenizer : 'standard',
                                filter : [
                                        'lowercase',
                                        'asciifolding'
                                ]
                            ]
                        ],
                        filter : [
                            shingle_filter : [
                                type : 'shingle',
                                min_shingle_size : 2,
                                max_shingle_size : 3,
                                output_unigrams : false
                            ]
                        ]
                    ]
                ]
            ).addMapping('suggest',
                 [
                     properties: [
                         suggest_string: [
                             type: 'string',
                             index_analyzer: 'suggester_index',
                             search_analyzer: 'suggester_search'
                         ],
                         department: [
                             type: 'string'
                         ]
                     ]
                 ]
            ).execute().actionGet()


        // Query the source index for 1000 documents
        // (For grabbing all documents, we should page by using scrolling)
        SearchResponse sr = client.prepareSearch()
            .setQuery( QueryBuilders.matchAllQuery() )
            .setIndices(SOURCE_INDEX_NAME)
            .setSize(1000)
            .execute().actionGet();


        // Throw 1000 suggestion/department combinations into the temporary index
        sr.hits.hits.each{  SearchHit hit ->
            client.prepareIndex(TEMP_INDEX_NAME, TEMP_TYPE).setSource(
                    [
                        suggest_string: hit.source[SOURCE_SUGGESTION_FIELD].toString(),
                        department: hit.source[SOURCE_DEPARTMENT_FIELD].toString()
                    ]
            ).execute()
        }

        // This will make sure the documents are present and ready for querying in the temporary index
        client.admin().indices().prepareRefresh(TEMP_INDEX_NAME).execute().get()


        // Delete the suggestion index if it exists
        try {
            client.admin().indices().delete(new DeleteIndexRequest(SUGGEST_INDEX_NAME)).actionGet()
        } catch (IndexMissingException ime) {
            println "Index $SUGGEST_INDEX_NAME missing"
        }

        // Create the suggestion index
        client.admin().indices().prepareCreate( SUGGEST_INDEX_NAME )
                .setSettings(
                    [
                        analysis : [
                            analyzer : [
                                suggestion_index : [
                                    type : 'custom',
                                    tokenizer : 'standard',
                                    filter : [
                                            'suggest_delimiter',  // Mainly to deal with cases like "ElasticSearch" by breaking it up into "Elastic Search"
                                            'lowercase',
                                            'asciifolding',
                                            'stop',
                                            'edge_left_tokenizer' // Allows us to type in "gui" and get "guitar" back
                                    ]
                                ],
                                suggestion_search : [
                                    type : 'custom',
                                    tokenizer : 'standard',
                                    filter : [
                                            'lowercase',
                                            'asciifolding'
                                    ]
                                ]
                            ],
                            filter : [
                                suggest_delimiter : [
                                    type : 'word_delimiter',
                                    generate_word_parts : true,
                                    generate_number_parts : true,
                                    catenate_words : true,
                                    catenate_numbers : true,
                                    catenate_all : true,
                                    split_on_case_change : true,
                                    preserve_original : true,
                                    split_on_numerics : true,
                                    stem_english_possessive : true
                                ],
                                edge_left_tokenizer : [
                                    type : 'edgeNGram',
                                    side : 'front',
                                    min_gram : 1,
                                    max_gram : 20
                                ]
                            ]
                        ]
                    ]
                )
                .addMapping( SUGGEST_TYPE,
                      [
                          properties: [
                                suggestion: [
                                    type: "string",
                                    index_analyzer: "suggestion_index",
                                    search_analyzer: "suggestion_search"
                                ],
                                department: [
                                    type: "string"
                                ],
                                indexOccurrences: [   // Field for keeping track of how many times it originally occurred
                                    type: "long"
                                ],
                                userUsages: [  // You could keep track of how often a suggestion is used here
                                    type: "long"
                                ],
                                weight: [ // Arbitrary weight to assign to a suggestion
                                    type: "float"
                                ]
                          ]
                      ]
                ).execute().actionGet()

        // Do an aggregation query on the temporary index to get back all the suggestions and
        // use a sub-aggregation to also extract the corresponding department
        AggregationBuilder fb = AggregationBuilders.terms(SUGGEST_FACET_NAME)
                .field(SUGGEST_FIELD)
                .size(Integer.MAX_VALUE).subAggregation(
                AggregationBuilders.terms(SUGGEST_DEPARTMENT_FACET_NAME)
                        .field(SUGGEST_DEPARTMENT_FIELD).size(5)
        )

        SearchRequestBuilder srb = client.prepareSearch()
                .setQuery( QueryBuilders.matchAllQuery() )
                .setIndices(TEMP_INDEX_NAME)
                .addAggregation( fb )


        sr = srb.execute().actionGet();


        StringTerms f = sr.getAggregations().getAsMap().get(SUGGEST_FACET_NAME);

        // Index the result of the aggregation into the suggestion index
        f.buckets.each { bucket ->
            def departments = bucket.getAggregations().getAsMap().get(SUGGEST_DEPARTMENT_FACET_NAME).buckets.collect{it.key}.join(" ")

            client.prepareUpdate(SUGGEST_INDEX_NAME, SUGGEST_TYPE, bucket.key).setDoc(
                    [
                            suggestion: bucket.key,
                            indexOccurrences: bucket.docCount,
                            department: departments,
                            userUsages: 0,
                            weight: 1.0
                    ]
            ).setDocAsUpsert(true).execute().actionGet()
        }

        client.close();
    }
}
