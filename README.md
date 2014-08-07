## Building an Elasticsearch Suggestion Index

This is a script that will generate a separate suggestion index based on a source index, by abusing Elasticsearch just a little.  This index can then be queried to create Amazon-style suggestions (e.g. "pen" could yield "Pencil in Paper Goods").  This could also be a starting off point to build a much more complex suggestion architecture.

## How to use

The top of the script contains some static variables.  Everything starting with "SOURCE_" will describe the Elasticsearch instance and index to pull the data from.  

The script will proceed to create a ``temp`` index on that same instance and then a ``suggestions`` index, which is what you will use to query for suggestions.  *Note that it will load only **some** (1000) of the documents into the suggestion index*

To launch the script, execute the Gradle task in the "transformation" directory:

``gradle buildSuggest``

## Sample query and response

Here is a sample query that will calculate a simple custom score based on the amount of times a suggestion occurs in the source index and how often a user utilizes the suggestion.  

Query:

```
POST suggestions/_search
{
  "fields": ["suggestion", "department"],
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "default_field": "suggestion",
            "query": "guit",
            "minimum_should_match": "100%"
          }
        }
      ]
    }
  },
  "highlight": {
    "fields": {
      "suggestion": {}
    }
  },
  "sort": {
    "_script": {
      "script": "Math.log(doc['indexOccurrences'].value + doc['userUsages'].value) * doc['weight'].value",
      "type": "number",
      "order": "desc"
    }
  },
  "size": 2
}
```

Response:

```
{
   "took": 20,
   "timed_out": false,
   "_shards": {
      "total": 5,
      "successful": 5,
      "failed": 0
   },
   "hits": {
      "total": 1256,
      "max_score": null,
      "hits": [
         {
            "_index": "suggestions",
            "_type": "suggestion",
            "_id": "acoustic guitar",
            "_score": null,
            "fields": {
               "department": [
                  "acoustic guitars"
               ],
               "suggestion": [
                  "acoustic guitar"
               ]
            },
            "highlight": {
               "suggestion": [
                  "acoustic <em>guitar</em>"
               ]
            },
            "sort": [
               3.6375861597263857
            ]
         },
         {
            "_index": "suggestions",
            "_type": "suggestion",
            "_id": "electric guitar",
            "_score": null,
            "fields": {
               "department": [
                  "guitars electric acoustic accessories"
               ],
               "suggestion": [
                  "electric guitar"
               ]
            },
            "highlight": {
               "suggestion": [
                  "electric <em>guitar</em>"
               ]
            },
            "sort": [
               3.5553480614894135
            ]
         }
      ]
   }
}

```