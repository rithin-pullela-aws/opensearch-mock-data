from opensearchpy import OpenSearch, helpers
import json

def create_products_index():
    # OpenSearch connection configuration
    host = 'localhost'
    port = 9200
    
    # Create OpenSearch client
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        use_ssl=False,  # No SSL for localhost
        verify_certs=False,
        ssl_show_warn=False
    )
    
    # Index name
    index_name = 'products'
    
    # Delete index if exists
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")
    
    # Index settings and mappings
    index_settings = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0  # Single node setup typically uses 0 replicas
            },
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "snowball"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "product_id": {
                    "type": "keyword"
                },
                "text": {
                    "type": "text",
                    "analyzer": "custom_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                }
            }
        }
    }
    
    # Create the index
    response = client.indices.create(
        index=index_name,
        body=index_settings
    )
    print(f"Created index: {index_name}")
    
    # Read the products data
    with open('processed_data.json', 'r') as f:
        products = json.load(f)
    
    # Prepare bulk indexing data
    bulk_data = []
    for product in products:
        bulk_data.append({
            "_index": index_name,
            "_id": product['product_id'],
            "_source": {
                "product_id": product['product_id'],
                "text": product['text']
            }
        })
    
    # Bulk index the data
    success, failed = helpers.bulk(client, bulk_data)
    print(f"Successfully indexed {success} documents")
    if failed:
        print(f"Failed to index {len(failed)} documents")

if __name__ == "__main__":
    create_products_index() 