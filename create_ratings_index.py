from opensearchpy import OpenSearch, helpers
import json

def create_ratings_index():
    # OpenSearch connection configuration
    host = 'search-rithin-test-a2jcswobchosmgqwnhgokwqz6i.us-east-2.es.amazonaws.com'
    port = 443  # Using HTTPS port
    auth = ('admin', 'MyPassword123!')
    
    # Create OpenSearch client
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False
    )
    
    # Index name
    index_name = 'product_ratings'
    
    # Index settings and mappings
    index_settings = {
        "settings": {
            "index": {
                "number_of_shards": 3,
                "number_of_replicas": 2
            }
        },
        "mappings": {
            "properties": {
                "product_id": {
                    "type": "keyword"
                },
                "rating": {
                    "type": "integer"
                }
            }
        }
    }
    
    # Create the index
    if not client.indices.exists(index=index_name):
        response = client.indices.create(
            index=index_name,
            body=index_settings
        )
        print(f"Created index: {index_name}")
    else:
        print(f"Index {index_name} already exists")
    
    # Read the ratings data
    with open('ratings.json', 'r') as f:
        ratings = json.load(f)
    
    # Prepare bulk indexing data
    bulk_data = []
    for rating in ratings:
        bulk_data.append({
            "_index": index_name,
            "_id": rating['product_id'],
            "_source": rating
        })
    
    # Bulk index the data
    success, failed = helpers.bulk(client, bulk_data)
    print(f"Successfully indexed {success} documents")
    if failed:
        print(f"Failed to index {len(failed)} documents")

if __name__ == "__main__":
    create_ratings_index() 