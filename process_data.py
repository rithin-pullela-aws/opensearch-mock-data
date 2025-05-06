import json

def process_data(input_file, output_file):
    # Read the input JSON file
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Extract hits from the data
    hits = data['hits']['hits']
    
    # Create a new list with only id and text
    products = []
    for hit in hits:
        processed_item = {
            'product_id': hit['_source']['id'],
            'text': hit['_source']['text']
        }
        products.append(processed_item)
    
    # Write the processed data to a new JSON file
    with open(output_file, 'w') as f:
        json.dump(products, f, indent=2)
    
    print(f"Processed {len(products)} items successfully!")
    print(f"Output written to {output_file}")

if __name__ == "__main__":
    input_file = "data.json"
    output_file = "products.json"
    process_data(input_file, output_file) 