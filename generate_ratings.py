import json
import random

def generate_ratings(input_file, output_file):
    # Read the processed data file to get all product IDs
    with open(input_file, 'r') as f:
        products = json.load(f)
    
    # Generate random ratings for each product
    ratings_data = []
    for product in products:
        rating_item = {
            'product_id': product['id'],
            'rating': random.randint(4, 10)  # Random rating between 4 and 10
        }
        ratings_data.append(rating_item)
    
    # Write the ratings data to a new JSON file
    with open(output_file, 'w') as f:
        json.dump(ratings_data, f, indent=2)
    
    print(f"Generated ratings for {len(ratings_data)} products!")
    print(f"Ratings written to {output_file}")

if __name__ == "__main__":
    input_file = "processed_data.json"
    output_file = "ratings.json"
    generate_ratings(input_file, output_file) 