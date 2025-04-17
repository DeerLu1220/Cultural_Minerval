from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
import os
import random
import sys
from urllib.error import HTTPError

# Set data directory
DATA_DIR = "data/raw"

def query_wikidata(sparql_query, endpoint_url="https://query.wikidata.org/sparql", max_retries=5):
    """
    Execute a SPARQL query against Wikidata with retry logic
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "WDCategoryDataExtractor/1.0")
    
    retry = 0
    while retry < max_retries:
        try:
            results = sparql.query().convert()
            return results["results"]["bindings"]
        except Exception as e:
            print(f"Query failed (attempt {retry+1}/{max_retries}): {e}")
            wait_time = 10 + (retry * 15) + (random.random() * 10)
            print(f"Waiting {wait_time:.2f} seconds before retry...")
            time.sleep(wait_time)
            retry += 1
    
    print("Max retries exceeded. Skipping this query.")
    return None

def process_category_minimal(category_id, category_name, chunk_size=20, max_items=200):
    """
    Process a category with absolute minimal query to avoid timeouts
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    # Define output file path
    output_file = os.path.join(DATA_DIR, f"{category_name.replace(' ', '_')}_items.csv")
    
    # Check if this category has already been processed
    # if os.path.exists(output_file):
    #     print(f"Category {category_name} already processed, skipping...")
    #     return output_file
    
    all_results = []
    offset = 0
    total_items = 0
    
    while total_items < max_items:
        # Use an extremely simplified query
        # Just find entities with English and Italian labels, linked to the category
        query = f"""
        SELECT DISTINCT ?item ?enLabel ?enDesc ?itLabel ?itDesc WHERE {{
          ?item wdt:P31/wdt:P279* wd:{category_id} .
          
          # English label and description
          ?item rdfs:label ?enLabel .
          FILTER(LANG(?enLabel) = "en")
          ?item schema:description ?enDesc .
          FILTER(LANG(?enDesc) = "en")
          
          # Italian label and description
          ?item rdfs:label ?itLabel .
          FILTER(LANG(?itLabel) = "it")
          ?item schema:description ?itDesc .
          FILTER(LANG(?itDesc) = "it")
        }}
        ORDER BY ?item
        LIMIT {chunk_size}
        OFFSET {offset}
        """
        
        print(f"Querying {category_name}: chunk {offset//chunk_size + 1} (offset: {offset})")
        print(f"Query length: {len(query)} characters")
        
        results = query_wikidata(query)
        
        if not results or len(results) == 0:
            print(f"No more results for {category_name} or query failed")
            break
            
        # Process results
        for result in results:
            all_results.append({
                'item': result['item']['value'].split('/')[-1],
                'itemLabel': result['enLabel']['value'],
                'itemDescription': result['enDesc']['value'],
                'itemItalianLabel': result['itLabel']['value'],
                'itemItalianDescription': result['itDesc']['value'],
                'category': category_name,
                'type': 'entity'  # Simplify to just entities for now
            })
        
        # Update counters and prepare for next chunk
        chunk_items = len(results)
        total_items += chunk_items
        offset += chunk_size
        
        print(f"Retrieved {chunk_items} items in this chunk, {total_items} total for {category_name}")
        
        # If we got fewer items than requested, there are no more results
        if chunk_items < chunk_size:
            break
            
        # Be respectful to the server with longer delays
        time.sleep(10 + random.random() * 10)
    
    # Try a second query for concepts/classes if we have room for more items
    if total_items < max_items:
        concept_query = f"""
        SELECT DISTINCT ?item ?enLabel ?enDesc ?itLabel ?itDesc WHERE {{
          ?item wdt:P279 wd:{category_id} .
          
          # English label and description
          ?item rdfs:label ?enLabel .
          FILTER(LANG(?enLabel) = "en")
          ?item schema:description ?enDesc .
          FILTER(LANG(?enDesc) = "en")
          
          # Italian label and description
          ?item rdfs:label ?itLabel .
          FILTER(LANG(?itLabel) = "it")
          ?item schema:description ?itDesc .
          FILTER(LANG(?itDesc) = "it")
        }}
        LIMIT {max_items - total_items}
        """
        
        print(f"Querying concepts for {category_name}")
        concept_results = query_wikidata(concept_query)
        
        if concept_results and len(concept_results) > 0:
            for result in concept_results:
                all_results.append({
                    'item': result['item']['value'].split('/')[-1],
                    'itemLabel': result['enLabel']['value'],
                    'itemDescription': result['enDesc']['value'],
                    'itemItalianLabel': result['itLabel']['value'],
                    'itemItalianDescription': result['itDesc']['value'],
                    'category': category_name,
                    'type': 'concept'
                })
            
            print(f"Retrieved {len(concept_results)} concepts for {category_name}")
    
    if all_results:
        # Save to CSV
        df = pd.DataFrame(all_results)
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df)} items for {category_name} to {output_file}")
        return output_file
    else:
        print(f"No results were successfully retrieved for {category_name}")
        return None

def merge_results(category_files, output_file=None):
    """
    Merge all individual category files into one master file with duplicates removed
    """
    if output_file is None:
        output_file = os.path.join(DATA_DIR, "all_categories_merged.csv")
        
    # Remove None values from the list
    category_files = [f for f in category_files if f is not None and os.path.exists(f)]
    
    if not category_files:
        print("No category files to merge")
        return
    
    print(f"Merging {len(category_files)} category files...")
    
    # Read and merge all dataframes
    dfs = []
    for file in category_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    if not dfs:
        print("No valid dataframes to merge")
        return
        
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Remove duplicates
    original_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['item'])
    dedup_count = len(merged_df)
    
    # Save the merged file
    merged_df.to_csv(output_file, index=False)
    print(f"Merged file saved to {output_file} with {dedup_count} unique items (removed {original_count - dedup_count} duplicates)")
    
    return output_file

def main():
    # Define categories with their Wikidata IDs
    categories = [
        ("Q8242", "Literature"),
        ("Q5891", "Philosophy"),
        ("Q9174", "Religion"),
        ("Q12684", "Fashion"),
        ("Q2095", "Food"),
        ("Q1004", "Comics"),
        ("Q1107", "Anime"),
        ("Q4502142", "Visual_Arts"),
        ("Q340169", "Media"),
        ("Q12072", "Performing_Arts"),
        ("Q420", "Biology"),
        ("Q11424", "Films"),
        ("Q638", "Music"),
        ("Q349", "Sports"),
        ("Q1071", "Geography"),
        ("Q12271", "Architecture"),
        ("Q7163", "Politics"),
        ("Q309", "History"),
        ("Q7590", "Transportation"),
        ("Q30057499", "Gestures_and_Habits"),
        ("Q571", "Books")
    ]
    
    # If command-line arguments are provided, only process those categories
    if len(sys.argv) > 1:
        selected_cats = sys.argv[1:]
        categories = [cat for cat in categories if cat[1] in selected_cats]
        print(f"Processing only selected categories: {', '.join([cat[1] for cat in categories])}")
    
    output_files = []
    
    # Process each category
    for cat_id, cat_name in categories:
        try:
            print(f"\n{'='*50}\nProcessing category: {cat_name}\n{'='*50}\n")
            output_file = process_category_minimal(cat_id, cat_name, chunk_size=20, max_items=5000)
            output_files.append(output_file)
            
            # Significant wait between categories
            wait_time = 20 + random.random() * 20
            print(f"Waiting {wait_time:.2f} seconds before next category...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Error processing category {cat_name}: {e}")
    
    # Merge all results into one file
    merge_results(output_files)

if __name__ == "__main__":
    main()