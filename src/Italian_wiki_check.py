import csv
import os
import requests

DATA_DIR = "data"

def check_wikilinks(file_path):
    '''
    Check the wikilinks in the given CSV file and extract Italian item labels.
    the csv file head should be like this:
    row_id,item,itemLabel,itemDescription,type,itemHypernymLabel,classification,countries of claim,explanation
    item is the wikilink
    '''
    italian_item_labels = []

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip the header row
        for row in reader:
            if len(row) < 2:
                continue
            wikilink = row[1]  # Assuming 'item' is in the second column
            italian_entry = False
            english_entry = False
            item_label = None

            try:
                response = requests.get(wikilink)
                if response.status_code == 200:
                    content = response.text
                    italian_entry = 'lang="it"' in content
                    english_entry = 'lang="en"' in content

                    if italian_entry:
                        # Extract Italian item label (simplified example)
                        start = content.find('<title>') + len('<title>')
                        end = content.find('</title>')
                        item_label = content[start:end].split(' - ')[0]

                if italian_entry and english_entry and item_label:
                    italian_item_labels.append(item_label)

            except Exception as e:
                print(f"Error processing {wikilink}: {e}")

    return italian_item_labels

if __name__ == "__main__":
    file_path = os.path.join(DATA_DIR,
                             'from_hw',
                             'Italy.csv')
    italian_labels = check_wikilinks(file_path)
    output_file = os.path.join(DATA_DIR,
                             'from_hw',
                             'Italy.csv')

    with open(output_file, mode='w', encoding='utf-8') as outfile:
        for label in italian_labels:
            outfile.write(label + '\n')

    print(f"Saved {len(italian_labels)} Italian item labels to {output_file}")