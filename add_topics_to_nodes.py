import pandas as pd
import json
import os
import requests
import time
from collections import defaultdict

# Read data
author_nodes = pd.read_csv("author_nodes.csv")
institution_nodes = pd.read_csv("institution_nodes.csv")
paper_topics = pd.read_csv("paper_topics.csv")

# Create mappings
paper_to_topic = dict(zip(paper_topics['id'], paper_topics['topic_name']))
author_name_to_id = dict(zip(author_nodes['Author'], author_nodes['ID']))
institution_name_to_id = dict(zip(institution_nodes['Institution'], institution_nodes['ID']))

# Count topics
author_topic_counts = defaultdict(lambda: defaultdict(int))
institution_topic_counts = defaultdict(lambda: defaultdict(int))

DATA_FOLDER = "/Users/denggeyileao/Desktop/metadata_include"
files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.json')]

print(f"Processing {len(files)} papers to assign topics...")

for i, filename in enumerate(files):
    if i % 100 == 0:
        print(f"Progress: {i}/{len(files)}")
    
    with open(os.path.join(DATA_FOLDER, filename), 'r') as f:
        data = json.load(f)
    
    paper_id = data['id']
    paper_topic = paper_to_topic.get(paper_id, "Unknown")
    
    for ref in data.get('referenced_works', []):
        ref_id = ref.split('/')[-1]
        
        try:
            response = requests.get(f"https://api.openalex.org/works/{ref_id}", timeout=10)
            if response.status_code == 200:
                ref_data = response.json()
                
                authorships = ref_data.get('authorships', [])
                if authorships:
                    first_author = authorships[0]
                    author_name = first_author.get('author', {}).get('display_name', '')
                    
                    # Count for author
                    if author_name and author_name in author_name_to_id:
                        author_id = author_name_to_id[author_name]
                        author_topic_counts[author_id][paper_topic] += 1
                    
                    # Count for institution
                    institutions = first_author.get('institutions', [])
                    if institutions:
                        inst_name = institutions[0].get('display_name', '')
                        if inst_name and inst_name in institution_name_to_id:
                            inst_id = institution_name_to_id[inst_name]
                            institution_topic_counts[inst_id][paper_topic] += 1
            
            time.sleep(0.3)
        except:
            continue

# Add topics to authors
author_topics = []
for author_id in author_nodes['ID']:
    if author_topic_counts[author_id]:
        main_topic = max(author_topic_counts[author_id].items(), key=lambda x: x[1])[0]
    else:
        main_topic = "Unknown"
    author_topics.append(main_topic)

author_nodes['Topic'] = author_topics

# Add topics to institutions
institution_topics = []
for inst_id in institution_nodes['ID']:
    if institution_topic_counts[inst_id]:
        main_topic = max(institution_topic_counts[inst_id].items(), key=lambda x: x[1])[0]
    else:
        main_topic = "Unknown"
    institution_topics.append(main_topic)

institution_nodes['Topic'] = institution_topics

# Save results
author_nodes.to_csv("author_nodes_with_topics.csv", index=False)
institution_nodes.to_csv("institution_nodes_with_topics.csv", index=False)

print("Files created:")
print("- author_nodes_with_topics.csv")
print("- institution_nodes_with_topics.csv")
print("\nTopic distribution in nodes:")
print("Authors:", author_nodes['Topic'].value_counts().to_dict())
print("Institutions:", institution_nodes['Topic'].value_counts().to_dict())