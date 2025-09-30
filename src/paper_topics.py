import pandas as pd
import json
import os
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import re

DATA_FOLDER = "/Users/denggeyileao/Desktop/metadata_include"
OUTPUT_FILE = "paper_topics.csv"

papers_data = []

for filename in os.listdir(DATA_FOLDER):
    if filename.endswith('.json'):
        with open(os.path.join(DATA_FOLDER, filename), 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            abstract_index = data.get('abstract_inverted_index', {})
            if abstract_index:
                words = []
                for word, positions in abstract_index.items():
                    for pos in positions:
                        words.append((pos, word))
                abstract = ' '.join([word for pos, word in sorted(words)])
            else:
                abstract = ""
            
            papers_data.append({
                'id': data['id'],
                'title': data.get('title', ''),
                'abstract': abstract
            })

df = pd.DataFrame(papers_data)
print(f"Found {len(df)} papers with abstracts")

def clean_text(text):
    if pd.isna(text) or text == "":
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

df['cleaned_abstract'] = df['abstract'].apply(clean_text)

vectorizer = CountVectorizer(
    max_df=0.95,
    min_df=2,
    stop_words='english',
    max_features=1000
)

doc_term_matrix = vectorizer.fit_transform(df['cleaned_abstract'])

lda = LatentDirichletAllocation(
    n_components=5,
    random_state=42,
    max_iter=10
)

lda.fit(doc_term_matrix)

feature_names = vectorizer.get_feature_names_out()

print("Topic keywords:")
for topic_idx, topic in enumerate(lda.components_):
    top_words = [feature_names[i] for i in topic.argsort()[-10:][::-1]]
    print(f"Topic {topic_idx}: {', '.join(top_words)}")

topic_names = {
    0: "Renewable_Energy_General",  # energy, production, land, bioenergy, biodiversity, renewable
    1: "Land_Use_Bioenergy",        # land, biogas, scenarios, crop, maize, crops  
    2: "Solar_Biomass_Production",  # biomass, solar, production, soil, energy
    3: "Wind_Energy_Impacts",       # wind, turbines, marine, offshore, impacts
    4: "Hydropower_Aquatic"         # hydropower, fish, water, river, plants
}

topic_distribution = lda.transform(doc_term_matrix)
df['dominant_topic'] = topic_distribution.argmax(axis=1)
df['topic_name'] = df['dominant_topic'].map(topic_names)
df['topic_confidence'] = topic_distribution.max(axis=1)

df[['id', 'title', 'dominant_topic', 'topic_name', 'topic_confidence']].to_csv(OUTPUT_FILE, index=False)
print(f"Saved to {OUTPUT_FILE}")

print("\nTopic distribution:")
print(df['topic_name'].value_counts())