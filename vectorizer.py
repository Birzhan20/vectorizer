from sentence_transformers import SentenceTransformer

local_model_path = "./model"
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
model.save(local_model_path)
vectorizer = SentenceTransformer(local_model_path)

def generate_embedding(query):
    return vectorizer.encode(query).tolist()
