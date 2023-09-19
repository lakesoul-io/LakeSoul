# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import sys

import numpy as np
from sentence_transformers import SentenceTransformer, util


def load_model():
    return SentenceTransformer('clip-ViT-B-32-multilingual-v1')

def get_image_embs(emb_file):
    embs = []
    imgs = {}
    with open(emb_file, 'r', encoding='utf8') as f:
        for line in f:
            line = line.strip('\r\n')
            if not line:
                continue
            row_id, img_url, img_emb = line.split('\t')[:3]
            img_emb = list(map(float, img_emb.split(' ')))
            img_id = len(embs)
            imgs[img_id] = {'url':img_url}
            embs.append(img_emb)
    return imgs, np.array(embs, dtype=np.float32)

def get_query_emb(query, model):
    query_emb = model.encode([query], convert_to_numpy=True, show_progress_bar=False)
    return query_emb

def search(query_emb, img_embs, imgs, k=3):
    hits = util.semantic_search(query_emb, img_embs, top_k=k)[0]
    results = []
    for hit in hits:
        img_id = hit['corpus_id']
        score = hit['score']
        if img_id not in imgs:
            continue
        img_url = imgs[img_id]['url']
        results.append({'score': score, 'image': img_url})
    return results


if __name__ == '__main__':
    emb_file = sys.argv[1]
    top_k = int(sys.argv[2])

    model = load_model()
    print("Model loaded successfully")
    imgs, img_embs = get_image_embs(emb_file)
    print("Vector database loaded successfully", img_embs.shape)

    while True:
        query = input("Please enter a keyword to search for images:").strip()
        query_emb = get_query_emb(query, model)
        results = search(query_emb, img_embs, imgs, k=top_k)
        print(results)
        print("*"*80)
