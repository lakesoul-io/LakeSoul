# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import sys
import os
import torch
import datasets
import lakesoul.huggingface
import pandas as pd

from io import BytesIO
from tqdm import tqdm
from PIL import Image
from sentence_transformers import SentenceTransformer, util


def batchify(dataset, batch_size):
    batch = []
    for i, item in enumerate(dataset):
        record = {
            "ids": i,
            "image_bytes": item["image_bytes"],
            "image_path": item["image_path"]
        }
        batch.append(record)

        if len(batch) == batch_size:
            yield batch
            batch = []

    # Handle the remaining records that don't fill up a full batch
    if len(batch) > 0:
        yield batch

if __name__ == '__main__':
    data_source = sys.argv[1]
    device = 'cuda'
    base_url = 'http://im-api.dmetasoul.com/food101'
    img_model = SentenceTransformer('clip-ViT-B-32')
    max_images = 10000
    max_images = -1
    
    img_id = 0
    dataset = datasets.IterableDataset.from_lakesoul(data_source)
    for batch in batchify(dataset, batch_size=4):
        ids = list(range(img_id, img_id+len(batch)))
        urls = [f"{base_url}/{row['image_path']}" for row in batch]
        images = [Image.open(BytesIO(row['image_bytes'])).convert('RGB') for row in batch]
        try:
            embs = img_model.encode(images, device=device, 
                convert_to_numpy=True, show_progress_bar=False, normalize_embeddings=True)
            embs = embs.tolist()
        except Exception as e:
            continue
    
        img_id += len(batch)
        for _id, _url, _emb in zip(ids, urls, embs):
            print(_id, _url, ' '.join(map(str, _emb)), sep='\t')
    
        if max_images > 0 and img_id > max_images:
            break
