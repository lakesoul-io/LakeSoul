# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

text = "This was a masterpiece. Not completely faithful to the books, but enthralling from beginning to end. Might be my favorite of the three."
model_path = "imdb/my_awesome_model/checkpoint-3120"

# Load the tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_path)
inputs = tokenizer(text, return_tensors="pt")

# Load the model
model = AutoModelForSequenceClassification.from_pretrained(model_path)

# Inference using the model
with torch.no_grad():
    logits = model(**inputs).logits

# Get the predicted class ID
predicted_class_id = logits.argmax().item()

# Print the predicted class label
print(model.config.id2label[predicted_class_id])
