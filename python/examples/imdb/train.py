# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import evaluate
import numpy as np
import torch
import datasets
import lakesoul.huggingface

from datasets import load_dataset, IterableDataset
from transformers import AutoTokenizer, DataCollatorWithPadding, AutoModelForSequenceClassification, TrainingArguments, Trainer

dataset_table = "imdb"

def read_text_table(datasource, split):
    dataset = datasets.IterableDataset.from_lakesoul(datasource, partitions={"split": split})
    for i, sample in enumerate(dataset):
        yield {"text": sample["text"], "label":sample["label"]}

# Preprocess function for tokenization
def preprocess_function(examples):
    return tokenizer(examples["text"], truncation=True)

# Compute evaluation metrics
def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    predictions = np.argmax(predictions, axis=1)
    return accuracy.compute(predictions=predictions, references=labels)

# Define ID-to-label and label-to-ID mappings
id2label = {0: "NEGATIVE", 1: "POSITIVE"}
label2id = {"NEGATIVE": 0, "POSITIVE": 1}

# Load tokenizer
tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")

# Tokenize the IMDb dataset
train_tokenized_imdb = IterableDataset\
    .from_generator(read_text_table, gen_kwargs={"datasource":dataset_table, "split":"train"})\
    .map(preprocess_function, batched=True)\
    .shuffle(seed=1337, buffer_size=25000)
test_tokenized_imdb = IterableDataset\
    .from_generator(read_text_table, gen_kwargs={"datasource":dataset_table, "split":"test"})\
    .map(preprocess_function, batched=True)

# Initialize data collator for padding
data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

# Load accuracy evaluation metric
accuracy = evaluate.load("accuracy")

# Load pre-trained model for sequence classification
model = AutoModelForSequenceClassification.from_pretrained(
    "distilbert-base-uncased", num_labels=2, id2label=id2label, label2id=label2id
)

# Define the training arguments
training_args = TrainingArguments(
    output_dir="imdb/my_awesome_model",
    learning_rate=2e-5,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    max_steps=3125,
    weight_decay=0.01,
    evaluation_strategy="steps",
    eval_steps=1560,
    save_strategy="steps",
    save_steps=1560,
    load_best_model_at_end=True,
    push_to_hub=False
)

# Initialize the trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_tokenized_imdb,
    eval_dataset=test_tokenized_imdb,
    tokenizer=tokenizer,
    data_collator=data_collator,
    compute_metrics=compute_metrics,
)

# Train the model
trainer.train()
