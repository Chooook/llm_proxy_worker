import json
import os
import pickle
from typing import List, Optional, Tuple

from transformers import AutoTokenizer, Pipeline
from langchain_community.retrievers import BM25Retriever
from langchain.document_loaders.text import TextLoader
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from langchain.vectorstores.utils import DistanceStrategy
from langchain.document_loaders import PyPDFDirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Путь к директории с PDF и Markdown файлам
base_1_0_directory = 'knowlege_base_pm/knowledge_base_v_1/BZ'
base_2_0_directory = 'knowlege_base_pm/KB'

# Загрузка Markdown файлов из директории и всех вложенных папок
def load_markdown_files(directory):

    markdown_documents = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                markdown_loader = TextLoader(file_path)
                markdown_documents.append(markdown_loader.load())

    return markdown_documents

# Загрузчик PDF файлов из директории
pdf_loader = PyPDFDirectoryLoader(base_1_0_directory)

# Загрузка данных
pdf_documents = pdf_loader.load()
markdown_documents_1 = load_markdown_files(base_1_0_directory)
markdown_documents_2 = load_markdown_files(base_2_0_directory)

# Объединение загруженных документов
bz1_documents = pdf_documents + [doc for sublist in markdown_documents_1 for doc in sublist]

EMBEDDING_MODEL_NAME = 'models/intfloat'

def split_documents(
    chunk_size: int,
    knowledge_base: bz1_documents,
    tokenizer_name: Optional[str] = EMBEDDING_MODEL_NAME,
) -> bz1_documents:
    """
    Split documents into chunks of maximum size `chunk_size` tokens and
    return a list of documents.
    """
    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        AutoTokenizer.from_pretrained(tokenizer_name),
        chunk_size=chunk_size,
        chunk_overlap=int(chunk_size / 10),
        add_start_index=True,
    )

    docs_processed = []
    for doc in bz1_documents:
        docs_processed += text_splitter.split_documents([doc])

    # удаляем дубли
    unique_texts = {}
    docs_processed_unique = []
    for doc in docs_processed:
        if doc.page_content not in unique_texts:
            unique_texts[doc.page_content] = True
            docs_processed_unique.append(doc)

    return docs_processed_unique

# разобьем документы на chunks в соответствии с ограничением эмбеддинг-модели
docs_processed_simple = split_documents(
    512,
    bz1_documents,
    tokenizer_name=EMBEDDING_MODEL_NAME,
)

docs_processed_simple = docs_processed_simple + [doc for sublist in markdown_documents_2 for doc in sublist]

embedding_model = HuggingFaceEmbeddings(
    model_name=EMBEDDING_MODEL_NAME
)

bm25_retriever = BM25Retriever.from_documents(
    docs_processed_simple
)
bm25_retriever.k = 10

# Создание готового bm25 ретривера
with open('bm25_retriever.pkl', 'wb') as f:
    pickle.dump(bm25_retriever, f)

KNOWLEDGE_VECTOR_DATABASE = FAISS.from_documents(
    docs_processed_simple, embedding_model, distance_strategy=DistanceStrategy.COSINE
)

KNOWLEDGE_VECTOR_DATABASE.save_local('VECTOR_DB')
