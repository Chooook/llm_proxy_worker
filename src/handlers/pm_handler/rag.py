import os
import sys
sys.path.insert(0, os.path.join(os.getcwd(), 'handlers','pm_handler'))

import json
import pickle
import time
import warnings
from typing import List

warnings.filterwarnings('ignore')

from FlagEmbedding import FlagReranker
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from langchain.retrievers import EnsembleRetriever
from langchain.schema import Document
from langchain.prompts import PromptTemplate
from urllib.parse import quote

from .gigachat_connect import get_answer
from .prompts import PROMPT_IN_CHAT_FORMAT
from .utils import add_docs_links


def init():
    pass

EMBEDDING_MODEL_NAME = 'models/intfloat'

embedding_model = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL_NAME)

with open('bm25_retriever.pkl', 'rb') as f:
    bm25_retriever = pickle.load(f)

KNOWLEDGE_VECTOR_DATABASE = FAISS.load_local(
    'VECTOR_DB', embedding_model, allow_dangerous_deserialization=True)

faiss_retriever = KNOWLEDGE_VECTOR_DATABASE.as_retriever(
    search_kwargs={'k': 5}
)

ensemble_retriever = EnsembleRetriever(
    retrievers=[bm25_retriever, faiss_retriever],
    weights=[0.5, 0.5]
)

reranker = FlagReranker('models/bge')

RAG_PROMPT_TEMPLATE = PromptTemplate.from_template(PROMPT_IN_CHAT_FORMAT)


def get_scores(reranker: FlagReranker,
               relevant_docs: List[dict],
               question: str) -> List[List]:
    """Get Documents scores.

    :param reranker: rerank-model in use
    :type reranker: FlagReranker
    :param relevant_docs: documents found
    :type relevant_docs: List[dict]
    :param question: user's request
    :type question: str
    :return: ranked documents (desc)
    :rtype: List[List]

    >>>  # `relevant_docs` hint
    >>> {
    >>>     'text': str,
    >>>     'link': str,
    >>>     'metadata': str
    >>> }
    >>>  # return hint
    >>> [
    >>>     [float, dict]  # read `relevant_docs` hint for dict
    >>> ]
    """
    query_to_doc, score_to_doc = [], []

    for doc in relevant_docs:
        query_to_doc.append([question, doc['text']])

    scores = reranker.compute_score(query_to_doc)

    for i, score in enumerate(scores):
        score_to_doc.append([score, relevant_docs[i]])

    score_to_doc = sorted(score_to_doc, key=lambda tup: tup[0], reverse=True)
    return score_to_doc


def get_top_docs(score_to_doc, num_docs):
    docs = []
    for i in range(num_docs):
        docs.append(score_to_doc[:num_docs][i][1])
    return docs


def parse_originals_json(doc_number: str) -> dict:
    try:
        with open('knowlege_base_pm/originals.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        doc_data = data.get('originals', {}).get(doc_number)
        if doc_data:
            doc_description = doc_data.get('internals_ids', {})

            return {
                'title': doc_description.get('filename'),
            }
        return None
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'Ошибка при чтении originals.json: {e}')
        return None


def build_document_link(doc_metadata: str) -> str:
    # Формируем ссылку
    base_url = 'https://df-bitbucket.ca.sbrf.ru/projects/KNOWLEDGE_BASE_SVA/repos/knowledge_base_pm/browse'

    doc_number = None

    # Извлекаем номер документа
    if '_doc_' in doc_metadata:  # Для формата card_66_doc_31.md
        doc_number = doc_metadata.split('_doc_')[-1].split('.')[0]
    elif doc_metadata.startswith('doc_'):  # Для формата doc_31.md
        doc_number = doc_metadata.split('_')[1].split('.')[0]

    # Получаем информацию о документе
    doc_info = parse_originals_json(doc_number)

    if not doc_number or not doc_info or not doc_info.get('title'):
        doc_relative_path = doc_metadata.replace('knowledge_base_pm/', '')
        return f'{base_url}/{doc_relative_path}'

    encoded_title = quote(doc_info['title'])

    return f'{base_url}/originals/{encoded_title}'


def answer_with_rag(prompt: str,
                    num_retrieved_docs: int = 100,
                    num_docs_final: int = 5) -> str:

    relevant_docs = ensemble_retriever.invoke(input=prompt, k=num_retrieved_docs)

    processed_docs = []
    for doc in relevant_docs:
        doc_content = doc.page_content
        doc_metadata = doc.metadata.get('source', '') if hasattr(doc, 'metadata') else ''
        doc_link = build_document_link(doc_metadata) if doc_metadata else None

        processed_docs.append({
            'text': doc_content,
            'link': doc_link,
            'metadata': doc_metadata
        })

    if reranker:
        score_to_doc = get_scores(reranker, processed_docs, prompt)
        processed_docs = get_top_docs(score_to_doc, num_docs_final)

    processed_docs = processed_docs[:num_docs_final]

    context = '\nExtracted documents:\n'
    for i, doc in enumerate(processed_docs):

        context += f'Document {str(i)}:::\n{doc["text"]}\n'

    final_prompt = RAG_PROMPT_TEMPLATE.format(
        prompt=prompt, context=context)

    answer = None
    while not answer:
        answer = get_answer(final_prompt)
        time.sleep(1)

    enriched_answer = add_docs_links(answer, processed_docs)

    return enriched_answer
