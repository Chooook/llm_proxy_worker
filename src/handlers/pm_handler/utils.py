
def add_docs_links(answer: str, processed_docs: list[dict]) -> str:
    """Enrich answer with document links.

    :param answer: generated text
    :type answer: str
    :param processed_docs: documents found
    :type processed_docs: List[dict]

    :return: enriched_answer
    :rtype: str

    # `processed_docs` hint
    {
        'text': str,
        'link': str,
        'metadata': str
    }
    """
    enriched_answer = answer

    for i, doc in enumerate(processed_docs):
        doc_template = f"Document {str(i)}"
        if doc_template in answer:
            enriched_answer = enriched_answer.replace(
                doc_template,
                f"[{doc_template}]({doc['link']})"
            )

    return enriched_answer
