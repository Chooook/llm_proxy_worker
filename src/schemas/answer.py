from pydantic import BaseModel


class Answer(BaseModel):
    text: str
    relevant_docs: dict[str, dict[str, str]] = {}
    context: str = ''
    score: str = ''
