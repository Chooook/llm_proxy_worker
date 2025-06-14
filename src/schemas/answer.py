from pydantic import BaseModel


class Answer(BaseModel):
    text: str
    relevant_docs: dict[str, str] = {}
