from pydantic import BaseModel


class LocalModel(BaseModel):
    name: str
    link: str
    path: str = ''  # commonly it's Path(os.getenv('LOCAL_MODELS_PATH')) / name
