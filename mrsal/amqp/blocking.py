from pydantic.dataclasses import dataclass
from mrsal.mrsal import Mrsal


@dataclass
class MrsalBlockingAMQP(Mrsal):
    use_blocking: bool = False
