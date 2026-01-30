from importlib.metadata import entry_points


def load_source(kind: str):
    for ep in entry_points(group="opactx.sources"):
        if ep.name == kind:
            return ep.load()
    raise ValueError(f"Unknown source type: {kind}")


def load_transform(kind: str):
    for ep in entry_points(group="opactx.transforms"):
        if ep.name == kind:
            return ep.load()
    raise ValueError(f"Unknown transform type: {kind}")
