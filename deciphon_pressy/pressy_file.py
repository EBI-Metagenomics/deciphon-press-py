import importlib.resources

__all__ = ["pressy_file"]


def pressy_file():
    t = importlib.resources.files("deciphon_pressy").joinpath("pressy")
    return importlib.resources.as_file(t)
