__all__ = ["datafile", "flags", "graphs"]
for module in __all__:
    __import__(__name__ + "." + module, globals(), locals())