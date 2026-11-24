"""Parse TaskVine execution logs and generate CSV projections."""

__all__ = ["DataParser", "CSVManager"]


def __getattr__(name):
    if name == "DataParser":
        from .data_parser import DataParser

        value = DataParser
    elif name == "CSVManager":
        from .csv_manager import CSVManager

        value = CSVManager
    else:
        raise AttributeError(name)
    globals()[name] = value
    return value
