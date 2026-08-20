"""
TaskVine Report Tool

Visualization and analysis tool for TaskVine execution logs.
"""

__version__ = "2026.8.1"
__author__ = "Collaborative Computing Lab (CCL), University of Notre Dame"
__email__ = "jzhou24@nd.edu"

# Package information
__title__ = "taskvine-report"
__description__ = "Visualization and analysis tool for TaskVine execution logs"
__url__ = "https://github.com/cooperative-computing-lab/taskvine-report-tool"

__all__ = ["DataParser"]


def __getattr__(name):
    """Keep the public convenience import without eagerly loading Polars.

    Eager parser imports used to initialize native compute thread pools before
    ``vine_report`` could apply its login-node resource budget.
    """
    if name == "DataParser":
        from .vine_parse.data_parser import DataParser
        return DataParser
    raise AttributeError(name)
