"""
TaskVine Report Tool

Visualization and analysis tool for TaskVine execution logs.
"""

__version__ = "2025.6.4"
__author__ = "Collaborative Computing Lab (CCL), University of Notre Dame"
__email__ = "jzhou24@nd.edu"

# Package information
__title__ = "taskvine-report"
__description__ = "Visualization and analysis tool for TaskVine execution logs"
__url__ = "https://github.com/cooperative-computing-lab/taskvine-report-tool"

# Import main components for easier access
from .src.data_parser import DataParser

__all__ = ['DataParser'] 