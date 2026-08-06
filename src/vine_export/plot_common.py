"""
Shared plotting helpers for vine_export modules.
"""

import os

from src.utils import (
    ensure_dir,
    read_csv_to_fd,
    compute_linear_tick_values,
    compute_discrete_tick_values,
)
from src.vine_export.config import (
    CANVAS_WIDTH_INCHES,
    CANVAS_HEIGHT_INCHES,
    LAYOUT_PAD,
    LAYOUT_W_PAD,
    LAYOUT_H_PAD,
    TITLE_SIZE,
    TITLE_PAD,
    LABEL_SIZE,
    X_LABEL_PAD,
    Y_LABEL_PAD,
    TICK_LABEL_SIZE,
    GRID_ALPHA,
    DEFAULT_LINEAR_TICK_COUNT,
    DEFAULT_DISCRETE_TICK_COUNT,
)


def make_figure(plt, width=None, height=None):
    width = width if width is not None else CANVAS_WIDTH_INCHES
    height = height if height is not None else CANVAS_HEIGHT_INCHES
    fig, ax = plt.subplots(figsize=(width, height), constrained_layout=True)
    if hasattr(fig, "set_constrained_layout_pads"):
        fig.set_constrained_layout_pads(
            w_pad=LAYOUT_PAD,
            h_pad=LAYOUT_PAD,
            wspace=LAYOUT_W_PAD,
            hspace=LAYOUT_H_PAD,
        )
    else:
        fig.get_layout_engine().set(w_pad=LAYOUT_PAD, h_pad=LAYOUT_PAD)
    return fig, ax


def style_axes(ax, title, x_label, y_label, show_grid=True):
    ax.set_title(title, fontsize=TITLE_SIZE, pad=TITLE_PAD)
    ax.set_xlabel(x_label, fontsize=LABEL_SIZE, labelpad=X_LABEL_PAD)
    ax.set_ylabel(y_label, fontsize=LABEL_SIZE, labelpad=Y_LABEL_PAD)
    ax.tick_params(axis="both", labelsize=TICK_LABEL_SIZE)
    if show_grid:
        ax.grid(True, alpha=GRID_ALPHA, linewidth=0.6)


def set_linear_ticks(ax, domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2, suffix=""):
    ticks = compute_linear_tick_values(domain, num_ticks=tick_count, round_digits=round_digits)
    if axis == "x":
        ax.set_xticks(ticks)
        if suffix:
            ax.set_xticklabels([f"{v:.2f}{suffix}" for v in ticks])
    else:
        ax.set_yticks(ticks)
        if suffix:
            ax.set_yticklabels([f"{v:.2f}{suffix}" for v in ticks])


def set_discrete_ticks(ax, values, axis="x", tick_count=DEFAULT_DISCRETE_TICK_COUNT):
    ticks = compute_discrete_tick_values(values, num_ticks=tick_count)
    if axis == "x":
        ax.set_xticks(ticks)
    else:
        ax.set_yticks(ticks)


def load_csv(csv_files_dir, filename):
    path = os.path.join(csv_files_dir, filename)
    return read_csv_to_fd(path)


def save_plot(fig, plt, png_files_dir, filename, dpi):
    ensure_dir(png_files_dir, replace=False)
    png_path = os.path.join(png_files_dir, filename)
    fig.savefig(png_path, dpi=dpi)
    plt.close(fig)
    return png_path

