function finitePoint(point) {
    return Array.isArray(point)
        && point.length >= 2
        && Number.isFinite(Number(point[0]))
        && Number.isFinite(Number(point[1]));
}

function lowerBound(points, x) {
    let lo = 0;
    let hi = points.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (points[mid][0] < x) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

let pinnedCursor = null;

export function normalizeLinePoints(rawPoints) {
    return (Array.isArray(rawPoints) ? rawPoints : [])
        .filter(finitePoint)
        .map(point => [Number(point[0]), Number(point[1])])
        .sort((a, b) => a[0] - b[0]);
}

function sampleSortedLineAtX(points, x, interpolation = 'linear') {
    if (!points.length || x < points[0][0] || x > points.at(-1)[0]) return null;

    const rightIndex = lowerBound(points, x);
    if (rightIndex < points.length && points[rightIndex][0] === x) {
        return [x, points[rightIndex][1]];
    }
    if (rightIndex === 0 || rightIndex >= points.length) return null;

    const left = points[rightIndex - 1];
    const right = points[rightIndex];
    if (interpolation === 'step' || right[0] === left[0]) return [x, left[1]];
    const ratio = (x - left[0]) / (right[0] - left[0]);
    return [x, left[1] + ratio * (right[1] - left[1])];
}

export function sampleLineAtX(rawPoints, x, interpolation = 'linear') {
    return sampleSortedLineAtX(normalizeLinePoints(rawPoints), x, interpolation);
}

function safeFormat(formatter, value) {
    try {
        return String(formatter(value));
    } catch (_error) {
        return String(value);
    }
}

function isActiveValue(value) {
    return Math.abs(Number(value)) > 1e-12;
}

export function sortCursorValues(values) {
    return [...values].sort((left, right) => {
        const activeOrder = Number(isActiveValue(right.y)) - Number(isActiveValue(left.y));
        if (activeOrder) return activeOrder;
        const magnitudeOrder = Math.abs(right.y) - Math.abs(left.y);
        if (magnitudeOrder) return magnitudeOrder;
        return left.index - right.index;
    });
}

export function formatCursorValue(value, unit, fallback = item => item) {
    const numeric = Number(value);
    if (unit !== 'seconds' || !Number.isFinite(numeric)) return safeFormat(fallback, value);
    const sign = numeric < 0 ? '-' : '';
    const absolute = Math.abs(numeric);
    if (absolute < 1) return `${sign}${d3.format('.3~f')(absolute * 1000)} ms`;
    if (absolute < 60) return `${sign}${d3.format('.3~f')(absolute)} s`;
    const hours = Math.floor(absolute / 3600);
    const minutes = Math.floor((absolute % 3600) / 60);
    const seconds = absolute % 60;
    const tail = seconds >= 0.05 ? ` ${d3.format('.3~f')(seconds)}s` : '';
    if (hours) return `${sign}${hours}h ${minutes}m${tail}`;
    return `${sign}${minutes}m${tail}`;
}

export class LineCursor {
    constructor({
        svg,
        xScale,
        yScale,
        series,
        tooltip,
        xFormatter = value => value,
        yFormatter = value => value,
        xLabel = 'X',
        xUnit = null,
        interpolation = 'linear',
        isVisible = () => true,
    }) {
        this.svg = svg;
        this.xScale = xScale;
        this.yScale = yScale;
        this.series = series.map(item => ({
            ...item,
            points: normalizeLinePoints(item.points || []),
        }));
        this.tooltip = tooltip;
        this.xFormatter = xFormatter;
        this.yFormatter = yFormatter;
        this.xLabel = xLabel ? `${xLabel[0].toUpperCase()}${xLabel.slice(1)}` : 'X';
        this.xUnit = xUnit;
        this.interpolation = interpolation;
        this.isVisible = isVisible;
        this.layer = null;
        this.guide = null;
        this.markers = null;
        this.hitbox = null;
        this._animationFrame = null;
        this._pendingPointer = null;
        this._lastRender = null;
        this.pinned = false;
    }

    mount() {
        if (!this.svg || !this.xScale?.invert || !this.series?.length || !this.tooltip) return;
        const [x0, x1] = d3.extent(this.xScale.range());
        const [y0, y1] = d3.extent(this.yScale.range());

        this.layer = this.svg.append('g')
            .attr('class', 'line-cursor-layer')
            .style('display', 'none')
            .attr('aria-hidden', 'true');
        this.guide = this.layer.append('line')
            .attr('class', 'line-cursor-guide')
            .attr('y1', y0)
            .attr('y2', y1);
        this.markers = this.layer.append('g').attr('class', 'line-cursor-markers');

        this.hitbox = this.svg.append('rect')
            .attr('class', 'line-cursor-hitbox')
            .attr('x', x0)
            .attr('y', y0)
            .attr('width', Math.max(0, x1 - x0))
            .attr('height', Math.max(0, y1 - y0))
            .attr('fill', 'transparent')
            .attr('pointer-events', 'all')
            .attr('tabindex', 0)
            .attr('role', 'button')
            .attr('aria-pressed', 'false')
            .attr('aria-label', 'Move across the plot to inspect every visible line')
            .on('pointermove', event => this._queueMove(event))
            .on('pointerleave', () => this.hide())
            .on('click', event => this._togglePin(event))
            .on('keydown', event => this._handleKey(event));
    }

    _valuesAt(x) {
        const [yMin, yMax] = d3.extent(this.yScale.domain());
        return this.series.flatMap((item, index) => {
            if (!this.isVisible(item, index)) return [];
            const point = sampleSortedLineAtX(item.points, x, item.interpolation || this.interpolation);
            if (!point || point[1] < yMin || point[1] > yMax) return [];
            return [{ ...item, index, x: point[0], y: point[1] }];
        });
    }

    _queueMove(event) {
        if (this.pinned || (pinnedCursor && pinnedCursor !== this)) return;
        const [pointerX] = d3.pointer(event, this.hitbox.node());
        this._pendingPointer = { pointerX, clientX: event.clientX, clientY: event.clientY };
        if (this._animationFrame != null) return;
        this._animationFrame = requestAnimationFrame(() => {
            this._animationFrame = null;
            const pointer = this._pendingPointer;
            this._pendingPointer = null;
            if (pointer && !this.pinned && (!pinnedCursor || pinnedCursor === this)) {
                this._move(pointer);
            }
        });
    }

    _move(event) {
        const { pointerX } = event;
        const x = this.xScale.invert(pointerX);
        const values = sortCursorValues(this._valuesAt(x));
        if (!values.length) {
            this._lastRender = null;
            this.hide();
            return;
        }

        const cursorX = this.xScale(x);
        this.layer.style('display', null);
        this.guide.attr('x1', cursorX).attr('x2', cursorX);
        this.markers.selectAll('circle')
            .data(values, item => item.id || item.name || item.index)
            .join('circle')
            .attr('class', 'line-cursor-marker')
            .attr('cx', cursorX)
            .attr('cy', item => this.yScale(item.y))
            .attr('r', 4)
            .attr('fill', '#fff')
            .attr('stroke', item => item.color || '#444');

        this._lastRender = { x, values, event };
        this._renderTooltip(x, values, event);
    }

    _togglePin(event) {
        if (this.pinned) {
            this._setPinned(false, event);
            return;
        }
        const [pointerX] = d3.pointer(event, this.hitbox.node());
        this._move({ pointerX, clientX: event.clientX, clientY: event.clientY });
        if (!this._lastRender) return;
        this._setPinned(true, event);
    }

    _setPinned(pinned, event = null) {
        if (pinned && pinnedCursor && pinnedCursor !== this) {
            pinnedCursor._setPinned(false);
            pinnedCursor.hide(true);
        }
        this.pinned = pinned;
        if (pinned) pinnedCursor = this;
        else if (pinnedCursor === this) pinnedCursor = null;
        if (pinned && this._animationFrame != null) {
            cancelAnimationFrame(this._animationFrame);
            this._animationFrame = null;
            this._pendingPointer = null;
        }
        this.hitbox?.attr('aria-pressed', String(pinned));
        this.layer?.classed('is-pinned', pinned);
        if (event && this._lastRender) {
            this._renderTooltip(this._lastRender.x, this._lastRender.values, event);
        }
    }

    _handleKey(event) {
        if (event.key === 'Escape') {
            this._setPinned(false);
            this.hide(true);
        } else if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            const [x0, x1] = d3.extent(this.xScale.range());
            const bounds = this.hitbox.node().getBoundingClientRect();
            const pointer = {
                pointerX: (x0 + x1) / 2,
                clientX: bounds.left + bounds.width / 2,
                clientY: bounds.top + bounds.height / 2,
            };
            if (this.pinned) {
                this._setPinned(false, pointer);
            } else {
                this._move(pointer);
                if (this._lastRender) this._setPinned(true, pointer);
            }
        }
    }

    _renderTooltip(x, values, event) {
        this.tooltip.replaceChildren();
        this.tooltip.classList.add('line-cursor-tooltip');

        const heading = document.createElement('div');
        heading.className = 'line-cursor-x';
        const coordinate = document.createElement('span');
        coordinate.textContent = `${this.xLabel}: ${formatCursorValue(x, this.xUnit, this.xFormatter)}`;
        const activeCount = values.filter(item => isActiveValue(item.y)).length;
        const status = document.createElement('span');
        status.className = 'line-cursor-status';
        status.textContent = this.pinned
            ? `Pinned · ${activeCount}/${values.length} active`
            : `${activeCount}/${values.length} active · Click to pin`;
        heading.append(coordinate, status);
        this.tooltip.appendChild(heading);

        const list = document.createElement('div');
        list.className = 'line-cursor-values';
        list.style.setProperty('--line-cursor-columns', values.length > 12 ? '2' : '1');
        values.forEach(item => {
            const row = document.createElement('div');
            row.className = `line-cursor-row${isActiveValue(item.y) ? '' : ' is-zero'}`;

            const swatch = document.createElement('span');
            swatch.className = 'line-cursor-swatch';
            swatch.style.backgroundColor = item.color || '#444';
            const label = document.createElement('span');
            label.className = 'line-cursor-label';
            label.textContent = item.name || `Series ${item.index + 1}`;
            const value = document.createElement('strong');
            value.className = 'line-cursor-value';
            value.textContent = safeFormat(this.yFormatter, item.y);

            row.append(swatch, label, value);
            list.appendChild(row);
        });
        this.tooltip.appendChild(list);
        this.tooltip.classList.toggle('cursor-pinned', this.pinned);
        this.tooltip.style.visibility = 'visible';

        const gap = 14;
        const rect = this.tooltip.getBoundingClientRect();
        const viewportPadding = 10;
        let left = event.clientX + gap;
        if (left + rect.width > window.innerWidth - viewportPadding) {
            left = event.clientX - rect.width - gap;
        }
        let top = event.clientY - Math.min(26, rect.height / 3);
        top = Math.max(viewportPadding, Math.min(top, window.innerHeight - rect.height - viewportPadding));
        this.tooltip.style.left = `${Math.max(viewportPadding, left)}px`;
        this.tooltip.style.top = `${top}px`;
    }

    hide(force = false) {
        if (pinnedCursor && pinnedCursor !== this) return;
        if (this.pinned && !force) return;
        if (this._animationFrame != null) cancelAnimationFrame(this._animationFrame);
        this._animationFrame = null;
        this._pendingPointer = null;
        this.layer?.style('display', 'none');
        if (!this.tooltip) return;
        this.tooltip.style.visibility = 'hidden';
        this.tooltip.classList.remove('line-cursor-tooltip', 'cursor-pinned');
    }

    destroy() {
        this._setPinned(false);
        this.hide(true);
        this.layer?.remove();
        this.hitbox?.remove();
    }
}
