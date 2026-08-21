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
            .attr('aria-label', 'Move across the plot to inspect every visible line')
            .on('pointermove', event => this._queueMove(event))
            .on('pointerleave', () => this.hide());
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
        const [pointerX] = d3.pointer(event, this.hitbox.node());
        this._pendingPointer = { pointerX, clientX: event.clientX, clientY: event.clientY };
        if (this._animationFrame != null) return;
        this._animationFrame = requestAnimationFrame(() => {
            this._animationFrame = null;
            const pointer = this._pendingPointer;
            this._pendingPointer = null;
            if (pointer) this._move(pointer);
        });
    }

    _move(event) {
        const { pointerX } = event;
        const x = this.xScale.invert(pointerX);
        const values = this._valuesAt(x);
        if (!values.length) {
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

        this._renderTooltip(x, values, event);
    }

    _renderTooltip(x, values, event) {
        this.tooltip.replaceChildren();
        this.tooltip.classList.add('line-cursor-tooltip');

        const heading = document.createElement('div');
        heading.className = 'line-cursor-x';
        heading.textContent = `${this.xLabel}: ${formatCursorValue(x, this.xUnit, this.xFormatter)}`;
        this.tooltip.appendChild(heading);

        const list = document.createElement('div');
        list.className = 'line-cursor-values';
        list.style.setProperty('--line-cursor-columns', values.length > 12 ? '2' : '1');
        values.forEach(item => {
            const row = document.createElement('div');
            row.className = 'line-cursor-row';

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

    hide() {
        if (this._animationFrame != null) cancelAnimationFrame(this._animationFrame);
        this._animationFrame = null;
        this._pendingPointer = null;
        this.layer?.style('display', 'none');
        if (!this.tooltip) return;
        this.tooltip.style.visibility = 'hidden';
        this.tooltip.classList.remove('line-cursor-tooltip');
    }

    destroy() {
        this.hide();
        this.layer?.remove();
        this.hitbox?.remove();
    }
}
