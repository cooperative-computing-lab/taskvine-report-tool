const DURATION_UNITS = [
    { maxSpan: 1, seconds: 1e-3, label: 'ms' },
    { maxSpan: 120, seconds: 1, label: 's' },
    { maxSpan: 7200, seconds: 60, label: 'min' },
    { maxSpan: 172800, seconds: 3600, label: 'h' },
    { maxSpan: Infinity, seconds: 86400, label: 'd' },
];

function finiteDomain(domain) {
    if (!Array.isArray(domain) || domain.length !== 2) return [0, 1];
    const values = domain.map(Number);
    return values.every(Number.isFinite) ? values : [0, 1];
}

function durationUnit(domain) {
    const [start, end] = finiteDomain(domain);
    const span = Math.abs(end - start);
    return DURATION_UNITS.find(unit => span < unit.maxSpan) || DURATION_UNITS.at(-1);
}

export function durationAxisSpec(domain, tickCount = 6) {
    const [start, end] = finiteDomain(domain);
    const unit = durationUnit([start, end]);
    const scaledStart = start / unit.seconds;
    const scaledEnd = end / unit.seconds;
    let scaledTicks = d3.ticks(scaledStart, scaledEnd, tickCount);
    if (!scaledTicks.length) scaledTicks = [scaledStart];
    const tickValues = scaledTicks.map(value => value * unit.seconds);
    const formatter = value => `${d3.format('.3~f')(Number(value) / unit.seconds)} ${unit.label}`;
    const formatterExpression = `(d) => d3.format(".3~f")(Number(d) / ${unit.seconds}) + " ${unit.label}"`;
    return { ...unit, tickValues, formatter, formatterExpression };
}

export function formatDuration(value) {
    if (value == null) return '—';
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return '—';
    const sign = numeric < 0 ? '-' : '';
    const absolute = Math.abs(numeric);
    if (absolute < 1) return `${sign}${d3.format('.3~f')(absolute * 1000)} ms`;
    if (absolute < 60) return `${sign}${d3.format('.3~f')(absolute)} s`;

    const days = Math.floor(absolute / 86400);
    const hours = Math.floor((absolute % 86400) / 3600);
    const minutes = Math.floor((absolute % 3600) / 60);
    const seconds = absolute % 60;
    const parts = [];
    if (days) parts.push(`${days}d`);
    if (hours || days) parts.push(`${hours}h`);
    if (minutes || hours || days) parts.push(`${minutes}m`);
    if (seconds >= 0.05) parts.push(`${d3.format('.3~f')(seconds)}s`);
    return sign + parts.join(' ');
}
