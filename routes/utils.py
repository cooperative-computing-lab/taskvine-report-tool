def compute_tick_values(domain, num_ticks=5, round_digits=2):
    start, end = domain
    if num_ticks < 2:
        raise ValueError("num_ticks must be at least 2")
    step = (end - start) / (num_ticks - 1)

    values = [start + i * step for i in range(num_ticks)]

    if round_digits is None:
        return values
    elif round_digits == 0:
        return [int(v) for v in values]
    else:
        return [round(v, round_digits) for v in values]


def d3_time_formatter():
    return '(d) => d3.format(".2f")(d) + " s"'

def d3_int_formatter():
    return '(d) => d3.format(".0f")(d)'

def d3_size_formatter(unit):
    return f'(d) => d3.format(".2f")(d) + " {unit}"'

def d3_percentage_formatter():
    return '(d) => d3.format(".2f")(d) + " %"'

def d3_worker_core_formatter():
    return '(d) => d.split("-")[0]'