import matplotlib.pyplot as plt
import seaborn as sns
from graphviz import Digraph
from collections import deque
import random

sns.set(style="whitegrid", context="talk")

graph = {
    1: [2, 3],
    2: [4, 5],
    3: [6, 7],
    4: [8, 9],
    5: [10, 11],
    6: [12, 13],
    7: [14, 15],
    8: [],
    9: [],
    10: [],
    11: [],
    12: [],
    13: [],
    14: [],
    15: []
}
graph = {
    1: [2, 3],
    2: [5],
    3: [5, 6],
    4: [6, 7],
    5: [8],
    6: [8, 9],
    7: [10],
    8: [11],
    9: [11, 12],
    10: [12, 13],
    11: [14],
    12: [14, 15],
    13: [15],
    14: [],
    15: []
}

execution_times = {
    1: 1, 2: 2, 3: 5, 4: 3, 5: 6, 6: 2, 7: 5,
    8: 6, 9: 1, 10: 5, 11: 9, 12: 8, 13: 2,
    14: 3, 15: 12
}

# Generate random file sizes between 1 and 10 MB
file_sizes = {task: random.randint(1, 10) for task in graph.keys()}

def draw_graph(g, times, file_sizes, filename='task_graph'):
    dot = Digraph()
    
    # Add task nodes as ellipses with task ID and execution time
    for t in g:
        label = f"<<b>{t}</b> ({times[t]}s)>"
        dot.node(f"task_{t}", label=label, shape='ellipse')
    
    # Add file nodes as rectangles with file size
    for t in g:
        label = f"{file_sizes[t]} MB"
        dot.node(f"file_{t}", label=label, shape='rect')
    
    # Connect task nodes to their output file nodes
    for u in g:
        dot.edge(f"task_{u}", f"file_{u}")  # Connect task to its output file
        for v in g[u]:
            dot.edge(f"file_{u}", f"task_{v}")  # Connect file to the next task
    
    dot.render(filename, format='pdf', cleanup=True)

def schedule_bfs(g, times, cores=4):
    core_time = {c: 0 for c in range(1, cores+1)}
    parents = {}
    for u in g:
        for v in g[u]:
            parents.setdefault(v, []).append(u)
    all_tasks = set(g.keys())
    for children in g.values():
        all_tasks.update(children)
    indeg = {t: 0 for t in all_tasks}
    for u in g:
        for v in g[u]:
            indeg[v] += 1
    q = deque(t for t in all_tasks if indeg[t] == 0)
    sched = {}
    while q:
        t = q.popleft()
        c = min(core_time, key=core_time.get)
        st = core_time[c]
        if t in parents:
            st = max(st, max(sched[p][1] for p in parents[t]))  # Ensure all parents are finished
        ft = st + times[t]
        sched[t] = (st, ft, c)
        core_time[c] = ft
        for child in g.get(t, []):
            indeg[child] -= 1
            if indeg[child] == 0:
                q.append(child)
    return sched

def schedule_dfs(g, times, cores=4):
    core_time = {c: 0 for c in range(1, cores+1)}
    visited = set()
    sched = {}

    def dfs(task):
        if task in visited:
            return
        visited.add(task)
        
        # Schedule the current task
        c = min(core_time, key=core_time.get)
        st = core_time[c]
        if task in parents:
            st = max(st, max(sched[p][1] for p in parents[task]))  # Ensure all parents are finished
        ft = st + times[task]
        sched[task] = (st, ft, c)
        core_time[c] = ft
        
        # Visit all children
        for child in g.get(task, []):
            dfs(child)

    # Start DFS from all nodes with no incoming edges
    all_tasks = set(g.keys())
    for children in g.values():
        all_tasks.update(children)
    indeg = {t: 0 for t in all_tasks}
    for u in g:
        for v in g[u]:
            indeg[v] += 1
    start_tasks = [t for t in all_tasks if indeg[t] == 0]

    # Create a parent map
    parents = {}
    for u in g:
        for v in g[u]:
            parents.setdefault(v, []).append(u)

    for task in start_tasks:
        dfs(task)

    return sched

def schedule_largest_input_first(g, times, file_sizes, cores=4):
    core_time = {c: 0 for c in range(1, cores+1)}
    parents = {}
    for u in g:
        for v in g[u]:
            parents.setdefault(v, []).append(u)
    all_tasks = set(g.keys())
    for children in g.values():
        all_tasks.update(children)
    indeg = {t: 0 for t in all_tasks}
    for u in g:
        for v in g[u]:
            indeg[v] += 1

    sched = {}
    ready_tasks = [t for t in all_tasks if indeg[t] == 0]

    while ready_tasks:
        # Sort ready tasks by the largest single input file size
        ready_tasks.sort(key=lambda t: max((file_sizes.get(p, 0) for p in parents.get(t, [])), default=0), reverse=True)
        
        t = ready_tasks.pop(0)  # Get the task with the largest input file
        c = min(core_time, key=core_time.get)
        st = core_time[c]
        if t in parents:
            st = max(st, max(sched[p][1] for p in parents[t]))  # Ensure all parents are finished
        ft = st + times[t]
        sched[t] = (st, ft, c)
        core_time[c] = ft

        for child in g.get(t, []):
            indeg[child] -= 1
            if indeg[child] == 0:
                ready_tasks.append(child)

    return sched

def calculate_max_time(schedules):
    max_time = 0
    for sched in schedules:
        for _, (start, finish, _) in sched.items():
            if finish > max_time:
                max_time = finish
    return max_time

def plot_gantt(sched, cores=4, filename=None, title='Graph Execution', max_time=None, figsize=(10, 6)):
    fig, ax = plt.subplots(figsize=figsize)
    for t, (s, f, c) in sched.items():
        ax.barh(c, f - s, left=s, color='#eeeeee', edgecolor='black', linewidth=1)
        ax.text((s + f) / 2, c, str(t), ha='center', va='center', color='black')
    ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
    ax.set_xlabel('Time', fontsize=16)
    ax.set_ylabel('Core', fontsize=16)
    ax.set_yticks(range(1, cores+1))
    
    # Set x-axis limit to max_time if provided
    if max_time is not None:
        ax.set_xlim(0, max_time)

    ax.grid(True, linestyle='--', linewidth=0.8)
    
    # Make the outer border darker
    for spine in ax.spines.values():
        spine.set_edgecolor('gray')
        spine.set_linewidth(2)  # Adjust the width as needed

    plt.tight_layout()
    
    if filename:
        plt.savefig(filename)
    else:
        plt.show()

def plot_disk_usage(schedules, file_sizes, titles, filename=None, title='Disk Usage Over Time', max_time=None, figsize=(18, 6)):
    plt.figure(figsize=figsize)
    
    for sched, plot_title in zip(schedules, titles):
        # Calculate disk usage over time
        events = []
        for task, (start, finish, core) in sched.items():
            events.append((finish, file_sizes[task]))  # Task completion event
        events.sort()  # Sort events by time

        time_points = []
        disk_usage = []
        current_usage = 0

        for time, size in events:
            current_usage += size
            time_points.append(time)
            disk_usage.append(current_usage)

        # Plotting
        plt.step(time_points, disk_usage, where='post', label=plot_title)

    plt.title(title, fontsize=20, fontweight='bold')  # Make title bold
    plt.xlabel('Time')
    plt.ylabel('Disk Usage (MB)')
    plt.grid(True, linestyle='--', linewidth=0.8)
    plt.legend()
    plt.tight_layout()
    
    # Set x-axis limit to max_time if provided
    if max_time is not None:
        plt.xlim(0, max_time)
    
    if filename:
        plt.savefig(filename)
    else:
        plt.show()

if __name__ == '__main__':
    draw_graph(graph, execution_times, file_sizes, filename='mygraph_task_graph.pdf')
    
    # Schedule using BFS
    sched_bfs = schedule_bfs(graph, execution_times, 4)
    print("BFS Schedule:")
    for t in sorted(sched_bfs):
        print(t, sched_bfs[t])
    
    # Schedule using DFS
    sched_dfs = schedule_dfs(graph, execution_times, 4)
    print("\nDFS Schedule:")
    for t in sorted(sched_dfs):
        print(t, sched_dfs[t])
    
    # Schedule using Largest Input First
    sched_lif = schedule_largest_input_first(graph, execution_times, file_sizes, 4)
    print("\nLargest Input First Schedule:")
    for t in sorted(sched_lif):
        print(t, sched_lif[t])
    
    # Calculate the maximum finish time for consistent x-axis limits
    max_time = calculate_max_time([sched_bfs, sched_dfs, sched_lif])
    max_time *= 1.1
    
    # Plot Gantt charts and save to files
    plot_gantt(sched_bfs, 4, filename='mygraph_bfs_schedule.png', title='Breadth-First Scheduling', max_time=max_time, figsize=(8, 5))
    plot_gantt(sched_dfs, 4, filename='mygraph_dfs_schedule.png', title='Depth-First Scheduling', max_time=max_time, figsize=(8, 5))
    plot_gantt(sched_lif, 4, filename='mygraph_lif_schedule.png', title='Largest Input First Scheduling', max_time=max_time, figsize=(8, 5))

    # Plot disk usage for all schedules on the same graph and save to file
    plot_disk_usage(
        [sched_bfs, sched_dfs, sched_lif],
        file_sizes,
        titles=['Breadth-First Scheduling', 'Depth-First Scheduling', 'Largest Input First Scheduling'],
        filename='mygraph_disk_usage_plot.png',
        max_time=max_time,
        figsize=(20, 8)
    )
