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
        label = f"{t} ({times[t]}s)"
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
            st = max(st, max(sched[p][1] for p in parents[t]))
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

    for task in start_tasks:
        dfs(task)

    return sched

def plot_gantt(sched, cores=4, filename=None):
    fig, ax = plt.subplots(figsize=(10,6))
    for t, (s, f, c) in sched.items():
        ax.barh(c, f - s, left=s, color='#eeeeee', edgecolor='black', linewidth=1)
        ax.text((s + f) / 2, c, str(t), ha='center', va='center', color='black')
    ax.set_title('Graph Execution', fontsize=20, pad=20)
    ax.set_xlabel('Time', fontsize=16)
    ax.set_ylabel('Core', fontsize=16)
    ax.set_yticks(range(1, cores+1))
    
    # Adjust ylim to add space at the top and bottom
    ax.set_ylim(0, cores + 1)  # Increase the ylim range

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

def plot_disk_usage(schedules, file_sizes, titles, filename=None):
    plt.figure(figsize=(10, 6))
    
    for sched, title in zip(schedules, titles):
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
        plt.step(time_points, disk_usage, where='post', label=title)

    plt.title('Disk Usage Over Time')
    plt.xlabel('Time')
    plt.ylabel('Disk Usage (MB)')
    plt.grid(True, linestyle='--', linewidth=0.8)
    plt.legend()
    plt.tight_layout()
    
    if filename:
        plt.savefig(filename)
    else:
        plt.show()

if __name__ == '__main__':
    draw_graph(graph, execution_times, file_sizes, filename='mygraph_task_graph.png')
    sched_bfs = schedule_bfs(graph, execution_times, 4)
    print("BFS Schedule:")
    for t in sorted(sched_bfs):
        print(t, sched_bfs[t])
    
    sched_dfs = schedule_dfs(graph, execution_times, 4)
    print("\nDFS Schedule:")
    for t in sorted(sched_dfs):
        print(t, sched_dfs[t])
    
    # Plot Gantt charts and save to files
    plot_gantt(sched_bfs, 4, filename='mygraph_bfs_schedule.png')
    plot_gantt(sched_dfs, 4, filename='mygraph_dfs_schedule.png')

    # Plot disk usage for both BFS and DFS on the same graph and save to file
    plot_disk_usage(
        [sched_bfs, sched_dfs],
        file_sizes,
        titles=['Disk Usage Over Time (BFS)', 'Disk Usage Over Time (DFS)'],
        filename='mygraph_disk_usage_plot.png'
    )
