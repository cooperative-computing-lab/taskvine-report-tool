import time
import ndcctools.taskvine as vine
from tqdm import tqdm  # 导入进度条库

# 定义一个非常简单的任务
def simple_task(x):
    import time
    time.sleep(1.6)
    return x * x

# 创建 TaskVine 管理器
queue = vine.Manager(port=[9123, 9130],
                     run_info_path="vine-run-info",
                     run_info_template="tt",
                    )

queue.set_name("jzhou24-hgg5")

# 提交任务
tasks = 100
submission_progress = tqdm(total=tasks, desc="Submitting Tasks")

for i in range(tasks):
    task = vine.PythonTask(simple_task, i)
    task.set_cores(1)
    queue.submit(task)
    submission_progress.update(1)

submission_progress.close()

# 等待所有任务完成
results = []
completion_progress = tqdm(total=tasks, desc="Completing Tasks")

while len(results) < tasks:
    task = queue.wait(5)
    if task:
        results.append(task.output)
        completion_progress.update(1)

completion_progress.close()

# 结果验证（例如计算所有平方的和）
total = sum(results)
print("Total sum of squares:", total)
