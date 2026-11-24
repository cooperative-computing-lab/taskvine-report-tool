"""Very long command lines, unicode, newlines, environment variables, and fake log text in output."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
fake = ("2026/01/01 00:00:00.00 vine_manager[1] vine: tx to localhost (127.0.0.1:1): task 99\n"
        "2026/01/01 00:00:00.00 vine_manager[1] vine: rx from localhost (127.0.0.1:1): complete 0 0 0 0 0 99\n"
        "end\ncmd 5\nstate change: 1 (READY) to 2 (RUNNING)\n")
commands = [
    "echo " + "x" * 60000,
    "echo 'ünïcødé ✓ 日本語 \\t tab'",
    "echo first\necho second\n\necho 'task 7'",
    "printf '%s' " + repr(fake),
    "printf ''",
    "echo $VINE_EX_A $VINE_EX_B",
    "head -c 3000000 /dev/zero | tr '\\0' 'y'",
    "echo 'complete 0 0 0 0 0 1' >&2; echo end",
    "true # cmd 12 env 3 infile outfile end",
]
tasks = []
for i, command in enumerate(commands):
    t = Task(command)
    t.set_env_var("VINE_EX_A", "value with spaces\nand newline")
    t.set_env_var("VINE_EX_B", "ü=1;" * 50)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=60)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
