find "logs" -mindepth 1 -maxdepth 1 -type d | sort |
while IFS= read -r subdir; do
    echo "=== processing $subdir..."
    python generate_csv.py "$subdir"
    # python generate_graph.py "$subdir"
done
