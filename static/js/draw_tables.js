const tableTextFontSize = '3.5rem';

var dataTableSettings = {
    "processing": true,
    "serverSide": true,
    "paging": true,
    "pageLength": 100,
    "destroy": true,
    "searching": false,
    "fixedHeader": false,
    "autoWidth": true,
    "lengthChange": false,     // Will Disabled Record number per page
    "scrollX": true,
    "sScrollXInner": "100%",
    "scrollY": "500px",
    "fixedColumns": {
        leftColumns: 1
    },
};

export function createTable(tableID, specificSettings) {
    var tableSettings = $.extend(true, {}, dataTableSettings, specificSettings);
    return $(tableID).DataTable(tableSettings);
}

function drawTaskCompletedTable(url) {
    var searchType = 'task-id';
    var searchValue = '';
    var timestampType = 'original';

    var specificSettings = {
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = searchType;
                d.search.value = searchValue;
                d.timestamp_type = timestampType;
            },
            "dataSrc": function(response) {
                response.data.forEach(function(task) {
                    task.task_id = parseInt(task.task_id, 10);
                    task.try_id = parseInt(task.try_id, 10);
                    task.worker_id = parseInt(task.worker_id, 10);
                });
                return response.data;
            }
        },
        "columns": [
            { "data": "task_id" },
            { "data": "try_id" },
            { "data": "worker_id" },
            { "data": "schedule_id" },
            { "data": "execution_time" },
            { "data": "when_ready" },
            { "data": "when_running" },
            { "data": "time_worker_start" },
            { "data": "time_worker_end" },
            { "data": "when_waiting_retrieval" },
            { "data": "when_retrieved" },
            { "data": "when_done" },
            { "data": "when_output_fully_lost" },
            { "data": "cores_requested" },
            { "data": "gpus_requested" },
            { "data": "memory_requested_mb" },
            { "data": "disk_requested_mb" },
            { "data": "category" },
            { "data": "graph_id" },
            { "data": "size_input_files_mb" },
            { "data": "size_output_files_mb" },
            { "data": "input_files" },
            { "data": "output_files" },
        ],
    };
    var table = createTable('#tasks-completed-table', specificSettings);

    $('#button-tasks-completed-reset-table').off('click').on('click', function() {
        if (document.getElementById('button-tasks-completed-convert-timestamp').classList.contains('report-button-active')) {
            document.getElementById('button-tasks-completed-convert-timestamp').classList.toggle('report-button-active');
        }
        searchValue = '';
        timestampType = 'original';
        table.ajax.reload();
    });
    $('#button-tasks-completed-convert-timestamp').off('click').on('click', function() {
        this.classList.toggle('report-button-active');
        if (this.classList.contains('report-button-active')) {
            timestampType = 'relative';
        } else {
            timestampType = 'original';
        }
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-id').off('click').on('click', function() {
        var inputValue = $('#input-tasks-completed-task-id').val();
        if (inputValue.includes(',')) {
            searchType = 'task-ids';
            searchValue = inputValue;
        } else {
            searchType = 'task-id';
            searchValue = inputValue
        }
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-category').off('click').on('click', function() {
        searchType = 'category';
        searchValue = $('#input-tasks-completed-category').val();
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-filename').off('click').on('click', function() {
        searchType = 'filename';
        searchValue = $('#input-tasks-completed-filename').val();
        table.ajax.reload();
    });
}

function drawTaskFailedTable(url) {
    var searchType = 'task-id';
    var searchValue = '';
    var timestampType = 'original';

    var specificSettings = {
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = searchType;
                d.search.value = searchValue;
                d.timestamp_type = timestampType;
            },
            "dataSrc": function(json) {
                json.data.forEach(function(task) {
                    task.task_id = parseInt(task.task_id, 10);
                    task.try_id = parseInt(task.try_id, 10);
                    task.worker_id = parseInt(task.worker_id, 10);
                });
                return json.data;
            }
        },
        "columns": [
            { "data": "task_id" },
            { "data": "try_id" },
            { "data": "worker_id" },
            { "data": "when_ready" },
            { "data": "when_running" },
            { "data": "when_failure_happens" },
            { "data": "category" },
            { "data": "cores_requested" },
            { "data": "gpus_requested" },
            { "data": "memory_requested_mb" },
            { "data": "disk_requested_mb" },
        ],
    };
    var table = createTable('#tasks-failed-table', specificSettings);

    $('#button-tasks-failed-reset-table').off('click').on('click', function() {
        if (document.getElementById('button-tasks-failed-convert-timestamp').classList.contains('report-button-active')) {
            document.getElementById('button-tasks-failed-convert-timestamp').classList.toggle('report-button-active');
        }
        searchValue = '';
        timestampType = 'original';
        table.ajax.reload();
    });
    $('#button-tasks-failed-convert-timestamp').off('click').on('click', function() {
        this.classList.toggle('report-button-active');
        if (this.classList.contains('report-button-active')) {
            timestampType = 'relative';
        } else {
            timestampType = 'original';
        }
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-id').off('click').on('click', function() {
        searchType = 'task-id';
        searchValue = $('#input-tasks-failed-task-id').val();
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-category').off('click').on('click', function() {
        searchType = 'category';
        searchValue = $('#input-tasks-failed-category').val();
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-worker-id').off('click').on('click', function() {
        searchType = 'worker-id';
        searchValue = $('#input-tasks-failed-worker-id').val();
        table.ajax.reload();
    });
}

function drawWorkerTable(url) {
    var specificSettings = {
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
            },
        },
        "columns": [
            { "data": "worker_id" },
            { "data": "worker_hash" },
            { "data": "worker_machine_name" },
            { "data": "worker_ip" },
            { "data": "worker_port" },
            { "data": "time_connected" },
            { "data": "time_disconnected" },
            { "data": "lifetime(s)" },
            { "data": "cores" },
            { "data": "memory_mb" },
            { "data": "disk_mb" },
            { "data": "num_tasks_completed" },
            { "data": "num_tasks_failed" },
            { "data": "avg_task_runtime(s)" },
            { "data": "peak_disk_usage(MB)" },
            { "data": "peak_disk_usage(%)" },
        ],
    };

    var table = createTable('#worker-table', specificSettings)
}

function drawGraphTable(url) {
    var specificSettings = {
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
            },
        },
        "columns": [
            { "data": "graph_id" },
            { "data": "num_tasks" },
            { "data": "time_completion" },
            { "data": "num_critical_tasks" },
            { "data": "critical_tasks" },
        ],
    };

    var table = createTable('#graph-table', specificSettings);
}

function drawFileTable(url) {
    var searchType = '';
    var searchValue = '';
    var specificSettings = {
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = searchType;
                d.search.value = searchValue;
            },
        },
        "columns": [
            { "data": "filename" },
            { "data": "size(MB)" },
            { "data": "producers" },
            { "data": "consumers" },
            { "data": "num_workers_holding" },
            { "data": "worker_holding" },
        ],
    };
    var table = createTable('#file-summary-table', specificSettings);

    const buttonReset = document.getElementById('button-file-summary-reset-table');
    const buttonSearchFilename = document.getElementById('button-file-summary-search-filename');
    const buttonHasProducer = document.getElementById('button-file-summary-has-producer');

    $('#' + buttonSearchFilename.id).off('click').on('click', function() {
        buttonHasProducer.classList.remove('report-button-active');
        searchType = 'filename';
        searchValue = $('#input-file-summary-search-filename').val();
        table.ajax.reload();
    });
    $('#' + buttonHasProducer.id).off('click').on('click', function() {
        buttonHasProducer.classList.toggle('report-button-active');
        searchType = 'has-producer';
        searchValue = '';
        table.ajax.reload();
    });
    $('#' + buttonReset.id).off('click').on('click', function() {
        buttonSearchFilename.classList.remove('report-button-active');
        buttonHasProducer.classList.remove('report-button-active');
        searchType = '';
        searchValue = '';
        table.ajax.reload();
    });
}

function loadPage(dataName, page, perPage) {
    var url = dataName;

    $.ajax({
        url: url,
        type: 'GET',
        data: {
            log_name: window.logName,
            page: page,
            per_page: perPage
        },
        success: function() {
            if (dataName === 'tasks_completed') {
                drawTaskCompletedTable(url); 
            } else if (dataName == 'tasksFailed') {
                drawTaskFailedTable(url);
            } else if (dataName === 'worker') {
                drawWorkerTable(url);
            } else if (dataName === 'graph') {
                drawGraphTable(url);
            } else if (dataName == 'file') {
                drawFileTable(url);
            }
        },
        error: function() {
            console.error('Error loading dataName:', dataName);
        }
    });
}

window.parent.document.addEventListener('dataLoaded', function() {
    loadPage('tasks_completed', 1, dataTableSettings.pageLength);
    document.getElementById('button-tasks-completed-reset-table').click();
    loadPage('tasksFailed', 1, dataTableSettings.pageLength);
    loadPage('worker', 1, dataTableSettings.pageLength);
    if (window.graphInfo !== null) {
        loadPage('graph', 1, dataTableSettings.pageLength);
    }
    loadPage('file', 1, dataTableSettings.pageLength);
});

