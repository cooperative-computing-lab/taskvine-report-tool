import { BaseModule } from './base.js';
import { getTaskInnerHTML, getWorkerInnerHTML } from './utils.js';


export class TaskExecutionDetailsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('band');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => d.split('-')[0]);
    }

    async fetchData(folder) {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data) {
            return;
        }

        this.data = data;

        this.setBottomDomain(data['x_domain']);
        this.setLeftDomain(data['y_domain']);

        this.setBottomTickValues(data['x_tick_values']);
        this.setLeftTickValues(data['y_tick_values']);
    }

    _getLegendColor(taskType) {
        return this.legendMap[taskType]?.color;
    }    

    initLegend() {
        if (!this.legendContainer) return;

        this.checkboxStates = {};
        this.legendMap = {};

        this.data['legend'].forEach(group => {
            group.items.forEach(item => {
                this.checkboxStates[item.id] = item.default_checked ?? true;
                this.legendMap[item.id] = {
                    color: item.color,
                    label: item.label
                };
            });
        });

        this.legendContainer.innerHTML = '';
        const flexContainer = document.createElement('div');
        flexContainer.className = 'legend-flex-container';
        this.legendContainer.appendChild(flexContainer);
    
        this.data['legend'].forEach(group => {
            const groupDiv = document.createElement('div');
            groupDiv.className = 'legend-group';
    
            const titleDiv = document.createElement('div');
            titleDiv.className = 'legend-group-title-container';
            const title = document.createElement('div');
            title.className = 'legend-group-title';
            title.textContent = `${group.group} (${group.total})`;
            titleDiv.appendChild(title);
            groupDiv.appendChild(titleDiv);
    
            group.items.forEach(item => {
                const legendItem = document.createElement('div');
                legendItem.className = `legend-item${this.checkboxStates[item.id] ? ' checked' : ''}`;
            
                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.id = `${item.id}-checkbox`;
                checkbox.checked = this.checkboxStates[item.id];
                checkbox.style.display = 'none';
            
                const colorBox = document.createElement('div');
                colorBox.className = 'legend-color';
                colorBox.style.setProperty('--color', item.color);
            
                const label = document.createElement('span');
                label.textContent = group.group === 'Successful Tasks'
                    ? item.label
                    : `${item.label} (${item.count})`;
            
                legendItem.appendChild(checkbox);
                legendItem.appendChild(colorBox);
                legendItem.appendChild(label);
            
                legendItem.addEventListener('click', () => {
                    checkbox.checked = !checkbox.checked;
                    legendItem.classList.toggle('checked');
                    this.checkboxStates[item.id] = checkbox.checked;
                    this.plot();
                });
            
                groupDiv.appendChild(legendItem);
            });            
    
            flexContainer.appendChild(groupDiv);
        });
    }    

    _isTaskTypeChecked(taskType) {
        const checkbox = document.getElementById(`${taskType}-checkbox`);
        return checkbox && checkbox.checked;
    }

    _plotTask(svg, task, primaryName, recoveryName, timeStart, timeEnd) {
        if (!timeStart || !timeEnd) return;
        timeStart = +timeStart;
        timeEnd = +timeEnd;
    
        const isRecovery = task.is_recovery_task;
        const recoveryChecked = isRecovery && this._isTaskTypeChecked(recoveryName);
        const primaryChecked = this._isTaskTypeChecked(primaryName);
    
        if (!recoveryChecked && !primaryChecked) return;
    
        const taskType = recoveryChecked ? recoveryName : primaryName;
        const fill = this._getLegendColor(taskType);
    
        const x = this.bottomScale(timeStart);
        const y = this.leftScale(`${task.worker_id}-${task.core_id}`);
        const width = this.bottomScale(timeEnd) - this.bottomScale(timeStart);
        const height = this.getBandWidth(this.leftScale);
        const innerHTML = getTaskInnerHTML(task);
    
        this.plotRect(svg, x, y, width, height, fill, 1, innerHTML);
    }

    _plotWorker(svg, worker) {
        for (let i = 0; i < worker["time_connected"].length; i++) {
            const height = Math.max(0, this.getBandWidth(this.leftScale) * worker.cores +
                (this.leftScale.step() - this.getBandWidth(this.leftScale)) * (worker.cores - 1));
            const x = this.bottomScale(worker["time_connected"][i]);
            const y = this.leftScale(worker.id + '-' + worker.cores);
            const width = Math.max(0, this.bottomScale(worker["time_disconnected"][i]) - this.bottomScale(worker["time_connected"][i]));
            const fill = this._getLegendColor('workers');
            const opacity = 0.3;
            const innerHTML = getWorkerInnerHTML(worker);
            this.plotRect(svg, x, y, width, height, fill, opacity, innerHTML);
        }
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        /* plot workers */
        if (this._isTaskTypeChecked('workers') && this.data['worker_info']) {
            this.data['worker_info'].forEach(worker => {
                this._plotWorker(svg, worker);
            });
        }

        /* plot successful tasks */
        this.data['successful_tasks'].forEach(task => {
            this._plotTask(svg, task, 'successful-committing-to-worker', 'recovery-successful', task.when_running, task.time_worker_start);
            this._plotTask(svg, task, 'successful-executing-on-worker', 'recovery-successful', task.time_worker_start, task.time_worker_end);
            this._plotTask(svg, task, 'successful-retrieving-to-manager', 'recovery-successful', task.time_worker_end, task.when_retrieved);
        });

        /* plot unsuccessful tasks */
        this.data['unsuccessful_tasks'].forEach(task => {
            this._plotTask(svg, task, task.unsuccessful_checkbox_name, 'recovery-unsuccessful', task.when_running, task.when_failure_happens);
        });

    }
}
