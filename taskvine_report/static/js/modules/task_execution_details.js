import { BaseModule } from './base.js';
import { getTaskInnerHTML, getWorkerInnerHTML } from './utils.js';

export class TaskExecutionDetailsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('band');
    }

    _getLegendColor(taskType) {
        return this.legendMap[taskType]?.color;
    }   

    legendOnToggle(id, visible) {
        this.checkboxStates[id] = visible;
        
        /* use selector to hide/show elements instead of replotting */
        const elements = this.svg.selectAll(`.task-type-${id}`);
        elements.style('display', visible ? null : 'none');
    }

    initLegend() {
        if (!this.legendContainer) return;
    
        this.checkboxStates = {};
        const groups = this.data['legend'].map(group => ({
            groupLabel: `${group.group} (${group.total})`,
            showGroupLabel: true,
            items: group.items.map(item => ({
                id: item.id,
                label: group.group === 'Successful Tasks' ? item.label : `${item.label} (${item.count})`,
                color: item.color,
                checked: true,
                showLabel: true
            }))
        }));
   
        this.createLegendGroup(groups, {
            checkboxName: 'task-details',
            onToggle: this.legendOnToggle.bind(this)
        });

        this.legendMap = {};
        groups.forEach(group => {
            group.items.forEach(item => {
                this.legendMap[item.id] = {
                    color: item.color,
                    label: item.label
                };
                /* initialize checkbox states */
                this.checkboxStates[item.id] = item.checked;
            });
        });
    }

    _isTaskTypeChecked(taskType) {
        const checkbox = document.getElementById(taskType);
        return checkbox && checkbox.checked;
    }

    _plotTask(task, primaryName, recoveryName, timeStart, timeEnd) {
        if (!timeStart || !timeEnd) return;
        timeStart = +timeStart;
        timeEnd = +timeEnd;

        const isRecovery = task.is_recovery_task;
        const recoveryChecked = isRecovery && this._isTaskTypeChecked(recoveryName);
        const primaryChecked = this._isTaskTypeChecked(primaryName);
    
        if (!recoveryChecked && !primaryChecked) return;

    
        const taskType = recoveryChecked ? recoveryName : primaryName;
        const fill = this._getLegendColor(taskType);
        const innerHTML = getTaskInnerHTML(task);
        
        const height = this.getScaleBandWidth(this.leftScale);
        const className = `task-type-${taskType}`;

        this.plotHorizontalRect(timeStart, timeEnd - timeStart, `${task.worker_id}-${task.core_id}`, height, fill, 1, innerHTML, className);
    }

    _plotWorker(worker) {
        for (let i = 0; i < worker["time_connected"].length; i++) {
            const timeStart = +worker["time_connected"][i];
            const timeEnd = +worker["time_disconnected"][i];
    
            const fill = this._getLegendColor('workers');
            const opacity = 0.3;
            const innerHTML = getWorkerInnerHTML(worker);

            const height = Math.max(0, this.getScaleBandWidth(this.leftScale) * worker.cores + 
                (this.leftScale.step() - this.getScaleBandWidth(this.leftScale)) * (worker.cores - 1));
            
            const className = 'task-type-workers';
            this.plotHorizontalRect(timeStart, timeEnd - timeStart, `${worker.id}-${worker.cores}`, height, fill, opacity, innerHTML, className);
        }
    }    

    async plot() {
        if (!this.data) return;

        /* plot workers */
        if (this._isTaskTypeChecked('workers') && this.data['workers']) {
            this.data['workers'].forEach(worker => {
                this._plotWorker(worker);
            });
        }

        /* plot successful tasks */
        this.data['successful_tasks'].forEach(task => {
            this._plotTask(task, 'successful-committing-to-worker', 'recovery-successful', task.when_running, task.time_worker_start);
            this._plotTask(task, 'successful-executing-on-worker', 'recovery-successful', task.time_worker_start, task.time_worker_end);
            this._plotTask(task, 'successful-retrieving-to-manager', 'recovery-successful', task.time_worker_end, task.when_retrieved);
        });

        /* plot unsuccessful tasks */
        this.data['unsuccessful_tasks'].forEach(task => {
            this._plotTask(task, task.unsuccessful_checkbox_name, 'recovery-unsuccessful', task.when_running, task.when_failure_happens);
        });
    }
}
