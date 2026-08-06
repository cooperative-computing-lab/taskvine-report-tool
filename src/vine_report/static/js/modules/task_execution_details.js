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
        if (this.checkboxStates && taskType in this.checkboxStates) {
            return this.checkboxStates[taskType];
        }
        const checkbox = document.getElementById(`${this.id}-${taskType}`);
        if (checkbox) return checkbox.checked;
        return true;
    }

    _plotTask(task, primaryName, recoveryName, timeStart, timeEnd) {
        if (timeStart == null || timeEnd == null || timeStart === undefined || timeEnd === undefined) return;
        timeStart = +timeStart;
        timeEnd = +timeEnd;
        if (Number.isNaN(timeStart) || Number.isNaN(timeEnd)) return;

        const isRecovery = task.is_recovery_task;
        const recoveryChecked = isRecovery && this._isTaskTypeChecked(recoveryName);
        const primaryChecked = this._isTaskTypeChecked(primaryName);
    
        if (!recoveryChecked && !primaryChecked) return;

    
        const taskType = recoveryChecked ? recoveryName : primaryName;
        const fill = this._getLegendColor(taskType) || '#999';
        const innerHTML = getTaskInnerHTML(task);
        
        /* Use actual band width so bars align correctly and do not overlap */
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

            const bandwidth = this.getScaleBandWidth(this.leftScale);
            const step = this.leftScale.step();
            const height = Math.max(0, bandwidth * worker.cores + (step - bandwidth) * (worker.cores - 1));
            /* Worker bar: y_domain is sorted (worker_id, core_id), range [svgHeight,0] puts 1-1 at bottom.
               So worker 1's top band is 1-cores; bar starts there and extends down to cover all cores. */
            const yBand = `${worker.id}-${worker.cores}`;
            
            const className = 'task-type-workers';
            this.plotHorizontalRect(timeStart, timeEnd - timeStart, yBand, height, fill, opacity, innerHTML, className);
        }
    }    

    async plot() {
        if (!this.data) return;

        /* plot workers first (background) */
        if (this._isTaskTypeChecked('workers') && this.data['workers']) {
            this.data['workers'].forEach(worker => {
                this._plotWorker(worker);
            });
        }

        /* plot tasks on top (foreground): unsuccessful first, then successful so colored bars are visible */
        this.data['unsuccessful_tasks'].forEach(task => {
            this._plotTask(task, task.unsuccessful_checkbox_name, 'recovery-unsuccessful', task.when_running, task.when_failure_happens);
        });
        this.data['successful_tasks'].forEach(task => {
            this._plotTask(task, 'successful-committing-to-worker', 'recovery-successful', task.when_running, task.time_worker_start);
            this._plotTask(task, 'successful-executing-on-worker', 'recovery-successful', task.time_worker_start, task.time_worker_end);
            this._plotTask(task, 'successful-retrieving-to-manager', 'recovery-successful', task.time_worker_end, task.when_waiting_retrieval);
        });
    }
}
