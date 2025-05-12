import { BaseModule } from './base.js';

export class TaskConcurrencyModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.taskTypes = [
            'tasks_waiting',
            'tasks_committing',
            'tasks_executing',
            'tasks_retrieving',
            'tasks_done'
        ];

        this.taskConfigs = {
            'tasks_waiting': { color: '#099652', label: 'Waiting' },
            'tasks_committing': { color: '#8327cf', label: 'Committing' },
            'tasks_executing': { color: '#5581b0', label: 'Executing' },
            'tasks_retrieving': { color: '#be612a', label: 'Retrieving' },
            'tasks_done': { color: '#2077B4', label: 'Done' }
        };
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        /* plot all types (legend only controls visibility/hiding) */
        for (const [type, config] of Object.entries(this.taskConfigs)) {
            if (this.data[type] && this.data[type].length > 0) {
                this.plotPath(svg, this.data[type], {
                    stroke: config.color,
                    className: `task-line`,
                    id: type
                });
            }
        }

        /* add legend using createLegendRow */
        const legendContainer = document.getElementById('task-concurrency-legend');
        if (legendContainer) {
            legendContainer.innerHTML = '';
            const legendItems = this.taskTypes.map(type => ({
                id: type,
                label: this.taskConfigs[type]?.label || type,
                color: this.taskConfigs[type]?.color || '#87CEEB'
            }));
            this.createLegendRow(legendContainer, legendItems, {
                lineWidth: 3,
                checkboxName: 'task-concurrency',
                onToggle: async (id, visible) => {
                    const path = svg.selectAll(`#${id}`);
                    path.style('display', visible ? null : 'none');
                }
            });
        }
    }

    _getLegendColor(taskType) {
        return this.taskConfigs[taskType]?.color || '#87CEEB';
    }

    _getTaskTypeLabel(taskType) {
        return this.taskConfigs[taskType]?.label || taskType;
    }

    _isTaskTypeChecked(taskType) {
        const checkbox = document.getElementById(`${taskType}-checkbox`);
        return checkbox && checkbox.checked;
    }

    _calculateMaxConcurrent() {
        let maxConcurrent = 0;
        this.taskTypes.forEach(taskType => {
            if (!this._isTaskTypeChecked(taskType)) return;
            const points = this.data[taskType];
            if (!points || points.length === 0) return;
            const taskMax = Math.max(...points.map(p => p[1]).filter(y => !isNaN(y)));
            if (!isNaN(taskMax)) {
                maxConcurrent = Math.max(maxConcurrent, taskMax);
            }
        });
        return maxConcurrent || 1;
    }

    _calculateYTickValues(domain) {
        const [min, max] = domain;
        const range = max - min;
        return [
            Math.ceil(min),
            Math.ceil(min + range * 0.25),
            Math.ceil(min + range * 0.5),
            Math.ceil(min + range * 0.75),
            Math.ceil(max)
        ];
    }
} 