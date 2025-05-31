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
            'tasks_committing': { color: '#0ecfc8', label: 'Committing' },
            'tasks_executing': { color: '#5581b0', label: 'Executing' },
            'tasks_retrieving': { color: '#be612a', label: 'Retrieving' },
            'tasks_done': { color: '#BF40BF', label: 'Done' }
        };
    }

    legendOnToggle(id, visible) {
        const path = this.svg.selectAll(`#${id}`);
        path.style('display', visible ? null : 'none');
    }

    initLegend() {
        const legendItems = this.taskTypes.map(type => ({
            id: type,
            label: this.taskConfigs[type]?.label || type,
            color: this.taskConfigs[type]?.color || '#87CEEB'
        }));
        this.createLegendRow(legendItems, {
            lineWidth: 3,
            checkboxName: 'task-concurrency',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        if (!this.data) return;

        for (const [type, config] of Object.entries(this.taskConfigs)) {
            if (this.data[type] && this.data[type].length > 0) {
                this.plotPath(this.data[type], {
                    stroke: config.color,
                    className: `task-line`,
                    id: type
                });
            }
        }
    }
} 