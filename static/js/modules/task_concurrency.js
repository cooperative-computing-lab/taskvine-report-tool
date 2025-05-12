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

    initLegend() {
        this.legendContainer.innerHTML = '';
        const legendItems = this.taskTypes.map(type => ({
            id: type,
            label: this.taskConfigs[type]?.label || type,
            color: this.taskConfigs[type]?.color || '#87CEEB'
        }));
        this.createLegendRow(this.legendContainer, legendItems, {
            lineWidth: 3,
            checkboxName: 'task-concurrency',
            onToggle: async (id, visible) => {
                const path = this.svg.selectAll(`#${id}`);
                path.style('display', visible ? null : 'none');
            }
        });
    }

    plot() {
        if (!this.data) return;

        this.initSVG();

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