import { BaseModule } from './base.js';

export class TaskConcurrencyModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => `${d3.format('.0f')(d)}`);

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

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data || !data.tasks_waiting) {
            console.warn('Invalid or missing task concurrency data');
            return;
        }

        this.data = data;

        this.setBottomDomain(data.x_domain);
        this.setLeftDomain(data.y_domain);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
    }

    _getLegendColor(taskType) {
        return this.taskConfigs[taskType]?.color || '#87CEEB';
    }

    _getTaskTypeLabel(taskType) {
        return this.taskConfigs[taskType]?.label || taskType;
    }

    initLegend() {
        if (!this.legendContainer || !this.data) return;

        this.legendContainer.innerHTML = '';
        const flexContainer = document.createElement('div');
        flexContainer.className = 'legend-flex-container';
        flexContainer.style.display = 'flex';
        flexContainer.style.justifyContent = 'space-between';
        flexContainer.style.width = '100%';
        this.legendContainer.appendChild(flexContainer);

        this.taskTypes.forEach(taskType => {
            if (!this.data[taskType] || this.data[taskType].length === 0) return;

            const legendItem = document.createElement('div');
            legendItem.className = 'legend-item checked';
            legendItem.style.display = 'flex';
            legendItem.style.alignItems = 'center';
            legendItem.style.gap = '4px';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `${taskType}-checkbox`;
            checkbox.checked = true;
            checkbox.style.display = 'none';

            const colorBox = document.createElement('div');
            colorBox.className = 'legend-color';
            colorBox.style.setProperty('--color', this._getLegendColor(taskType));

            const label = document.createElement('span');
            label.textContent = this._getTaskTypeLabel(taskType);

            legendItem.appendChild(checkbox);
            legendItem.appendChild(colorBox);
            legendItem.appendChild(label);

            legendItem.addEventListener('click', () => {
                checkbox.checked = !checkbox.checked;
                legendItem.classList.toggle('checked');
                this.plot();
            });

            flexContainer.appendChild(legendItem);
        });
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

    plot() {
        if (!this.data) return;
        
        /* calculate scaling factor based on visible task types */
        const originalMax = this.data.y_domain[1];
        const currentMax = this._calculateMaxConcurrent();
        const scaleFactor = currentMax / originalMax;

        /* apply scaling to y domain and tick values */
        const scaledYDomain = [
            Math.ceil(this.data.y_domain[0] * scaleFactor),
            Math.ceil(this.data.y_domain[1] * scaleFactor)
        ];
        const scaledYTickValues = this._calculateYTickValues(scaledYDomain);

        this.setLeftDomain(scaledYDomain);
        this.setLeftTickValues(scaledYTickValues);

        const svg = this.initSVG();
        
        /* plot each task type */
        for (const [type, config] of Object.entries(this.taskConfigs)) {
            if (this.data[type] && this.data[type].length > 0) {
                this.plotPath(svg, this.data[type], {
                    stroke: config.color,
                    className: type,
                    id: type
                });
            }
        }
    }
} 