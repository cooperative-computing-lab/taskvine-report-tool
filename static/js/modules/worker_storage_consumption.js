import { BaseModule } from './base.js';

export class WorkerStorageConsumptionModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => `${d3.format('.2f')(d)} ${this.data?.file_size_unit || 'MB'}`);
    }

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data) {
            return;
        }

        this.data = data;

        this.setBottomDomain(data.x_domain);
        this.setLeftDomain(data.y_domain);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
    }

    _getLegendColor(workerId) {
        const index = Object.keys(this.data['storage_data']).indexOf(workerId);
        return d3.schemeCategory10[index % 10];
    }

    initLegend() {
        if (!this.legendContainer) return;

        this.legendContainer.innerHTML = '';
        const flexContainer = document.createElement('div');
        flexContainer.className = 'legend-flex-container';
        this.legendContainer.appendChild(flexContainer);

        const groupDiv = document.createElement('div');
        groupDiv.className = 'legend-group';

        const titleDiv = document.createElement('div');
        titleDiv.className = 'legend-group-title-container';
        const title = document.createElement('div');
        title.className = 'legend-group-title';
        title.textContent = 'Worker Storage Consumption';
        titleDiv.appendChild(title);
        groupDiv.appendChild(titleDiv);

        Object.entries(this.data['storage_data']).forEach(([workerId, points]) => {
            const legendItem = document.createElement('div');
            legendItem.className = 'legend-item checked';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `${workerId}-checkbox`;
            checkbox.checked = true;
            checkbox.style.display = 'none';

            const colorBox = document.createElement('div');
            colorBox.className = 'legend-color';
            colorBox.style.setProperty('--color', this._getLegendColor(workerId));

            const label = document.createElement('span');
            label.textContent = workerId;

            legendItem.appendChild(checkbox);
            legendItem.appendChild(colorBox);
            legendItem.appendChild(label);

            legendItem.addEventListener('click', () => {
                checkbox.checked = !checkbox.checked;
                legendItem.classList.toggle('checked');
                this.plot();
            });

            groupDiv.appendChild(legendItem);
        });

        flexContainer.appendChild(groupDiv);
    }

    _isWorkerChecked(workerId) {
        const checkbox = document.getElementById(`${workerId}-checkbox`);
        return checkbox && checkbox.checked;
    }

    _plotWorkerLine(svg, workerId, points) {
        if (!this._isWorkerChecked(workerId)) return;

        const validPoints = points.filter(p => 
            !isNaN(p[0]) && !isNaN(p[1]) && 
            p[0] >= this.bottomDomain[0] && p[0] <= this.bottomDomain[1] && 
            p[1] >= this.leftDomain[0] && p[1] <= this.leftDomain[1]
        );

        if (validPoints.length === 0) return;

        const color = this._getLegendColor(workerId);
        const safeWorkerId = workerId.replace(/[.:]/g, '\\$&');
        const workerResource = this.data['worker_resources']?.[workerId] || {};

        const line = d3.line()
            .x(d => this.bottomScale(d[0]))
            .y(d => this.leftScale(d[1]))
            .defined(d => !isNaN(d[0]) && !isNaN(d[1]) && d[1] >= 0)
            .curve(d3.curveStepAfter);

        svg.append('path')
            .datum(validPoints)
            .attr('fill', 'none')
            .attr('stroke', color)
            .attr('stroke-width', 0.8)
            .attr('class', `worker-line worker-${safeWorkerId}`)
            .attr('d', line)
            .on('mouseover', (event) => {
                d3.select(event.currentTarget)
                    .attr('stroke', this.highlightColor)
                    .attr('stroke-width', 3)
                    .raise();

                svg.selectAll('.worker-line')
                    .filter(function() {
                        return !this.classList.contains(`worker-${safeWorkerId}`);
                    })
                    .attr('stroke', '#ddd')
                    .attr('stroke-width', 0.8);

                let lastNonZeroIndex = points.length - 1;
                while (lastNonZeroIndex >= 0 && points[lastNonZeroIndex][1] === 0) {
                    lastNonZeroIndex--;
                }
                const currentValue = (lastNonZeroIndex >= 0 ? points[lastNonZeroIndex][1] : 0).toFixed(2);

                const tooltip = document.getElementById('vine-tooltip');
                tooltip.style.visibility = 'visible';
                tooltip.innerHTML = `
                    Worker: ${workerId}<br>
                    Current Usage: ${currentValue} ${this.data['file_size_unit']}<br>
                    Cores: ${workerResource.cores || 'N/A'}<br>
                    Memory: ${workerResource.memory_mb ? this._formatSize(workerResource.memory_mb, 'MB') : 'N/A'}<br>
                    Disk: ${workerResource.disk_mb ? this._formatSize(workerResource.disk_mb, 'MB') : 'N/A'}<br>
                    ${workerResource.gpus ? `GPUs: ${workerResource.gpus}<br>` : ''}
                `;
                tooltip.style.top = (event.pageY - 15) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', (event) => {
                d3.select(event.currentTarget)
                    .attr('stroke', color)
                    .attr('stroke-width', 0.8);

                svg.selectAll('.worker-line')
                    .attr('stroke', (d, i) => this._getLegendColor(Object.keys(this.data['storage_data'])[i]))
                    .attr('stroke-width', 0.8);

                const tooltip = document.getElementById('vine-tooltip');
                tooltip.style.visibility = 'hidden';
            });
    }

    _formatSize(size, unit) {
        if (size >= 1024 && unit === 'MB') {
            return `${(size/1024).toFixed(2)} GB`;
        }
        return `${size.toFixed(2)} ${unit}`;
    }

    plot() {
        if (!this.data || !this.data['storage_data']) return;

        this.clearSVG();

        const svg = this.plotAxes();

        Object.entries(this.data['storage_data']).forEach(([workerId, points]) => {
            this._plotWorkerLine(svg, workerId, points);
        });
    }
}

