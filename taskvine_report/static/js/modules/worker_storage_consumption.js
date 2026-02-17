import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerStorageConsumptionModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
        
        this.defineFetchDataParams({
            accumulated: false
        });
        
        this.currentDisplayMode = 'separate';
    }

        _addCustomToolboxItems() {
        const displayModeSelector = this.toolbox.createSelectorItem(
            `${this.id}-display-mode`,
            'Display',
            [
                { value: 'separate', label: 'Separate By Workers' },
                { value: 'accumulated', label: 'Accumulated' }
            ],
            (id, value) => {
                this.currentDisplayMode = value;
                const accumulated = value === 'accumulated';
                this.updateFetchDataParam('accumulated', accumulated);
                this.fetchDataAndPlot();
            }
        );
        
        this.addToolboxSelectorItem(displayModeSelector);
        
        // Set the selector value after toolbox is mounted
        setTimeout(() => {
            this._restoreSelectorState();
        }, 0);
    }
    
    _restoreSelectorState() {
        const selectorElement = document.getElementById(`selector-${this.id}-display-mode`);
        if (selectorElement) {
            selectorElement.value = this.currentDisplayMode;
        }
    }

    _isWorkerStorageSeriesKey(key) {
        if (typeof key !== 'string' || key === 'workflow_completion_percentage') {
            return false;
        }
        const parts = key.split(':');
        if (parts.length < 3) {
            return false;
        }
        const core = parts[parts.length - 1];
        const port = parts[parts.length - 2];
        const host = parts.slice(0, parts.length - 2).join(':');
        return Boolean(host) && /^\d+$/.test(port) && /^\d+$/.test(core);
    }

    _getSanitizedStorageData() {
        const raw = this.data?.storage_data;
        if (!raw || typeof raw !== 'object') {
            return {};
        }
        return Object.fromEntries(
            Object.entries(raw).filter(([worker]) => this._isWorkerStorageSeriesKey(worker))
        );
    }

    legendOnToggle(id, visible) {
        if (this._fetchDataParams.accumulated) {
            // In accumulated mode, toggle the single line
            const path = this.svg.selectAll(`#${this.id}-storage-accumulated`);
            path.style('display', visible ? null : 'none');
        } else {
            // In separate mode, toggle individual worker lines
            const path = this.svg.selectAll(`#${this.id}-storage-${id}`);
            path.style('display', visible ? null : 'none');
        }
    }

    initLegend() {
        if (this._fetchDataParams.accumulated) {
            // Accumulated mode: clear legend
            this.clearLegend();
        } else {
            // Separate mode: legend for each worker
            const storageData = this._getSanitizedStorageData();
            if (Object.keys(storageData).length > 0) {
                const legendItems = Object.keys(storageData).map((worker, idx) => ({
                    id: escapeWorkerId(worker),
                    label: worker,
                    color: getWorkerColor(worker, idx)
                }));
                
                this.createLegendRow(legendItems, {
                    checkboxName: 'storage-consumption-separate',
                    onToggle: (id, visible) => {
                        this.legendOnToggle(id, visible);
                    }
                });
            }
        }
    }

    plot() {
        if (!this.data) return;
        
        if (this._fetchDataParams.accumulated) {
            // Accumulated mode: plot single line
            if (this.data.accumulated_data) {
                this.plotPath(this.data.accumulated_data, {
                    stroke: '#2077B4',
                    strokeWidth: 2,
                    className: 'storage-line',
                    id: `${this.id}-storage-accumulated`,
                    tooltipInnerHTML: 'Total Storage Consumption'
                });
            }
        } else {
            // Separate mode: plot line for each worker
            const storageData = this._getSanitizedStorageData();
            if (Object.keys(storageData).length > 0) {
                Object.entries(storageData).forEach(([worker, points], idx) => {
                    const safeId = escapeWorkerId(worker);
                    const color = getWorkerColor(worker, idx);
                    this.plotPath(points, {
                        stroke: color,
                        className: 'storage-line',
                        id: `${this.id}-storage-${safeId}`,
                        tooltipInnerHTML: `${worker}`
                    });
                });
            }
        }
    }
}
