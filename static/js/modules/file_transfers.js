import { BaseModule } from './base.js';

export class FileTransfersModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => `${d3.format('.0f')(d)}`);

        this.PRIMARY_COLOR = '#2077B4';
        this.HIGHLIGHT_COLOR = 'orange';
        this.LINE_WIDTH = 0.8;
        this.HIGHLIGHT_WIDTH = 2;

        this.showIncoming = true;
    }

    initControls() {
        const buttonToggleType = document.getElementById('button-toggle-transfer-type');
        const transferTypeDisplay = document.getElementById('transfer-type-display');

        buttonToggleType.addEventListener('click', async () => {
            this.showIncoming = !this.showIncoming;
            buttonToggleType.textContent = this.showIncoming ? 'Show Outgoing' : 'Show Incoming';
            transferTypeDisplay.textContent = this.showIncoming ? 'Incoming' : 'Outgoing';
            await this.fetchData();
            this.plot();
        });
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        // Plot each worker's transfers
        Object.entries(this.data.transfers).forEach(([worker, points]) => {
            this.plotPath(svg, points, {
                stroke: this.PRIMARY_COLOR,
                className: 'transfer-line',
                id: `transfer-${worker}`
            });
        });

        // Add legend
        const legendContainer = document.getElementById('file-transfers-legend');
        if (legendContainer) {
            legendContainer.innerHTML = '';
            const legendItems = Object.keys(this.data.transfers).map(worker => ({
                id: worker,
                label: worker,
                color: this.PRIMARY_COLOR
            }));

            this.createLegend(legendContainer, legendItems, {
                lineWidth: 3,
                onToggle: async (id, visible) => {
                    const path = svg.select(`#transfer-${id}`);
                    path.style('display', visible ? null : 'none');
                }
            });
        }
    }
} 