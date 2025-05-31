import { BaseModule } from './base.js';

export class FileRetentionTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        this.plotPoints(this.data.points, {
            tooltipFormatter: d => `File Name: ${this.data.file_idx_to_names[d[0]]}<br>Retention Time: ${d[1]}s`,
            className: 'file-retention-time-point'
        });
    }
} 